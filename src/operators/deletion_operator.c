#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<rhendb/mvcc_header.h>

#include<rhendb/table_operator_output_type.h>

#include<tupleindexer/heap_page/heap_page.h>

#include<cutlery/value_arraylist.h>

#include<cutlery/comparator_interface.h>

#include<stdlib.h>

// a single tuple that is pending to be marked deleted, as consumed from the input operator
typedef struct pending_deletion pending_deletion;
struct pending_deletion
{
	uint64_t partition_id;

	tuple_pointer tptr;
};

data_definitions_value_arraylist(pending_deletions, pending_deletion)
declarations_value_arraylist(pending_deletions, pending_deletion, static inline)
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(pending_deletions, pending_deletion, static inline)

typedef struct input_values input_values;
struct input_values
{
	const tuple_def* input_tuple_def;

	consumption_iterator* input_iterator;

	uint64_t table_id;

	// all the partitions of the above table, and their respective record definitions
	// both of the below arrays are partitions_count in length, and are ordered by partition_id
	rhendb_table_partition* table_partitions;
	tuple_def** partition_tuple_defs;
	uint64_t partitions_count;

	positional_accessor* partition_id_from_source_positional_accessor;
	positional_accessor* tuple_pointer_from_source_positional_accessor;

	// number of pending_deletion-s that we buffer, before we sort, and go and mark them deleted
	uint64_t deletion_batch_size;

	// the buffer of the tuples that are yet to be marked deleted, it never grows beyond deletion_batch_size elements
	pending_deletions to_be_deleted;

	int output_flags;

	const tuple_def* output_tuple_def;

	transaction* tx;
};

// orders the pending_deletion-s by partition_id, then by page_id and then by tuple_index,
// this makes all the deletions to be performed on any given page contiguous in the to_be_deleted buffer
static int compare_pending_deletions_by_partition_id_and_tuple_pointer(const void* pd1_vp, const void* pd2_vp)
{
	const pending_deletion* pd1 = pd1_vp;
	const pending_deletion* pd2 = pd2_vp;

	if(pd1->partition_id != pd2->partition_id)
		return compare_numbers(pd1->partition_id, pd2->partition_id);

	if(pd1->tptr.page_id != pd2->tptr.page_id)
		return compare_numbers(pd1->tptr.page_id, pd2->tptr.page_id);

	if(pd1->tptr.tuple_index != pd2->tptr.tuple_index)
		return compare_numbers(pd1->tptr.tuple_index, pd2->tptr.tuple_index);

	return 0;
}

// produces the output tuple for a pending_deletion that was just marked deleted,
// returns 0, only if the tuple could not be produced, in which case the caller must kill itself
static int produce_output_for_pending_deletion(operator* o, const pending_deletion* pd)
{
	input_values* inputs = o->inputs;

	rage_engine* engine = &(inputs->tx->rdb->persistent_acid_rage_engine);

	if(inputs->output_flags == 0)
		return 1;

	uint32_t output_tuple_size = get_minimum_tuple_size(inputs->output_tuple_def);
	uint64_t output_tuple_capacity = output_tuple_size;
	void* output_tuple = malloc(output_tuple_capacity);
	init_tuple(inputs->output_tuple_def, output_tuple);

	uint32_t attr_index = 0;

	if(MUST_OUTPUT_TABLE_ID(inputs->output_flags))
	{
		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = inputs->table_id}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

		attr_index++;
	}

	if(MUST_OUTPUT_PARTITION_ID(inputs->output_flags))
	{
		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = pd->partition_id}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

		attr_index++;
	}

	if(MUST_OUTPUT_TUPLE_POINTER_ID(inputs->output_flags))
	{
		char tptr_tpl[sizeof(tuple_pointer)];
		set_tuple_pointer(tptr_tpl, pd->tptr, &(engine->pam_p->pas));

		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.tuple_value = tptr_tpl}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

		attr_index++;
	}

	int produced = produce_tuple_from_operator(o, output_tuple);
	free(output_tuple);

	return produced;
}

static int mark_buffered_tuples_deleted(operator* o, const char** kill_reason)
{
	input_values* inputs = o->inputs;

	rage_engine* engine = &(inputs->tx->rdb->persistent_acid_rage_engine);

	cy_uint to_be_deleted_count = get_element_count_pending_deletions(&(inputs->to_be_deleted));
	if(to_be_deleted_count == 0)
		return 1;

	if(!merge_sort_pending_deletions(&(inputs->to_be_deleted), 0, to_be_deleted_count - 1, &simple_comparator(compare_pending_deletions_by_partition_id_and_tuple_pointer), STD_C_mem_allocator))
		exit(-1);

	// index over the to_be_deleted buffer, and index over the partitions of the table,
	// both of them are only ever incremented, making the below loop linear in their lengths
	cy_uint i = 0;
	uint64_t p = 0;

	while(i < to_be_deleted_count)
	{
		uint64_t partition_id = get_from_front_of_pending_deletions(&(inputs->to_be_deleted), i)->partition_id;

		// advance over all the partitions that no tuple is to be deleted from
		while(p < inputs->partitions_count && inputs->table_partitions[p].partition_id < partition_id)
			p++;

		if(p == inputs->partitions_count || inputs->table_partitions[p].partition_id != partition_id)
		{
			printf("ISSUE in (deleter_operator) :: no partition with partition_id = %" PRIu64 " in table with table_id = %" PRIu64 "\n", partition_id, inputs->table_id);
			exit(-1);
		}

		const tuple_def* partition_tuple_def = inputs->partition_tuple_defs[p];

		// the mvcc_header is always the first element of the record of any partition
		tuple_def mvcc_def;
		initialize_tuple_def(&mvcc_def, (data_type_info*)(partition_tuple_def->type_info->containees[0].al.type_info));

		// all the tuples to be deleted on a single page, are marked deleted in a single mini transaction
		while(i < to_be_deleted_count && get_from_front_of_pending_deletions(&(inputs->to_be_deleted), i)->partition_id == partition_id)
		{
			uint64_t page_id = get_from_front_of_pending_deletions(&(inputs->to_be_deleted), i)->tptr.page_id;

			uint64_t page_latches_to_be_borrowed = 0;
			int abort_error = 0;
			void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

			persistent_page ppage = acquire_persistent_page_with_lock(engine->pam_p, min_tx_id, page_id, WRITE_LOCK, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			// index of the first pending_deletion on this page, we come back to it to produce the outputs
			// required for redo on abort and to producing the output
			cy_uint page_first_index = i;

			// mark every tuple that is to be deleted on this very page, deleted
			while(i < to_be_deleted_count)
			{
				const pending_deletion* pd = get_from_front_of_pending_deletions(&(inputs->to_be_deleted), i);
				if(pd->partition_id != partition_id || pd->tptr.page_id != page_id)
					break;

				const void* record = get_nth_tuple_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(partition_tuple_def->size_def), pd->tptr.tuple_index);
				if(record == NULL)
				{
					printf("ISSUE in (deleter_operator) :: no tuple at tuple_index = %" PRIu32 " on page_id = %" PRIu64 "\n", pd->tptr.tuple_index, page_id);
					exit(-1);
				}

				// write a non NULL xmax (our transaction) on the mvcc_header of this tuple, in place, marking it deleted
				datum mvcc_hdr_datum;
				get_value_from_element_from_tuple(&mvcc_hdr_datum, partition_tuple_def, STATIC_POSITION(0), record);

				mvcc_header hdr;
				read_mvcc_header(&hdr, mvcc_hdr_datum.tuple_value, &mvcc_def);
				hdr.is_xmax_NULL = 0;
				hdr.xmax = (transaction_id_with_hints){.is_committed = 0, .is_aborted = 0, .transaction_id = inputs->tx->snapshot->self_transaction_id};

				// keep the serialized mvcc buffer in its own block so its variable length type does not span the goto
				{
					char mvcc_hdr_serialized[sizeof(mvcc_header)];
					init_tuple(&mvcc_def, mvcc_hdr_serialized);
					write_mvcc_header(mvcc_hdr_serialized, &mvcc_def, &hdr);
					set_element_in_tuple_in_place_on_persistent_page(engine->pmm_p, min_tx_id, &ppage, engine->pam_p->pas.page_size, partition_tuple_def, pd->tptr.tuple_index, STATIC_POSITION(0), &((datum){.tuple_value = mvcc_hdr_serialized}), &abort_error);
				}
				if(abort_error)
					goto ABORT_ERROR;

				i++;
			}

			// release lock on this page
			release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			// all the tuples on this page are marked deleted now, so commit this mini transaction
			engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);

			// only now that they are committed, produce the outputs for all the tuples deleted on this page
			for(cy_uint j = page_first_index; j < i; j++)
			{
				if(!produce_output_for_pending_deletion(o, get_from_front_of_pending_deletions(&(inputs->to_be_deleted), j)))
				{
					(*kill_reason) = "could_not_produce";
					return 0;
				}
			}

			continue;

			ABORT_ERROR:;
			if(!is_persistent_page_NULL(&ppage, engine->pam_p))
				release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
			engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);

			// if aborted retry for this very same page
			i = page_first_index;
			continue;
		}
	}

	// every buffered tuple is marked deleted now, so the buffer can be emptied
	remove_all_from_pending_deletions(&(inputs->to_be_deleted));

	return 1;
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	rage_engine* engine = &(inputs->tx->rdb->persistent_acid_rage_engine);

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data)
		{
			// the input operator has been killed, so mark whatever we have buffered, deleted
			const char* kill_reason = NULL;
			if(!mark_buffered_tuples_deleted(o, &kill_reason))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring(kill_reason));
				return ;
			}

			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
			return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
			return ;
		}

		if(tuple == NULL)
			break;

		// fetch the partition_id and the tuple_pointer of the tuple that is to be deleted
		pending_deletion pd;
		{
			datum partition_id_datum;
			if(!get_value_from_element_from_tuple(&partition_id_datum, inputs->input_tuple_def, (*(inputs->partition_id_from_source_positional_accessor)), tuple))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("deletion_failed_NULL_partition_id"));
				return ;
			}

			datum tuple_pointer_datum;
			if(!get_value_from_element_from_tuple(&tuple_pointer_datum, inputs->input_tuple_def, (*(inputs->tuple_pointer_from_source_positional_accessor)), tuple))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("deletion_failed_NULL_tuple_pointer"));
				return ;
			}

			pd = (pending_deletion){.partition_id = partition_id_datum.uint_value, .tptr = get_tuple_pointer(tuple_pointer_datum.tuple_value, &(engine->pam_p->pas))};
		}

		if(!push_back_to_pending_deletions(&(inputs->to_be_deleted), &pd))
			exit(-1);

		// the buffer is full, so mark everything that we have buffered, deleted
		if(get_element_count_pending_deletions(&(inputs->to_be_deleted)) >= inputs->deletion_batch_size)
		{
			const char* kill_reason = NULL;
			if(!mark_buffered_tuples_deleted(o, &kill_reason))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring(kill_reason));
				return ;
			}
		}

		continue;
	}

	return ;
}

static void clean_up_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->input_iterator != NULL)
	{
		destroy_consumption_iterator(inputs->input_iterator);
		inputs->input_iterator = NULL;
	}

	deinitialize_pending_deletions(&(inputs->to_be_deleted));

	if(inputs->table_partitions != NULL)
	{
		free(inputs->table_partitions);
		inputs->table_partitions = NULL;
	}
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->output_tuple_def != NULL)
	{
		// all the possible outputs are static types
		free((void*)(inputs->output_tuple_def->type_info));
		free((void*)(inputs->output_tuple_def));
		inputs->output_tuple_def = NULL;
	}

	if(inputs->partition_tuple_defs != NULL)
	{
		for(uint64_t i = 0; i < inputs->partitions_count; i++)
		{
			destroy_type_info_recursively(inputs->partition_tuple_defs[i]->type_info, NULL);
			free((void*)(inputs->partition_tuple_defs[i]));
		}
		free(inputs->partition_tuple_defs);
		inputs->partition_tuple_defs = NULL;
	}

	free(inputs);
}

operator_resource_counter setup_deletion_operator(operator* o, operator* input_operator, positional_accessor* partition_id_from_source_positional_accessor, positional_accessor* tuple_pointer_from_source_positional_accessor, uint64_t table_id, uint64_t deletion_batch_size, int output_flags)
{
	transaction* tx = input_operator->self_query_plan->curr_tx;

	if(tx->snapshot == NULL)
	{
		printf("must have writable snapshot for deletion_operator\n");
		exit(-1);
	}

	if(!tx->snapshot->has_self_transaction_id)
	{
		printf("must have self_transaction_id for deletion_operator\n");
		exit(-1);
	}

	if(deletion_batch_size == 0)
	{
		printf("must have a non-zero deletion_batch_size for deletion_operator\n");
		exit(-1);
	}

	const tuple_def* input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator);

	// fetch all the partitions of this table, and a record definition for each one of them
	uint64_t partitions_count = 0;
	rhendb_table_partition* table_partitions = get_partitions_for_table_from_catalog(&(tx->rdb->cat_mgr), tx->snapshot, table_id, &partitions_count);
	if(table_partitions == NULL || partitions_count == 0)
	{
		printf("no partitions for the table to be deleted from for deletion_operator\n");
		exit(-1);
	}

	tuple_def** partition_tuple_defs = malloc(sizeof(tuple_def*) * partitions_count);
	if(partition_tuple_defs == NULL)
		exit(-1);
	for(uint64_t i = 0; i < partitions_count; i++)
	{
		data_type_info* partition_type_info = get_data_type_info_for_rhendb_table_partition_from_catalog(&(tx->rdb->cat_mgr), tx->snapshot, &(table_partitions[i]));
		if(!are_identical_type_info(partition_type_info->containees[0].al.type_info, tx->rdb->mvcc_hdr_type_info))
		{
			printf("must have mvcc_header for deletion_operator in the tuples of table_partition for deletion_operator\n");
			exit(-1);
		}
		partition_tuple_defs[i] = malloc(sizeof(tuple_def));
		initialize_tuple_def(partition_tuple_defs[i], partition_type_info);
	}

	operator_resource_counter result = {.buffer_counter = 1, .job_counter = 1, .thread_counter = 1}; // lock a page delete tuples and exit, so 1 buffer atmost
	if(o == NULL)
	{
		for(uint64_t i = 0; i < partitions_count; i++)
		{
			destroy_type_info_recursively(partition_tuple_defs[i]->type_info, NULL);
			free((partition_tuple_defs[i]));
		}
		free(partition_tuple_defs);
		free(table_partitions);
		return result;
	}

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = free_resources;

	tuple_def* output_tuple_def = NULL;
	if(output_flags != 0)
	{
		output_tuple_def = malloc(sizeof(tuple_def));
		data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(10)); // over allocation for being safe from future changes

		uint32_t output_dti_element_count = 0;
		uint32_t output_tuple_max_size = 8;

		if(MUST_OUTPUT_TABLE_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "table_id", 64);
			output_dti->containees[output_dti_element_count].al.type_info = UINT_NON_NULLABLE[8];

			output_tuple_max_size += 8;
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_PARTITION_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "partition_id", 64);
			output_dti->containees[output_dti_element_count].al.type_info = UINT_NON_NULLABLE[8];

			output_tuple_max_size += 8;
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_TUPLE_POINTER_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "tuple_pointer", 64);
			output_dti->containees[output_dti_element_count].al.type_info = &(tx->rdb->persistent_acid_rage_engine.pam_p->pas.tuple_pointer_type_info);

			output_tuple_max_size += sizeof(tuple_pointer);
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_HEAP_TUPLE(output_flags))
		{
			printf("outputting the deleted heap tuple is not supported by deletion_operator\n");
			exit(-1);
		}

		initialize_tuple_data_type_info(output_dti, "deleted_tuple_context", 0, output_tuple_max_size, output_dti_element_count);

		initialize_tuple_def(output_tuple_def, output_dti);
	}

	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_tuple_def = input_tuple_def,
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.table_id = table_id,
		.table_partitions = table_partitions,
		.partition_tuple_defs = partition_tuple_defs,
		.partitions_count = partitions_count,
		.partition_id_from_source_positional_accessor = partition_id_from_source_positional_accessor,
		.tuple_pointer_from_source_positional_accessor = tuple_pointer_from_source_positional_accessor,
		.deletion_batch_size = deletion_batch_size,
		.output_flags = output_flags,
		.output_tuple_def = output_tuple_def,
		.tx = tx,
	};

	// the buffer of the tuples to be deleted is initialized after `inputs` exists, directly on it,
	// it is initialized to hold exactly deletion_batch_size many pending_deletion-s, as it never grows beyond that
	input_values* inputs = o->inputs;
	if(!initialize_pending_deletions(&(inputs->to_be_deleted), deletion_batch_size))
		exit(-1);

	return result;
}