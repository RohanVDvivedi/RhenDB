#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<rhendb/mvcc_header.h>

#include<rhendb/table_operator_output_type.h>
#include<rhendb/fetched_table.h>

#include<tupleindexer/heap_page/heap_page.h>

#include<cutlery/value_arraylist.h>

#include<cutlery/comparator_interface.h>

#include<stdlib.h>

// a single tuple that is pending to be looked up, as consumed from the input operator
typedef struct pending_lookup pending_lookup;
struct pending_lookup
{
	uint64_t partition_id;

	tuple_pointer tptr;
};

data_definitions_value_arraylist(pending_lookups, pending_lookup)
declarations_value_arraylist(pending_lookups, pending_lookup, static inline)
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(pending_lookups, pending_lookup, static inline)

typedef struct input_values input_values;
struct input_values
{
	const tuple_def* input_tuple_def;

	consumption_iterator* input_iterator;

	const fetched_table* ftabl;

	positional_accessor* partition_id_from_source_positional_accessor;
	positional_accessor* tuple_pointer_from_source_positional_accessor;

	// number of pending_lookup-s that we buffer, before we sort, and go and read them
	uint64_t lookup_batch_size;

	// the buffer of the tuples that are yet to be looked up, it never grows beyond lookup_batch_size elements
	pending_lookups to_be_looked_up;

	int output_flags;

	const tuple_def* output_tuple_def;

	int additional_flags; // same flags as given in transaction.h, toggles the additional book keeping this operator performs

	transaction* tx;
};

// orders the pending_lookup-s by partition_id, then by page_id and then by tuple_index,
// this makes all the lookups to be performed on any given page contiguous in the to_be_looked_up buffer
static int compare_pending_lookups_by_partition_id_and_tuple_pointer(const void* pl1_vp, const void* pl2_vp)
{
	const pending_lookup* pl1 = pl1_vp;
	const pending_lookup* pl2 = pl2_vp;

	if(pl1->partition_id != pl2->partition_id)
		return compare_numbers(pl1->partition_id, pl2->partition_id);

	if(pl1->tptr.page_id != pl2->tptr.page_id)
		return compare_numbers(pl1->tptr.page_id, pl2->tptr.page_id);

	if(pl1->tptr.tuple_index != pl2->tptr.tuple_index)
		return compare_numbers(pl1->tptr.tuple_index, pl2->tptr.tuple_index);

	return 0;
}

// produces the output tuple for a pending_lookup that was just found visible,
// returns 0, only if the tuple could not be produced, in which case the caller must kill itself
static int produce_output_for_pending_lookup(operator* o, const pending_lookup* pl, const void* heap_record, uint64_t partition_index_in_info)
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
		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = inputs->ftabl->table_info.id}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

		attr_index++;
	}

	if(MUST_OUTPUT_PARTITION_ID(inputs->output_flags))
	{
		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = pl->partition_id}), output_tuple_capacity - output_tuple_size))
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
		set_tuple_pointer(tptr_tpl, pl->tptr, &(engine->pam_p->pas));

		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.tuple_value = tptr_tpl}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

		attr_index++;
	}

	if(MUST_OUTPUT_HEAP_TUPLE(inputs->output_flags))
	{
		void* final_readers_heap_record = project_to_final_readers_tuple_def(inputs->ftabl, heap_record, partition_index_in_info);

		// ensure there are enough bytes in the output_tuple, as we try to insert this datum
		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.tuple_value = final_readers_heap_record}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);
		free(final_readers_heap_record);

		attr_index++;
	}

	int produced = produce_tuple_from_operator(o, output_tuple);
	free(output_tuple);

	return produced;
}

// reads every buffered tuple_pointer, and produces an output ONLY for the ones that are visible to
// the snapshot of this transaction, the invisible ones are silently skipped.
// this is a read only operator, so no mini transaction is ever allotted, pages are only read locked.
// returns 0, only if an output could not be produced, in which case the caller must kill itself
static int lookup_buffered_tuples(operator* o, const char** kill_reason)
{
	input_values* inputs = o->inputs;

	rage_engine* engine = &(inputs->tx->rdb->persistent_acid_rage_engine);

	cy_uint to_be_looked_up_count = get_element_count_pending_lookups(&(inputs->to_be_looked_up));
	if(to_be_looked_up_count == 0)
		return 1;

	if(!merge_sort_pending_lookups(&(inputs->to_be_looked_up), 0, to_be_looked_up_count - 1, &simple_comparator(compare_pending_lookups_by_partition_id_and_tuple_pointer), STD_C_mem_allocator))
		exit(-1);

	// index over the to_be_looked_up buffer, and index over the partitions of the table,
	// both of them are only ever incremented, making the below loop linear in their lengths
	cy_uint i = 0;
	uint64_t p = 0;

	while(i < to_be_looked_up_count)
	{
		uint64_t partition_id = get_from_front_of_pending_lookups(&(inputs->to_be_looked_up), i)->partition_id;

		// advance over all the partitions that no tuple is to be looked up from
		while(p < inputs->ftabl->partitions_count && inputs->ftabl->table_partitions_info[p].partition_id < partition_id)
			p++;

		if(p == inputs->ftabl->partitions_count || inputs->ftabl->table_partitions_info[p].partition_id != partition_id)
		{
			printf("ISSUE in (pointer_lookup_operator) :: no partition with partition_id = %" PRIu64 " in table with table_id = %" PRIu64 "\n", partition_id, inputs->ftabl->table_info.id);
			exit(-1);
		}

		const tuple_def* partition_tuple_def = &(inputs->ftabl->table_partition_tuple_defs[p]);

		// the mvcc_header is always the first element of the record of any partition
		tuple_def mvcc_def;
		initialize_tuple_def(&mvcc_def, (data_type_info*)(partition_tuple_def->type_info->containees[0].al.type_info));

		// all the tuples to be looked up on a single page, are read under a single read lock on it
		while(i < to_be_looked_up_count && get_from_front_of_pending_lookups(&(inputs->to_be_looked_up), i)->partition_id == partition_id)
		{
			uint64_t page_id = get_from_front_of_pending_lookups(&(inputs->to_be_looked_up), i)->tptr.page_id;

			int abort_error = 0;

			// this operator never writes, so it needs no mini transaction, a NULL transaction_id
			// with a READ_LOCK is all that is required to read the page
			persistent_page ppage = acquire_persistent_page_with_lock(engine->pam_p, NULL, page_id, READ_LOCK, &abort_error);
			if(abort_error)
			{
				(*kill_reason) = "pointer_lookup_read_only_page_read_aborted";
				return 0;
			}

			// read every tuple that is to be looked up on this very page
			while(i < to_be_looked_up_count)
			{
				const pending_lookup* pl = get_from_front_of_pending_lookups(&(inputs->to_be_looked_up), i);
				if(pl->partition_id != partition_id || pl->tptr.page_id != page_id)
					break;

				// a tomb stone, or a tuple_index beyond the page, there is nothing here to produce
				if(!exists_tuple_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(partition_tuple_def->size_def), pl->tptr.tuple_index))
				{
					i++;
					continue;
				}

				const void* record = get_nth_tuple_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(partition_tuple_def->size_def), pl->tptr.tuple_index);
				if(record == NULL)
				{
					i++;
					continue;
				}

				// every tuple must pass the mvcc visibility check, before it is even projected for the
				// output, an invisible tuple is simply not produced
				{
					datum mvcc_hdr_datum;
					get_value_from_element_from_tuple(&mvcc_hdr_datum, partition_tuple_def, STATIC_POSITION(0), record);

					mvcc_header mvcchdr;
					read_mvcc_header(&mvcchdr, mvcc_hdr_datum.tuple_value, &mvcc_def);

					int were_hints_updated = 0;
					if(!is_tuple_visible_to_mvcc_snapshot(inputs->tx->snapshot, &mvcchdr, &(inputs->tx->rdb->tsg), &were_hints_updated))
					{
						i++;
						continue;
					}
				}

				if(!produce_output_for_pending_lookup(o, pl, record, p))
				{
					release_lock_on_persistent_page(engine->pam_p, NULL, &ppage, NONE_OPTION, &abort_error);
					(*kill_reason) = "could_not_produce";
					return 0;
				}

				i++;
			}

			// release the read lock on this page, we are done with every tuple on it
			release_lock_on_persistent_page(engine->pam_p, NULL, &ppage, NONE_OPTION, &abort_error);
			if(abort_error)
			{
				(*kill_reason) = "pointer_lookup_read_only_page_release_aborted";
				return 0;
			}
		}
	}

	// every buffered tuple is looked up now, so the buffer can be emptied
	remove_all_from_pending_lookups(&(inputs->to_be_looked_up));

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
			// the input operator has been killed, so look up whatever we still have buffered
			const char* kill_reason = NULL;
			if(!lookup_buffered_tuples(o, &kill_reason))
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

		// fetch the partition_id and the tuple_pointer of the tuple that is to be looked up
		pending_lookup pl;
		{
			datum partition_id_datum;
			if(!get_value_from_element_from_tuple(&partition_id_datum, inputs->input_tuple_def, (*(inputs->partition_id_from_source_positional_accessor)), tuple))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("pointer_lookup_failed_NULL_partition_id"));
				return ;
			}

			datum tuple_pointer_datum;
			if(!get_value_from_element_from_tuple(&tuple_pointer_datum, inputs->input_tuple_def, (*(inputs->tuple_pointer_from_source_positional_accessor)), tuple))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("pointer_lookup_failed_NULL_tuple_pointer"));
				return ;
			}

			pl = (pending_lookup){.partition_id = partition_id_datum.uint_value, .tptr = get_tuple_pointer(tuple_pointer_datum.tuple_value, &(engine->pam_p->pas))};
		}

		if(!push_back_to_pending_lookups(&(inputs->to_be_looked_up), &pl))
			exit(-1);

		// the buffer is full, so look up everything that we have buffered
		if(get_element_count_pending_lookups(&(inputs->to_be_looked_up)) >= inputs->lookup_batch_size)
		{
			const char* kill_reason = NULL;
			if(!lookup_buffered_tuples(o, &kill_reason))
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

	deinitialize_pending_lookups(&(inputs->to_be_looked_up));
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->output_tuple_def != NULL)
	{
		free((void*)(inputs->output_tuple_def->type_info));
		free((void*)(inputs->output_tuple_def));
		inputs->output_tuple_def = NULL;
	}

	free(inputs);
}

operator_resource_counter setup_pointer_lookup_operator(operator* o, operator* input_operator, positional_accessor* partition_id_from_source_positional_accessor, positional_accessor* tuple_pointer_from_source_positional_accessor, const fetched_table* ftabl, uint64_t lookup_batch_size, int output_flags, int additional_flags)
{
	transaction* tx = input_operator->self_query_plan->curr_tx;

	// this operator only reads, so it needs a snapshot to be visible against,
	// but unlike the deletion_operator it does NOT need a self_transaction_id
	if(tx->snapshot == NULL)
	{
		printf("must have a snapshot for pointer_lookup_operator\n");
		exit(-1);
	}

	if(lookup_batch_size == 0)
	{
		printf("must have a non-zero lookup_batch_size for pointer_lookup_operator\n");
		exit(-1);
	}

	const tuple_def* input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator);

	operator_resource_counter result = {.buffer_counter = 1, .job_counter = 1, .thread_counter = 1}; // read lock a page, read tuples and exit, so 1 buffer atmost
	if(o == NULL)
		return result;

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
			strncpy(output_dti->containees[output_dti_element_count].field_name, ftabl->table_info.name, 64);
			output_dti->containees[output_dti_element_count].al.type_info = ftabl->final_readers_tuple_def.type_info;

			output_tuple_max_size += 8 + get_maximum_tuple_size(&(ftabl->final_readers_tuple_def));
			output_dti_element_count++;
		}

		initialize_tuple_data_type_info(output_dti, "looked_up_tuple_context", 0, output_tuple_max_size, output_dti_element_count);

		initialize_tuple_def(output_tuple_def, output_dti);
	}

	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_tuple_def = input_tuple_def,
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.ftabl = ftabl,
		.partition_id_from_source_positional_accessor = partition_id_from_source_positional_accessor,
		.tuple_pointer_from_source_positional_accessor = tuple_pointer_from_source_positional_accessor,
		.lookup_batch_size = lookup_batch_size,
		.output_flags = output_flags,
		.output_tuple_def = output_tuple_def,
		.additional_flags = additional_flags,
		.tx = tx,
	};

	// the buffer of the tuples to be looked up is initialized after `inputs` exists, directly on it,
	// it is initialized to hold exactly lookup_batch_size many pending_lookup-s, as it never grows beyond that
	input_values* inputs = o->inputs;
	if(!initialize_pending_lookups(&(inputs->to_be_looked_up), lookup_batch_size))
		exit(-1);

	return result;
}