#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<rhendb/mvcc_header.h>

#include<rhendb/table_operator_output_type.h>

#include<rhendb/nullable_type_info_maker.h>

#include<tupleindexer/heap_page/heap_page.h>
#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/utils/heap_table_accumulative_notifier.h>

#include<stdlib.h>

// heap_table_accumulative_notifier: accumulate stale (page_id, unused_space) entries and only actually
// fix them in the heap_table once at least FIX_THRESHOLD have piled up (write-amplification control).
// CAPACITY is the ceiling the notifier is allowed to hold before we must drain it.
#define HTAN_CAPACITY       50
#define HTAN_FIX_THRESHOLD  25

/*
	heap table insertion operator
*/

typedef struct input_values input_values;
struct input_values
{
	const tuple_def* input_tuple_def;

	consumption_iterator* input_iterator;

	rhendb_table_partition insertion_table_partition;

	positional_accessor* insertion_from_source_positional_accessors;

	const tuple_def* partition_tuple_def;

	// heap_table free-space defs for the partition's heap (record_def == partition_tuple_def)
	heap_table_tuple_defs httd;

	// accumulative notifiers: one for the partition's heap table, one for its blob store.
	heap_table_accumulative_notifier heap_htan;
	heap_table_accumulative_notifier blob_htan;

	int output_flags; // same flags as table_operator_output_type.h
	const tuple_def* output_tuple_def; // will remain set to NULL, if output_flags is 0

	// cached here for snapshot and transaction_id
	transaction* tx;
};

typedef struct extended_column_data extended_column_data;
struct extended_column_data
{
	uint32_t index; // index of the attribute in heap row

	// both will be non-NULL if they used even a single byte on the blob store of the table
	tuple_pointer head_chunk_pointer;
	tuple_pointer tail_chunk_pointer;

	uint64_t written_size; // total_size - written_size are values pending to be written

	uint64_t total_size;

	// char* for blob, text and uint64_t* numeric
	void* value;
};

static void fix_unused_space_entries_after_insertions(rage_engine* engine, heap_table_accumulative_notifier* htan_p, const heap_table_tuple_defs* httd_p)
{
	if(get_notification_count_for_heap_table_accumulative_notifier(htan_p) < HTAN_FIX_THRESHOLD)
		return;

	uint64_t page_latches_to_be_borrowed = 0;
	int abort_error = 0;
	void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

	uint64_t root_page_id, page_id;
	uint32_t unused_space;
	while(pop_from_heap_table_accumulative_notifier(htan_p, &root_page_id, &unused_space, &page_id))
	{
		// do this in a separate mini transaction, even if it fail we will be just fine
		uint64_t page_latches_to_be_borrowed = 0;
		int abort_error = 0;
		void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

		fix_unused_space_in_heap_table(root_page_id, unused_space, page_id, httd_p, engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);

		engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed); // no lfushing needed
	}
}

// builds the partition heap record for one input tuple, returning it (malloc'd, caller frees), or NULL
// on failure:
//   - position 0 is the mvcc header, stamped with this transaction's id as xmin (xmax NULL = live)
//   - positions 1..n-1 are copied from the input tuple through the source positional accessors
// BASIC VERSION: assumes every partition attribute is inline (its value fits in the heap tuple). An
// extended (text/blob/numeric) attribute needs its prefix set here and its overflow appended to the
// blob store after the heap insert -- that is the next step and is called out below.
static void* build_heap_record(input_values* inputs, const void* input_tuple, extended_column_data** ext_col_data, uint32_t* ext_col_data_size)
{
	(*ext_col_data_size) = 0;
	(*ext_col_data) = NULL;

	uint32_t record_size = get_minimum_tuple_size(inputs->partition_tuple_def);
	uint64_t record_capacity = record_size;
	void* record = malloc(capacity);
	init_tuple(inputs->partition_tuple_def, record);

	// (0) -> mvcc header
	{
		tuple_def mvcc_def;
		initialize_tuple_def(&mvcc_def, (data_type_info*)(inputs->partition_tuple_def->type_info->containees[0].al.type_info));

		mvcc_header hdr = (mvcc_header){0};
		hdr.is_xmin_NULL = 0;
		hdr.xmin.transaction_id = inputs->tx->snapshot->self_transaction_id;
		hdr.xmin.is_committed = 0; hdr.xmin.is_aborted = 0;
		hdr.is_xmax_NULL = 1;

		char mvcc_buf[sizeof(hdr)];
		init_tuple(&mvcc_def, mvcc_buf);
		write_mvcc_header(mvcc_buf, &mvcc_def, &hdr);

		while(!set_element_in_tuple(inputs->partition_tuple_def, STATIC_POSITION(0), record, &(datum){.tuple_value = mvcc_buf}, record_capacity - record_size))
		{
			record_capacity *= 2;
			record = realloc(record, record_capacity);
		}
		record_size = get_tuple_size(inputs->partition_tuple_def, record);
	}

	// (1..n-1) copy the source attributes through the positional accessors; skip / write NULL for absent
	// add 1 entry in ext_col_data and write 2 page worth of bytes in it, and preserve the head and tail chunk pointers
	// for now store in 20 bytes in the record, we will optimize this later
	for(uint32_t i = 1; i < inputs->partition_tuple_def->type_info->element_count; i++)
	{
		datum src;
		if(!get_value_from_element_from_tuple(&src, inputs->input_tuple_def, inputs->insertion_from_source_positional_accessors[i - 1], input_tuple))
			src = (*NULL_DATUM);

		// NOTE: extended attributes would instead get a prefix here and be blob-appended after insert.
		while(!set_element_in_tuple(inputs->partition_tuple_def, STATIC_POSITION(i), record, &src, record_capacity - record_size))
		{
			record_capacity *= 2;
			record = realloc(record, record_capacity);
		}
		record_size = get_tuple_size(inputs->partition_tuple_def, record);
	}

	return record;
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	// this function stores everything here in this persistent acid engine
	rage_engine* engine = &(inputs->tx->rdb->persistent_acid_rage_engine);

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data)
		{
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

		extended_column_data* ext_col_data = NULL;
		uint32_t ext_col_data_size = 0;
		void* heap_record = build_heap_record_without_extensions(inputs, tuple, &ext_col_data, &ext_col_data_size); // numeric -> sign_bits and exponent are populated here

		int should_retry = 1;
		while(should_retry)
		{
			uint64_t page_latches_to_be_borrowed = 0;
			int abort_error = 0;
			void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

			// build the heap record for this input tuple (mvcc header + mapped attributes)
			build_heap_record_with_prefix_bytes(inputs, tuple, heap_record, ext_col_data, ext_col_data_size, min_tx_id, &abort_error);
			if(heap_record == NULL)
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_build_record"));
				return ;
			}

			tuple_pointer tptr = get_NULL_tuple_pointer(&(engine->pam_p->pas));

			// 1. find (or allocate) a heap page with room, insert the record, note the (page_id, index) pointer
			uint32_t required_space = get_space_to_be_occupied_by_tuple_on_persistent_page(engine->pam_p->pas.page_size, &(inputs->partition_tuple_def->size_def), heap_record);

			int is_new_page = 0;
			uint32_t unused_space_in_entry = 0;
			persistent_page ppage = find_heap_page_with_enough_unused_space_from_heap_table(inputs->insertion_table_partition.heap_root_page_id, required_space, &unused_space_in_entry, &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->heap_htan)), &(inputs->httd), engine->pam_p, min_tx_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			if(is_persistent_page_NULL(&ppage, engine->pam_p))
			{
				ppage = get_new_heap_page_with_write_lock(&(engine->pam_p->pas), inputs->partition_tuple_def, engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;
				is_new_page = 1;
			}

			uint32_t possible_insertion_index = 0;
			uint32_t tuple_index = insert_in_heap_page(&ppage, heap_record, &possible_insertion_index, inputs->partition_tuple_def, &(engine->pam_p->pas), engine->pmm_p, min_tx_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;
			if(tuple_index == INVALID_TUPLE_INDEX)   // record too big for even a fresh heap page
			{
				printf("FAILED (inserter) :: record too large for a heap page\n");
				should_retry = 0;
				goto ABORT_ERROR;
			}
			tptr = (tuple_pointer){.page_id = ppage.page_id, .tuple_index = tuple_index};

			// 2. a fresh page must be tracked (reads the page, so before releasing the lock)
			if(is_new_page)
			{
				track_unused_space_in_heap_table(inputs->insertion_table_partition.heap_root_page_id, &ppage, &(inputs->httd), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;
			}

			// 3. release the page (sets ppage back to NULL). The now-stale free-space entries were pushed
			//    to inputs->heap_htan during the search; they are fixed later, outside this mini transaction.
			release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			// 4. register the inserted tuple_pointer so scans skip it, then commit the insert mini transaction
			register_inserted_tuple_pointer(inputs->tx, tptr);
			engine->complete_sub_transaction(engine->context, min_tx_id, 1 /* flush/commit */, NULL, 0, &page_latches_to_be_borrowed);
			break;

			ABORT_ERROR:
			if(is_persistent_page_NULL(&ppage))
				release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
			engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);

			if(should_retry == 0)
				// kill with abort error and exit

			continue;
		}

		// 5. now that the insert mini transaction is committed, opportunistically fix the accumulated
		//    stale free-space entries -- in their OWN mini transaction(s), outside this insert, and only
		//    once enough have piled up. Best effort: an abort here is fine, we just try again later.
		fix_unused_space_entries_after_insertions(engine, &(inputs->heap_htan), &(inputs->httd));

		// write the remaining bytes from ext_col_data, each in it's own transaction
		for(uint32_t i = 0; i < ext_col_data_size; i++)
		{

		}

		if(ext_col_data != NULL)
		{
			free(ext_col_data);
			ext_col_data = NULL;
		}

		// (blob store, once extended attributes are written:)
		fix_unused_space_entries_after_insertions(engine, &(inputs->blob_htan), &(engine->bstd.httd));

		// 6. optionally emit the inserted tuple's identity + attributes for a chained operator
		if(inputs->output_flags != 0)
		{
			void* output_tuple = NULL;
			// TODO: project { table_id, partition_id, tuple_pointer=tptr, heap_tuple=heap_record }
			// into output_tuple per inputs->output_flags (skipping NULLs in the heap tuple).
			int produced = produce_tuple_from_operator(o, output_tuple);
			free(output_tuple);
			if(!produced)
			{
				free(heap_record);
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
				return ;
			}
		}

		free(heap_record);
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
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->output_tuple_def != NULL)
	{
		// all other possible outputs are static types
		if(MUST_OUTPUT_HEAP_TUPLE(inputs->output_flags))
		{
			uint32_t heap_tuple_position = inputs->output_tuple_def->type_info->element_count - 1;
			uint32_t heap_tuple_element_count = inputs->partition_tuple_def->type_info->element_count;
			for(uint32_t i = 1; i < heap_tuple_element_count; i++) // destroy all shallow copied outputs, except for the mvcc header at position 0
				free(inputs->output_tuple_def->type_info->containees[heap_tuple_position].al.type_info->containees[i].al.type_info);
			free((void*)(inputs->output_tuple_def->type_info->containees[heap_tuple_position].al.type_info));
		}
		free((void*)(inputs->output_tuple_def->type_info));
		free((void*)(inputs->output_tuple_def));
		inputs->output_tuple_def = NULL;
	}

	// drain/destroy the two accumulative notifiers and the heap_table defs
	deinitialize_heap_table_accumulative_notifier(&(inputs->heap_htan));
	deinitialize_heap_table_accumulative_notifier(&(inputs->blob_htan));
	deinit_heap_table_tuple_definitions(&(inputs->httd));

	if(inputs->partition_tuple_def != NULL)
	{
		destroy_type_info_recursively(inputs->partition_tuple_def->type_info, NULL);
		free((void*)(inputs->partition_tuple_def));
		inputs->partition_tuple_def = NULL;
	}

	free(inputs);
}

operator_resource_counter setup_insertion_operator(operator* o, operator* input_operator, positional_accessor* insertion_from_source_positional_accessors, rhendb_table_partition* table_partition, int output_flags)
{
	transaction* tx = input_operator->self_query_plan->curr_tx;

	if(tx->snapshot == NULL)
	{
		printf("must have writable snapshot for insertion_operator\n");
		exit(-1);
	}

	if(!tx->snapshot->has_self_transaction_id)
	{
		printf("must have self_transaction_id for insertion_operator\n");
		exit(-1);
	}

	const tuple_def* input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator);

	data_type_info* partition_type_info = get_data_type_info_for_rhendb_table_partition_from_catalog(&(tx->rdb->cat_mgr), tx->snapshot, table_partition);
	if(!are_identical_type_info(partition_type_info->containees[0].al.type_info, tx->rdb->mvcc_hdr_type_info))
	{
		printf("must have mvcc_header for insertion_operator in the tuples of table_partition\n");
		exit(-1);
	}
	tuple_def* partition_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(partition_tuple_def, partition_type_info);

	operator_resource_counter result = {.job_counter = 1};
	if(o == NULL)
	{
		// counting pass: nothing is stored on `o`, so don't hold any allocations
		// (partition_type_info / partition_tuple_def are pre-existing leaks on this path -- see note)
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
			// make each element instead of the first one a shallow copied nullable type info
			data_type_info* intermediate_table_data_type_info = malloc(sizeof_tuple_data_type_info(partition_type_info->element_count));
			{
				uint32_t intermediate_table_data_type_info_max_size = 8 + sizeof(mvcc_header);

				// we already made sure that the first element is the mvcc_header

				strncpy(intermediate_table_data_type_info->containees[0].field_name, partition_type_info->containees[0].field_name, sizeof(intermediate_table_data_type_info->containees[0].field_name));
				intermediate_table_data_type_info->containees[0].al.type_info = partition_type_info->containees[0].al.type_info;

				// clone the rest while making them nullable ahallowly with same field and type names
				for(uint32_t i = 1; i < partition_type_info->element_count; i++)
				{
					if(partition_type_info->containees[i].al.type_info->type == BIT_FIELD)
						intermediate_table_data_type_info_max_size += 9;
					else
						intermediate_table_data_type_info_max_size += partition_type_info->containees[i].al.type_info->is_variable_sized ? (8 + partition_type_info->containees[i].al.type_info->max_size) : (1 + partition_type_info->containees[i].al.type_info->size);

					strncpy(intermediate_table_data_type_info->containees[i].field_name, partition_type_info->containees[i].field_name, sizeof(intermediate_table_data_type_info->containees[i].field_name));
					intermediate_table_data_type_info->containees[i].al.type_info = shallow_clone_into_nullable_type(partition_type_info->containees[i].al.type_info);
				}

				initialize_tuple_data_type_info(intermediate_table_data_type_info, partition_type_info->type_name, 0, intermediate_table_data_type_info_max_size, partition_type_info->element_count);
				finalize_type_info(intermediate_table_data_type_info);
			}

			// insert this new type_info that has each of it's elements nullable as the last attribute
			strncpy(output_dti->containees[output_dti_element_count].field_name, partition_type_info->type_name, 64);
			output_dti->containees[output_dti_element_count].al.type_info = intermediate_table_data_type_info;

			output_tuple_max_size += intermediate_table_data_type_info->max_size + 8;
			output_dti_element_count++;
		}

		initialize_tuple_data_type_info(output_dti, "inserted_tuple_context", 0, output_tuple_max_size, output_dti_element_count);

		initialize_tuple_def(output_tuple_def, output_dti);
	}

	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_tuple_def = input_tuple_def,
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.insertion_table_partition = *table_partition,
		.insertion_from_source_positional_accessors = insertion_from_source_positional_accessors,
		.partition_tuple_def = partition_tuple_def,
		.output_flags = output_flags,
		.output_tuple_def = output_tuple_def,
		.tx = tx,
	};

	// all heap_table-specific structs are initialized after `inputs` exists, directly on it:
	// the partition heap-table free-space defs and the two accumulative notifiers (heap + blob store),
	// both drained (fixed) at HTAN_FIX_THRESHOLD and holding up to HTAN_CAPACITY entries.
	input_values* inputs = o->inputs;
	init_heap_table_tuple_definitions(&(inputs->httd), &(tx->rdb->persistent_acid_rage_engine.pam_p->pas), partition_tuple_def);
	initialize_heap_table_accumulative_notifier(&(inputs->heap_htan), HTAN_CAPACITY);
	initialize_heap_table_accumulative_notifier(&(inputs->blob_htan), HTAN_CAPACITY);

	return result;
}