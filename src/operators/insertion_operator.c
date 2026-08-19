#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<rhendb/mvcc_header.h>

#include<rhendb/table_operator_output_type.h>
#include<rhendb/fetched_table.h>

#include<rhendb/nullable_type_info_maker.h>

#include<tupleindexer/heap_page/heap_page.h>
#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/utils/heap_table_accumulative_notifier.h>

#include<tuplelargetypes/text_extended.h>
#include<tuplelargetypes/blob_extended.h>
#include<tuplelargetypes/numeric_extended.h>
#include<tuplelargetypes/jsonb_extended.h>

#include<rhendb/util_materialization.h>

#include<stdlib.h>

/*
	heap table insertion operator
*/

#define PAGE_FILL_PER_TUPLE 0.47

// minimum tuple_size for the partition's tuple_def + 32 * number of extended types must fit PAGE_FILL_PER_TUPLE * page_size of the persistent rage engine

typedef enum column_kind column_kind;
enum column_kind
{
	PRIMITIVE_NUMERAL_INSERTION_CASE,
	TEXT_INSERTION_CASE,
	BLOB_INSERTION_CASE,
	NUMERIC_INSERTION_CASE,
	JSONB_INSERTION_CASE,
};

typedef struct extended_column_data extended_column_data;
struct extended_column_data
{
	uint32_t index; // index of the attribute in heap row

	// both will be non-NULL if they used even a single byte on the blob store of the table
	uint32_t prefix_size; // in bytes for text/blob/jsonb, in base-10^12 digits for numeric
	tuple_pointer head_chunk_pointer;
	tuple_pointer tail_chunk_pointer;

	uint64_t written_size; // total_size - written_size are values pending to be written

	uint64_t total_size; // in bytes for text/blob/jsonb, in base-10^12 digits for numeric

	int is_numeric; // picks the write iterator: digit_write_iterator vs binary_write_iterator

	// selected by is_numeric
	union
	{
		struct
		{
			void* value;       // char* for blob, text and jsonb
			int must_free_value;
		};

		// only digits of numeric, if any required
		digits_list digits;
	};
};

// releases whatever the union holds, for the case selected by is_numeric
static void deinit_extended_column_data(extended_column_data* e)
{
	if(e->is_numeric)
		deinitialize_digits_list(&(e->digits));
	else if(e->must_free_value)
		free(e->value);
}

typedef struct input_values input_values;
struct input_values
{
	const tuple_def* input_tuple_def;

	consumption_iterator* input_iterator;

	const fetched_table* ftabl; // fetched_table contains everything about the table

	positional_accessor* insertion_from_source_positional_accessors;

	// array to quickly select a particular case for column insertion, for each one of the columns in partition vs insertion_from_source_positional_accessors
	column_kind* column_kinds;

	// heap_table free-space defs for the partition's heap (record_def == partition_tuple_def)
	heap_table_tuple_defs httd;

	// global notifier is used for finding the right page for insertion, otherwise inserts will reorder pages being scanned by the scan operators
	heap_table_notifier* global_heap_notifier;

	// accumulative notifiers: one for the partition's heap table, one for its blob store.
	heap_table_accumulative_notifier local_heap_htan;
	heap_table_accumulative_notifier local_blob_htan;

	int output_flags; // same flags as table_operator_output_type.h

	const tuple_def* output_tuple_def; // will remain set to NULL, if output_flags is 0

	int additional_flags; // same flags as given in transaction.h, toggles the additional book keeping this operator performs

	// cached here for snapshot and transaction_id
	transaction* tx;

	// this is the tuple_size at which finding free page, we previously failed
	uint32_t will_fail_finding_free_enough_page_on_required_space;

	// tuple builder's context
	void* inited_heap_record;
	uint32_t inited_heap_record_size;
	void* heap_record_without_extensions;
	void* heap_record_with_extensions;
	extended_column_data* ext_col_data;
	uint32_t ext_col_data_size;
};

#define HTAN_CAPACITY       30
#define HTAN_FIX_THRESHOLD  13

static void fix_unused_space_entries_after_insertions(rage_engine* engine, heap_table_accumulative_notifier* htan_p, const heap_table_tuple_defs* httd_p, int force_fix)
{
	if(!force_fix)
	{
		if(get_notification_count_for_heap_table_accumulative_notifier(htan_p) < HTAN_FIX_THRESHOLD)
			return;
	}

	uint64_t root_page_id, page_id;
	uint32_t unused_space;
	while(pop_from_heap_table_accumulative_notifier(htan_p, &root_page_id, &unused_space, &page_id))
	{
		// do this in a separate mini transaction, even if it fails we will be just fine
		uint64_t page_latches_to_be_borrowed = 0;
		int abort_error = 0;
		void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

		fix_unused_space_in_heap_table(root_page_id, unused_space, page_id, httd_p, engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);

		engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);
	}
}

static int build_heap_record_without_extensions(input_values* inputs, const void* input_tuple)
{
	const tuple_def* partition_tuple_def = &(inputs->ftabl->table_partition_tuple_defs[inputs->ftabl->partitions_count-1]);

	inputs->ext_col_data_size = 0;

	memory_move(inputs->heap_record_without_extensions, inputs->inited_heap_record, inputs->inited_heap_record_size);

	transaction* tx = inputs->tx;

	for(uint32_t i = 1; i < partition_tuple_def->type_info->element_count; i++)
	{
		datum src;
		if(!get_value_from_element_from_tuple(&src, inputs->input_tuple_def, inputs->insertion_from_source_positional_accessors[i-1], input_tuple) || is_datum_NULL(&src))
			continue; // leave the column NULL

		// const data_type_info* col_dti = partition_tuple_def->type_info->containees[i].al.type_info;
		const data_type_info* src_dti = get_type_info_for_element_from_tuple_def(inputs->input_tuple_def, inputs->insertion_from_source_positional_accessors[i-1]);

		switch(inputs->column_kinds[i])
		{
			case PRIMITIVE_NUMERAL_INSERTION_CASE :
			{
				set_element_in_tuple_from_tuple(partition_tuple_def, STATIC_POSITION(i), inputs->heap_record_without_extensions, inputs->input_tuple_def, inputs->insertion_from_source_positional_accessors[i-1], input_tuple, UINT32_MAX);
				break;
			}
			case NUMERIC_INSERTION_CASE :
			{
				set_element_in_tuple(partition_tuple_def, STATIC_POSITION(i), inputs->heap_record_without_extensions, EMPTY_DATUM, UINT32_MAX);

				extended_column_data e = {
					.index = i,
					.head_chunk_pointer = get_NULL_tuple_pointer(&(tx->rdb->persistent_acid_rage_engine.pam_p->pas)),
					.tail_chunk_pointer = get_NULL_tuple_pointer(&(tx->rdb->persistent_acid_rage_engine.pam_p->pas)),
					.written_size = 0
				};

				int mrc = 0;
				materialized_numeric m = materialize_numeric1(src, src_dti, tx, &mrc);
				if(mrc)
					return 0;

				numeric_sign_bits sign_bits; int16_t exponent;
				get_sign_bits_and_exponent_for_materialized_numeric(&m, &sign_bits, &exponent);
				set_sign_bits_and_exponent_for_numeric(sign_bits, exponent, inputs->heap_record_without_extensions, partition_tuple_def, STATIC_POSITION(i));

				uint32_t digit_count = get_digits_count_for_materialized_numeric(&m);
				if(digit_count == 0)
				{
					deinitialize_materialized_numeric(&m);
					continue; // sign+exponent fully describe it, nothing to stream
				}

				e.is_numeric = 1; e.total_size = digit_count; e.digits = m.digits;

				inputs->ext_col_data[inputs->ext_col_data_size++] = e;

				break;
			}
			case TEXT_INSERTION_CASE :
			case BLOB_INSERTION_CASE :
			case JSONB_INSERTION_CASE :
			// text / blob / jsonb
			{
				set_element_in_tuple(partition_tuple_def, STATIC_POSITION(i), inputs->heap_record_without_extensions, EMPTY_DATUM, UINT32_MAX);

				extended_column_data e = {
					.index = i,
					.head_chunk_pointer = get_NULL_tuple_pointer(&(tx->rdb->persistent_acid_rage_engine.pam_p->pas)),
					.tail_chunk_pointer = get_NULL_tuple_pointer(&(tx->rdb->persistent_acid_rage_engine.pam_p->pas)),
					.written_size = 0
				};

				uint32_t len = 0, cap = 0; int mrc = 0;
				char* bytes = materialize_tb(src, src_dti, tx, &len, &cap, &mrc);
				if(mrc)
					return 0;
				e.is_numeric = 0; e.total_size = len; e.value = bytes; e.must_free_value = (cap > 0);

				inputs->ext_col_data[inputs->ext_col_data_size++] = e;

				break;
			}
		}
	}

	return 1;
}

static void build_heap_record_with_prefix_bytes(input_values* inputs, void* min_tx_id, int* abort_error, int* should_retry)
{
	rage_engine* engine = &(inputs->tx->rdb->persistent_acid_rage_engine);

	const tuple_def* partition_tuple_def = &(inputs->ftabl->table_partition_tuple_defs[inputs->ftabl->partitions_count-1]);

	const rhendb_table_partition* insertion_table_partition = &(inputs->ftabl->table_partitions_info[inputs->ftabl->partitions_count-1]);

	memory_move(inputs->heap_record_with_extensions, inputs->heap_record_without_extensions, get_tuple_size(partition_tuple_def, inputs->heap_record_with_extensions));

	for(uint32_t k = 0; k < inputs->ext_col_data_size; k++)
	{
		extended_column_data* e = &(inputs->ext_col_data[k]);
		e->written_size = 0;

		e->prefix_size = 0;
		{
			// calculate remaining size for prefix, if negative skip
			int64_t remaining_space = (engine->pam_p->pas.page_size * PAGE_FILL_PER_TUPLE) - get_tuple_size(partition_tuple_def, inputs->heap_record_with_extensions);
			if(remaining_space < 0)
			{
				(*abort_error) = -5002;
				(*should_retry) = 0;
				engine->mark_sub_transaction_aborted(engine->context, min_tx_id, *abort_error);
				return;
			}

			// calculate agains if it is less than 4, i.e. no space for it's size then fail
			int64_t remaining_space_per_element = remaining_space / (inputs->ext_col_data_size - k);
			if(remaining_space_per_element < 4)
			{
				(*abort_error) = -5002;
				(*should_retry) = 0;
				engine->mark_sub_transaction_aborted(engine->context, min_tx_id, *abort_error);
				return;
			}

			// calculate actual prefix size, can be 0
			e->prefix_size = remaining_space_per_element - 4;
		}
		e->prefix_size = min(e->prefix_size, engine->max_prefix_size_in_bytes); // cap it by what the engine supports in it's prefix at best
		if(e->is_numeric) // if numeric count the number of digits it has
			e->prefix_size /= BYTES_PER_NUMERIC_DIGIT;
		e->prefix_size = min(e->prefix_size, e->total_size); // cap it by the total size


		// calculate the limit for phase 2
		uint64_t limit = 0;
		if(e->is_numeric)
			limit = e->prefix_size + (engine->bstd.max_data_bytes_in_chunk / BYTES_PER_NUMERIC_DIGIT);
		else
			limit = e->prefix_size + engine->bstd.max_data_bytes_in_chunk;
		limit = min(limit, e->total_size);

		if(e->is_numeric)
		{
			digit_write_iterator* it = get_new_digit_write_iterator(inputs->heap_record_with_extensions, partition_tuple_def, STATIC_POSITION(e->index), insertion_table_partition->blobs_root_page_id, get_NULL_tuple_pointer(&(engine->pam_p->pas)), e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
			while(e->written_size < limit)
			{
				cy_uint next_digits_count = 0;
				const uint64_t* next_digits = peek_all_contiguous_from_front_in_digits_list(&(e->digits), e->written_size, &next_digits_count); // there will be non-empty returned if the counts up above are correct

				uint32_t w = append_to_digit_write_iterator(it, next_digits, min((uint32_t)(limit - e->written_size), next_digits_count) , &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->local_blob_htan)), min_tx_id, abort_error);
				if(*abort_error)
				{
					delete_digit_write_iterator(it, min_tx_id, abort_error);
					return;
				}
				if(w == 0)
					break;
				e->written_size += w;
			}
			e->head_chunk_pointer = it->extension_head;
			e->tail_chunk_pointer = it->extension_tail;
			delete_digit_write_iterator(it, min_tx_id, abort_error);
			if(*abort_error)
				return;
		}
		else
		{
			binary_write_iterator* it = get_new_binary_write_iterator(inputs->heap_record_with_extensions, partition_tuple_def, STATIC_POSITION(e->index), insertion_table_partition->blobs_root_page_id, get_NULL_tuple_pointer(&(engine->pam_p->pas)), e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
			while(e->written_size < limit)
			{
				uint32_t w = append_to_binary_write_iterator(it, (const char*)e->value + e->written_size, (uint32_t)(limit - e->written_size), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->local_blob_htan)), min_tx_id, abort_error);
				if(*abort_error)
				{
					delete_binary_write_iterator(it, min_tx_id, abort_error);
					return;
				}
				if(w == 0)
					break;
				e->written_size += w;
			}
			e->head_chunk_pointer = it->extension_head;
			e->tail_chunk_pointer = it->extension_tail;
			delete_binary_write_iterator(it, min_tx_id, abort_error);
			if(*abort_error)
				return;
		}
	}
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	// this function stores everything here in this persistent acid engine
	rage_engine* engine = &(inputs->tx->rdb->persistent_acid_rage_engine);

	const rhendb_table_partition* insertion_table_partition = &(inputs->ftabl->table_partitions_info[inputs->ftabl->partitions_count-1]);

	const tuple_def* partition_tuple_def = &(inputs->ftabl->table_partition_tuple_defs[inputs->ftabl->partitions_count-1]);

	// ppage to be release on every return path if it is not NULL
	// use this heap_page between tuple_inserts
	persistent_page ppage = get_NULL_persistent_page(engine->pam_p);
	uint32_t possible_insertion_index = 0;
	int is_ppage_self_created_new_page = 0;
	uint32_t ppage_unused_space_in_entry = 0;

	// for heap_table insertions (does not concern blob_store insertions)
	uint64_t page_latches_to_be_borrowed = 0;

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data)
		{
			// release ppage
			if(!is_persistent_page_NULL(&ppage, engine->pam_p))
			{
				int abort_error = 0;
				release_lock_on_persistent_page(engine->pam_p, NULL, &ppage, NONE_OPTION, &abort_error);
				page_latches_to_be_borrowed--; // done customarily
			}

			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
			return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			// release ppage
			if(!is_persistent_page_NULL(&ppage, engine->pam_p))
			{
				int abort_error = 0;
				release_lock_on_persistent_page(engine->pam_p, NULL, &ppage, NONE_OPTION, &abort_error);
				page_latches_to_be_borrowed--; // done customarily
			}

			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
			return ;
		}

		if(tuple == NULL)
			break;

		if(!build_heap_record_without_extensions(inputs, tuple)) // numeric -> sign_bits and exponent are also populated here
		{
			// release ppage
			if(!is_persistent_page_NULL(&ppage, engine->pam_p))
			{
				int abort_error = 0;
				release_lock_on_persistent_page(engine->pam_p, NULL, &ppage, NONE_OPTION, &abort_error);
				page_latches_to_be_borrowed--; // done customarily
			}

			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("insertion_failed_materialization"));
			return;
		}

		tuple_pointer tptr = get_NULL_tuple_pointer(&(engine->pam_p->pas));

		int should_retry = 1;
		while(should_retry)
		{
			int abort_error = 0;
			void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

			// make it permanent with valid head_chunk pointers, if they are needed
			build_heap_record_with_prefix_bytes(inputs, min_tx_id, &abort_error, &should_retry);
			if(abort_error)
				goto ABORT_ERROR;

			if(get_tuple_size(partition_tuple_def, inputs->heap_record_with_extensions) > (PAGE_FILL_PER_TUPLE * engine->pam_p->pas.page_size))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("record_too_big"));
				abort_error = -5001;
				engine->mark_sub_transaction_aborted(engine->context, min_tx_id, abort_error);
				should_retry = 0;
				goto ABORT_ERROR;
			}

			// find the right page to insert this inputs->heap_record_with_extensions it into
			int is_new_page = 0; // flag to suggest of this is new page for this iteration

			while(1) // keep finding a new page until insertion succeeds
			{
				is_new_page = 0;
				while(is_persistent_page_NULL(&ppage, engine->pam_p))
				{
					// find the right amount of space this record will need
					uint32_t required_space = get_space_to_be_occupied_by_tuple_on_persistent_page(engine->pam_p->pas.page_size, &(partition_tuple_def->size_def), inputs->heap_record_with_extensions);
					if(required_space < inputs->will_fail_finding_free_enough_page_on_required_space) // try only if we are attempting to insert an even smaller tuple
					{
						ppage = find_heap_page_with_enough_unused_space_from_heap_table(insertion_table_partition->heap_root_page_id, required_space, &ppage_unused_space_in_entry, inputs->global_heap_notifier, &(inputs->httd), engine->pam_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
						is_new_page = 0;

						// if we just failed mark that we failed at this size
						if(is_persistent_page_NULL(&ppage, engine->pam_p))
							inputs->will_fail_finding_free_enough_page_on_required_space = min(required_space, inputs->will_fail_finding_free_enough_page_on_required_space);
					}
					if(is_persistent_page_NULL(&ppage, engine->pam_p))
					{
						ppage = get_new_heap_page_with_write_lock(&(engine->pam_p->pas), partition_tuple_def, engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
						is_new_page = 1;
					}

					// populate this call related locals
					possible_insertion_index = 0;
					is_ppage_self_created_new_page = is_new_page;

					break;
				}

				// insert tuple on the page, this would surely not fail
				uint32_t tuple_index = insert_in_heap_page(&ppage, inputs->heap_record_with_extensions, &possible_insertion_index, partition_tuple_def, &(engine->pam_p->pas), engine->pmm_p, min_tx_id, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;
				if(tuple_index == INVALID_TUPLE_INDEX)
				{
					if(!is_new_page && is_ppage_self_created_new_page) // this also implies new_page was surely put into tracking in some previous iteration
					{
						// and it's unused space varied from what it is being tracked at, so put it in local htan, this is out new page it's entries in it's free space can be fixed
						if(ppage_unused_space_in_entry != get_unused_space_on_heap_page(&ppage, &(engine->pam_p->pas), partition_tuple_def))
							push_to_heap_table_accumulative_notifier(&(inputs->local_heap_htan), insertion_table_partition->heap_root_page_id, ppage_unused_space_in_entry, ppage.page_id);
					}

					// release lock on this page
					release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
					if(abort_error)
						goto ABORT_ERROR;
					possible_insertion_index = 0;
					is_ppage_self_created_new_page = 0;
					ppage_unused_space_in_entry = 0;

					continue; // try again, with a new page
				}
				tptr = (tuple_pointer){.page_id = ppage.page_id, .tuple_index = tuple_index};
				break;
			}

			// if it is new page created in this iteration, get it tracked
			if(is_new_page)
			{
				ppage_unused_space_in_entry = get_unused_space_on_heap_page(&ppage, &(engine->pam_p->pas), partition_tuple_def);
				track_unused_space_in_heap_table(insertion_table_partition->heap_root_page_id, &ppage, &(inputs->httd), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;
			}

			// now if we are sure that no other tuple will fit on this page, then we will release lock on this page in advance
			// this allows concurrent transactions to proceed without stalling
			if(get_minimum_tuple_size(partition_tuple_def) > get_unused_space_on_heap_page(&ppage, &(engine->pam_p->pas), partition_tuple_def))
			{
				if(!is_new_page && is_ppage_self_created_new_page) // this also implies new_page was surely put into tracking in some previous iteration
				{
					// and it's unused space varied from what it is being tracked at, so put it in local htan, this is out new page it's entries in it's free space can be fixed
					if(ppage_unused_space_in_entry != get_unused_space_on_heap_page(&ppage, &(engine->pam_p->pas), partition_tuple_def))
						push_to_heap_table_accumulative_notifier(&(inputs->local_heap_htan), insertion_table_partition->heap_root_page_id, ppage_unused_space_in_entry, ppage.page_id);
				}

				release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;
				possible_insertion_index = 0;
				is_ppage_self_created_new_page = 0;
				ppage_unused_space_in_entry = 0;
			}

			// register the inserted tuple_pointer, only if rescan protection was asked for, so that the
			// scans in this very query do not rescan it, then commit the mini transaction
			if(IS_RESCAN_PROTECTION_ENABLED(inputs->additional_flags))
				register_inserted_tuple_pointer(inputs->tx, tptr);

			// complete commit
			engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);
			break;

			ABORT_ERROR:

			// release ppage
			if(!is_persistent_page_NULL(&ppage, engine->pam_p))
				release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
			possible_insertion_index = 0;
			is_ppage_self_created_new_page = 0;
			ppage_unused_space_in_entry = 0;

			// complete abort
			engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);

			if(should_retry == 0)
			{
				for(uint32_t i = 0; i < inputs->ext_col_data_size; i++)
					deinit_extended_column_data(&(inputs->ext_col_data[i]));
				inputs->ext_col_data_size = 0;
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("insert_failed"));
				return ; // ppage is surely released on this path
			}

			continue; // retry
		}

		// fix heap table htan
		fix_unused_space_entries_after_insertions(engine, &(inputs->local_heap_htan), &(inputs->httd), 1);

		// fix blob store htan, we inserted there too
		fix_unused_space_entries_after_insertions(engine, &(inputs->local_blob_htan), &(engine->bstd.httd), 0);

		// append the leftover bytes/digits from each extended column
		for(uint32_t k = 0; k < inputs->ext_col_data_size; k++)
		{
			const extended_column_data* e = &(inputs->ext_col_data[k]);

			if(e->written_size == e->total_size)
			{
				deinit_extended_column_data((extended_column_data*)e);
				continue;
			}
			else if(e->written_size > e->total_size)
			{
				printf("BUG (inserter) :: written more than total size by the inserter operator\n");
				exit(-1);
			}

			while(1)
			{
				uint64_t page_latches_to_be_borrowed2 = 0; // another one for the minitransactions concerning the blob_store, always starts with 0
				int abort_error = 0;
				void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed2);

				uint32_t wrote = 0;
				if(e->is_numeric)
				{
					digit_write_iterator* it = get_new_digit_write_iterator(inputs->heap_record_with_extensions, partition_tuple_def, STATIC_POSITION(e->index), insertion_table_partition->blobs_root_page_id, e->tail_chunk_pointer, e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
					uint64_t curr_written_size = e->written_size;
					while(curr_written_size < e->total_size)
					{
						cy_uint next_digits_count = 0;
						const uint64_t* next_digits = peek_all_contiguous_from_front_in_digits_list(&(e->digits), curr_written_size, &next_digits_count); // there will be non-empty returned if the counts up above are correct

						wrote = append_to_digit_write_iterator(it, next_digits, min(min(e->total_size - curr_written_size, next_digits_count), UINT32_MAX), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->local_blob_htan)), min_tx_id, &abort_error);
						if(abort_error)
						{
							delete_digit_write_iterator(it, min_tx_id, &abort_error);
							goto ABORT_ERROR1;
						}
						curr_written_size += wrote;
						if(wrote == 0)
							break;
					}
					delete_digit_write_iterator(it, min_tx_id, &abort_error);
					if(abort_error)
						goto ABORT_ERROR1;
				}
				else
				{
					binary_write_iterator* it = get_new_binary_write_iterator(inputs->heap_record_with_extensions, partition_tuple_def, STATIC_POSITION(e->index), insertion_table_partition->blobs_root_page_id, e->tail_chunk_pointer, e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
					uint64_t curr_written_size = e->written_size;
					while(curr_written_size < e->total_size)
					{
						wrote = append_to_binary_write_iterator(it, (const char*)e->value + curr_written_size, min(e->total_size - curr_written_size, UINT32_MAX), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->local_blob_htan)), min_tx_id, &abort_error);
						if(abort_error)
						{
							delete_binary_write_iterator(it, min_tx_id, &abort_error);
							goto ABORT_ERROR1;
						}
						curr_written_size += wrote;
						if(wrote == 0)
							break;
					}
					delete_binary_write_iterator(it, min_tx_id, &abort_error);
					if(abort_error)
						goto ABORT_ERROR1;
				}

				engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed2);
				break;

				ABORT_ERROR1:;
				engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed2);
				continue;
			}

			// keep the blob store's free-space entries in check as we go (best effort, own mini txns)
			fix_unused_space_entries_after_insertions(engine, &(inputs->local_blob_htan), &(engine->bstd.httd), 0);

			deinit_extended_column_data((extended_column_data*)e);
		}

		inputs->ext_col_data_size = 0;

		// optionally emit the inserted tuple's identity + attributes for a chained operator
		if(inputs->output_flags != 0)
		{
			uint32_t output_tuple_size = get_minimum_tuple_size(inputs->output_tuple_def);
			uint64_t output_tuple_capacity = output_tuple_size;
			void* output_tuple = malloc(output_tuple_capacity);
			init_tuple(inputs->output_tuple_def, output_tuple);

			uint32_t attr_index = 0;

			if(MUST_OUTPUT_TABLE_ID(inputs->output_flags))
			{
				// ensure there are enough bytes in the output_tuple, as we try to insert this datum
				while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = insertion_table_partition->table_id}), output_tuple_capacity - output_tuple_size))
				{
					output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
					output_tuple = realloc(output_tuple, output_tuple_capacity);
				}
				output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

				attr_index++;
			}

			if(MUST_OUTPUT_PARTITION_ID(inputs->output_flags))
			{
				// ensure there are enough bytes in the output_tuple, as we try to insert this datum
				while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = insertion_table_partition->partition_id}), output_tuple_capacity - output_tuple_size))
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
				set_tuple_pointer(tptr_tpl, tptr, &(engine->pam_p->pas));

				// ensure there are enough bytes in the output_tuple, as we try to insert this datum
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
				void* final_readers_heap_record = project_to_final_readers_tuple_def(inputs->ftabl, inputs->heap_record_with_extensions, inputs->ftabl->partitions_count - 1);
				
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
			if(!produced)
			{
				// release ppage
				if(!is_persistent_page_NULL(&ppage, engine->pam_p))
				{
					int abort_error = 0;
					release_lock_on_persistent_page(engine->pam_p, NULL, &ppage, NONE_OPTION, &abort_error);
					page_latches_to_be_borrowed--; // done customarily
				}

				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
				return ;
			}
		}

		continue;
	}

	// release ppage
	if(!is_persistent_page_NULL(&ppage, engine->pam_p))
	{
		int abort_error = 0;
		release_lock_on_persistent_page(engine->pam_p, NULL, &ppage, NONE_OPTION, &abort_error);
		page_latches_to_be_borrowed--; // done customarily
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

	if(inputs->column_kinds != NULL)
	{
		free(inputs->column_kinds);
		inputs->column_kinds = NULL;
	}

	deinitialize_heap_table_accumulative_notifier(&(inputs->local_heap_htan));
	deinitialize_heap_table_accumulative_notifier(&(inputs->local_blob_htan));
	deinit_heap_table_tuple_definitions(&(inputs->httd));

	if(inputs->inited_heap_record)
	{
		free(inputs->inited_heap_record);
		inputs->inited_heap_record = NULL;
	}

	if(inputs->heap_record_without_extensions != NULL)
	{
		free(inputs->heap_record_without_extensions);
		inputs->heap_record_without_extensions = NULL;
	}

	if(inputs->heap_record_with_extensions != NULL)
	{
		free(inputs->heap_record_with_extensions);
		inputs->heap_record_with_extensions = NULL;
	}

	if(inputs->ext_col_data != NULL)
	{
		for(uint32_t p = 0; p < inputs->ext_col_data_size; p++)
			deinit_extended_column_data(&(inputs->ext_col_data[p]));
		inputs->ext_col_data_size = 0;
		free(inputs->ext_col_data);
		inputs->ext_col_data = NULL;
	}
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

operator_resource_counter setup_insertion_operator(operator* o, operator* input_operator, positional_accessor* insertion_from_source_positional_accessors, const fetched_table* ftabl, int output_flags, heap_table_notifier* global_heap_notifier, int additional_flags)
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

	const tuple_def* partition_tuple_def = &(ftabl->table_partition_tuple_defs[ftabl->partitions_count-1]);
	for(uint32_t i = 0; i < partition_tuple_def->type_info->element_count - 1; i++)
	{
		const data_type_info* col_dti = get_type_info_for_element_from_tuple_def(partition_tuple_def, STATIC_POSITION(i+1));
		const data_type_info* inp_dti = get_type_info_for_element_from_tuple_def(input_tuple_def, insertion_from_source_positional_accessors[i]);
		if(is_primitive_numeral_type_info(col_dti))
		{
			if(!is_primitive_numeral_type_info(inp_dti))
			{
				printf("incompatible type info for primitive_numeral column of table_partition for insertion_operator\n");
				exit(-1);
			}
		}
		else if(is_text_extended_type_info(col_dti))
		{
			if(!is_text_type_info(inp_dti))
			{
				printf("incompatible type info for text column of table_partition for insertion_operator\n");
				exit(-1);
			}
		}
		else if(is_blob_extended_type_info(col_dti))
		{
			if(!is_blob_type_info(inp_dti))
			{
				printf("incompatible type info for blob column of table_partition for insertion_operator\n");
				exit(-1);
			}
		}
		else if(is_numeric_extended_type_info(col_dti))
		{
			if(!is_numeric_type_info(inp_dti))
			{
				printf("incompatible type info for numeric column of table_partition for insertion_operator\n");
				exit(-1);
			}
		}
		else if(is_jsonb_extended_type_info(col_dti))
		{
			if(!is_jsonb_extended_type_info(inp_dti))
			{
				printf("incompatible type info for jsonb column of table_partition for insertion_operator\n");
				exit(-1);
			}
		}
		else
		{
			printf("unsupported type info in column of table_partition for insertion_operator\n");
			exit(-1);
		}
	}

	operator_resource_counter result = {.buffer_counter = 8, .job_counter = 1}; // 8 maximum buffers as we do not expect any btree in the system to exceed this height
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

		initialize_tuple_data_type_info(output_dti, "inserted_tuple_context", 0, output_tuple_max_size, output_dti_element_count);

		initialize_tuple_def(output_tuple_def, output_dti);
	}

	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	input_values* inputs = o->inputs;
	(*inputs) = (input_values){
		.input_tuple_def = input_tuple_def,
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.ftabl = ftabl,
		.insertion_from_source_positional_accessors = insertion_from_source_positional_accessors,
		.column_kinds = malloc(partition_tuple_def->type_info->element_count * sizeof(column_kind)),
		.global_heap_notifier = global_heap_notifier,
		.output_flags = output_flags,
		.output_tuple_def = output_tuple_def,
		.additional_flags = additional_flags,
		.tx = tx,
		.will_fail_finding_free_enough_page_on_required_space = tx->rdb->persistent_acid_rage_engine.pam_p->pas.page_size, // assume we will always succeed
	};

	for(uint32_t i = 1; i < partition_tuple_def->type_info->element_count; i++)
	{
		const data_type_info* col_dti = get_type_info_for_element_from_tuple_def(partition_tuple_def, STATIC_POSITION(i));
		if(is_primitive_numeral_type_info(col_dti))
			inputs->column_kinds[i] = PRIMITIVE_NUMERAL_INSERTION_CASE;
		else if(is_text_extended_type_info(col_dti))
			inputs->column_kinds[i] = TEXT_INSERTION_CASE;
		else if(is_blob_extended_type_info(col_dti))
			inputs->column_kinds[i] = BLOB_INSERTION_CASE;
		else if(is_numeric_extended_type_info(col_dti))
			inputs->column_kinds[i] = NUMERIC_INSERTION_CASE;
		else if(is_jsonb_extended_type_info(col_dti))
			inputs->column_kinds[i] = JSONB_INSERTION_CASE;
		else
		{
			printf("unsupported type info in column of table_partition for insertion_operator (not possible)\n");
			exit(-1);
		}
	}

	// all heap_table-specific structs are initialized after `inputs` exists, directly on it:
	// the partition heap-table free-space defs and the two accumulative notifiers (heap + blob store),
	// both drained (fixed) at HTAN_FIX_THRESHOLD and holding up to HTAN_CAPACITY entries.
	init_heap_table_tuple_definitions(&(inputs->httd), &(tx->rdb->persistent_acid_rage_engine.pam_p->pas), partition_tuple_def);
	initialize_heap_table_accumulative_notifier(&(inputs->local_heap_htan), HTAN_CAPACITY);
	initialize_heap_table_accumulative_notifier(&(inputs->local_blob_htan), HTAN_CAPACITY);

	// init the repeated allocation state for this operator
	inputs->inited_heap_record_size = get_minimum_tuple_size(partition_tuple_def);
	inputs->inited_heap_record = malloc(inputs->inited_heap_record_size);
	init_tuple(partition_tuple_def, inputs->inited_heap_record);
	{
		tuple_def mvcc_def;
		initialize_tuple_def(&mvcc_def, (data_type_info*)(partition_tuple_def->type_info->containees[0].al.type_info));

		mvcc_header hdr = (mvcc_header){0};
		hdr.is_xmin_NULL = 0;
		hdr.xmin.transaction_id = inputs->tx->snapshot->self_transaction_id;
		hdr.xmin.is_committed = 0; hdr.xmin.is_aborted = 0;
		hdr.is_xmax_NULL = 1;

		char mvcc_buf[sizeof(mvcc_header)];
		init_tuple(&mvcc_def, mvcc_buf);
		write_mvcc_header(mvcc_buf, &mvcc_def, &hdr);

		set_element_in_tuple(partition_tuple_def, STATIC_POSITION(0), inputs->inited_heap_record, &(datum){.tuple_value = mvcc_buf}, UINT32_MAX);
	}
	inputs->heap_record_without_extensions = malloc(tx->rdb->persistent_acid_rage_engine.pam_p->pas.page_size);
	inputs->heap_record_with_extensions = malloc(tx->rdb->persistent_acid_rage_engine.pam_p->pas.page_size);
	inputs->ext_col_data = malloc(sizeof(extended_column_data) * partition_tuple_def->type_info->element_count);
	inputs->ext_col_data_size = 0;

	return result;
}