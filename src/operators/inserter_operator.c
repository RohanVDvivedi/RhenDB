#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<rhendb/mvcc_header.h>

#include<rhendb/table_operator_output_type.h>

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

#define PAGE_FILL_PER_TUPLE 0.4

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
	uint32_t prefix_size; // in bytes for text/blob/jsonb, in base-10^12 digits for numeric
	tuple_pointer head_chunk_pointer;
	tuple_pointer tail_chunk_pointer;

	uint64_t written_size; // total_size - written_size are values pending to be written

	uint64_t total_size; // in bytes for text/blob/jsonb, in base-10^12 digits for numeric

	int is_numeric; // picks the write iterator: digit_write_iterator vs binary_write_iterator

	// char* for blob, text and uint64_t* numeric
	void* value;
};

#define HTAN_CAPACITY       50
#define HTAN_FIX_THRESHOLD  25

static void fix_unused_space_entries_after_insertions(rage_engine* engine, heap_table_accumulative_notifier* htan_p, const heap_table_tuple_defs* httd_p)
{
	if(get_notification_count_for_heap_table_accumulative_notifier(htan_p) < HTAN_FIX_THRESHOLD)
		return;

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

static void* build_heap_record_without_extensions(input_values* inputs, const void* input_tuple, extended_column_data** ext_col_data, uint32_t* ext_col_data_size)
{
	(*ext_col_data_size) = 0;
	(*ext_col_data) = malloc(sizeof(extended_column_data) * inputs->partition_tuple_def->type_info->element_count);

	uint64_t record_capacity = inputs->tx->rdb->persistent_acid_rage_engine.pam_p->pas.page_size;
	uint32_t record_size = get_minimum_tuple_size(inputs->partition_tuple_def);
	void* record = malloc(record_capacity);
	init_tuple(inputs->partition_tuple_def, record);

	// (0) mvcc header
	{
		tuple_def mvcc_def;
		initialize_tuple_def(&mvcc_def, (data_type_info*)(inputs->partition_tuple_def->type_info->containees[0].al.type_info));

		mvcc_header hdr = (mvcc_header){0};
		hdr.is_xmin_NULL = 0;
		hdr.xmin.transaction_id = inputs->tx->snapshot->self_transaction_id;
		hdr.xmin.is_committed = 0; hdr.xmin.is_aborted = 0;
		hdr.is_xmax_NULL = 1;

		char mvcc_buf[sizeof(mvcc_header)];
		init_tuple(&mvcc_def, mvcc_buf);
		write_mvcc_header(mvcc_buf, &mvcc_def, &hdr);

		while(!set_element_in_tuple(inputs->partition_tuple_def, STATIC_POSITION(0), record, &(datum){.tuple_value = mvcc_buf}, record_capacity - record_size))
		{
			record_capacity *= 2;
			record = realloc(record, record_capacity);
		}
		record_size = get_tuple_size(inputs->partition_tuple_def, record);
	}

	transaction* tx = inputs->tx;

	for(uint32_t i = 1; i < inputs->partition_tuple_def->type_info->element_count; i++)
	{
		datum src;
		if(!get_value_from_element_from_tuple(&src, inputs->input_tuple_def, inputs->insertion_from_source_positional_accessors[i-1], input_tuple) || is_datum_NULL(&src))
			continue; // leave the column NULL

		const data_type_info* col_dti = inputs->partition_tuple_def->type_info->containees[i].al.type_info;
		const data_type_info* src_dti = get_type_info_for_element_from_tuple_def(inputs->input_tuple_def, inputs->insertion_from_source_positional_accessors[i-1]);

		if(is_primitive_numeral_type_info(col_dti))
		{
			while(!set_element_in_tuple_from_tuple(inputs->partition_tuple_def, STATIC_POSITION(i), record, inputs->input_tuple_def, inputs->insertion_from_source_positional_accessors[i-1], input_tuple, record_capacity - record_size))
			{
				record_capacity *= 2;
				record = realloc(record, record_capacity);
			}
			record_size = get_tuple_size(inputs->partition_tuple_def, record);
			continue;
		}

		if(!is_extended_type_info(col_dti))
		{
			printf("BUG (inserter) :: attribute %u is neither primitive-numeral nor extended\n", i);
			exit(-1);
		}

		while(!set_element_in_tuple(inputs->partition_tuple_def, STATIC_POSITION(i), record, EMPTY_DATUM, record_capacity - record_size))
		{
			record_capacity *= 2;
			record = realloc(record, record_capacity);
		}
		record_size = get_tuple_size(inputs->partition_tuple_def, record);

		extended_column_data e = {
			.index = i,
			.head_chunk_pointer = get_NULL_tuple_pointer(&(tx->rdb->persistent_acid_rage_engine.pam_p->pas)),
			.tail_chunk_pointer = get_NULL_tuple_pointer(&(tx->rdb->persistent_acid_rage_engine.pam_p->pas)),
			.written_size = 0
		};

		if(is_numeric_type_info(col_dti))
		{
			int mrc = 0;
			materialized_numeric m = materialize_numeric1(src, src_dti, tx, &mrc);
			if(mrc)
			{
				for(uint32_t p = 0; p < (*ext_col_data_size); p++)
					free((*ext_col_data)[p].value);
				free(*ext_col_data);
				(*ext_col_data) = NULL;
				(*ext_col_data_size) = 0;
				free(record);
				return NULL;
			}

			numeric_sign_bits sign_bits; int16_t exponent;
			get_sign_bits_and_exponent_for_materialized_numeric(&m, &sign_bits, &exponent);
			set_sign_bits_and_exponent_for_numeric(sign_bits, exponent, record, inputs->partition_tuple_def, STATIC_POSITION(i));
			record_size = get_tuple_size(inputs->partition_tuple_def, record);

			uint32_t digit_count = get_digits_count_for_materialized_numeric(&m);
			if(digit_count == 0)
			{
				deinitialize_materialized_numeric(&m);
				continue; // sign+exponent fully describe it, nothing to stream
			}

			uint64_t* digits = malloc(sizeof(uint64_t) * digit_count);
			for(uint32_t i = 0; i < digit_count; i++)
				digits[i] = get_nth_digit_from_materialized_numeric(&m, i);
			deinitialize_materialized_numeric(&m);

			e.is_numeric = 1; e.total_size = digit_count; e.value = digits;
		}
		else // text / blob / jsonb
		{
			uint32_t len = 0, cap = 0; int mrc = 0;
			char* bytes = materialize_tb(src, src_dti, tx, &len, &cap, &mrc);
			if(mrc)
			{
				for(uint32_t p = 0; p < (*ext_col_data_size); p++)
					free((*ext_col_data)[p].value);
				free(*ext_col_data);
				(*ext_col_data) = NULL;
				(*ext_col_data_size) = 0;
				free(record);
				return NULL;
			}
			e.is_numeric = 0; e.total_size = len; e.value = bytes;
		}

		(*ext_col_data)[(*ext_col_data_size)++] = e;
	}

	return record;
}

static void build_heap_record_with_prefix_bytes(input_values* inputs, void* record, extended_column_data* ext_col_data, uint32_t ext_col_data_size, void* min_tx_id, int* abort_error, int* should_retry)
{
	rage_engine* engine = &(inputs->tx->rdb->persistent_acid_rage_engine);

	for(uint32_t k = 0; k < ext_col_data_size; k++)
	{
		extended_column_data* e = &(ext_col_data[k]);
		e->written_size = 0;

		e->prefix_size = 0;
		{
			// calculate remaining size for prefix, if negative skip
			int64_t remaining_space = (engine->pam_p->pas.page_size * PAGE_FILL_PER_TUPLE) - get_tuple_size(inputs->partition_tuple_def, record);
			if(remaining_space < 0)
			{
				(*abort_error) = -5002;
				(*should_retry) = 0;
				engine->mark_sub_transaction_aborted(engine->context, min_tx_id, *abort_error);
				return;
			}

			// calculate agains if it is less than 4, i.e. no space for it's size then fail
			int64_t remaining_space_per_element = remaining_space / (ext_col_data_size - k);
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
			digit_write_iterator* it = get_new_digit_write_iterator(record, inputs->partition_tuple_def, STATIC_POSITION(e->index), inputs->insertion_table_partition.blobs_root_page_id, get_NULL_tuple_pointer(&(engine->pam_p->pas)), e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
			while(e->written_size < limit)
			{
				uint32_t w = append_to_digit_write_iterator(it, ((uint64_t*)e->value) + e->written_size, (uint32_t)(limit - e->written_size), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->blob_htan)), min_tx_id, abort_error);
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
			binary_write_iterator* it = get_new_binary_write_iterator(record, inputs->partition_tuple_def, STATIC_POSITION(e->index), inputs->insertion_table_partition.blobs_root_page_id, get_NULL_tuple_pointer(&(engine->pam_p->pas)), e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
			while(e->written_size < limit)
			{
				uint32_t w = append_to_binary_write_iterator(it, (const char*)e->value + e->written_size, (uint32_t)(limit - e->written_size), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->blob_htan)), min_tx_id, abort_error);
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
		void* heap_record = build_heap_record_without_extensions(inputs, tuple, &ext_col_data, &ext_col_data_size); // numeric -> sign_bits and exponent are also populated here
		if(heap_record == NULL)
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("insertion_failed_materialization"));
			return;
		}

		tuple_pointer tptr = get_NULL_tuple_pointer(&(engine->pam_p->pas));

		int should_retry = 1;
		while(should_retry)
		{
			// clone heap record base, into a static/fixed insertable heap_record
			void* heap_record_clone = malloc(engine->pam_p->pas.page_size);
			memory_move(heap_record_clone, heap_record, get_tuple_size(inputs->partition_tuple_def, heap_record));

			uint64_t page_latches_to_be_borrowed = 0;
			int abort_error = 0;
			void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

			// init an empty persistent page
			persistent_page ppage = get_NULL_persistent_page(engine->pam_p);

			// make it permanent with valid head_chunk pointers, if they are needed
			build_heap_record_with_prefix_bytes(inputs, heap_record_clone, ext_col_data, ext_col_data_size, min_tx_id, &abort_error, &should_retry);
			if(abort_error)
				goto ABORT_ERROR;

			if(get_tuple_size(inputs->partition_tuple_def, heap_record_clone) > (PAGE_FILL_PER_TUPLE * engine->pam_p->pas.page_size))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("record_too_big"));
				abort_error = -5001;
				engine->mark_sub_transaction_aborted(engine->context, min_tx_id, abort_error);
				should_retry = 0;
				goto ABORT_ERROR;
			}

			// find the right amount of space this record will need
			uint32_t required_space = get_space_to_be_occupied_by_tuple_on_persistent_page(engine->pam_p->pas.page_size, &(inputs->partition_tuple_def->size_def), heap_record_clone);

			// find the right page to insert this heap_record_clone it into
			int is_new_page = 0;
			uint32_t unused_space_in_entry = 0;
			ppage = find_heap_page_with_enough_unused_space_from_heap_table(inputs->insertion_table_partition.heap_root_page_id, required_space, &unused_space_in_entry, &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->heap_htan)), &(inputs->httd), engine->pam_p, min_tx_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;
			if(is_persistent_page_NULL(&ppage, engine->pam_p))
			{
				ppage = get_new_heap_page_with_write_lock(&(engine->pam_p->pas), inputs->partition_tuple_def, engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;
				is_new_page = 1;
			}

			// insert tuple on the page, this would surely not fail
			uint32_t possible_insertion_index = 0;
			uint32_t tuple_index = insert_in_heap_page(&ppage, heap_record_clone, &possible_insertion_index, inputs->partition_tuple_def, &(engine->pam_p->pas), engine->pmm_p, min_tx_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;
			tptr = (tuple_pointer){.page_id = ppage.page_id, .tuple_index = tuple_index};

			// if it is new page, get it tracked
			if(is_new_page)
			{
				track_unused_space_in_heap_table(inputs->insertion_table_partition.heap_root_page_id, &ppage, &(inputs->httd), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;
			}

			// release lock on this page
			release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			// register the inserted tuple_pointer so scans skip it, then commit the insert mini transaction
			register_inserted_tuple_pointer(inputs->tx, tptr);
			engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);

			// everything went successfully now make heap_record_clone the read heap_record
			free(heap_record);
			heap_record = heap_record_clone;
			break;

			ABORT_ERROR:
			free(heap_record_clone);
			if(!is_persistent_page_NULL(&ppage, engine->pam_p))
				release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
			engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);

			if(should_retry == 0)
			{
				free(heap_record);
				for(uint32_t i = 0; i < ext_col_data_size; i++)
					free(ext_col_data[i].value);
				free(ext_col_data);
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("insert_failed"));
				return ;
			}

			continue; // retry
		}

		// fix heap table htan
		fix_unused_space_entries_after_insertions(engine, &(inputs->heap_htan), &(inputs->httd));

		// fix blob store htan, we inserted there too
		fix_unused_space_entries_after_insertions(engine, &(inputs->blob_htan), &(engine->bstd.httd));

		// append the leftover bytes/digits from each extended column
		for(uint32_t k = 0; k < ext_col_data_size; k++)
		{
			extended_column_data* e = &(ext_col_data[k]);

			while(1)
			{
				uint64_t page_latches_to_be_borrowed = 0;
				int abort_error = 0;
				void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

				uint32_t wrote = 0;
				if(e->is_numeric)
				{
					digit_write_iterator* it = get_new_digit_write_iterator(heap_record, inputs->partition_tuple_def, STATIC_POSITION(e->index), inputs->insertion_table_partition.blobs_root_page_id, e->tail_chunk_pointer, e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
					uint64_t curr_written_size = e->written_size;
					while(curr_written_size < e->total_size)
					{
						wrote = append_to_digit_write_iterator(it, ((uint64_t*)e->value) + curr_written_size, (uint32_t)(e->total_size - curr_written_size), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->blob_htan)), min_tx_id, &abort_error);
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
					binary_write_iterator* it = get_new_binary_write_iterator(heap_record, inputs->partition_tuple_def, STATIC_POSITION(e->index), inputs->insertion_table_partition.blobs_root_page_id, e->tail_chunk_pointer, e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
					uint64_t curr_written_size = e->written_size;
					while(curr_written_size < e->total_size)
					{
						wrote = append_to_binary_write_iterator(it, (const char*)e->value + curr_written_size, (uint32_t)(e->total_size - curr_written_size), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->blob_htan)), min_tx_id, &abort_error);
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

				engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);
				break;

				ABORT_ERROR1:;
				engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);
				continue;
			}

			// keep the blob store's free-space entries in check as we go (best effort, own mini txns)
			fix_unused_space_entries_after_insertions(engine, &(inputs->blob_htan), &(engine->bstd.httd));

			free(e->value);
		}

		free(ext_col_data);
		ext_col_data = NULL;

		fix_unused_space_entries_after_insertions(engine, &(inputs->blob_htan), &(engine->bstd.httd));

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
				while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = inputs->insertion_table_partition.table_id}), output_tuple_capacity - output_tuple_size))
				{
					output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
					output_tuple = realloc(output_tuple, output_tuple_capacity);
				}

				// recompute tuple_size
				output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

				attr_index++;
			}

			if(MUST_OUTPUT_PARTITION_ID(inputs->output_flags))
			{
				// ensure there are enough bytes in the output_tuple, as we try to insert this datum
				while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = inputs->insertion_table_partition.partition_id}), output_tuple_capacity - output_tuple_size))
				{
					output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
					output_tuple = realloc(output_tuple, output_tuple_capacity);
				}

				// recompute tuple_size
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

				// recompute tuple_size
				output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

				attr_index++;
			}

			if(MUST_OUTPUT_HEAP_TUPLE(inputs->output_flags))
			{
				// first set it to empty
				{
					// ensure there are enough bytes in the output_tuple, as we try to insert this datum
					while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, EMPTY_DATUM, output_tuple_capacity - output_tuple_size))
					{
						output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
						output_tuple = realloc(output_tuple, output_tuple_capacity);
					}

					// recompute tuple_size
					output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);
				}

				// then set the attributes
				for(uint32_t i = 0; i < inputs->partition_tuple_def->type_info->element_count; i++)
				{
					// ensure there are enough bytes in the output_tuple, as we try to insert this datum
					while(!set_element_in_tuple_from_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index, i), output_tuple, inputs->partition_tuple_def, STATIC_POSITION(i), heap_record, output_tuple_capacity - output_tuple_size))
					{
						output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
						output_tuple = realloc(output_tuple, output_tuple_capacity);
					}

					// recompute tuple_size
					output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);
				}

				attr_index++;
			}

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
		printf("must have mvcc_header for insertion_operator in the tuples of table_partition for insertion_operator\n");
		exit(-1);
	}
	tuple_def* partition_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(partition_tuple_def, partition_type_info);

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
	{
		destroy_type_info_recursively(partition_tuple_def->type_info, NULL);
		free((partition_tuple_def));
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