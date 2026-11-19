#include<rhendb/dml.h>

#include<rhendb/mvcc_header.h>
#include<rhendb/scan_dml_output_type.h>

#include<tupleindexer/heap_page/heap_page.h>
#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/utils/heap_table_accumulative_notifier.h>

#include<tuplelargetypes/text_extended.h>
#include<tuplelargetypes/blob_extended.h>
#include<tuplelargetypes/numeric_extended.h>
#include<tuplelargetypes/jsonb_extended.h>

#include<rhendb/util_materialization.h>

#include<stdlib.h>
#include<string.h>

// the largest fraction of a page a single record (inlined prefixes included) is allowed to occupy
#define PAGE_FILL_PER_TUPLE 0.47

// bytes kept mapped at a time over an interim_tuple_store cursor
#define INTERIM_STORE_MMAP_BATCH   (1u << 20)

// accumulative-notifier capacity, and the pending count at which it is drained
#define HTAN_CAPACITY        30
#define HTAN_FIX_THRESHOLD   13

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
	uint32_t index; // position of this attribute in the heap record

	uint32_t prefix_size; // bytes inlined in the record (base-10^12 digits for numeric)
	tuple_pointer head_chunk_pointer;
	tuple_pointer tail_chunk_pointer;

	uint64_t written_size; // of total_size, how much has been streamed to the blob store so far
	uint64_t total_size;   // bytes for text/blob/jsonb, base-10^12 digits for numeric

	int is_numeric; // selects digit_write_iterator over binary_write_iterator

	union
	{
		struct
		{
			void* value; // char* for text, blob and jsonb
			int must_free_value;
		};
		digits_list digits; // for numeric
	};
};

static void deinit_extended_column_data(extended_column_data* e)
{
	if(e->is_numeric)
		deinitialize_digits_list(&(e->digits));
	else if(e->must_free_value)
		free(e->value);
}

static void fix_unused_space_entries_after_insertions(rage_engine* engine, heap_table_accumulative_notifier* htan_p, const heap_table_tuple_defs* httd_p, int force_fix)
{
	if(!force_fix && get_notification_count_for_heap_table_accumulative_notifier(htan_p) < HTAN_FIX_THRESHOLD)
		return;

	uint64_t root_page_id, page_id;
	uint32_t unused_space;
	while(pop_from_heap_table_accumulative_notifier(htan_p, &root_page_id, &unused_space, &page_id))
	{
		// each fixup runs in its own mini transaction; a failure here is harmless
		uint64_t page_latches_to_be_borrowed = 0;
		int abort_error = 0;
		void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

		fix_unused_space_in_heap_table(root_page_id, unused_space, page_id, httd_p, engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);

		engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);
	}
}

// fills heap_record_without_extensions with the mvcc header (from inited_heap_record) and the primitive
// columns, and records every extended (text/blob/numeric/jsonb) column in ext_col_data for later streaming.
// returns 0 on a failed materialization of an extended value.
static int build_heap_record_without_extensions(const tuple_def* partition_tuple_def, const tuple_def* input_tuple_def, const positional_accessor* insertion_from_source_positional_accessors, const column_kind* column_kinds, transaction* tx, const void* input_tuple, const void* inited_heap_record, uint32_t inited_heap_record_size, void* heap_record_without_extensions, extended_column_data* ext_col_data, uint32_t* ext_col_data_size)
{
	*ext_col_data_size = 0;
	memmove(heap_record_without_extensions, inited_heap_record, inited_heap_record_size);

	for(uint32_t i = 1; i < partition_tuple_def->type_info->element_count; i++)
	{
		const data_type_info* src_dti;
		datum src;
		if(!get_value_from_element_from_tuple(&src, &src_dti, input_tuple_def, insertion_from_source_positional_accessors[i-1], input_tuple) || is_datum_NULL(&src))
			continue; // leave the column NULL

		switch(column_kinds[i])
		{
			case PRIMITIVE_NUMERAL_INSERTION_CASE :
			{
				set_element_in_tuple_from_tuple(partition_tuple_def, STATIC_POSITION(i), heap_record_without_extensions, input_tuple_def, insertion_from_source_positional_accessors[i-1], input_tuple, UINT32_MAX);
				break;
			}
			case NUMERIC_INSERTION_CASE :
			{
				set_element_in_tuple(partition_tuple_def, STATIC_POSITION(i), heap_record_without_extensions, EMPTY_DATUM, UINT32_MAX);

				extended_column_data e = {
					.index = i,
					.head_chunk_pointer = get_NULL_tuple_pointer(&(tx->rdb->persistent_acid_rage_engine.pam_p->pas)),
					.tail_chunk_pointer = get_NULL_tuple_pointer(&(tx->rdb->persistent_acid_rage_engine.pam_p->pas)),
					.written_size = 0
				};

				int materialization_error = 0;
				materialized_numeric m = materialize_numeric1(src, src_dti, tx, 0, &materialization_error);
				if(materialization_error)
					return 0;

				numeric_sign_bits sign_bits; int16_t exponent;
				get_sign_bits_and_exponent_for_materialized_numeric(&m, &sign_bits, &exponent);
				set_sign_bits_and_exponent_for_numeric(sign_bits, exponent, heap_record_without_extensions, partition_tuple_def, STATIC_POSITION(i));

				uint32_t digit_count = get_digits_count_for_materialized_numeric(&m);
				if(digit_count == 0) // sign and exponent describe it fully, nothing to stream
				{
					deinitialize_materialized_numeric(&m);
					continue;
				}

				e.is_numeric = 1; e.total_size = digit_count; e.digits = m.digits;
				ext_col_data[(*ext_col_data_size)++] = e;
				break;
			}
			case TEXT_INSERTION_CASE :
			case BLOB_INSERTION_CASE :
			case JSONB_INSERTION_CASE :
			{
				set_element_in_tuple(partition_tuple_def, STATIC_POSITION(i), heap_record_without_extensions, EMPTY_DATUM, UINT32_MAX);

				extended_column_data e = {
					.index = i,
					.head_chunk_pointer = get_NULL_tuple_pointer(&(tx->rdb->persistent_acid_rage_engine.pam_p->pas)),
					.tail_chunk_pointer = get_NULL_tuple_pointer(&(tx->rdb->persistent_acid_rage_engine.pam_p->pas)),
					.written_size = 0
				};

				uint32_t length = 0, capacity = 0; int materialization_error = 0;
				char* bytes = materialize_tbj(src, src_dti, tx, &length, &capacity, 0, &materialization_error);
				if(materialization_error)
					return 0;

				e.is_numeric = 0; e.total_size = length; e.value = bytes; e.must_free_value = (capacity > 0);
				ext_col_data[(*ext_col_data_size)++] = e;
				break;
			}
		}
	}

	return 1;
}

// copies the primitive record and inlines, for each extended column, as large a prefix as fits the record,
// streaming any excess to the blob store as its head chunk. on abort, *abort_error is set (and *should_retry
// cleared if the record can never fit).
static void build_heap_record_with_prefix_bytes(const tuple_def* partition_tuple_def, const rhendb_table_partition* insertion_table_partition, transaction* tx, const void* heap_record_without_extensions, void* heap_record_with_extensions, extended_column_data* ext_col_data, uint32_t ext_col_data_size, heap_table_accumulative_notifier* local_blob_htan, void* min_tx_id, int* abort_error, int* should_retry)
{
	rage_engine* engine = &(tx->rdb->persistent_acid_rage_engine);

	memmove(heap_record_with_extensions, heap_record_without_extensions, get_tuple_size(partition_tuple_def, heap_record_without_extensions));

	for(uint32_t k = 0; k < ext_col_data_size; k++)
	{
		extended_column_data* e = &(ext_col_data[k]);
		e->written_size = 0;

		// split the remaining record budget evenly across the columns still to be inlined
		int64_t remaining_space = (engine->pam_p->pas.page_size * PAGE_FILL_PER_TUPLE) - get_tuple_size(partition_tuple_def, heap_record_with_extensions);
		int64_t remaining_space_per_element = (remaining_space >= 0) ? remaining_space / (ext_col_data_size - k) : -1;
		if(remaining_space_per_element < 4)
		{
			(*abort_error) = -5002;
			(*should_retry) = 0;
			engine->mark_sub_transaction_aborted(engine->context, min_tx_id, *abort_error);
			return;
		}

		e->prefix_size = remaining_space_per_element - 4;
		e->prefix_size = min(e->prefix_size, engine->max_prefix_size_in_bytes);
		if(e->is_numeric)
			e->prefix_size /= BYTES_PER_NUMERIC_DIGIT;
		e->prefix_size = min(e->prefix_size, e->total_size);

		// write the inlined prefix plus at most one blob chunk of overflow in this mini transaction
		uint64_t limit = e->prefix_size + (e->is_numeric ? engine->bstd.max_data_bytes_in_chunk / BYTES_PER_NUMERIC_DIGIT : engine->bstd.max_data_bytes_in_chunk);
		limit = min(limit, e->total_size);

		if(e->is_numeric)
		{
			digit_write_iterator* it = get_new_digit_write_iterator(heap_record_with_extensions, partition_tuple_def, STATIC_POSITION(e->index), insertion_table_partition->blobs_root_page_id, get_NULL_tuple_pointer(&(engine->pam_p->pas)), e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
			while(e->written_size < limit)
			{
				cy_uint next_digits_count = 0;
				const uint64_t* next_digits = peek_all_contiguous_from_front_in_digits_list(&(e->digits), e->written_size, &next_digits_count);

				uint32_t wrote = append_to_digit_write_iterator(it, next_digits, min((uint32_t)(limit - e->written_size), next_digits_count), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(local_blob_htan), min_tx_id, abort_error);
				if(*abort_error)
				{
					delete_digit_write_iterator(it, min_tx_id, abort_error);
					return;
				}
				if(wrote == 0)
					break;
				e->written_size += wrote;
			}
			e->head_chunk_pointer = it->extension_head;
			e->tail_chunk_pointer = it->extension_tail;
			delete_digit_write_iterator(it, min_tx_id, abort_error);
		}
		else
		{
			binary_write_iterator* it = get_new_binary_write_iterator(heap_record_with_extensions, partition_tuple_def, STATIC_POSITION(e->index), insertion_table_partition->blobs_root_page_id, get_NULL_tuple_pointer(&(engine->pam_p->pas)), e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
			while(e->written_size < limit)
			{
				uint32_t wrote = append_to_binary_write_iterator(it, (const char*)e->value + e->written_size, (uint32_t)(limit - e->written_size), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(local_blob_htan), min_tx_id, abort_error);
				if(*abort_error)
				{
					delete_binary_write_iterator(it, min_tx_id, abort_error);
					return;
				}
				if(wrote == 0)
					break;
				e->written_size += wrote;
			}
			e->head_chunk_pointer = it->extension_head;
			e->tail_chunk_pointer = it->extension_tail;
			delete_binary_write_iterator(it, min_tx_id, abort_error);
		}

		if(*abort_error)
			return;
	}
}

// builds the flag-driven output row (table_id, partition_id, tuple_pointer, heap tuple -- the same shape the
// scan and pointer_lookup operators produce) for a just-inserted tuple and appends it to ots_p
static void append_output_tuple(transaction* tx, const tuple_def* output_tuple_def, const fetched_table* ftabl, tuple_pointer tptr, const void* heap_record_with_extensions, int output_flags, interim_tuple_store* ots_p)
{
	void* output_tuple = project_to_scan_dml_output_tuple_def(tx->rdb, output_tuple_def, ftabl, ftabl->partitions_count-1, tptr, heap_record_with_extensions, output_flags);

	append_tuple_to_interim_tuple_store2(ots_p, &(ots_p->embed_regions[0]), output_tuple, &(output_tuple_def->size_def), INTERIM_STORE_MMAP_BATCH);

	free(output_tuple);
}

uint64_t dml_insert(
	interim_tuple_store* ots_p,
	interim_tuple_store* its_p,
	const tuple_def* input_tuple_def,
	const positional_accessor* insertion_from_source_positional_accessors,
	const fetched_table* ftabl,
	transaction* tx,
	int output_flags,
	int additional_flags,
	int* dml_error)
{
	(*dml_error) = DML_SUCCESS;
	uint64_t inserted_count = 0;

	if(tx->snapshot == NULL || !(tx->snapshot->has_self_transaction_id))
	{
		(*dml_error) = DML_INSERT_FAILED;
		return 0;
	}

	rage_engine* engine = &(tx->rdb->persistent_acid_rage_engine);
	const tuple_def* partition_tuple_def = &(ftabl->table_partition_tuple_defs[ftabl->partitions_count-1]);
	const rhendb_table_partition* insertion_table_partition = &(ftabl->table_partitions_info[ftabl->partitions_count-1]);

	int do_output = (ots_p != NULL) && (output_flags != 0);

	// only open the interim stores (the caller still owns them); the embed regions are unmapped at the end
	open_file_for_interim_tuple_store(its_p);
	if(do_output)
		open_file_for_interim_tuple_store(ots_p);

	// the output row shape, built once; NULL when nothing is produced
	tuple_def* output_tuple_def = NULL;
	if(do_output)
		output_tuple_def = get_scan_dml_output_tuple_def(tx->rdb, ftabl, output_flags);

	// one insertion case per partition attribute
	column_kind* column_kinds = malloc(partition_tuple_def->type_info->element_count * sizeof(column_kind));
	for(uint32_t i = 1; i < partition_tuple_def->type_info->element_count; i++)
	{
		const data_type_info* col_dti = get_type_info_for_element_from_tuple_def(partition_tuple_def, STATIC_POSITION(i));
		if(is_primitive_numeral_type_info(col_dti))
			column_kinds[i] = PRIMITIVE_NUMERAL_INSERTION_CASE;
		else if(is_text_extended_type_info(col_dti))
			column_kinds[i] = TEXT_INSERTION_CASE;
		else if(is_blob_extended_type_info(col_dti))
			column_kinds[i] = BLOB_INSERTION_CASE;
		else if(is_numeric_extended_type_info(col_dti))
			column_kinds[i] = NUMERIC_INSERTION_CASE;
		else if(is_jsonb_extended_type_info(col_dti))
			column_kinds[i] = JSONB_INSERTION_CASE;
		else
		{
			printf("unsupported type info in column of table_partition for dml_insert\n");
			exit(-1);
		}
	}

	heap_table_tuple_defs httd;
	init_heap_table_tuple_definitions(&httd, &(engine->pam_p->pas), partition_tuple_def);
	heap_table_accumulative_notifier local_heap_htan, local_blob_htan;
	initialize_heap_table_accumulative_notifier(&local_heap_htan, HTAN_CAPACITY);
	initialize_heap_table_accumulative_notifier(&local_blob_htan, HTAN_CAPACITY);

	// every record inserted by this call starts from this template: xmin = self transaction, xmax NULL
	uint32_t inited_heap_record_size = get_minimum_tuple_size(partition_tuple_def);
	void* inited_heap_record = malloc(inited_heap_record_size);
	init_tuple(partition_tuple_def, inited_heap_record);
	{
		tuple_def mvcc_def;
		initialize_tuple_def(&mvcc_def, (data_type_info*)(partition_tuple_def->type_info->containees[0].al.type_info));

		mvcc_header header = (mvcc_header){0};
		header.is_xmin_NULL = 0;
		header.xmin = (transaction_id_with_hints){.is_committed = 0, .is_aborted = 0, .transaction_id = tx->snapshot->self_transaction_id};
		header.is_xmax_NULL = 1;

		char mvcc_header_serialized[sizeof(mvcc_header)];
		init_tuple(&mvcc_def, mvcc_header_serialized);
		write_mvcc_header(mvcc_header_serialized, &mvcc_def, &header);
		set_element_in_tuple(partition_tuple_def, STATIC_POSITION(0), inited_heap_record, &(datum){.tuple_value = mvcc_header_serialized}, UINT32_MAX);
	}

	void* heap_record_without_extensions = malloc(engine->pam_p->pas.page_size);
	void* heap_record_with_extensions = malloc(engine->pam_p->pas.page_size);
	extended_column_data* ext_col_data = malloc(sizeof(extended_column_data) * partition_tuple_def->type_info->element_count);
	uint32_t ext_col_data_size = 0;

	// smallest required_space at which finding a free-enough page has already failed this call
	uint32_t will_fail_finding_free_enough_page_on_required_space = engine->pam_p->pas.page_size;

	// one page is filled to the brim before the next, so the locked heap page and the borrowed-latch count
	// persist across rows and the page is released only once, after the loop (declaring them per row would
	// leak the last page's latch and deadlock scans) -- this mirrors the master operator
	persistent_page ppage = get_NULL_persistent_page(engine->pam_p);
	uint32_t possible_insertion_index = 0;
	int is_ppage_self_created_new_page = 0;
	uint32_t ppage_unused_space_in_entry = 0;
	uint64_t page_latches_to_be_borrowed = 0;

	uint64_t read_offset = 0;
	while(mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), read_offset, &(input_tuple_def->size_def), INTERIM_STORE_MMAP_BATCH))
	{
		const void* input_tuple = its_p->embed_regions[0].tuple;

		if(!build_heap_record_without_extensions(partition_tuple_def, input_tuple_def, insertion_from_source_positional_accessors, column_kinds, tx, input_tuple, inited_heap_record, inited_heap_record_size, heap_record_without_extensions, ext_col_data, &ext_col_data_size))
		{
			(*dml_error) = DML_INSERT_MATERIALIZATION_FAILED;
			goto DONE;
		}

		tuple_pointer tptr = get_NULL_tuple_pointer(&(engine->pam_p->pas));

		int should_retry = 1;
		while(should_retry)
		{
			int abort_error = 0;
			void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

			build_heap_record_with_prefix_bytes(partition_tuple_def, insertion_table_partition, tx, heap_record_without_extensions, heap_record_with_extensions, ext_col_data, ext_col_data_size, &local_blob_htan, min_tx_id, &abort_error, &should_retry);
			if(abort_error)
				goto ABORT_ERROR;

			if(get_tuple_size(partition_tuple_def, heap_record_with_extensions) > (PAGE_FILL_PER_TUPLE * engine->pam_p->pas.page_size))
			{
				abort_error = -5001;
				engine->mark_sub_transaction_aborted(engine->context, min_tx_id, abort_error);
				should_retry = 0;
				(*dml_error) = DML_INSERT_RECORD_TOO_BIG;
				goto ABORT_ERROR;
			}

			int is_new_page = 0;
			while(1) // keep finding a page until the record is inserted
			{
				is_new_page = 0;
				while(is_persistent_page_NULL(&ppage, engine->pam_p))
				{
					uint32_t required_space = get_space_to_be_occupied_by_tuple_on_persistent_page(engine->pam_p->pas.page_size, &(partition_tuple_def->size_def), heap_record_with_extensions);
					if(required_space < will_fail_finding_free_enough_page_on_required_space)
					{
						ppage = find_heap_page_with_enough_unused_space_from_heap_table(insertion_table_partition->heap_root_page_id, required_space, &ppage_unused_space_in_entry, &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&local_heap_htan), &httd, engine->pam_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
						if(is_persistent_page_NULL(&ppage, engine->pam_p))
							will_fail_finding_free_enough_page_on_required_space = min(required_space, will_fail_finding_free_enough_page_on_required_space);
						else
						{
							local_heap_htan.locked_page_ids[local_heap_htan.locked_page_ids_count++] = ppage.page_id;
							remove_from_heap_table_accumulative_notifier(&local_heap_htan, ppage.page_id);
						}
					}
					if(is_persistent_page_NULL(&ppage, engine->pam_p))
					{
						ppage = get_new_heap_page_with_write_lock(&(engine->pam_p->pas), partition_tuple_def, engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
						is_new_page = 1;
						local_heap_htan.locked_page_ids[local_heap_htan.locked_page_ids_count++] = ppage.page_id;
					}

					possible_insertion_index = 0;
					is_ppage_self_created_new_page = is_new_page;
					break;
				}

				uint32_t tuple_index = insert_in_heap_page(&ppage, heap_record_with_extensions, &possible_insertion_index, partition_tuple_def, &(engine->pam_p->pas), engine->pmm_p, min_tx_id, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;
				if(tuple_index != INVALID_TUPLE_INDEX)
				{
					tptr = (tuple_pointer){.page_id = ppage.page_id, .tuple_index = tuple_index};
					break;
				}

				// the page we picked could not hold the record; return its free-space entry for fixing and retry
				if(!is_new_page && is_ppage_self_created_new_page && ppage_unused_space_in_entry != get_unused_space_on_heap_page(&ppage, &(engine->pam_p->pas), partition_tuple_def))
					push_to_heap_table_accumulative_notifier(&local_heap_htan, insertion_table_partition->heap_root_page_id, ppage_unused_space_in_entry, ppage.page_id);

				release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;
				local_heap_htan.locked_page_ids_count = 0;
				possible_insertion_index = 0;
				is_ppage_self_created_new_page = 0;
				ppage_unused_space_in_entry = 0;
			}

			if(is_new_page)
			{
				ppage_unused_space_in_entry = get_unused_space_on_heap_page(&ppage, &(engine->pam_p->pas), partition_tuple_def);
				track_unused_space_in_heap_table(insertion_table_partition->heap_root_page_id, &ppage, &httd, engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;
			}

			// if nothing more can fit, release the page now so concurrent writers do not stall on it
			if(get_minimum_tuple_size(partition_tuple_def) > get_unused_space_on_heap_page(&ppage, &(engine->pam_p->pas), partition_tuple_def))
			{
				if(!is_new_page && is_ppage_self_created_new_page && ppage_unused_space_in_entry != get_unused_space_on_heap_page(&ppage, &(engine->pam_p->pas), partition_tuple_def))
					push_to_heap_table_accumulative_notifier(&local_heap_htan, insertion_table_partition->heap_root_page_id, ppage_unused_space_in_entry, ppage.page_id);

				release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;
				local_heap_htan.locked_page_ids_count = 0;
				possible_insertion_index = 0;
				is_ppage_self_created_new_page = 0;
				ppage_unused_space_in_entry = 0;
			}

			engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);

			if(IS_SAVEPOINT_LOGGING_ENABLED(additional_flags))
				log_to_savepoint_log(tx, INSERTION_SAVEPOINT_LOG, insertion_table_partition->table_id, insertion_table_partition->partition_id, tptr);

			break;

			ABORT_ERROR:

			if(!is_persistent_page_NULL(&ppage, engine->pam_p))
				release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
			local_heap_htan.locked_page_ids_count = 0;
			possible_insertion_index = 0;
			is_ppage_self_created_new_page = 0;
			ppage_unused_space_in_entry = 0;

			engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);

			if(should_retry == 0)
			{
				for(uint32_t i = 0; i < ext_col_data_size; i++)
					deinit_extended_column_data(&(ext_col_data[i]));
				ext_col_data_size = 0;
				if((*dml_error) == DML_SUCCESS)
					(*dml_error) = DML_INSERT_FAILED;
				goto DONE;
			}
		}

		fix_unused_space_entries_after_insertions(engine, &local_heap_htan, &httd, 0);
		fix_unused_space_entries_after_insertions(engine, &local_blob_htan, &(engine->bstd.httd), 0);

		// stream the leftover bytes/digits of each extended column past the inlined prefix
		for(uint32_t k = 0; k < ext_col_data_size; k++)
		{
			const extended_column_data* e = &(ext_col_data[k]);

			if(e->written_size == e->total_size)
			{
				deinit_extended_column_data((extended_column_data*)e);
				continue;
			}
			if(e->written_size > e->total_size)
			{
				printf("BUG (dml_insert) :: streamed more than the column's total size\n");
				exit(-1);
			}

			while(1)
			{
				uint64_t page_latches_to_be_borrowed_leftover = 0;
				int abort_error = 0;
				void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed_leftover);

				if(e->is_numeric)
				{
					digit_write_iterator* it = get_new_digit_write_iterator(heap_record_with_extensions, partition_tuple_def, STATIC_POSITION(e->index), insertion_table_partition->blobs_root_page_id, e->tail_chunk_pointer, e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
					uint64_t curr_written_size = e->written_size;
					while(curr_written_size < e->total_size)
					{
						cy_uint next_digits_count = 0;
						const uint64_t* next_digits = peek_all_contiguous_from_front_in_digits_list(&(e->digits), curr_written_size, &next_digits_count);

						uint32_t wrote = append_to_digit_write_iterator(it, next_digits, min(min(e->total_size - curr_written_size, next_digits_count), UINT32_MAX), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&local_blob_htan), min_tx_id, &abort_error);
						if(abort_error)
						{
							delete_digit_write_iterator(it, min_tx_id, &abort_error);
							goto ABORT_ERROR_LEFTOVER;
						}
						curr_written_size += wrote;
						if(wrote == 0)
							break;
					}
					delete_digit_write_iterator(it, min_tx_id, &abort_error);
					if(abort_error)
						goto ABORT_ERROR_LEFTOVER;
				}
				else
				{
					binary_write_iterator* it = get_new_binary_write_iterator(heap_record_with_extensions, partition_tuple_def, STATIC_POSITION(e->index), insertion_table_partition->blobs_root_page_id, e->tail_chunk_pointer, e->prefix_size, &(engine->bstd), engine->pam_p, engine->pmm_p);
					uint64_t curr_written_size = e->written_size;
					while(curr_written_size < e->total_size)
					{
						uint32_t wrote = append_to_binary_write_iterator(it, (const char*)e->value + curr_written_size, min(e->total_size - curr_written_size, UINT32_MAX), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&local_blob_htan), min_tx_id, &abort_error);
						if(abort_error)
						{
							delete_binary_write_iterator(it, min_tx_id, &abort_error);
							goto ABORT_ERROR_LEFTOVER;
						}
						curr_written_size += wrote;
						if(wrote == 0)
							break;
					}
					delete_binary_write_iterator(it, min_tx_id, &abort_error);
					if(abort_error)
						goto ABORT_ERROR_LEFTOVER;
				}

				engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed_leftover);
				break;

				ABORT_ERROR_LEFTOVER:;
				engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed_leftover);
			}

			fix_unused_space_entries_after_insertions(engine, &local_blob_htan, &(engine->bstd.httd), 0);
			deinit_extended_column_data((extended_column_data*)e);
		}
		ext_col_data_size = 0;

		inserted_count++;

		if(do_output)
			append_output_tuple(tx, output_tuple_def, ftabl, tptr, heap_record_with_extensions, output_flags, ots_p);

		read_offset = next_tuple_offset_for_interim_tuple_region(&(its_p->embed_regions[0]));
	}

	// release the page kept locked for reuse across rows
	if(!is_persistent_page_NULL(&ppage, engine->pam_p))
	{
		int abort_error = 0;
		release_lock_on_persistent_page(engine->pam_p, NULL, &ppage, NONE_OPTION, &abort_error);
		if(page_latches_to_be_borrowed > 0)
			page_latches_to_be_borrowed--;
	}

	DONE:;

	free(column_kinds);
	deinitialize_heap_table_accumulative_notifier(&local_heap_htan);
	deinitialize_heap_table_accumulative_notifier(&local_blob_htan);
	deinit_heap_table_tuple_definitions(&httd);
	free(inited_heap_record);
	free(heap_record_without_extensions);
	free(heap_record_with_extensions);
	for(uint32_t k = 0; k < ext_col_data_size; k++) // non-empty only on an error exit mid-row
		deinit_extended_column_data(&(ext_col_data[k]));
	free(ext_col_data);
	if(output_tuple_def != NULL)
		destroy_scan_dml_output_tuple_def(output_tuple_def);

	unmap_all_embed_regions_in_interim_tuple_store(its_p);
	if(do_output)
		unmap_all_embed_regions_in_interim_tuple_store(ots_p);

	return inserted_count;
}