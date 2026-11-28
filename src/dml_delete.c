#include<rhendb/dml.h>

#include<rhendb/mvcc_header.h>
#include<rhendb/scan_dml_output_type.h>

#include<tupleindexer/heap_page/heap_page.h>

#include<cutlery/value_arraylist.h>
#include<cutlery/comparator_interface.h>

#include<stdlib.h>

// rows buffered before a batch is sorted and marked deleted; same-page deletions in a batch share a mini tx
#define DELETE_BATCH_SIZE        (4096ULL)

// bytes kept mapped at a time over an interim_tuple_store cursor
#define INTERIM_STORE_MMAP_BATCH (1u << 20)

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

// orders by partition_id, then page_id, then tuple_index, so all deletions on a page are contiguous
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

// builds the flag-driven output row (table_id, partition_id, tuple_pointer, heap tuple -- the same shape the
// scan and pointer_lookup operators produce) for a just-deleted tuple and appends it to ots_p
static void append_output_tuple(transaction* tx, const tuple_def* output_tuple_def, const fetched_table* ftabl, uint64_t partition_index_in_info, const pending_deletion* pd, const void* heap_record, int output_flags, interim_tuple_store* ots_p)
{
	void* output_tuple = project_to_scan_dml_output_tuple_def(tx->rdb, output_tuple_def, ftabl, partition_index_in_info, pd->tptr, heap_record, output_flags);

	append_tuple_to_interim_tuple_store2(ots_p, &(ots_p->embed_regions[0]), output_tuple, &(output_tuple_def->size_def), INTERIM_STORE_MMAP_BATCH);

	free(output_tuple);
}

// sorts the buffer, then marks every buffered tuple deleted one page at a time (output rows, if any, are
// appended to ots_p). returns 0 (and sets *dml_error) if a buffered row does not resolve to a live tuple.
static int mark_buffered_tuples_deleted(const fetched_table* ftabl, pending_deletions* to_be_deleted, transaction* tx, int output_flags, const tuple_def* output_tuple_def, int additional_flags, interim_tuple_store* ots_p, uint64_t* deleted_count, int* dml_error)
{
	rage_engine* engine = &(tx->rdb->persistent_acid_rage_engine);

	cy_uint to_be_deleted_count = get_element_count_pending_deletions(to_be_deleted);
	if(to_be_deleted_count == 0)
		return 1;

	if(!merge_sort_pending_deletions(to_be_deleted, 0, to_be_deleted_count - 1, &simple_comparator(compare_pending_deletions_by_partition_id_and_tuple_pointer), STD_C_mem_allocator))
		exit(-1);

	cy_uint i = 0;
	uint64_t p = 0; // partition cursor, kept in step with the sorted partition_id order

	while(i < to_be_deleted_count)
	{
		uint64_t partition_id = get_from_front_of_pending_deletions(to_be_deleted, i)->partition_id;

		while(p < ftabl->partitions_count && ftabl->table_partitions_info[p].partition_id < partition_id)
			p++;
		if(p == ftabl->partitions_count || ftabl->table_partitions_info[p].partition_id != partition_id)
		{
			(*dml_error) = DML_DELETE_MISSING_PARTITION;
			return 0;
		}

		const tuple_def* partition_tuple_def = &(ftabl->table_partition_tuple_defs[p]);

		tuple_def mvcc_def; // the mvcc_header is always element 0 of a partition record
		initialize_tuple_def(&mvcc_def, (data_type_info*)(partition_tuple_def->type_info->containees[0].al.type_info));

		while(i < to_be_deleted_count && get_from_front_of_pending_deletions(to_be_deleted, i)->partition_id == partition_id)
		{
			uint64_t page_id = get_from_front_of_pending_deletions(to_be_deleted, i)->tptr.page_id;

			// captured before the acquire so the abort path (i = page_first_index) is always valid
			cy_uint page_first_index = i;

			uint64_t page_latches_to_be_borrowed = 0;
			int abort_error = 0;
			void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

			persistent_page ppage = acquire_persistent_page_with_lock(engine->pam_p, min_tx_id, page_id, WRITE_LOCK, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			while(i < to_be_deleted_count)
			{
				const pending_deletion* pd = get_from_front_of_pending_deletions(to_be_deleted, i);
				if(pd->partition_id != partition_id || pd->tptr.page_id != page_id)
					break;

				const void* record = get_nth_tuple_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(partition_tuple_def->size_def), pd->tptr.tuple_index);
				if(record == NULL)
				{
					release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
					engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);
					(*dml_error) = DML_DELETE_MISSING_TUPLE;
					return 0;
				}

				// mark deleted by writing our transaction id into a non-NULL xmax on the mvcc_header in place
				const data_type_info* dti_temp;
				datum mvcc_hdr_datum;
				get_value_from_element_from_tuple(&mvcc_hdr_datum, &dti_temp, partition_tuple_def, STATIC_POSITION(0), record);

				mvcc_header header;
				read_mvcc_header(&header, mvcc_hdr_datum.tuple_value, &mvcc_def);
				header.is_xmax_NULL = 0;
				header.xmax = (transaction_id_with_hints){.is_committed = 0, .is_aborted = 0, .transaction_id = tx->snapshot->self_transaction_id};

				char mvcc_header_serialized[sizeof(mvcc_header)];
				init_tuple(&mvcc_def, mvcc_header_serialized);
				write_mvcc_header(mvcc_header_serialized, &mvcc_def, &header);
				set_element_in_tuple_in_place_on_persistent_page(engine->pmm_p, min_tx_id, &ppage, engine->pam_p->pas.page_size, partition_tuple_def, pd->tptr.tuple_index, STATIC_POSITION(0), &((datum){.tuple_value = mvcc_header_serialized}), &abort_error);
				if(abort_error)
					goto ABORT_ERROR;

				i++;
			}

			engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);

			(*deleted_count) += (i - page_first_index);

			if(IS_SAVEPOINT_LOGGING_ENABLED(additional_flags))
			{
				for(cy_uint j = page_first_index; j < i; j++)
				{
					const pending_deletion* pd = get_from_front_of_pending_deletions(to_be_deleted, j);
					log_to_savepoint_log(tx, DELETION_SAVEPOINT_LOG, ftabl->table_info.id, pd->partition_id, pd->tptr);
				}
			}

			// emit outputs only after the commit; ppage stays read-valid while we still hold its latch
			if(ots_p != NULL)
			{
				for(cy_uint j = page_first_index; j < i; j++)
				{
					const pending_deletion* pd = get_from_front_of_pending_deletions(to_be_deleted, j);
					const void* record = get_nth_tuple_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(partition_tuple_def->size_def), pd->tptr.tuple_index);
					append_output_tuple(tx, output_tuple_def, ftabl, p, pd, record, output_flags, ots_p);
				}
			}

			// releasing the latch after the mini tx has completed is allowed while FREE_PAGE is not requested
			release_lock_on_persistent_page(engine->pam_p, NULL, &ppage, NONE_OPTION, &abort_error);
			continue;

			ABORT_ERROR:;
			if(!is_persistent_page_NULL(&ppage, engine->pam_p))
				release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
			engine->complete_sub_transaction(engine->context, min_tx_id, 0, NULL, 0, &page_latches_to_be_borrowed);

			i = page_first_index; // retry this page from its first pending_deletion
		}
	}

	remove_all_from_pending_deletions(to_be_deleted);
	return 1;
}

uint64_t dml_delete(
	interim_tuple_store* ots_p,
	interim_tuple_store* its_p,
	const tuple_def* input_tuple_def,
	const positional_accessor* partition_id_from_source_positional_accessor,
	const positional_accessor* tuple_pointer_from_source_positional_accessor,
	const fetched_table* ftabl,
	transaction* tx,
	int output_flags,
	int additional_flags,
	int* dml_error)
{
	(*dml_error) = DML_SUCCESS;
	uint64_t deleted_count = 0;

	if(tx->snapshot == NULL || !(tx->snapshot->has_self_transaction_id))
	{
		(*dml_error) = DML_DELETE_FAILED;
		return 0;
	}

	rage_engine* engine = &(tx->rdb->persistent_acid_rage_engine);

	int do_output = (ots_p != NULL) && (output_flags != 0);

	// only open the interim stores (the caller still owns them); the embed regions are unmapped at the end
	open_file_for_interim_tuple_store(its_p);
	if(do_output)
		open_file_for_interim_tuple_store(ots_p);

	// the output row shape, built once; NULL when nothing is produced
	tuple_def* output_tuple_def = NULL;
	if(do_output)
		output_tuple_def = get_scan_dml_output_tuple_def(tx->rdb, ftabl, output_flags);

	pending_deletions to_be_deleted;
	if(!initialize_pending_deletions(&to_be_deleted, DELETE_BATCH_SIZE))
		exit(-1);

	uint64_t read_offset = 0;
	while(mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), read_offset, &(input_tuple_def->size_def), INTERIM_STORE_MMAP_BATCH))
	{
		const void* input_tuple = its_p->embed_regions[0].tuple;

		const data_type_info* dti_temp;

		datum partition_id_datum;
		if(!get_value_from_element_from_tuple(&partition_id_datum, &dti_temp, input_tuple_def, (*partition_id_from_source_positional_accessor), input_tuple))
		{
			(*dml_error) = DML_DELETE_NULL_PARTITION_ID;
			goto DONE;
		}

		datum tuple_pointer_datum;
		if(!get_value_from_element_from_tuple(&tuple_pointer_datum, &dti_temp, input_tuple_def, (*tuple_pointer_from_source_positional_accessor), input_tuple))
		{
			(*dml_error) = DML_DELETE_NULL_TUPLE_POINTER;
			goto DONE;
		}

		pending_deletion pd = {.partition_id = partition_id_datum.uint_value, .tptr = get_tuple_pointer(tuple_pointer_datum.tuple_value, &(engine->pam_p->pas))};
		if(!push_back_to_pending_deletions(&to_be_deleted, &pd))
			exit(-1);

		if(get_element_count_pending_deletions(&to_be_deleted) >= DELETE_BATCH_SIZE)
		{
			if(!mark_buffered_tuples_deleted(ftabl, &to_be_deleted, tx, output_flags, output_tuple_def, additional_flags, do_output ? ots_p : NULL, &deleted_count, dml_error))
				goto DONE;
		}

		read_offset = next_tuple_offset_for_interim_tuple_region(&(its_p->embed_regions[0]));
	}

	mark_buffered_tuples_deleted(ftabl, &to_be_deleted, tx, output_flags, output_tuple_def, additional_flags, do_output ? ots_p : NULL, &deleted_count, dml_error); // final partial batch

	DONE:;

	deinitialize_pending_deletions(&to_be_deleted);
	if(output_tuple_def != NULL)
		destroy_scan_dml_output_tuple_def(output_tuple_def, output_flags);

	unmap_all_embed_regions_in_interim_tuple_store(its_p);
	if(do_output)
		unmap_all_embed_regions_in_interim_tuple_store(ots_p);

	return deleted_count;
}