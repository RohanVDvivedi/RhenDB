#include<rhendb/rage_engine_min_tx_engine.h>
#include<mintxengine/mini_transaction_engine.h>
#include<tupleindexer/interface/page_access_methods.h>
#include<tupleindexer/interface/page_modification_methods.h>

static void* get_new_page_with_write_lock_mtx(void* context, const void* transaction_id, uint64_t* page_id_returned, int* abort_error)
{
	void* result = get_new_page_with_write_latch_for_mini_tx(context, (void*)transaction_id, page_id_returned);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	if(result == NULL && (*abort_error) == 0)
	{
		printf("Bug in get_new_page_with_write_lock_mtx, result failed but abort error not set\n");
		exit(-1);
	}
	return result;
}
static void* acquire_page_with_reader_lock_mtx(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	void* result = acquire_page_with_reader_latch_for_mini_tx(context, (void*)transaction_id, page_id);
	if(transaction_id != NULL)
	{
		(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
		if(result == NULL && (*abort_error) == 0)
		{
			printf("Bug in acquire_page_with_reader_lock_mtx, result failed but abort error not set\n");
			exit(-1);
		}
	}
	else // transaction_id can be NULL for this function, if so set abort_error if the function call failed
	{
		if(result == NULL)
			(*abort_error) = -100;
	}
	return result;
}
static void* acquire_page_with_writer_lock_mtx(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	void* result = acquire_page_with_writer_latch_for_mini_tx(context, (void*)transaction_id, page_id);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	if(result == NULL && (*abort_error) == 0)
	{
		printf("Bug in acquire_page_with_writer_lock_mtx, result failed but abort error not set\n");
		exit(-1);
	}
	return result;
}
static int downgrade_writer_lock_to_reader_lock_on_page_mtx(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	int result = downgrade_writer_latch_to_reader_latch_on_page_for_mini_tx(context, (void*)transaction_id, pg_ptr);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	if(result == 0 && (*abort_error) == 0)
	{
		printf("Bug in downgrade_writer_lock_to_reader_lock_on_page_mtx, result failed but abort error not set\n");
		exit(-1);
	}
	return result;
}
static int upgrade_reader_lock_to_writer_lock_on_page_mtx(void* context, const void* transaction_id, void* pg_ptr, int* abort_error)
{
	int result = upgrade_reader_latch_to_writer_latch_on_page_for_mini_tx(context, (void*)transaction_id, pg_ptr);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	if(result == 0 && (*abort_error) == 0)
	{
		printf("Bug in upgrade_reader_lock_to_writer_lock_on_page_mtx, result failed but abort error not set\n");
		exit(-1);
	}
	return result;
}
static int release_reader_lock_on_page_mtx(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	int result = release_reader_latch_on_page_for_mini_tx(context, (void*)transaction_id, pg_ptr, !!(opts & FREE_PAGE));
	if(transaction_id != NULL)
	{
		(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
		if(result == 0 && (*abort_error) == 0)
		{
			printf("Bug in release_reader_lock_on_page_mtx, result failed but abort error not set\n");
			exit(-1);
		}
	}
	else // transaction_id can be NULL for this function, if so set abort_error if the function call failed
	{
		if(result == 0)
			(*abort_error) = -100;
	}
	return result;
}
static int release_writer_lock_on_page_mtx(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	int result = release_writer_latch_on_page_for_mini_tx(context, (void*)transaction_id, pg_ptr, !!(opts & FREE_PAGE));
	if(transaction_id != NULL)
	{
		(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
		if(result == 0 && (*abort_error) == 0)
		{
			printf("Bug in release_writer_lock_on_page_mtx, result failed but abort error not set\n");
			exit(-1);
		}
	}
	else // transaction_id can be NULL for this function, if so set abort_error if the function call failed
	{
		if(result == 0)
			(*abort_error) = -100;
	}
	return result;
}
static int free_page_mtx(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	int result = free_page_for_mini_tx(context, (void*)transaction_id, page_id);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	if(result == 0 && (*abort_error) == 0)
	{
		printf("Bug in free_page_mtx, result failed but abort error not set\n");
		exit(-1);
	}
	return result;
}

static void initialize_pam_for_mte(page_access_methods* pam_p, mini_transaction_engine* mte)
{
	pam_p->get_new_page_with_write_lock = get_new_page_with_write_lock_mtx;
	pam_p->acquire_page_with_reader_lock = acquire_page_with_reader_lock_mtx;
	pam_p->acquire_page_with_writer_lock = acquire_page_with_writer_lock_mtx;
	pam_p->downgrade_writer_lock_to_reader_lock_on_page = downgrade_writer_lock_to_reader_lock_on_page_mtx;
	pam_p->upgrade_reader_lock_to_writer_lock_on_page = upgrade_reader_lock_to_writer_lock_on_page_mtx;
	pam_p->release_reader_lock_on_page = release_reader_lock_on_page_mtx;
	pam_p->release_writer_lock_on_page = release_writer_lock_on_page_mtx;
	pam_p->free_page = free_page_mtx;
	pam_p->pas = (page_access_specs){};
	pam_p->context = mte;

	if(!initialize_page_access_specs(&(pam_p->pas), mte->user_stats.page_id_width, mte->user_stats.page_size, mte->user_stats.NULL_PAGE_ID))
		exit(-1);
}

static int init_page_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, uint32_t page_header_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	int result = init_page_for_mini_tx(context, (void*)transaction_id, page, page_header_size, tpl_sz_d);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return result;
}
static void set_page_header_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, const void* hdr, int* abort_error)
{
	set_page_header_for_mini_tx(context, (void*)transaction_id, page, hdr);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return ;
}
static int append_tuple_on_page_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple, int* abort_error)
{
	int result = append_tuple_on_page_for_mini_tx(context, (void*)transaction_id, page, tpl_sz_d, external_tuple);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return result;
}
static int insert_tuple_on_page_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error)
{
	int result = insert_tuple_on_page_for_mini_tx(context, (void*)transaction_id, page, tpl_sz_d, index, external_tuple);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return result;
}
static int update_tuple_on_page_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error)
{
	int result = update_tuple_on_page_for_mini_tx(context, (void*)transaction_id, page, tpl_sz_d, index, external_tuple);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return result;
}
static int discard_tuple_on_page_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, int* abort_error)
{
	int result = discard_tuple_on_page_for_mini_tx(context, (void*)transaction_id, page, tpl_sz_d, index);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return result;
}
static void discard_all_tuples_on_page_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	discard_all_tuples_on_page_for_mini_tx(context, (void*)transaction_id, page, tpl_sz_d);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return ;
}
static uint32_t discard_trailing_tomb_stones_on_page_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	uint32_t result = discard_trailing_tomb_stones_on_page_for_mini_tx(context, (void*)transaction_id, page, tpl_sz_d);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return result;
}
static int swap_tuples_on_page_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2, int* abort_error)
{
	int result = swap_tuples_on_page_for_mini_tx(context, (void*)transaction_id, page, tpl_sz_d, i1, i2);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return result;
}
static int set_element_in_tuple_in_place_on_page_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, const tuple_def* tpl_d, uint32_t tuple_index, positional_accessor element_index, const datum* value, int* abort_error)
{
	int result = set_element_in_tuple_in_place_on_page_for_mini_tx(context, (void*)transaction_id, page, tpl_d, tuple_index, element_index, value);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return result;
}
static void clone_page_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* page_src, int* abort_error)
{
	clone_page_for_mini_tx(context, (void*)transaction_id, page, tpl_sz_d, page_src);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return ;
}
static int run_page_compaction_mtx(void* context, const void* transaction_id, void* page, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	int result = run_page_compaction_for_mini_tx(context, (void*)transaction_id, page, tpl_sz_d);
	(*abort_error) = get_abort_error_for_mini_tx(context, (void*)transaction_id);
	return result;
}

static void initialize_pmm_for_mte(page_modification_methods* pmm_p, mini_transaction_engine* mte)
{
	pmm_p->init_page = init_page_mtx;
	pmm_p->set_page_header = set_page_header_mtx;
	pmm_p->append_tuple_on_page = append_tuple_on_page_mtx;
	pmm_p->insert_tuple_on_page = insert_tuple_on_page_mtx;
	pmm_p->update_tuple_on_page = update_tuple_on_page_mtx;
	pmm_p->discard_tuple_on_page = discard_tuple_on_page_mtx;
	pmm_p->discard_all_tuples_on_page = discard_all_tuples_on_page_mtx;
	pmm_p->discard_trailing_tomb_stones_on_page = discard_trailing_tomb_stones_on_page_mtx;
	pmm_p->swap_tuples_on_page = swap_tuples_on_page_mtx;
	pmm_p->set_element_in_tuple_in_place_on_page = set_element_in_tuple_in_place_on_page_mtx;
	pmm_p->clone_page = clone_page_mtx;
	pmm_p->run_page_compaction = run_page_compaction_mtx;
	pmm_p->context = mte;
}

rage_engine get_rage_engine_for_min_tx_engine(const char* database_file_name, uint32_t page_size, uint32_t page_id_width, uint32_t log_sequence_number_width, uint64_t bufferpool_frame_count, uint64_t wale_append_only_buffer_block_count, uint64_t latch_wait_timeout_in_microseconds, uint64_t write_lock_wait_timeout_in_microseconds, uint64_t checkpointing_period_in_microseconds, uint64_t checkpointing_LSN_diff_in_bytes, uint64_t max_wal_file_size_in_bytes)
{
	rage_engine e = {};

	// allocate/reset all the three components

	e.context = malloc(sizeof(mini_transaction_engine));
	if(e.context == NULL)
		exit(-1);

	e.pam_p = malloc(sizeof(page_access_methods));
	if(e.pam_p == NULL)
		exit(-1);

	e.pmm_p = malloc(sizeof(page_modification_methods));
	if(e.pmm_p == NULL)
		exit(-1);

	// initialize all the three components

	if(!initialize_mini_transaction_engine(((mini_transaction_engine*)(e.context)), database_file_name, page_size, page_id_width, log_sequence_number_width, bufferpool_frame_count, wale_append_only_buffer_block_count, latch_wait_timeout_in_microseconds, write_lock_wait_timeout_in_microseconds, checkpointing_period_in_microseconds, checkpointing_LSN_diff_in_bytes, max_wal_file_size_in_bytes))
	{
		printf("FAILED to initialize persistent store\n");
		exit(-1);
	}

	initialize_pam_for_mte(e.pam_p, ((mini_transaction_engine*)(e.context)));

	initialize_pmm_for_mte(e.pmm_p, ((mini_transaction_engine*)(e.context)));

	e.allot_new_sub_transaction_id = (void* (*)(void*, uint64_t))mte_allot_mini_tx;

	e.complete_sub_transaction = (uint256 (*)(void*, void*, int, const void*, uint32_t, uint64_t*))mte_complete_mini_tx;

	e.mark_sub_transaction_aborted = (int (*)(void*, void*, int))mark_aborted_for_mini_tx;

	return e;
}