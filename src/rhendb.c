#include<rhendb/rhendb.h>

#include<rhendb/rage_engine_min_tx_engine.h>
#include<rhendb/rage_engine_volatile_page_store.h>

#include<unistd.h>

void initialize_rhendb(rhendb* rdb, const char* database_file_name,
		uint32_t page_id_width,
		uint32_t page_size_mte, uint32_t lsn_width, uint64_t bufferpool_frame_count, uint64_t wale_buffer_count,
			uint64_t page_latch_wait_us, uint64_t page_lock_wait_us,
			uint64_t checkpoint_period_us,
		uint32_t page_size_vps,
			uint64_t truncator_period_us,
		uint64_t max_concurrent_users_count)
{
	// 10 times the user count, will be size of queue and same will be the number of threads, wait 1 second before you kill the thread
	rdb->cached_thread_pool = new_executor(CACHED_THREAD_POOL_EXECUTOR, max_concurrent_users_count * 10, max_concurrent_users_count * 10, 1000000ULL, NULL, NULL, NULL);

	// initialize the compute thread pool with as many threads as the hardware has and with the same queue size
	rdb->compute_thread_pool = new_executor(FIXED_THREAD_COUNT_EXECUTOR, sysconf(_SC_NPROCESSORS_ONLN), max_concurrent_users_count * 10, 0, NULL, NULL, NULL);

	rdb->bufferpool_usage_limiter = new_resource_usage_limiter(bufferpool_frame_count);

	rdb->persistent_acid_rage_engine = get_rage_engine_for_min_tx_engine(database_file_name, page_size_mte, page_id_width, lsn_width, bufferpool_frame_count, wale_buffer_count, page_latch_wait_us, page_lock_wait_us, checkpoint_period_us, 2 * 1000000, 200 * 1000000);

	rdb->volatile_rage_engine = get_rage_engine_for_volatile_page_store(page_size_vps, page_id_width, truncator_period_us);
}

void deinitialize_rhendb(rhendb* rdb)
{
	shutdown_executor(rdb->cached_thread_pool, 1);
	delete_executor(rdb->cached_thread_pool);

	shutdown_executor(rdb->compute_thread_pool, 1);
	delete_executor(rdb->compute_thread_pool);

	delete_resource_usage_limiter(rdb->bufferpool_usage_limiter, 0);

	deinitialize_mini_transaction_engine((mini_transaction_engine*)(rdb->persistent_acid_rage_engine.context));

	deinitialize_volatile_page_store((volatile_page_store*)(rdb->volatile_rage_engine.context));
}