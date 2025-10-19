#ifndef RHENDB_H
#define RHENDB_H

#include<boompar/resource_usage_limiter.h>
#include<boompar/executor.h>

#include<rhendb/rage_engine.h>

#include<stdint.h>

typedef struct rhendb rhendb;
struct rhendb
{
	// cached threadpool for disk accessor operators like scan and writers to tables
	// max thread pool size = max job queue size = max_concurrent_users_count * 10
	executor* cached_thread_pool;

	// fixed sized theadpool for non io operators
	executor* compute_thread_pool;

	rage_engine persistent_acid_rage_engine;

	// same as bufferpool size in persistent_acid_rage_engine
	resource_usage_limiter* bufferpool_usage_limiter;

	rage_engine volatile_rage_engine;
};

void initialize_rhendb(rhendb* rdb, const char* database_file_name,
		uint32_t page_id_width,
		uint32_t page_size_mte, uint32_t lsn_width, uint64_t bufferpool_frame_count, uint64_t wale_buffer_count,
			uint64_t page_latch_wait_us, uint64_t page_lock_wait_us,
			uint64_t checkpoint_period_us,
		uint32_t page_size_vps,
			uint64_t truncator_period_us,
		uint64_t max_concurrent_users_count);

void deinitialize_rhendb(rhendb* rdb);

#endif