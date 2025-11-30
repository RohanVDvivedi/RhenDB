#ifndef RHENDB_H
#define RHENDB_H

#include<rhendb/transaction_table.h>
#include<rhendb/lock_manager.h>

#include<boompar/resource_usage_limiter.h>
#include<boompar/executor.h>

#include<rhendb/rage_engine.h>

#include<stdint.h>

typedef struct rhendb rhendb;
struct rhendb
{
	// prevent from over allocating operator threads
	resource_usage_limiter* operator_thread_pool_usage_limiter;

	// cached threadpool for operators
	// max thread pool size =  max_concurrent_users_count * 10
	executor* operator_thread_pool;

	rage_engine persistent_acid_rage_engine;

	// same as bufferpool size in persistent_acid_rage_engine
	resource_usage_limiter* bufferpool_usage_limiter;

	rage_engine volatile_rage_engine;

	// components of the database the system table structures are here

	// there will be a bplus_tree storing system_table_name -> page_id (it's root) at page_id 1

	// transaction table => stored on the persistent_acid_rage_engine
	transaction_table tx_table;
	transaction_status_getter tsg; // an interface to get the transaction statuses

	// external lock for the lock
	// and the lck_table => stored on the volatile_rage_engine
	pthread_mutex_t lock_manager_external_lock;
	lock_manager lck_table;
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