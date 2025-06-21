#ifndef RONDB_H
#define RONDB_H

#include<mintxengine/mini_transaction_engine.h>
#include<volatilepagestore/volatile_page_store.h>
#include<tupleindexer/interface/page_access_methods.h>
#include<tupleindexer/interface/page_modification_methods.h>

typedef struct rondb rondb;
struct rondb
{
	// path for database file
	const char* database_file_name;

	// attributes to access persistent store
	mini_transaction_engine mte;
	page_access_methods mte_pam;
	page_modification_methods mte_pmm;

	// attributes to access volatile store
	volatile_page_store vps;
	page_access_methods vps_pam;
	page_modification_methods vps_pmm;
};

void initialize_rondb(rondb* rdb, const char* database_file_name,
		uint32_t page_id_width,
		uint32_t page_size_mte, uint32_t lsn_width, uint64_t bufferpool_frame_count, uint64_t wale_buffer_count,
			uint64_t page_latch_wait_us, uint64_t page_lock_wait_us,
			uint64_t checkpoint_period_us,
		uint32_t page_size_vps,
			uint64_t truncator_period_us);

void deinitialize_rondb(rondb* rdb);

#endif