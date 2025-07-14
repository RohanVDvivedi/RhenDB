#ifndef RONDB_H
#define RONDB_H

#include<rondb/rage_engine.h>

#include<stdint.h>

typedef struct rondb rondb;
struct rondb
{
	rage_engine persistent_acid_rage_engine;

	rage_engine volatile_rage_engine;
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