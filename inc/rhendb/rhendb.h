#ifndef RHENDB_H
#define RHENDB_H

#include<rhendb/rage_engine.h>

#include<stdint.h>

typedef struct rhendb rhendb;
struct rhendb
{
	rage_engine persistent_acid_rage_engine;

	rage_engine volatile_rage_engine;
};

void initialize_rhendb(rhendb* rdb, const char* database_file_name,
		uint32_t page_id_width,
		uint32_t page_size_mte, uint32_t lsn_width, uint64_t bufferpool_frame_count, uint64_t wale_buffer_count,
			uint64_t page_latch_wait_us, uint64_t page_lock_wait_us,
			uint64_t checkpoint_period_us,
		uint32_t page_size_vps,
			uint64_t truncator_period_us);

void deinitialize_rhendb(rhendb* rdb);

#endif