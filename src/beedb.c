#include<beedb.h>

void init_beedb(beedb* bdb, const char* database_file_name,
		uint32_t page_id_width,
		uint32_t page_size_mtx, uint32_t lsn_width, uint64_t bufferpool_frame_count, uint64_t wale_buffer_count,
			uint64_t page_latch_wait_us, uint64_t page_lock_wait_us,
			uint64_t checkpoint_period_us,
		uint32_t page_size_vps,
			uint64_t truncator_period_in_microseconds)
{
	// TODO
}