#include<rondb/rondb.h>

#include<rondb/persistent_store_handlers.h>
#include<rondb/volatile_store_handlers.h>

void initialize_rondb(rondb* rdb, const char* database_file_name,
		uint32_t page_id_width,
		uint32_t page_size_mte, uint32_t lsn_width, uint64_t bufferpool_frame_count, uint64_t wale_buffer_count,
			uint64_t page_latch_wait_us, uint64_t page_lock_wait_us,
			uint64_t checkpoint_period_us,
		uint32_t page_size_vps,
			uint64_t truncator_period_us)
{
	rdb->database_file_name = database_file_name;

	if(!initialize_mini_transaction_engine(&(rdb->mte), rdb->database_file_name, page_size_mte, page_id_width, lsn_width, bufferpool_frame_count, wale_buffer_count, page_latch_wait_us, page_lock_wait_us, checkpoint_period_us, 2 * 1000000, 200 * 1000000))
	{
		printf("FAILED to initialize persistent store\n");
		exit(-1);
	}

	initialize_pam_for_mte(&(rdb->mte_pam), &(rdb->mte));
	initialize_pmm_for_mte(&(rdb->mte_pmm), &(rdb->mte));

	if(!initialize_volatile_page_store(&(rdb->vps), ".", page_size_vps, page_id_width, truncator_period_us))
	{
		printf("FAILED to initialize volatile store\n");
		exit(-1);
	}

	initialize_pam_for_vps(&(rdb->vps_pam), &(rdb->vps));
	initialize_pmm_for_vps(&(rdb->vps_pmm), &(rdb->vps));
}

void deinitialize_rondb(rondb* rdb)
{
	deinitialize_mini_transaction_engine(&(rdb->mte));
	deinitialize_volatile_page_store(&(rdb->vps));
}