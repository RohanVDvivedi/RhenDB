#include<beedb.h>

#include<persistent_store_handlers.h>
#include<volatile_store_handlers.h>

void initialize_beedb(beedb* bdb, const char* database_file_name,
		uint32_t page_id_width,
		uint32_t page_size_mte, uint32_t lsn_width, uint64_t bufferpool_frame_count, uint64_t wale_buffer_count,
			uint64_t page_latch_wait_us, uint64_t page_lock_wait_us,
			uint64_t checkpoint_period_us,
		uint32_t page_size_vps,
			uint64_t truncator_period_us)
{
	bdb->database_file_name = database_file_name;

	if(!initialize_mini_transaction_engine(&(bdb->mte), bdb->database_file_name, page_size_mte, page_id_width, lsn_width, bufferpool_frame_count, wale_buffer_count, page_latch_wait_us, page_lock_wait_us, checkpoint_period_us, 2 * 1000000, 200 * 1000000))
	{
		printf("FAILED to initialize persistent store\n");
		exit(-1);
	}

	initialize_pam_for_mte(&(bdb->mte_pam), &(bdb->mte));
	initialize_pmm_for_mte(&(bdb->mte_pmm), &(bdb->mte));

	if(!initialize_volatile_page_store(&(bdb->vps), ".", page_size_vps, page_id_width, truncator_period_us))
	{
		printf("FAILED to initialize volatile store\n");
		exit(-1);
	}

	initialize_pam_for_vps(&(bdb->vps_pam), &(bdb->vps));
	initialize_pmm_for_vps(&(bdb->vps_pmm), &(bdb->vps));
}

void deinitialize_beedb(beedb* bdb)
{
	deinitialize_mini_transaction_engine(&(bdb->mte));
	deinitialize_volatile_page_store(&(bdb->vps));
}