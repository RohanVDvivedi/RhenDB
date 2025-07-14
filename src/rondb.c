#include<rondb/rondb.h>

#include<rondb/rage_engine_min_tx_engine.h>
#include<rondb/rage_engine_volatile_page_store.h>

void initialize_rondb(rondb* rdb, const char* database_file_name,
		uint32_t page_id_width,
		uint32_t page_size_mte, uint32_t lsn_width, uint64_t bufferpool_frame_count, uint64_t wale_buffer_count,
			uint64_t page_latch_wait_us, uint64_t page_lock_wait_us,
			uint64_t checkpoint_period_us,
		uint32_t page_size_vps,
			uint64_t truncator_period_us)
{

	rdb->persistent_acid_rage_engine = get_rage_engine_for_min_tx_engine(database_file_name, page_size_mte, page_id_width, lsn_width, bufferpool_frame_count, wale_buffer_count, page_latch_wait_us, page_lock_wait_us, checkpoint_period_us, 2 * 1000000, 200 * 1000000);

	rdb->volatile_rage_engine = get_rage_engine_for_volatile_page_store(page_size_vps, page_id_width, truncator_period_us);
}

void deinitialize_rondb(rondb* rdb)
{
	deinitialize_mini_transaction_engine((mini_transaction_engine*)(rdb->persistent_acid_rage_engine.context));

	deinitialize_volatile_page_store((volatile_page_store*)(rdb->volatile_rage_engine.context));
}