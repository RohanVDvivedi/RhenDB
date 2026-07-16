#include<rhendb/transaction.h>

transaction initialize_transaction(rhendb* rdb)
{
	transaction tx = {
		.rdb = rdb,
		.snapshot = NULL,
		.transaction_id = NULL,
	};

	for(uint32_t i = 0; i < TEMPORARY_EXTENSION_STORE_COUNT; i++)
	{
		const void* transaction_id = NULL;
		int abort_error_dummy = 0;
		tx.temp_ext_stores[i].blob_store_root_page_id = get_new_blob_store(&(rdb->volatile_rage_engine.bstd), rdb->volatile_rage_engine.pam_p, rdb->volatile_rage_engine.pmm_p, transaction_id, &abort_error_dummy);
		initialize_rwlock(&(tx.temp_ext_stores[i].blob_store_lock), NULL);
	}

	return tx;
}

void reset_temp_ext_stores_in_transaction(transaction* tx)
{
	for(uint32_t i = 0; i < TEMPORARY_EXTENSION_STORE_COUNT; i++)
	{
		const void* transaction_id = NULL;
		int abort_error_dummy = 0;
		destroy_blob_store(tx->temp_ext_stores[i].blob_store_root_page_id, &(tx->rdb->volatile_rage_engine.bstd), tx->rdb->volatile_rage_engine.pam_p, transaction_id, &abort_error_dummy);
		tx->temp_ext_stores[i].blob_store_root_page_id = get_new_blob_store(&(tx->rdb->volatile_rage_engine.bstd), tx->rdb->volatile_rage_engine.pam_p, tx->rdb->volatile_rage_engine.pmm_p, transaction_id, &abort_error_dummy);
	}
}

void deinitialize_transaction(transaction* tx)
{
	for(uint32_t i = 0; i < TEMPORARY_EXTENSION_STORE_COUNT; i++)
	{
		const void* transaction_id = NULL;
		int abort_error_dummy = 0;
		destroy_blob_store(tx->temp_ext_stores[i].blob_store_root_page_id, &(tx->rdb->volatile_rage_engine.bstd), tx->rdb->volatile_rage_engine.pam_p, transaction_id, &abort_error_dummy);
		deinitialize_rwlock(&(tx->temp_ext_stores[i].blob_store_lock));
	}

	tx->rdb = NULL;
	tx->snapshot = NULL;
	tx->transaction_id = NULL;
}
