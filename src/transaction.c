#include<rhendb/transaction.h>

#include<tuplelargetypes/common_extended.h>

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
		initialize_heap_table_accumulative_notifier(&(tx.temp_ext_stores[i].htan), MAX_ENTRIES_IN_VOL_BLOBS_HTAN);
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
		deinitialize_heap_table_accumulative_notifier(&(tx->temp_ext_stores[i].htan));

		tx->temp_ext_stores[i].blob_store_root_page_id = get_new_blob_store(&(tx->rdb->volatile_rage_engine.bstd), tx->rdb->volatile_rage_engine.pam_p, tx->rdb->volatile_rage_engine.pmm_p, transaction_id, &abort_error_dummy);
		initialize_heap_table_accumulative_notifier(&(tx->temp_ext_stores[i].htan), MAX_ENTRIES_IN_VOL_BLOBS_HTAN);
	}
}

static void extension_blob_read_begin_event(extension_reader_iterator_callback* callback, const datum* uval, const data_type_info* dti, const page_access_methods* pam_p)
{
	transaction* tx = callback->context1;
	rwlock* lk = callback->context2;

	if(lk == NULL)
	{
		// find the lock
		datum uval_c;
		const data_type_info* dti_c;
		if(!get_nested_containee_from_datum(&uval_c, &dti_c, uval, dti, EXTENDED_PREFIX_POS_ACC))
			uval_c = (*NULL_DATUM);

		uint64_t hash = hash_datum(&uval_c, dti_c, FNV_64_TUPLE_HASHER) % TEMPORARY_EXTENSION_STORE_COUNT;

		lk = &(tx->temp_ext_stores[hash].blob_store_lock);

		// now place lock pointer in the contest
		callback->context2 = lk;
	}

	read_lock(lk, READ_PREFERRING, BLOCKING);
}

static void extension_blob_read_ended_event(extension_reader_iterator_callback* callback, const datum* uval, const data_type_info* dti, const page_access_methods* pam_p)
{
	//transaction* tx = callback->context1;
	rwlock* lk = callback->context2;

	// release the lock
	read_unlock(lk);
}

extension_reader_iterator_callback* get_callback_and_engine_for_extended_type(transaction* tx, const data_type_info* dti_p, rage_engine** ex_engine, extension_reader_iterator_callback* pass_through)
{
	if(!is_extended_type_info(dti_p))
	{
		(*ex_engine) = NULL;
		return NULL;
	}

	uint32_t ext_sub_type_len = 0;
	const char* ext_sub_type = get_extension_sub_type_for_extended_type(dti_p, &ext_sub_type_len);

	if(ext_sub_type_len != 1)
	{
		(*ex_engine) = NULL;
		return NULL;
	}

	if(ext_sub_type[0] == PERSISTENT_EXT_SUB_TYPE[0])
	{
		(*ex_engine) = &(tx->rdb->persistent_acid_rage_engine);
		return NULL;
	}

	if(ext_sub_type[0] == VOLATILE_EXT_SUB_TYPE[0])
	{
		(*ex_engine) = &(tx->rdb->volatile_rage_engine);
		(*pass_through) = (extension_reader_iterator_callback){
			.context1 = tx,
			.context2 = NULL, // we do not yet know which lock needs to be freed
			.extension_blob_read_begin_event = extension_blob_read_begin_event,
			.extension_blob_read_ended_event = extension_blob_read_ended_event,
		};
		return pass_through;
	}

	(*ex_engine) = NULL;
	return NULL;
}

void deinitialize_transaction(transaction* tx)
{
	for(uint32_t i = 0; i < TEMPORARY_EXTENSION_STORE_COUNT; i++)
	{
		const void* transaction_id = NULL;
		int abort_error_dummy = 0;
		destroy_blob_store(tx->temp_ext_stores[i].blob_store_root_page_id, &(tx->rdb->volatile_rage_engine.bstd), tx->rdb->volatile_rage_engine.pam_p, transaction_id, &abort_error_dummy);
		deinitialize_heap_table_accumulative_notifier(&(tx->temp_ext_stores[i].htan));
		deinitialize_rwlock(&(tx->temp_ext_stores[i].blob_store_lock));
	}

	tx->rdb = NULL;
	tx->snapshot = NULL;
	tx->transaction_id = NULL;
}
