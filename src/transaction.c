#include<rhendb/transaction.h>

#include<tuplelargetypes/common_extended.h>

#define MAX_ENTRIES_IN_VOL_BLOBS_HTAN        56 // threshold should be something like 20 to 24 for fixing the accumulated entries
#define TEMP_EXT_BLOB_STORE_FIX_THRESHOLD    25

// we already know that tuple_pointer has only 2 fixed sized unsigned integral attributes
positional_accessor key_element_positions_for_hashset_for_tuple_pointers[2] = {
	STATIC_POSITION(0),
	STATIC_POSITION(1),
};

positional_accessor key_element_positions_for_hashset_for_savepoint_names[1] = {
	SELF,
};

transaction initialize_transaction(rhendb* rdb)
{
	transaction tx = {
		.rdb = rdb,
		.snapshot = NULL,
		.transaction_id = NULL,
	};

	{
		const void* transaction_id = NULL;
		int abort_error_dummy = 0;
		init_hash_table_tuple_definitions(&(tx.inserted_tuple_pointers.httd), &(rdb->volatile_rage_engine.pam_p->pas), &(rdb->persistent_acid_rage_engine.pam_p->pas.tuple_pointer_tuple_def), key_element_positions_for_hashset_for_tuple_pointers, sizeof(key_element_positions_for_hashset_for_tuple_pointers)/ sizeof(positional_accessor), FNV_64_TUPLE_HASHER);
		tx.inserted_tuple_pointers.root_handle = get_new_hash_table(64, &(tx.inserted_tuple_pointers.httd), rdb->volatile_rage_engine.pam_p, rdb->volatile_rage_engine.pmm_p, transaction_id, &abort_error_dummy);
		initialize_rwlock(&(tx.inserted_tuple_pointers.hash_table_lock), NULL);
	}

	{
		const void* transaction_id = NULL;
		int abort_error_dummy = 0;

		tx.savepoint_logs.savepoint_name_dti = malloc(sizeof(data_type_info));
		(*(tx.savepoint_logs.savepoint_name_dti)) = get_variable_length_string_type("savepoint_name", 80);
		tx.savepoint_logs.savepoint_name_def = malloc(sizeof(tuple_def));
		initialize_tuple_def(tx.savepoint_logs.savepoint_name_def, tx.savepoint_logs.savepoint_name_dti);

		tx.savepoint_logs.savepoint_log_dti = malloc(sizeof_tuple_data_type_info(5));
		strcpy(tx.savepoint_logs.savepoint_log_dti->containees[0].field_name, "log_type");
		tx.savepoint_logs.savepoint_log_dti->containees[0].al.type_info = UINT_NON_NULLABLE[1];
		strcpy(tx.savepoint_logs.savepoint_log_dti->containees[1].field_name, "savepoint_name");
		tx.savepoint_logs.savepoint_log_dti->containees[1].al.type_info = tx.savepoint_logs.savepoint_name_dti;
		strcpy(tx.savepoint_logs.savepoint_log_dti->containees[2].field_name, "table_id");
		tx.savepoint_logs.savepoint_log_dti->containees[2].al.type_info = UINT_NON_NULLABLE[8];
		strcpy(tx.savepoint_logs.savepoint_log_dti->containees[3].field_name, "partition_id");
		tx.savepoint_logs.savepoint_log_dti->containees[3].al.type_info = UINT_NON_NULLABLE[8];
		strcpy(tx.savepoint_logs.savepoint_log_dti->containees[4].field_name, "tuple_pointer");
		tx.savepoint_logs.savepoint_log_dti->containees[4].al.type_info = &(rdb->persistent_acid_rage_engine.pam_p->pas.tuple_pointer_type_info);
		initialize_tuple_data_type_info(tx.savepoint_logs.savepoint_log_dti, "savepoint_log_def", 1, 8 + 4 + 8 + 80 + 8 + 8 + sizeof(tuple_pointer), 5)
		tx.savepoint_logs.savepoint_log_def = malloc(sizeof(tuple_def));
		initialize_tuple_def(tx.savepoint_logs.savepoint_log_def, tx.savepoint_logs.savepoint_log_dti);

		init_linked_page_list_tuple_definitions(&(tx.savepoint_logs.savepoint_log_defs), &(rdb->volatile_rage_engine.pam_p->pas), tx.savepoint_logs.savepoint_log_def);
		tx.savepoint_logs.savepoint_log_root = get_new_linked_page_list(&(tx.savepoint_logs.savepoint_log_defs), rdb->volatile_rage_engine.pam_p, rdb->volatile_rage_engine.pmm_p, transaction_id, &abort_error_dummy);
		tx.savepoint_logs.savepoint_logs_count = 0;

		init_hash_table_tuple_definitions(&(tx.savepoint_logs.savepoint_names_set_tuple_defs), &(rdb->volatile_rage_engine.pam_p->pas), tx.savepoint_logs.savepoint_name_def, key_element_positions_for_hashset_for_savepoint_names, 1, FNV_64_TUPLE_HASHER);
		tx.savepoint_logs.savepoint_names_set_handle = get_new_hash_table(64, &(tx.savepoint_logs.savepoint_names_set_tuple_defs), rdb->volatile_rage_engine.pam_p, rdb->volatile_rage_engine.pmm_p, transaction_id, &abort_error_dummy);
		tx.savepoint_logs.savepoint_names_count = 0;
		
		pthread_mutex_init(&(tx.savepoint_logs.savepoint_lock), NULL);
	}

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

void register_inserted_tuple_pointer(transaction* tx, tuple_pointer tptr)
{
	char tptr_tpl[20];
	set_tuple_pointer(tptr_tpl, tptr, &(tx->rdb->persistent_acid_rage_engine.pam_p->pas)); // tuple_pointer belongs to the persistent_acid_rage_engine so this is the way to serialize it

	const void* transaction_id = NULL;
	int abort_error_dummy = 0;

	write_lock(&(tx->inserted_tuple_pointers.hash_table_lock), BLOCKING);

	// perform the insert and increment entry_count
	{
		hash_table_iterator* hti_p = get_new_hash_table_iterator(&(tx->inserted_tuple_pointers.root_handle), (bucket_range){0,0}, tptr_tpl, &(tx->inserted_tuple_pointers.httd), tx->rdb->volatile_rage_engine.pam_p, tx->rdb->volatile_rage_engine.pmm_p, transaction_id, &abort_error_dummy);

		insert_in_hash_table_iterator(hti_p, tptr_tpl, transaction_id, &abort_error_dummy);
		tx->inserted_tuple_pointers.entries_count++;

		hash_table_vaccum_params htvp;
		delete_hash_table_iterator(hti_p, &htvp, transaction_id, &abort_error_dummy);
	}

	// expand if required, we do not want to lookup more than a single page, 70% page fill works better for speed and efficient memory utilization, hence the 0.7
	if( ((double)(tx->inserted_tuple_pointers.entries_count) * 12.0)
		/ ((double)(tx->inserted_tuple_pointers.root_handle.bucket_count))
		/ ((double)(tx->rdb->volatile_rage_engine.pam_p->pas.page_size))     > 0.7)
		expand_hash_table(&(tx->inserted_tuple_pointers.root_handle), &(tx->inserted_tuple_pointers.httd), tx->rdb->volatile_rage_engine.pam_p, tx->rdb->volatile_rage_engine.pmm_p, transaction_id, &abort_error_dummy);

	write_unlock(&(tx->inserted_tuple_pointers.hash_table_lock));
}

int was_registered_as_inserted_tuple_pointer(transaction* tx, tuple_pointer tptr)
{
	char tptr_tpl[20];
	set_tuple_pointer(tptr_tpl, tptr, &(tx->rdb->persistent_acid_rage_engine.pam_p->pas)); // tuple_pointer belongs to the persistent_acid_rage_engine so this is the way to serialize it

	const void* transaction_id = NULL;
	int abort_error_dummy = 0;

	int was_registered = 0;

	read_lock(&(tx->inserted_tuple_pointers.hash_table_lock), READ_PREFERRING, BLOCKING);

	hash_table_iterator* hti_p = get_new_hash_table_iterator(&(tx->inserted_tuple_pointers.root_handle), (bucket_range){0,0}, tptr_tpl, &(tx->inserted_tuple_pointers.httd), tx->rdb->volatile_rage_engine.pam_p, NULL, transaction_id, &abort_error_dummy);

	if(!is_curr_bucket_empty_for_hash_table_iterator(hti_p))
	{
		while(1)
		{
			if(get_tuple_hash_table_iterator(hti_p) != NULL) // we found this exact tptr
			{
				was_registered = 1;
				break;
			}

			if(!next_hash_table_iterator(hti_p, GO_NEXT_TUPLE_IN_SAME_BUCKET, transaction_id, &abort_error_dummy)) // if we can not go to the next one in the same bucket, we are done searching
				break;
		}
	}

	hash_table_vaccum_params htvp;
	delete_hash_table_iterator(hti_p, &htvp, transaction_id, &abort_error_dummy);

	read_unlock(&(tx->inserted_tuple_pointers.hash_table_lock));

	return was_registered;
}

void reset_inserted_tuple_pointers(transaction* tx)
{
	const void* transaction_id = NULL;
	int abort_error_dummy = 0;

	destroy_hash_table(&(tx->inserted_tuple_pointers.root_handle), &(tx->inserted_tuple_pointers.httd), tx->rdb->volatile_rage_engine.pam_p, transaction_id, &abort_error_dummy);

	tx->inserted_tuple_pointers.root_handle = get_new_hash_table(64, &(tx->inserted_tuple_pointers.httd), tx->rdb->volatile_rage_engine.pam_p, tx->rdb->volatile_rage_engine.pmm_p, transaction_id, &abort_error_dummy);
	tx->inserted_tuple_pointers.entries_count = 0;
}

void log_to_savepoint_log(transaction* tx, savepoint_log_type type, uint64_t table_id, uint64_t partition_id, tuple_pointer tptr)
{
	// only these two can be logged using this function
	if(type != INSERTION_SAVEPOINT_LOG && type != DELETION_SAVEPOINT_LOG)
		return;

	const void* transaction_id = NULL;
	int abort_error_dummy = 0;

	pthread_mutex_lock(&(tx->savepoint_logs.savepoint_lock));

	// open iterator to savepoint_log_root
	linked_page_list_iterator* lpli_p = get_new_linked_page_list_iterator(tx->savepoint_logs.savepoint_log_root, &(tx->savepoint_logs.savepoint_log_defs), tx->rdb->volatile_rage_engine.pam_p, tx->rdb->volatile_rage_engine.pmm_p, transaction_id, &abort_error_dummy);

	// unconditionally go to tail, by going previous
	prev_linked_page_list_iterator(lpli_p, transaction_id, &abort_error_dummy);

	// create savepoint log tuple
	char log_tuple[8 + 4 + 8 + 8 + 8 + sizeof(tuple_pointer)];
	char tptr_tpl[20];
	set_tuple_pointer(tptr_tpl, tptr, &(tx->rdb->persistent_acid_rage_engine.pam_p->pas));
	init_tuple(tx->savepoint_logs.savepoint_log_def, log_tuple);
	set_element_in_tuple(tx->savepoint_logs.savepoint_log_def, STATIC_POSITION(0), log_tuple, &((datum){.uint_value = type}), 0);
	set_element_in_tuple(tx->savepoint_logs.savepoint_log_def, STATIC_POSITION(2), log_tuple, &((datum){.uint_value = table_id}), 0);
	set_element_in_tuple(tx->savepoint_logs.savepoint_log_def, STATIC_POSITION(3), log_tuple, &((datum){.uint_value = partition_id}), 0);
	set_element_in_tuple(tx->savepoint_logs.savepoint_log_def, STATIC_POSITION(4), log_tuple, &((datum){.tuple_value = tptr_tpl}), 0);

	// insert after the tail
	insert_at_linked_page_list_iterator(lpli_p, log_tuple, INSERT_AFTER_LINKED_PAGE_LIST_ITERATOR, transaction_id, &abort_error_dummy);

	// increment savepoint_logs_count
	tx->savepoint_logs.savepoint_logs_count++;

	// delete the iterator
	delete_linked_page_list_iterator(lpli_p, transaction_id, &abort_error_dummy);

	pthread_mutex_unlock(&(tx->savepoint_logs.savepoint_lock));
}

int insert_new_savepoint(transaction* tx, const dstring* savepoint_name);

void delete_savepoint(transaction* tx, const dstring* savepoint_name);

int exists_savepoint(transaction* tx, const dstring* savepoint_name);

int rollback_to_savepoint(transaction* tx, const dstring* savepoint_name);

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
		printf("BUG in storing extended type, ext_sub_type not identified, it is %.*s\n", (int)ext_sub_type_len, (ext_sub_type_len == 0) ? "" : ext_sub_type);
		exit(-1);
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

	printf("BUG in storing extended type, ext_sub_type not identified, it is %.*s\n", (int)ext_sub_type_len, (ext_sub_type_len == 0) ? "" : ext_sub_type);
	exit(-1);
}

void fix_unused_space_entries_in_store(transaction* tx, temporary_extension_store* temp_ext_store)
{
	uint32_t entries_to_fix = get_notification_count_for_heap_table_accumulative_notifier(&(temp_ext_store->htan));
	if(entries_to_fix < TEMP_EXT_BLOB_STORE_FIX_THRESHOLD)
		return;

	int abort_error_dummy = 0;
	uint64_t blob_store_root_page_id;
	uint32_t unused_bytes_in_entry;
	uint64_t page_id;
	while(pop_from_heap_table_accumulative_notifier(&(temp_ext_store->htan), &blob_store_root_page_id, &unused_bytes_in_entry, &page_id))
	{
		if(blob_store_root_page_id == temp_ext_store->blob_store_root_page_id)
			fix_unused_space_in_heap_table(blob_store_root_page_id, unused_bytes_in_entry, page_id, &(tx->rdb->volatile_rage_engine.bstd.httd), tx->rdb->volatile_rage_engine.pam_p, tx->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);
	}
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

	{
		const void* transaction_id = NULL;
		int abort_error_dummy = 0;

		destroy_hash_table(&(tx->savepoint_logs.savepoint_names_set_handle), &(tx->savepoint_logs.savepoint_names_set_tuple_defs), tx->rdb->volatile_rage_engine.pam_p, transaction_id, &abort_error_dummy);
		deinit_hash_table_tuple_definitions(&(tx->savepoint_logs.savepoint_names_set_tuple_defs));

		destroy_linked_page_list(tx->savepoint_logs.savepoint_log_root, &(tx->savepoint_logs.savepoint_log_defs), tx->rdb->volatile_rage_engine.pam_p, transaction_id, &abort_error_dummy);
		deinit_linked_page_list_tuple_definitions(&(tx->savepoint_logs.savepoint_log_defs));

		free(tx->savepoint_logs.savepoint_log_dti);
		free(tx->savepoint_logs.savepoint_log_def);
		free(tx->savepoint_logs.savepoint_name_dti);
		free(tx->savepoint_logs.savepoint_name_def);
		pthread_mutex_destroy(&(tx->savepoint_logs.savepoint_lock));
	}

	{
		const void* transaction_id = NULL;
		int abort_error_dummy = 0;
		destroy_hash_table(&(tx->inserted_tuple_pointers.root_handle), &(tx->inserted_tuple_pointers.httd), tx->rdb->volatile_rage_engine.pam_p, transaction_id, &abort_error_dummy);
		deinit_hash_table_tuple_definitions(&(tx->inserted_tuple_pointers.httd));
		deinitialize_rwlock(&(tx->inserted_tuple_pointers.hash_table_lock));
	}

	tx->rdb = NULL;
	tx->snapshot = NULL;
	tx->transaction_id = NULL;
}
