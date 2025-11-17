#include<rhendb/rhendb.h>

#include<rhendb/rage_engine_min_tx_engine.h>
#include<rhendb/rage_engine_volatile_page_store.h>

#include<rhendb/transaction.h>

#include<unistd.h>

#define TX_TABLE_ROOT_PAGE_ID_KEY "AAA-tx_table_root_page_id"

static void initialize_system_root_tables(rhendb* rdb, uint64_t max_concurrent_users_count)
{
	data_type_info* system_roots_record = malloc(sizeof_tuple_data_type_info(2));
	initialize_tuple_data_type_info(system_roots_record, "system_roots_record", 0, 64, 2);

	data_type_info system_roots_name = get_variable_length_string_type("system_record_name", 40);

	strcpy(system_roots_record->containees[0].field_name, "table_name");
	system_roots_record->containees[0].al.type_info = &system_roots_name;

	strcpy(system_roots_record->containees[1].field_name, "page_id");
	system_roots_record->containees[1].al.type_info = &(rdb->persistent_acid_rage_engine.pam_p->pas.page_id_type_info);

	tuple_def system_roots_record_def;
	initialize_tuple_def(&system_roots_record_def, system_roots_record);

	// initialize the bplus_tree_tuple_defs for this
	bplus_tree_tuple_defs bpttd;
	positional_accessor key_position[] = {STATIC_POSITION(1)};
	compare_direction key_compare_direction[] = {ASC};
	init_bplus_tree_tuple_definitions(&bpttd, &(rdb->persistent_acid_rage_engine.pam_p->pas), &(system_roots_record_def), key_position, key_compare_direction, 1);

	// root is expecto be at page_id 1, because that is the first page that get's created/allocated
	uint64_t root_page_id = 1;

	// set this flag for creation
	int creation_needed = 0;
	{
		int abort_error = 0;
		persistent_page root_page = acquire_persistent_page_with_lock(rdb->persistent_acid_rage_engine.pam_p, NULL, root_page_id, READ_LOCK, &abort_error);
		if(abort_error) // no page lock acquired, no need to release any thing, this call only fail if there was no page allocated at that place and the database is empty
			creation_needed = 1;
		else
		{
			creation_needed = 0;
			release_lock_on_persistent_page(rdb->persistent_acid_rage_engine.pam_p, NULL, &root_page, NONE_OPTION, &abort_error);
		}
	}

	if(creation_needed) // create all tables
	{
		printf("Rhendb database sytem being created\n");

		uint64_t page_latches_to_be_borrowed = 0;
		void* sub_transaction_id = rdb->persistent_acid_rage_engine.allot_new_sub_transaction_id(rdb->persistent_acid_rage_engine.context, page_latches_to_be_borrowed);
		if(sub_transaction_id == NULL)
		{
			printf("FAILED TO CREATE SYSTEM TABLES\n");
			exit(-1);
		}

		int abort_error = 0;

		uint64_t temp_root_page_id = get_new_bplus_tree(&bpttd, rdb->persistent_acid_rage_engine.pam_p, rdb->persistent_acid_rage_engine.pmm_p, sub_transaction_id, &abort_error);
		if(temp_root_page_id != root_page_id)
		{
			printf("FAILED TO CREATE SYSTEM TABLES WHOSE ROOT MUST BE AT PAGE ID %"PRIu64"\n", root_page_id);
			exit(-1);
		}

		char tuple_buffer[100];

		// create transaction table here
		{
			uint64_t tx_table_root_page_id = 0;
			initialize_transaction_table(&(rdb->tx_table), &(tx_table_root_page_id), &(rdb->persistent_acid_rage_engine), max_concurrent_users_count);

			init_tuple(&(system_roots_record_def), tuple_buffer);
			set_element_in_tuple(&(system_roots_record_def), STATIC_POSITION(0), tuple_buffer, &((user_value){.string_value = TX_TABLE_ROOT_PAGE_ID_KEY, .string_size = sizeof(TX_TABLE_ROOT_PAGE_ID_KEY)}), 100);
			set_element_in_tuple(&(system_roots_record_def), STATIC_POSITION(1), tuple_buffer, &((user_value){.uint_value = tx_table_root_page_id}), 100);

			insert_in_bplus_tree(root_page_id, tuple_buffer, &bpttd, rdb->persistent_acid_rage_engine.pam_p, rdb->persistent_acid_rage_engine.pmm_p, sub_transaction_id, &abort_error);
			if(abort_error)
			{
				printf("FAILED TO CREATE SYSTEM TABLES SECIFICALLY TRANSACTION TABLE\n");
				exit(-1);
			}
		}

		// create more system tables here
		{
		}

		rdb->persistent_acid_rage_engine.complete_sub_transaction(rdb->persistent_acid_rage_engine.context, sub_transaction_id, 1, NULL, 0, &page_latches_to_be_borrowed);
	}
	else // read and initialize all structures
	{
		printf("Rhendb database sytem table roots being read\n");

		// this is just going to be read, so no need for a sub_transaction

		int abort_error = 0;

		bplus_tree_iterator* bpi_p = find_in_bplus_tree(root_page_id, NULL, KEY_ELEMENT_COUNT, MIN, 0, READ_LOCK, &bpttd, rdb->persistent_acid_rage_engine.pam_p, NULL, NULL, &abort_error);
		if(abort_error)
		{
			printf("FAILED TO READ SYSTEM TABLES ROOTS\n");
			exit(-1);
		}

		if(is_empty_bplus_tree(bpi_p))
		{
			printf("FAILED TO READ SYSTEM TABLES ROOTS - NO TABLES FOUND - IMPOSSIBLE\n");
			exit(-1);
		}

		while(!is_beyond_max_tuple_bplus_tree_iterator(bpi_p))
		{
			const void* curr_tuple = get_tuple_bplus_tree_iterator(bpi_p);
			print_tuple(curr_tuple, &(system_roots_record_def));

			user_value system_table_name = {};
			uint64_t system_root_page_id = 0;
			{
				get_value_from_element_from_tuple(&system_table_name, &(system_roots_record_def), STATIC_POSITION(0), curr_tuple);
				user_value uval;
				get_value_from_element_from_tuple(&uval, &(system_roots_record_def), STATIC_POSITION(1), curr_tuple);
				system_root_page_id = uval.uint_value;
			}

			if(system_root_page_id != rdb->persistent_acid_rage_engine.pam_p->pas.NULL_PAGE_ID)
			{
				// compare with key and initialize transaction table here
				if(0 == strncmp(TX_TABLE_ROOT_PAGE_ID_KEY, system_table_name.string_value, system_table_name.string_size))
				{
					initialize_transaction_table(&(rdb->tx_table), &(system_root_page_id), &(rdb->persistent_acid_rage_engine), max_concurrent_users_count);
				}

				// initialize more system tables here
				{
				}
			}

			next_bplus_tree_iterator(bpi_p, NULL, &abort_error);
			if(abort_error)
			{
				printf("FAILED TO GO-NEXT SYSTEM TABLES ROOTS\n");
				exit(-1);
			}
		}

		delete_bplus_tree_iterator(bpi_p, NULL, &abort_error);
		if(abort_error)
		{
			printf("FAILED TO READ-CLOSE SYSTEM TABLES ROOTS\n");
			exit(-1);
		}
	}

	deinit_bplus_tree_tuple_definitions(&bpttd);
	free(system_roots_record);
}

static void notify_unblocked(void* context_p, uint256 transaction_id, uint32_t task_id);

static void notify_deadlocked(void* context_p, uint256 transaction_id);

void initialize_rhendb(rhendb* rdb, const char* database_file_name,
		uint32_t page_id_width,
		uint32_t page_size_mte, uint32_t lsn_width, uint64_t bufferpool_frame_count, uint64_t wale_buffer_count,
			uint64_t page_latch_wait_us, uint64_t page_lock_wait_us,
			uint64_t checkpoint_period_us,
		uint32_t page_size_vps,
			uint64_t truncator_period_us,
		uint64_t max_concurrent_users_count)
{
	// 10 times the user count, will be size of queue and same will be the number of threads, wait 1 second before you kill the thread
	rdb->cached_thread_pool = new_executor(CACHED_THREAD_POOL_EXECUTOR, min(max_concurrent_users_count * 10, 500), min(max_concurrent_users_count * 10, 2000), 1000000ULL, NULL, NULL, NULL, 0);

	// initialize the compute thread pool with as many threads as the hardware has and with the same queue size
	rdb->compute_thread_pool = new_executor(FIXED_THREAD_COUNT_EXECUTOR, sysconf(_SC_NPROCESSORS_ONLN), UNBOUNDED_SYNC_QUEUE, 0, NULL, NULL, NULL, 0);

	rdb->bufferpool_usage_limiter = new_resource_usage_limiter(bufferpool_frame_count);

	rdb->persistent_acid_rage_engine = get_rage_engine_for_min_tx_engine(database_file_name, page_size_mte, page_id_width, lsn_width, bufferpool_frame_count, wale_buffer_count, page_latch_wait_us, page_lock_wait_us, checkpoint_period_us, 2 * 1000000, 200 * 1000000);

	rdb->volatile_rage_engine = get_rage_engine_for_volatile_page_store(page_size_vps, page_id_width, truncator_period_us);

	// system table initialization

	// for tx_table, ...
	initialize_system_root_tables(rdb, max_concurrent_users_count);

	// for lck_table
	pthread_mutex_init(&(rdb->lock_manager_external_lock), NULL);
	initialize_lock_manager(&(rdb->lck_table), &(rdb->lock_manager_external_lock), &((const lock_manager_notifier){rdb, notify_unblocked, notify_deadlocked}), rdb->tx_table.overflow_transaction_id, &(rdb->volatile_rage_engine));

	initialize_hashmap(&(rdb->active_transactions), ELEMENTS_AS_RED_BLACK_BST, ((max_concurrent_users_count / 5) + 5), &simple_hasher(hash_transaction_by_transaction_id), &simple_comparator(compare_transaction_by_transaction_id), offsetof(transaction, embed_node_active_transactions_in_rhendb));
}

void deinitialize_rhendb(rhendb* rdb)
{
	shutdown_executor(rdb->cached_thread_pool, 1);
	delete_executor(rdb->cached_thread_pool);

	shutdown_executor(rdb->compute_thread_pool, 1);
	delete_executor(rdb->compute_thread_pool);

	delete_resource_usage_limiter(rdb->bufferpool_usage_limiter, 0);

	deinitialize_mini_transaction_engine((mini_transaction_engine*)(rdb->persistent_acid_rage_engine.context));

	deinitialize_volatile_page_store((volatile_page_store*)(rdb->volatile_rage_engine.context));
}

void register_transaction_with_rhendb(rhendb* rdb, transaction* tx)
{
	pthread_mutex_lock(&(rdb->lock_manager_external_lock));

	insert_in_hashmap(&(rdb->active_transactions), tx);

	pthread_mutex_unlock(&(rdb->lock_manager_external_lock));
}

void deregister_transaction_with_rhendb(rhendb* rdb, transaction* tx)
{
	pthread_mutex_lock(&(rdb->lock_manager_external_lock));

	remove_from_hashmap(&(rdb->active_transactions), tx);

	pthread_mutex_unlock(&(rdb->lock_manager_external_lock));
}

// TODO: rewrite the below functions for waking up the transaction_id's current query's operator's respective thread

static void print_transaction_id(uint256 transaction_id)
{
	{
		char temp[80] = {};
		serialize_to_decimal_uint256(temp, transaction_id);
		printf("%s", temp);
	}
}

static void notify_unblocked(void* context_p, uint256 transaction_id, uint32_t task_id)
{
	// rhendb provided the callback so we are the context
	rhendb* rdb = context_p;

	int operator_found = 0;

	// wake up the right operator, for the corresponding transaction
	{
		pthread_mutex_lock(&(rdb->lock_manager_external_lock));

		transaction* tx = find_from_hashmap(&(rdb->active_transactions), &((const transaction){.transaction_id = &transaction_id}));
		if(tx != NULL && tx->curr_query != NULL)
		{
			operator* o = get_operator_for_query_plan(tx->curr_query, task_id);
			if(o != NULL)
			{
				pthread_cond_broadcast(&(o->wait_on_lock_table_for_lock));
				operator_found = 1;
			}
		}

		pthread_mutex_unlock(&(rdb->lock_manager_external_lock));
	}

	// debug print of the operator was not found
	if(!operator_found)
	{
		printf("notify_unblocked( trx_id = ");
		print_transaction_id(transaction_id);
		printf(" , task_id = %"PRIu32" )\n\n", task_id);
	}
}

static void notify_deadlocked(void* context_p, uint256 transaction_id)
{
	// rhendb provided the callback so we are the context
	rhendb* rdb = context_p;

	printf("notify_deadlocked( trx_id = ");
	print_transaction_id(transaction_id);
	printf(" )\n\n");
}