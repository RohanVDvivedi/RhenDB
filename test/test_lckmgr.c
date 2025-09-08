#include<rondb/rondb.h>
#include<rondb/lock_manager.h>

#include<stdlib.h>

void print_transaction_id(uint256 transaction_id)
{
	{
		char temp[80] = {};
		serialize_to_decimal_uint256(temp, transaction_id);
		printf("%s", temp);
	}
}

void notify_unblocked(void* context_p, uint256 transaction_id, uint32_t task_id)
{
	printf("notify_unblocked( trx_id = ");
	print_transaction_id(transaction_id);
	printf(" , task_id =  %"PRIu32" )\n\n", task_id);
}

void notify_deadlocked(void* context_p, uint256 transaction_id)
{
	printf("notify_deadlocked( trx_id = ");
	print_transaction_id(transaction_id);
	printf(" )\n\n");
}

void get_lock_mode(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id, uint32_t resource_type, uint64_t resource_id)
{
	printf("<-get_lock_mode( trx_id = ");
	print_transaction_id(transaction_id);
	printf(" , task_id = %"PRIu32" , r_type = %"PRIu32" , r_id = %"PRIu64" )\n\n", task_id, resource_type, resource_id);
	uint32_t res = get_lock_mode_for_lock_from_lock_manager(lckmgr_p, transaction_id, task_id, resource_type, (uint8_t*)(&resource_id), sizeof(uint64_t));
	switch(res)
	{
		case 0 :
		{
			printf("-> READ\n\n");
			break;
		}
		case 1 :
		{
			printf("-> WRITE\n\n");
			break;
		}
		case NO_LOCK_HELD_LOCK_MODE :
		{
			printf("-> NO_LOCK\n\n");
			break;
		}
		default :
		{
			printf("-> UNKNOWN\n\n");
			break;
		}
	}
}

void acquire_lock(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id, uint32_t resource_type, uint64_t resource_id, uint32_t new_lock_mode, int non_blocking)
{
	printf("<-acquire_lock( trx_id = ");
	print_transaction_id(transaction_id);
	printf(" , task_id = %"PRIu32" , r_type = %"PRIu32" , r_id = %"PRIu64", %s , %s )\n\n", task_id, resource_type, resource_id, ((new_lock_mode == 0) ? "READ" : "WRITE"), ((non_blocking) ? "NON_BLOCKING" : "BLOCKING"));
	lock_result res = acquire_lock_with_lock_manager(lckmgr_p, transaction_id, task_id, resource_type, (uint8_t*)(&resource_id), sizeof(uint64_t), new_lock_mode, non_blocking);
	printf("-> %s\n\n", lock_result_strings[res]);
}

void release_lock(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id, uint32_t resource_type, uint64_t resource_id)
{
	printf("<-release_lock( trx_id = ");
	print_transaction_id(transaction_id);
	printf(" , task_id = %"PRIu32" , r_type = %"PRIu32" , r_id = %"PRIu64" )\n\n", task_id, resource_type, resource_id);
	release_lock_with_lock_manager(lckmgr_p, transaction_id, task_id, resource_type, (uint8_t*)(&resource_id), sizeof(uint64_t));
	printf("-> POSSIBLY_RELEASED\n\n");
}

void conclude_all_business(lock_manager* lckmgr_p, uint256 transaction_id)
{
	printf("<-conclude_all_business( trx_id = ");
	print_transaction_id(transaction_id);
	printf(" )\n\n");
	conclude_all_business_with_lock_manager(lckmgr_p, transaction_id);
	printf("-> POSSIBLY_CONCLUDED\n\n");
}

int main()
{
	rondb rdb;
	initialize_rondb(&rdb, "./test.db",
		5,
		512, 8, 10, 10,
			10000ULL, 100000ULL,
			10000000ULL,
		4096,
			10000000ULL);
	printf("database initialized\n\n");

	lock_manager lckmgr;
	initialize_lock_manager(&lckmgr, NULL, ((lock_manager_notifier){NULL, notify_unblocked, notify_deadlocked}), get_uint256(65536), &(rdb.volatile_rage_engine));
	printf("lock_manager initialized\n\n");

	uint32_t RESOURCE_TYPE_0 =register_lock_type_with_lock_manager(&lckmgr, RW_DB_LOCK);
	uint32_t RESOURCE_TYPE_1 =register_lock_type_with_lock_manager(&lckmgr, RW_DB_LOCK);

	printf("lock types initialized\n\n");

	debug_print_lock_manager_tables(&lckmgr);

	acquire_lock(&lckmgr, get_uint256(0), 0, RESOURCE_TYPE_0, 0, RW_DB_LOCK_R_MODE, 1);
	acquire_lock(&lckmgr, get_uint256(0), 1, RESOURCE_TYPE_0, 1, RW_DB_LOCK_W_MODE, 1);
	acquire_lock(&lckmgr, get_uint256(1), 0, RESOURCE_TYPE_1, 0, RW_DB_LOCK_R_MODE, 1);
	acquire_lock(&lckmgr, get_uint256(1), 1, RESOURCE_TYPE_1, 1, RW_DB_LOCK_W_MODE, 1);

	debug_print_lock_manager_tables(&lckmgr);

	acquire_lock(&lckmgr, get_uint256(0), 0, RESOURCE_TYPE_1, 0, RW_DB_LOCK_W_MODE, 1);
	acquire_lock(&lckmgr, get_uint256(0), 1, RESOURCE_TYPE_1, 1, RW_DB_LOCK_R_MODE, 1);
	acquire_lock(&lckmgr, get_uint256(1), 0, RESOURCE_TYPE_0, 0, RW_DB_LOCK_R_MODE, 1);
	acquire_lock(&lckmgr, get_uint256(1), 1, RESOURCE_TYPE_0, 1, RW_DB_LOCK_W_MODE, 1);

	debug_print_lock_manager_tables(&lckmgr);

	return 0;
}