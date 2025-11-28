#include<rhendb/rhendb.h>
#include<rhendb/lock_manager.h>

#include<stdlib.h>

#define USERS_COUNT 10

void get_lock_mode(lock_manager* lckmgr_p, uintptr_t transaction_id, uintptr_t task_id, uint32_t resource_type, uint64_t resource_id)
{
	printf("<-get_lock_mode( trx_id = %"PRIuPTR" , task_id = %"PRIuPTR" , r_type = %"PRIu32" , r_id = %"PRIu64" )\n\n", transaction_id, task_id, resource_type, resource_id);
	uint32_t res = get_lock_mode_for_lock_from_lock_manager(lckmgr_p, (void*)transaction_id, (void*)task_id, resource_type, (uint8_t*)(&resource_id), sizeof(uint64_t));
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

void acquire_lock(lock_manager* lckmgr_p, uintptr_t transaction_id, uintptr_t task_id, uint32_t resource_type, uint64_t resource_id, uint32_t new_lock_mode, int non_blocking)
{
	printf("<-acquire_lock( trx_id = %"PRIuPTR" , task_id = %"PRIuPTR" , r_type = %"PRIu32" , r_id = %"PRIu64", %s , %s )\n\n", transaction_id, task_id, resource_type, resource_id, ((new_lock_mode == 0) ? "READ" : "WRITE"), ((non_blocking) ? "NON_BLOCKING" : "BLOCKING"));
	lock_result res = acquire_lock_with_lock_manager(lckmgr_p, (void*)transaction_id, (void*)task_id, resource_type, (uint8_t*)(&resource_id), sizeof(uint64_t), new_lock_mode, non_blocking);
	printf("-> %s\n\n", lock_result_strings[res]);
}

void release_lock(lock_manager* lckmgr_p, uintptr_t transaction_id, uintptr_t task_id, uint32_t resource_type, uint64_t resource_id)
{
	printf("<-release_lock( trx_id = %"PRIuPTR" , task_id = %"PRIuPTR" , r_type = %"PRIu32" , r_id = %"PRIu64" )\n\n", transaction_id, task_id, resource_type, resource_id);
	release_lock_with_lock_manager(lckmgr_p, (void*)transaction_id, (void*)task_id, resource_type, (uint8_t*)(&resource_id), sizeof(uint64_t));
	printf("-> POSSIBLY_RELEASED\n\n");
}

void conclude_all_business(lock_manager* lckmgr_p, uintptr_t transaction_id)
{
	printf("<-conclude_all_business( trx_id = %"PRIuPTR" )\n\n", transaction_id);
	conclude_all_business_with_lock_manager(lckmgr_p, (void*)transaction_id);
	printf("-> POSSIBLY_CONCLUDED\n\n");
}

int main()
{
	rhendb rdb;
	initialize_rhendb(&rdb, "./test.db",
		5,
		512, 8, 10, 10,
			10000ULL, 100000ULL,
			10000000ULL,
		4096,
			10000000ULL,
		USERS_COUNT);
	printf("database initialized\n\n");

	uint32_t RESOURCE_TYPE_0 = register_lock_type_with_lock_manager(&(rdb.lck_table), RW_DB_LOCK);
	uint32_t RESOURCE_TYPE_1 = register_lock_type_with_lock_manager(&(rdb.lck_table), RW_DB_LOCK);

	printf("lock types initialized\n\n");

	debug_print_lock_manager_tables(&(rdb.lck_table));

	acquire_lock(&(rdb.lck_table), 0, 0, RESOURCE_TYPE_0, 0, RW_DB_LOCK_R_MODE, 1);
	acquire_lock(&(rdb.lck_table), 0, 1, RESOURCE_TYPE_0, 1, RW_DB_LOCK_W_MODE, 1);
	acquire_lock(&(rdb.lck_table), 1, 0, RESOURCE_TYPE_1, 0, RW_DB_LOCK_R_MODE, 1);
	acquire_lock(&(rdb.lck_table), 1, 1, RESOURCE_TYPE_1, 1, RW_DB_LOCK_W_MODE, 1);

	debug_print_lock_manager_tables(&(rdb.lck_table));

	acquire_lock(&(rdb.lck_table), 0, 0, RESOURCE_TYPE_1, 0, RW_DB_LOCK_W_MODE, 0);
	acquire_lock(&(rdb.lck_table), 0, 1, RESOURCE_TYPE_1, 1, RW_DB_LOCK_R_MODE, 0);
	acquire_lock(&(rdb.lck_table), 1, 0, RESOURCE_TYPE_0, 0, RW_DB_LOCK_R_MODE, 0);
	acquire_lock(&(rdb.lck_table), 1, 1, RESOURCE_TYPE_0, 1, RW_DB_LOCK_W_MODE, 0);

	debug_print_lock_manager_tables(&(rdb.lck_table));

	acquire_lock(&(rdb.lck_table), 3, 0, RESOURCE_TYPE_0, 0, RW_DB_LOCK_W_MODE, 0);
	acquire_lock(&(rdb.lck_table), 3, 1, RESOURCE_TYPE_0, 1, RW_DB_LOCK_R_MODE, 0);
	acquire_lock(&(rdb.lck_table), 3, 2, RESOURCE_TYPE_1, 0, RW_DB_LOCK_W_MODE, 0);
	acquire_lock(&(rdb.lck_table), 3, 3, RESOURCE_TYPE_1, 1, RW_DB_LOCK_R_MODE, 0);

	debug_print_lock_manager_tables(&(rdb.lck_table));

	acquire_lock(&(rdb.lck_table), 0, 0, RESOURCE_TYPE_0, 0, RW_DB_LOCK_W_MODE, 0);
	acquire_lock(&(rdb.lck_table), 1, 1, RESOURCE_TYPE_1, 1, RW_DB_LOCK_R_MODE, 0);

	debug_print_lock_manager_tables(&(rdb.lck_table));

	acquire_lock(&(rdb.lck_table), 1, 0, RESOURCE_TYPE_0, 1, RW_DB_LOCK_R_MODE, 0);

	debug_print_lock_manager_tables(&(rdb.lck_table));

	release_lock(&(rdb.lck_table), 1, 0, RESOURCE_TYPE_0, 0);

	debug_print_lock_manager_tables(&(rdb.lck_table));

	get_lock_mode(&(rdb.lck_table), 0, 1, RESOURCE_TYPE_1, 33);
	get_lock_mode(&(rdb.lck_table), 1, 1, RESOURCE_TYPE_1, 1);

	debug_print_lock_manager_tables(&(rdb.lck_table));

	release_lock(&(rdb.lck_table), 0, 0, RESOURCE_TYPE_0, 0);
	release_lock(&(rdb.lck_table), 1, 1, RESOURCE_TYPE_1, 1);

	debug_print_lock_manager_tables(&(rdb.lck_table));

	release_lock(&(rdb.lck_table), 0, 1, RESOURCE_TYPE_0, 1);
	release_lock(&(rdb.lck_table), 1, 0, RESOURCE_TYPE_1, 0);

	debug_print_lock_manager_tables(&(rdb.lck_table));

	conclude_all_business(&(rdb.lck_table), 0);
	conclude_all_business(&(rdb.lck_table), 1);
	conclude_all_business(&(rdb.lck_table), 3);

	debug_print_lock_manager_tables(&(rdb.lck_table));

	deinitialize_rhendb(&rdb);

	return 0;
}