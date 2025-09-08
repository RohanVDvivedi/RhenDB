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
	printf("notify_unblocked( ");
	print_transaction_id(transaction_id);
	printf(", %"PRIu32" )\n\n", task_id);
}

void notify_deadlocked(void* context_p, uint256 transaction_id)
{
	printf("notify_deadlocked( ");
	print_transaction_id(transaction_id);
	printf(" )\n\n");
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

	return 0;
}