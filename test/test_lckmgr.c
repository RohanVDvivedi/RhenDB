#include<rondb/rondb.h>
#include<rondb/lock_manager.h>

#include<stdlib.h>

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

	lock_manager lckmgr;
	initialize_lock_manager(&lckmgr, NULL, lock_manager_notifier notifier, 65536, &(rdb.volatile_rage_engine));

	return 0;
}