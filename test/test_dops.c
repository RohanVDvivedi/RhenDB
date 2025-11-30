#include<rhendb/rhendb.h>

#include<stdlib.h>

#define USERS_COUNT 10

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


	deinitialize_rhendb(&rdb);

	return 0;
}