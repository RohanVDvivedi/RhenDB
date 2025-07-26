#include<rondb/rondb.h>
#include<rondb/transaction_table.h>

int main()
{
	rondb rdb;
	initialize_rondb(&rdb, "test_db",
		5,
		512, 8, 10, 10,
			10000ULL, 100000ULL,
			10000000ULL,
		4096,
			10000000ULL);

	uint64_t root_page_id = 0;

	transaction_table ttbl;
	initialize_transaction_table(&ttbl, &root_page_id, &(rdb->persistent_acid_rage_engine), 3);

	return 0;
}