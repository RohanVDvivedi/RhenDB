#include<rhendb/rhendb.h>
#include<rhendb/transaction_table.h>

#include<stdlib.h>

void print_vaccum_horizon_transaction_id(transaction_table* ttbl)
{
	uint256 vhtxid = get_vaccum_horizon_transaction_id(ttbl);
	printf("vhtxid = ");
	char temp[80] = {};
	serialize_to_decimal_uint256(temp, vhtxid);
	printf("%s\n", temp);
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
			10000000ULL);

	uint64_t root_page_id = 0;

	transaction_table ttbl;
	initialize_transaction_table(&ttbl, &root_page_id, &(rdb.persistent_acid_rage_engine), 3);

	mvcc_snapshot* t1 = get_new_transaction_id(&ttbl);
	print_mvcc_snapshot(t1);
	print_vaccum_horizon_transaction_id(&ttbl);

	mvcc_snapshot* t2 = get_new_transaction_id(&ttbl);
	print_mvcc_snapshot(t2);
	print_vaccum_horizon_transaction_id(&ttbl);

	mvcc_snapshot* t3 = get_new_transaction_id(&ttbl);
	print_mvcc_snapshot(t3);
	print_vaccum_horizon_transaction_id(&ttbl);

	update_transaction_status(&ttbl, t2->transaction_id, TX_ABORTED);
	print_vaccum_horizon_transaction_id(&ttbl);

	mvcc_snapshot* t4 = get_new_transaction_id(&ttbl);
	print_mvcc_snapshot(t4);
	print_vaccum_horizon_transaction_id(&ttbl);

	update_transaction_status(&ttbl, t3->transaction_id, TX_COMMITTED);
	print_vaccum_horizon_transaction_id(&ttbl);

	mvcc_snapshot* t5 = get_new_transaction_id(&ttbl);
	print_mvcc_snapshot(t5);
	print_vaccum_horizon_transaction_id(&ttbl);

	deinitialize_mvcc_snapshot(t5);
	free(t5);

	deinitialize_mvcc_snapshot(t4);
	free(t4);

	deinitialize_mvcc_snapshot(t3);
	free(t3);

	deinitialize_mvcc_snapshot(t2);
	free(t2);

	deinitialize_mvcc_snapshot(t1);
	free(t1);

	for(uint256 tid = get_0_uint256(); tid.limbs[0] < 5; add_uint256(&tid, tid, get_1_uint256()))
	{
		transaction_status status = get_transaction_status(&ttbl, tid);
		printf("%"PRIu64" -> %s\n", tid.limbs[0], transaction_status_string[status]);
	}

	deinitialize_rhendb(&rdb);

	return 0;
}