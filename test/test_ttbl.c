#include<rhendb/rhendb.h>
#include<rhendb/transaction_table.h>

#include<stdlib.h>

#define USERS_COUNT 10

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
			10000000ULL,
		USERS_COUNT);

	mvcc_snapshot* t1 = get_new_transaction_id(&(rdb.tx_table));
	print_mvcc_snapshot(t1);
	print_vaccum_horizon_transaction_id(&(rdb.tx_table));

	mvcc_snapshot* t2 = get_new_transaction_id(&(rdb.tx_table));
	print_mvcc_snapshot(t2);
	print_vaccum_horizon_transaction_id(&(rdb.tx_table));

	mvcc_snapshot* t3 = get_new_transaction_id(&(rdb.tx_table));
	print_mvcc_snapshot(t3);
	print_vaccum_horizon_transaction_id(&(rdb.tx_table));

	update_transaction_status(&(rdb.tx_table), t2->transaction_id, TX_ABORTED);
	print_vaccum_horizon_transaction_id(&(rdb.tx_table));

	mvcc_snapshot* t4 = get_new_transaction_id(&(rdb.tx_table));
	print_mvcc_snapshot(t4);
	print_vaccum_horizon_transaction_id(&(rdb.tx_table));

	update_transaction_status(&(rdb.tx_table), t3->transaction_id, TX_COMMITTED);
	print_vaccum_horizon_transaction_id(&(rdb.tx_table));

	mvcc_snapshot* t5 = get_new_transaction_id(&(rdb.tx_table));
	print_mvcc_snapshot(t5);
	print_vaccum_horizon_transaction_id(&(rdb.tx_table));

	uint256 last_txid_in_session = t5->transaction_id;

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

	for(uint256 tid = get_0_uint256(); compare_uint256(tid, last_txid_in_session) <= 0; add_uint256(&tid, tid, get_1_uint256()))
	{
		transaction_status status = get_transaction_status_for_transaction_id(&(rdb.tsg), tid);
		printf("%"PRIu64" -> %s\n", tid.limbs[0], transaction_status_string[status]);
	}

	deinitialize_rhendb(&rdb);

	return 0;
}