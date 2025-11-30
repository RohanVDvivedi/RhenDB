#include<rhendb/transaction.h>

transaction initialize_transaction(rhendb* db)
{
	transaction tx = {
		.db = db,
		.transaction_id = NULL,
		.curr_query = NULL,
	};

	initialize_mvcc_snapshot(&(tx.snapshot));

	return tx;
}

void deinitialize_transaction(transaction* tx)
{
	tx->db = NULL;
	deinitialize_mvcc_snapshot(&(tx->snapshot));
	tx->transaction_id = NULL;
	tx->curr_query = NULL;
}

int compare_transaction_by_transaction_id(const void* tx1, const void* tx2)
{
	return compare_uint256_with_ptrs(((const transaction*)tx1)->transaction_id, ((const transaction*)tx2)->transaction_id);
}

cy_uint hash_transaction_by_transaction_id(const void* tx)
{
	return ((const transaction*)tx)->transaction_id->limbs[0];
}