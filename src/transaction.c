#include<rhendb/transaction.h>

transaction initialize_transaction(rhendb* db)
{
	transaction tx = {
		.db = db,
		.snapshot = NULL,
		.transaction_id = NULL,
		.curr_query = NULL,
	};

	return tx;
}

void deinitialize_transaction(transaction* tx)
{
	tx->db = NULL;
	tx->snapshot = NULL;
	tx->transaction_id = NULL;
	tx->curr_query = NULL;
}
