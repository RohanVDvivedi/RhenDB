#include<rhendb/transaction_status.h>

char const * const transaction_status_string[] = {
	[TX_IN_PROGRESS] = "TX_IN_PROGRESS",
	[TX_ABORTED] = "TX_ABORTED",
	[TX_COMMITTED] = "TX_COMMITTED",
};

transaction_status get_transaction_status_for_transaction_id(transaction_status_getter* tsg, uint256 transaction_id)
{
	return tsg->get_transaction_status(tsg->context, transaction_id);
}