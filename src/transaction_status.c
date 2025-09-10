#include<rhendb/transaction_status.h>

char const * const transaction_status_string[] = {
	[TX_IN_PROGRESS] = "TX_IN_PROGRESS",
	[TX_ABORTED] = "TX_ABORTED",
	[TX_COMMITTED] = "TX_COMMITTED",
};