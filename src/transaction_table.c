#include<rondb/transaction_table.h>

/*
	internal table functions
	must be called with global lock (transaction_table_lock) held in right mode
*/

// reads the transaction status as is from the table, fails if unassigned or if the entry does not exists
static int get_transaction_status_from_table(transaction_table* ttbl, uint256 transaction_id, transaction_status* status);

// updates transaction status for the transaction_id on the table, as is creates a new page_table entry and a bitmap page if required
// must never fail
static int set_transaction_status_from_table(transaction_table* ttbl, uint256 transaction_id, transaction_status status);

// --

/*
	internal cache functions
	must be called with global lock (transaction_table_lock) held in right mode
*/

// reads the transaction status as is from the cache, fails if the entry does not exists in the cache
// if an entry is found it is bumped in the cache, so as to avoid immediate eviction
static int get_transaction_status_from_cache(transaction_table* ttbl, uint256 transaction_id, transaction_status* status);

// inserts a new entry in the cache for the given transaction_id OR updates and bumps it, if an entry for the transaction_id exists
static int set_transaction_status_from_cache(transaction_table* ttbl, uint256 transaction_id, transaction_status status);

// --

mvcc_snapshot* get_new_transaction_id(transaction_table* ttbl);

transaction_status get_transaction_status(transaction_table* ttbl, uint256 transaction_id);

int update_transaction_status(transaction_table* ttbl, uint256 transaction_id, transaction_status status);