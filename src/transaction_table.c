#include<rondb/transaction_table.h>

// entry for the currently_active_transaction_ids bst
typedef struct active_transaction_id_entry active_transaction_id_entry;
struct active_transaction_id_entry
{
	// this transaction_id will be in TX_IN_PROGRESS status
	uint256 transaction_id;

	bstnode embed_node;
};

// entry for the transaction_table_cache cachemap
typedef struct passive_transaction_id_entry passive_transaction_id_entry;
struct passive_transaction_id_entry
{
	// this transaction_id will be in either TX_ABORTED or TX_COMMITTED status, check the status attribute
	uint256 transaction_id;

	// this status will be either TX_ABORTED or TX_COMMITTED, only
	transaction_status status;

	cchnode embed_node;
};

/*
	internal table functions
	Note: You do not need lock to access the persistent transaction table as the MinTxEngine will take care of the ACID-compliant access to it
	Things become simple, as you only read transaction_ids of other tansactions and update your own
*/

// reads the transaction status as is from the table, fails if unassigned or if the entry does not exists
static int get_transaction_status_from_table(transaction_table* ttbl, uint256 transaction_id, transaction_status* status);

// updates transaction status for the transaction_id on the table, as is creates a new page_table entry and a bitmap page if required
// must never fail
static int set_transaction_status_from_table(transaction_table* ttbl, uint256 transaction_id, transaction_status status);

// --

/*
	internal cache functions
	must be called with transaction_table_cache_lock held in write lock mode (exclusive lock)
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