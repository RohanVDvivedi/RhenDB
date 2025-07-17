#include<rondb/transaction_table.h>

// entry for the currently_active_transaction_ids bst
typedef struct active_transaction_id_entry active_transaction_id_entry;
struct active_transaction_id_entry
{
	// this transaction_id will be in TX_IN_PROGRESS status
	uint256 transaction_id;

	bstnode embed_node;
};

static int compare_active_transaction_id_entry(const void* a, const void* b)
{
	return compare_uint256_with_ptrs(&(((const active_transaction_id_entry*)a)->transaction_id), &(((const active_transaction_id_entry*)a)->transaction_id));
}

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

static int compare_passive_transaction_id_entry(const void* a, const void* b)
{
	return compare_uint256_with_ptrs(&(((const passive_transaction_id_entry*)a)->transaction_id), &(((const passive_transaction_id_entry*)a)->transaction_id));
}

static cy_uint hash_passive_transaction_id_entry(const void* a)
{
	return ((const passive_transaction_id_entry*)a)->transaction_id.limbs[0];
}

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

/*
	internal functions for currently_active_transaction_ids
	must be called with transaction_table_cache_lock held in read OR write lock mode as per access type
*/

// checks if a transaction_id exists in currently_active_transaction_ids
static const active_transaction_id_entry* find_in_currently_active_transaction_ids(const transaction_table* ttbl, uint256 transaction_id);

// iterate over all the currently_active_transaction_ids
static void for_each_in_order_in_currently_active_transaction_ids(const transaction_table* ttbl);

// insert to currently_active_transaction_ids
static int insert_in_currently_active_transaction_ids(transaction_table* ttbl, uint256 transaction_id);

// remove a entry from the currently_active_transaction_ids
static int remove_from_currently_active_transaction_ids(transaction_table* ttbl, const active_transaction_id_entry* active_transaction_id_entry_p);

// --

mvcc_snapshot* get_new_transaction_id(transaction_table* ttbl);

transaction_status get_transaction_status(transaction_table* ttbl, uint256 transaction_id);

int update_transaction_status(transaction_table* ttbl, uint256 transaction_id, transaction_status status);