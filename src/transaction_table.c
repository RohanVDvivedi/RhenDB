#include<rondb/transaction_table.h>

#include<stdlib.h>

// entry for the currently_active_transaction_ids bst
typedef struct active_transaction_id_entry active_transaction_id_entry;
struct active_transaction_id_entry
{
	// this transaction_id will be in TX_IN_PROGRESS status
	uint256 transaction_id; // note: it should always be the first attribute

	bstnode embed_node;
};

static int compare_active_transaction_id_entry(const void* a, const void* b)
{
	return compare_uint256_with_ptrs(&(((const active_transaction_id_entry*)a)->transaction_id), &(((const active_transaction_id_entry*)b)->transaction_id));
}

// entry for the transaction_table_cache cachemap
typedef struct passive_transaction_id_entry passive_transaction_id_entry;
struct passive_transaction_id_entry
{
	// this transaction_id will be in either TX_ABORTED or TX_COMMITTED status, check the status attribute
	uint256 transaction_id; // note: it should always be the first attribute

	// this status will be either TX_ABORTED or TX_COMMITTED, only
	transaction_status status;

	cchnode embed_node;
};

static int compare_passive_transaction_id_entry(const void* a, const void* b)
{
	return compare_uint256_with_ptrs(&(((const passive_transaction_id_entry*)a)->transaction_id), &(((const passive_transaction_id_entry*)b)->transaction_id));
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

// this is the maximum transaction_id that was ever assigned, if a 0 is returned then it means no transaction_id is ever assigned, then you must start from 0
static int get_max_allotted_transaction_id(transaction_table* ttbl, uint256* transaction_id);

// updates transaction status for the transaction_id on the table, as is creates a new page_table entry and a bitmap page if required
// must never fail
static int set_transaction_status_in_table(transaction_table* ttbl, uint256 transaction_id, transaction_status status);

// --

/*
	internal cache functions
	must be called with transaction_table_cache_lock held in write lock mode (exclusive lock)
*/

// reads the transaction status as is from the cache, fails if the entry does not exists in the cache
// if an entry is found it is bumped in the cache, so as to avoid immediate eviction
static int get_transaction_status_from_cache(transaction_table* ttbl, uint256 transaction_id, transaction_status* status)
{
	// find one that equals, else fail with 0
	const passive_transaction_id_entry* ptid_p = find_equals_in_cachemap(&(ttbl->transaction_table_cache), &transaction_id);
	if(ptid_p == NULL)
		return 0;

	// then bump the found entry, retrieve status and return 1
	bump_element_in_cachemap(&(ttbl->transaction_table_cache), ptid_p);
	(*status) = ptid_p->status;
	return 1;
}

// inserts a new entry in the cache for the given transaction_id OR updates and bumps it, if an entry for the transaction_id exists
static void set_transaction_status_in_cache(transaction_table* ttbl, uint256 transaction_id, transaction_status status)
{
	// find one that equals
	passive_transaction_id_entry* ptid_p = (passive_transaction_id_entry*) find_equals_in_cachemap(&(ttbl->transaction_table_cache), &transaction_id);

	if(ptid_p == NULL)
	{
		// if there is space for a new entry create a new one
		if(ttbl->transaction_table_cache_capacity < get_element_count_cachemap(&(ttbl->transaction_table_cache)))
		{
			// if find fails, allocate a new entry
			ptid_p = malloc(sizeof(passive_transaction_id_entry));
			if(ptid_p == NULL)
				exit(-1);
		}
		else // else evict an existing one
		{
			ptid_p = (passive_transaction_id_entry*) get_evictable_element_from_cachemap(&(ttbl->transaction_table_cache));
			remove_from_cachemap(&(ttbl->transaction_table_cache), ptid_p);
		}

		// initialize it
		ptid_p->transaction_id = transaction_id;
		ptid_p->status = status;
		initialize_cchnode(&(ptid_p->embed_node));

		// insert it
		insert_in_cachemap(&(ttbl->transaction_table_cache), ptid_p);
	}
	else
	{
		// else if found, set status and bump the element
		ptid_p->status = status;
		bump_element_in_cachemap(&(ttbl->transaction_table_cache), ptid_p);
	}
}

// --

/*
	internal functions for currently_active_transaction_ids
	must be called with transaction_table_cache_lock held in read OR write lock mode as per access type
*/

// checks if a transaction_id exists in currently_active_transaction_ids
static const active_transaction_id_entry* find_in_currently_active_transaction_ids(const transaction_table* ttbl, uint256 transaction_id)
{
	return find_equals_in_bst(&(ttbl->currently_active_transaction_ids), &transaction_id, FIRST_OCCURENCE);
}

// iterate over all the currently_active_transaction_ids
static void for_each_in_order_in_currently_active_transaction_ids(const transaction_table* ttbl, void (*operation)(const void* data, const void* additional_params), const void* additional_params)
{
	for_each_in_bst(&(ttbl->currently_active_transaction_ids), IN_ORDER, operation, additional_params);
}

// insert to currently_active_transaction_ids
static int insert_in_currently_active_transaction_ids(transaction_table* ttbl, uint256 transaction_id)
{
	active_transaction_id_entry* atid_p = (active_transaction_id_entry*) find_equals_in_bst(&(ttbl->currently_active_transaction_ids), &transaction_id, FIRST_OCCURENCE);
	if(atid_p != NULL)
		return 0;

	// malloc a new entry
	atid_p = malloc(sizeof(active_transaction_id_entry));
	if(atid_p == NULL)
		exit(-1);

	// initialize it
	atid_p->transaction_id = transaction_id;
	initialize_bstnode(&(atid_p->embed_node));

	// then insert it
	return insert_in_bst(&(ttbl->currently_active_transaction_ids), atid_p);
}

// remove a entry from the currently_active_transaction_ids
static int remove_from_currently_active_transaction_ids(transaction_table* ttbl, active_transaction_id_entry* atid_p)
{
	int removed = remove_from_bst(&(ttbl->currently_active_transaction_ids), atid_p);

	free(atid_p);

	return removed;
}

// --

static void basic_structural_initialization(transaction_table* ttbl, cy_uint transaction_table_cache_capacity)
{
	initialize_rwlock(&(ttbl->transaction_table_cache_lock), NULL);

	initialize_bst(&(ttbl->currently_active_transaction_ids), RED_BLACK_TREE, &simple_comparator(compare_active_transaction_id_entry), offsetof(active_transaction_id_entry, embed_node));

	ttbl->transaction_table_cache_capacity = transaction_table_cache_capacity;

	initialize_cachemap(&(ttbl->transaction_table_cache), NULL, NEVER_PINNED, ((transaction_table_cache_capacity / 5) + 5), &simple_hasher(hash_passive_transaction_id_entry), &simple_comparator(compare_passive_transaction_id_entry), offsetof(passive_transaction_id_entry, embed_node));

	initialize_rwlock(&(ttbl->transaction_table_lock), NULL);
}

#include<rondb/mvcc_snapshot.h>

static void mvcc_snapshot_inserter(const void* data, const void* additional_params)
{
	mvcc_snapshot* snp = (mvcc_snapshot*) additional_params;
	const active_transaction_id_entry* atid_p = data;
	insert_in_progress_transaction_in_mvcc_snapshot(snp, atid_p->transaction_id);
}

mvcc_snapshot* get_new_transaction_id(transaction_table* ttbl)
{
	// allocate a mvcc snapshot
	mvcc_snapshot* snp = malloc(sizeof(mvcc_snapshot));
	if(snp == NULL)
		exit(-1);

	write_lock(&(ttbl->transaction_table_cache_lock), BLOCKING);

	// initialize mvcc snapshot
	initialize_mvcc_snapshot(snp, ttbl->next_assignable_transaction_id);
	for_each_in_order_in_currently_active_transaction_ids(ttbl, mvcc_snapshot_inserter, ttbl);
	finalize_mvcc_snapshot(snp);

	// increment ttbl->next_assignable_transaction_id
	if(!add_overflow_safe_uint256(&(ttbl->next_assignable_transaction_id), ttbl->next_assignable_transaction_id, get_1_uint256(), ttbl->overflow_transaction_id))
	{
		// overflow occurred
		printf("BUG (in transaction_table) :: transaction id overflow occurred\n");
		exit(-1);
	}

	// insert it as an active new transaction_id
	insert_in_currently_active_transaction_ids(ttbl, snp->transaction_id);

	write_lock(&(ttbl->transaction_table_lock), BLOCKING);

	write_unlock(&(ttbl->transaction_table_cache_lock));

	// set the transaction status in the persistent table
	set_transaction_status_in_table(ttbl, snp->transaction_id, TX_IN_PROGRESS);

	write_unlock(&(ttbl->transaction_table_lock));

	return snp;
}

transaction_status get_transaction_status(transaction_table* ttbl, uint256 transaction_id)
{
	write_lock(&(ttbl->transaction_table_cache_lock), BLOCKING);

	// check if it is currently active
	if(NULL != find_in_currently_active_transaction_ids(ttbl, transaction_id))
	{
		write_unlock(&(ttbl->transaction_table_cache_lock));
		return TX_IN_PROGRESS;
	}

	// try to get the cached copy
	transaction_status status;
	if(get_transaction_status_from_cache(ttbl, transaction_id, &status))
	{
		write_unlock(&(ttbl->transaction_table_cache_lock));
		return status;
	}

	read_lock(&(ttbl->transaction_table_lock), READ_PREFERRING, BLOCKING);

	write_unlock(&(ttbl->transaction_table_cache_lock));

	// try and fetch it from the disk
	if(!get_transaction_status_from_table(ttbl, transaction_id, &status))
	{
		// if you can not even find it in the table then it is a bug
		printf("BUG (in transaction_table) :: attempt to get transaction status for transaction id, that was never assigned\n");
		exit(-1);
	}

	// any transaction that was in status TX_IN_PROGRESS, before boot is considered to be committed
	if(compare_uint256(transaction_id, ttbl->next_assignable_transaction_id_at_boot) < 0 && status == TX_IN_PROGRESS)
	{
		set_transaction_status_in_table(ttbl, transaction_id, TX_ABORTED); // this operation can be done in just read lock as it is idempotent, and concurrent reads will always eventually get the fixed value
		status = TX_ABORTED;
	}

	read_unlock(&(ttbl->transaction_table_lock));

	return status;
}

int update_transaction_status(transaction_table* ttbl, uint256 transaction_id, transaction_status status)
{
	// you can voluntarily only write committed or aborted to the transaction_id
	if(status != TX_ABORTED && status != TX_COMMITTED)
	{
		printf("BUG (in transaction_table) :: caller attempting to set transaction status to something other than TX_ABORTED or TX_COMMITTED\n");
		exit(-1);
	}

	write_lock(&(ttbl->transaction_table_cache_lock), BLOCKING);

	// find a corresponsing active_transaction_id_entry and remove it from the active transactions
	active_transaction_id_entry* atid_p = (active_transaction_id_entry*) find_in_currently_active_transaction_ids(ttbl, transaction_id);
	if(NULL == atid_p)
	{
		printf("BUG (in transaction_table) :: caller attempting to update transaction status for a non-TX_IN_PROGRESS transaction\n");
		exit(-1);
	}
	remove_from_currently_active_transaction_ids(ttbl, atid_p);

	// insert a cached copy for it
	set_transaction_status_in_cache(ttbl, transaction_id, status);

	write_lock(&(ttbl->transaction_table_lock), BLOCKING);

	write_unlock(&(ttbl->transaction_table_cache_lock));

	set_transaction_status_in_table(ttbl, transaction_id, status);

	write_unlock(&(ttbl->transaction_table_lock));

	return 1;
}