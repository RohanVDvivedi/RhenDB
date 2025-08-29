#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include<lockking/rwlock.h>
#include<lockking/glock.h>

#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/bplus_tree/bplus_tree.h>

#include<rondb/rage_engine.h>

/*
	1 transaction_id can lock 1 resource in only 1 lock_mode; i.e. (transaction_id, (resource_type, resource_id)) -> lock_mode, mapping has a unique mapping
*/

typedef struct lock_manager lock_manager;
struct lock_manager
{
	// main mutex controlling everything inside the lock_manager
	pthread_mutex_t manager_lock;

	// every registered glock_matrix for all lock_type-s go here
	// protected solely by the manager_lock, it is just an index fetch away, so no need to have a rwlock for it
	uint32_t locks_type_count;
	glock_matrix* lock_matrices;

	// active_transactions are expected to be finite, limiting the size of the below hashmaps to just that many

	// hashmap of active transactions, that are either holding locks, or are waiting for a lock
	// transaction_id -> active_transaction_entry
	hashmap active_transactions;

	// since the lock_table required a volatile non-ACID rage_engine
	// we need this lock to protect the volatile contents of the lock_table
	// that includes the lock_table and it's volatile indices
	rwlock table_lock;

	// this is the record_def for the lock_table's records
	tuple_def* lock_record_def;

	// lock_record_def looks like the lock_entry
	// it only preserves and makes lock_table hold locks that are held by some transaction

	// index that stores (transaction_id, resource_type, resource_id) -> lock_mode
	/* used to
		1. release locks on all resources for a transaction_id, search locks for a particular transaction_id
	*/
	uint64_t tx_locks_root_page_id;
	bplus_tree_tuple_defs* tx_locks_td;

	// index that stores (resource_type, resource_id, transaction_id) -> lock_mode
	/* used to
		1. release acquired lock, searched by resource, transaction_id and lock_mode
		2. check conflicts to acquire lock or modify lock_mode, seach locks for a particular resource
	*/
	uint64_t rs_locks_root_page_id;
	bplus_tree_tuple_defs* rs_locks_td;

	// above tables can only be modified by the the transaction that acquire, release or transition locks

	// composed of 2 transaction_id-s for waits_for_td and waits_back_td
	tuple_def* wait_record_def;

	// the bplus_tree that stores entries for (waiting_transaction_id, waits_for(transaction_id), waits_for(resource_type), waits_for(resource_id))
	/*
	**	used for deadlock detection, traversal_flags is the scratch pad
	*/
	uint64_t waits_for_root_page_id;
	bplus_tree_tuple_defs* waits_for_td;

	// the bplus_tree that stores entries for (waits_for(transaction_id), waits_for(resource_type), waits_for(resource_id), waiting_transaction_id)
	/*
	**	used for waking up all waiting_transaction_id, when locks are released or transitioned by any particular transaction, traversal_flags is the scratch pad
	*/
	uint64_t waits_back_root_page_id;
	bplus_tree_tuple_defs* waits_back_td;

	// above tables can only be modified by the waiting_transaction_id, going to or returning from the wait, and only waiting_transaction_id is allowed to insert or remove it's entry from these tables
	// waits_for(transaction_id), can only read them and broadcast the corresponding waiting_transaction_id

	// waits_for graph dfs for deadlock detection uses a linked_page_list to store the stack of all the seen entries

	// below is the volatile non-ACID rage_engine that powers the transaction_table
	// preferrably an implementation of the VolatilePageStore based rage_engine
	rage_engine* ltbl_engine;
};

// maximum number of bytes to be allocated for the resource_id of the resource to be locked
#define MAX_RESOURCE_ID_SIZE 16

fail_build_on(MAX_RESOURCE_ID_SIZE > 100)

// max_active_transaction_count is the capacity used to initialize the bucket_count for the active_transactions
void initialize_lock_manager(lock_manager* lckmgr_p, uint256 overflow_transaction_id, uint32_t max_active_transaction_count, rage_engine* ltbl_engine);

// registering a lock_type is same as registering a resource_type
// both of them dictate what lock_mode-s you can use with them
uint32_t register_lock_type_with_lock_manager(lock_manager* lckmgr_p, glock_matrix lock_matrix);

typedef enum lock_result lock_result;
enum lock_result
{
	ACQUIRED,     // success OR the lock is already held in the desired lock_mode, making the call acquire_*() and modify_*() functions reentrant

	// all the three below enum values signify the call to acquire_*() and modify_*() has failed

	ALREADY_HELD, // lock for the given resource_type and resource_id is held in some other lock_mode by the transaction_id in context
	FAILED,       // happens when timeout_in_seconds = NON_BLOCKING, or the old_lock_mode can not transition into the new_lock_mode, due to conflicts with lock held in lock_mode-s with other transactions
	TIMEOUT,      // happens when timeout_in_seconds = BLOCKING or some non-zero value
	DEADLOCK,     // you must abort
};

// timeout value can also be BLOCKING and NON_BLOCKING
// acquires the lock or transition the lock into the new_lock_mode, old_lock_mode can be equal to the lock not being held
lock_result acquire_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint16_t new_lock_mode, uint64_t timeout_in_milliseconds);

void release_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint16_t lock_mode);

void release_all_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id);

#endif