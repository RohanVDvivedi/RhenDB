#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include<lockking/rwlock.h>
#include<lockking/glock.h>

#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/bplus_tree/bplus_tree.h>

#include<rondb/rage_engine.h>

#include<cutlery/hashmap.h>

/*
	1 transaction_id can lock 1 resource in some known lock_mode; i.e. (transaction_id, (resource_type, resource_id)) -> lock_mode, mapping has a unique mapping

	1 transaction_id may have multiple threads (referenced using the task_id) executing the transaction, hence multiple threads of the same transactions are allowed to acquire multiple locks simultaneously and block until it is acquired or transitioned

	locks are not owned by the task's of the transaction, the are owned by the transaction itself
	it is just that specific task that is waiting for the lock is woken up / notified if it could be unblocked
*/

typedef struct lock_manager_notifier lock_manager_notifier;
struct lock_manager_notifier
{
	void* context_p;

	// the below callback is called when a task belonging to a transaction is unblocked, due to some other transaction having released the locks that it waiting on
	void (*notify_unblocked)(void* context_p, uint256 transaction_id, uint32_t task_id);

	// notify that a transaction has encountered a deadlock and must step down, and abort itself
	void (*notify_deadlocked)(void* context_p, uint256 transaction_id);

	// both the above functions are called with the external_lock mutex held
};

typedef struct lock_manager lock_manager;
struct lock_manager
{
	// external_lock is only ever locked by the deadlock detection thread and noone else
	pthread_mutex_t* external_lock;

	// all notify_*() functons are called by the lock_manager with the external_lock (above) held
	lock_manager_notifier notifier;

	// every registered glock_matrix for all lock_type-s go here
	uint32_t locks_type_count;
	glock_matrix* lock_matrices;

	// this is the record_def for the lock_table's records
	tuple_def* lock_record_def;

	// lock_record_def looks like the lock_entry
	// it only preserves and makes lock_table hold locks that are held by some transaction

	// index that stores (transaction_id, resource_type, resource_id) -> lock_mode
	uint64_t tx_locks_root_page_id;
	bplus_tree_tuple_defs* tx_locks_td;

	// index that stores (resource_type, resource_id, transaction_id) -> lock_mode
	uint64_t rs_locks_root_page_id;
	bplus_tree_tuple_defs* rs_locks_td;

	// above tables can only be modified by the the transaction that acquire, release or transition locks

	// this is the record_def for the wait_table's records
	tuple_def* wait_record_def;

	// the bplus_tree that stores entries for (waiting_transaction_id, waiting_task_id, waits_for(transaction_id), waits_for(resource_type), waits_for(resource_id))
	uint64_t waits_for_root_page_id;
	bplus_tree_tuple_defs* waits_for_td;

	// the bplus_tree that stores entries for (waits_for(transaction_id), waits_for(resource_type), waits_for(resource_id), waiting_transaction_id, waiting_task_id)
	uint64_t waits_back_root_page_id;
	bplus_tree_tuple_defs* waits_back_td;

	// above tables can only be modified by the (waiting_transaction_id, waiting_task_id) going to or returning from the wait, using the acquire function
	// waits_for(transaction_id), can only read them and call the notify_unblocked(waiting_transaction_id, waiting_task_id), never modify the entries

	// below is the volatile non-ACID rage_engine that powers the transaction_table
	// preferrably an implementation of the VolatilePageStore based rage_engine
	rage_engine* ltbl_engine;
};

// maximum number of bytes to be allocated for the resource_id of the resource to be locked
#define MAX_RESOURCE_ID_SIZE 16

fail_build_on(MAX_RESOURCE_ID_SIZE > 100)

// max_active_transaction_count is the capacity used to initialize the bucket_count for the active_transactions
void initialize_lock_manager(lock_manager* lckmgr_p, pthread_mutex_t* external_lock, lock_manager_notifier notifier, uint256 overflow_transaction_id, rage_engine* ltbl_engine);

// registering a lock_type is same as registering a resource_type
// both of them dictate what lock_mode-s you can use with them
uint32_t register_lock_type_with_lock_manager(lock_manager* lckmgr_p, glock_matrix lock_matrix);

typedef enum lock_result lock_result;
enum lock_result
{
	// success results
	ACQUIRED,
	TRANSITIONED, // successfully changed lock_mode to a new one, acquire and try_lock are reentrant, and they change the lock_mode instead if you try to lock the same resource
	ALREADY_HELD, // the old_lock_mode == lock_mode passed

	// failure results
	FAILED, // returned when there are conflicts in acquiring the lock or transitioning the lock_mode, and the non_blocking = 1
	MUST_BLOCK, // returned when there are conflicts in acquiring the lock or transitioning the lock_mode, and the non_blocking = 0
	// when MUST_BLOCK is returned, the wait_entry-s for the lock have been already inserted, and the caller is expected to block or try again later
};

// no wait_entry-s are inserted on a non_blocking = 1 call, and instead FAILED is returned on encountering a conflict
// task_id is expected to be one of the individual threads working on behalf of the transaction_id
// a lock is held not by the task_id of a transaction_id instead by the transaction_id itself
// but multiple tasks are allowed to wait for the same resource (hopefully not by design)
// i.e. a task_id can take on locks acquired by another task_id
// while a notify_on_unblocked call back is task specific, waking up only the respective task that is blocked waiting for the lock
// by design you must call this and all lock_manager functions with external global mutex held, and wait using condition variable or deschedule while this mutex is held, to avoid missed notifications to wakeup from blocked state
lock_result acquire_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, int non_blocking);

// discards all wait_entries for the transaction_id and task_id
// this is done by the acquire_lock_*() everytime you call it, but if you want to start winding up, for safety you may call this, to let the lock_manager know so that it would not send you a deadlock signal out of the blue
// ideally, on receiving MUST_BLOCK, you must block ad again call the acquire_lock_*() function to again possibly get the same lock, after being unblocked
// this function is to let the lock_manager know that you are no longer waiting for the same lock
void notify_task_unblocked_to_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id);

void release_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size);

void release_all_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id);

#endif