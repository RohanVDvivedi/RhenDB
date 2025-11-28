#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include<pthread.h>

#include<tupleindexer/bplus_tree/bplus_tree.h>

#include<rhendb/rage_engine.h>

#include<cutlery/hashmap.h>

/*
	1 transaction can lock 1 resource in some known lock_mode; i.e. (transaction, (resource_type, resource_id)) -> lock_mode, mapping has a unique mapping

	1 transaction may have multiple threads (referenced using the task) executing the transaction, hence multiple threads of the same transactions are allowed to acquire multiple locks simultaneously and block until it is acquired or transitioned

	locks are not owned by the task's of the transaction, they are owned by the transaction itself
	it is just that specific task that is waiting for the lock is woken up / notified if it could be unblocked
*/

/*
	The api is designed to take in transaction (a void pointer), and task (again a void pointer), they must not be NULL
	They must be stable pointers (data not copyable to a new location) pointing to your struct representations at fixed locations

	These structs and their pointers (because they are stable pointers) must represent a logical transaction or it's belonging task uniquely
*/

typedef struct lock_manager_notifier lock_manager_notifier;
struct lock_manager_notifier
{
	void* context_p;

	// the below callback is called when a task belonging to a transaction is unblocked, due to some other transaction having released the locks that it is waiting on
	void (*notify_unblocked)(void* context_p, void* transaction, void* task);

	// notify that a transaction has encountered a deadlock and must step down, and abort itself
	void (*notify_deadlocked)(void* context_p, void* transaction);

	// both the above functions are called with the external_lock mutex held
};

typedef struct lock_manager lock_manager;
struct lock_manager
{
	// external_lock is only ever locked by the deadlock detection thread and noone else
	// this lock is left for external users to co-ordinate with so that (to-be) waiters can actually put themselves to wait right after they have been notified for the same
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

	// index that stores (transaction, resource_type, resource_id) -> lock_mode
	uint64_t tx_locks_root_page_id;
	bplus_tree_tuple_defs* tx_locks_td;

	// index that stores (resource_type, resource_id, transaction) -> lock_mode
	uint64_t rs_locks_root_page_id;
	bplus_tree_tuple_defs* rs_locks_td;

	// above tables can only be modified by the the transaction that acquire, release or transition locks

	// this is the record_def for the wait_table's records
	tuple_def* wait_record_def;

	// the bplus_tree that stores entries for (waiting_transaction, waiting_task, waits_for(transaction), waits_for(resource_type), waits_for(resource_id))
	uint64_t waits_for_root_page_id;
	bplus_tree_tuple_defs* waits_for_td;

	// the bplus_tree that stores entries for (waits_for(transaction), waits_for(resource_type), waits_for(resource_id), waiting_transaction, waiting_task)
	uint64_t waits_back_root_page_id;
	bplus_tree_tuple_defs* waits_back_td;

	// above tables can only be modified by the (waiting_transaction, waiting_task) going for or returning from the wait, using the acquire function
	// waits_for(transaction), can only read them and call the notify_unblocked(waiting_transaction, waiting_task), and never modify the entries

	// below is the volatile non-ACID rage_engine that powers the transaction_table
	// preferrably an implementation of the VolatilePageStore based rage_engine
	rage_engine* lckmgr_engine;

	// for internal use
	data_type_info* resource_id_type_info;
};

/*
	If you have a multi threaded function, you must use the external_lock and must call any of the functions below with the external_lock held
*/

// maximum number of bytes to be allocated for the resource_id of the resource to be locked
#define MAX_RESOURCE_ID_SIZE 25

fail_build_on(MAX_RESOURCE_ID_SIZE > 100)

fail_build_on(sizeof(void*) > sizeof(uint64_t))

// max_active_transaction_count is the capacity used to initialize the bucket_count for the active_transactions
void initialize_lock_manager(lock_manager* lckmgr_p, pthread_mutex_t* external_lock, const lock_manager_notifier* notifier, rage_engine* ltckmgrengine);

// registering a lock_type is same as registering a resource_type
// both of them dictate what lock_mode-s you can use with them
uint32_t register_lock_type_with_lock_manager(lock_manager* lckmgr_p, glock_matrix lock_matrix);

// below function lets you query if the transaction in context holds lock on the provided resource
// this function removes all wait_entries corresponding the transaction and it's task, because making this call proves that this task is no longer waiting/blocked
// but remember that the lock is always held by the transaction as a whole and never by it's task, so task is only used to remove the wait entries
#define NO_LOCK_HELD_LOCK_MODE UINT32_MAX
uint32_t get_lock_mode_for_lock_from_lock_manager(lock_manager* lckmgr_p, void* transaction, void* task, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size);

typedef enum lock_result lock_result;
enum lock_result
{
	// success results
	LOCK_ACQUIRED,
	LOCK_TRANSITIONED, // successfully changed lock_mode to a new one, acquire and try_lock are reentrant, and they change the lock_mode instead if you try to lock the same resource
	LOCK_ALREADY_HELD, // the old_lock_mode == lock_mode passed

	// failure results
	LOCKING_FAILED, // returned when there are conflicts in acquiring the lock or transitioning the lock_mode, and the non_blocking = 1
	MUST_BLOCK_FOR_LOCK, // returned when there are conflicts in acquiring the lock or transitioning the lock_mode, and the non_blocking = 0
	// when MUST_BLOCK is returned, the wait_entry-s for the lock have been already inserted, and the caller is expected to block or try again later
};

extern char const * const lock_result_strings[];

// no wait_entry-s are inserted on a non_blocking = 1 call, and instead FAILED is returned on encountering a conflict
// task is expected to be one of the individual threads working on behalf of the transaction

// a lock is held not by the task of a transaction, but instead by the transaction itself
// but multiple tasks are allowed to wait for the same resource
// i.e. a task can take on locks acquired by another task

// by design you must call this and all lock_manager functions with external global mutex held, and wait using condition variable or deschedule while this mutex is held, to avoid missed notifications to wakeup from blocked state
lock_result acquire_lock_with_lock_manager(lock_manager* lckmgr_p, void* transaction, void* task, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, int non_blocking);

// since a task is not actually holding the lock, its the transaction that holds the lock, so any task can release the lock that was priorly acquired by any other task
// i.e. you may release locks using a dummy task (lets say = 100) that were in the past, requested by and granted to some (already completed) task = 55
// the task is accpeted by this function just to discard all the pending wait-entries by this transaction, task, because it is now known to have been not blocked and is deemed to be active
void release_lock_with_lock_manager(lock_manager* lckmgr_p, void* transaction, void* task, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size);

// the below 2 tasks are identical, they just remove wair-entries where the (transaction, task) are set as the (waiting_transaction, waiting_task)
// these functions can be used when the task of a transaction terminates, or the particular query terminates, to discard all the corresponding wait_entries, as they no longer will be waiting
void discard_all_wait_entries_for_task_in_lock_manager(lock_manager* lckmgr_p, void* transaction, void* task);
void discard_all_wait_entries_for_transaction_in_lock_manager(lock_manager* lckmgr_p, void* transaction);

// this function does remove all the wait-entries for the transaction as a whole for all it's tasks
// this will also release all the locks and also issue notify_unblocked() to all the transactions and tasks that were waiting for the resource that this transaction has locks on
// so call this function only after you join all the tasks executing on behalf of the transaction (rigth after committing)
void conclude_all_business_with_lock_manager(lock_manager* lckmgr_p, void* transaction);

// prints all the contents of the lock manager to the printf
void debug_print_lock_manager_tables(lock_manager* lckmgr_p);

extern const glock_matrix RW_DB_LOCK;
#define RW_DB_LOCK_R_MODE 0
#define RW_DB_LOCK_W_MODE 1

#endif