#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include<lockking/rwlock.h>
#include<lockking/glock.h>

#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/bplus_tree/bplus_tree.h>

#include<rondb/rage_engine.h>

typedef struct lock_manager lock_manager;
struct lock_manager
{
	// main mutex controlling everything inside the lock_manager
	pthread_mutex_t manager_lock;

	// every registered glock_matrix for all lock_type-s go here
	// protected solely by the manager_lock, it is just an index fetch away, so no need to have a rwlock for it
	uint32_t locks_type_count;
	glock_matrix* lock_matrices;

	// since the lock_table required a volatile non-ACID rage_engine
	// we need this lock to protect the volatile contents of the lock_table
	// that includes the lock_table and it's volatile indices
	rwlock table_lock;

	// this is the record_def for the lock_table's records
	// this tuple_def is similar to the lock_entry given below
	tuple_def* lock_record_def;

	// root of the lock_table and it's heap_table's tuple_defs
	uint64_t lock_table_root_page_id;
	heap_table_tuple_defs* lock_table_td;

	// index that stores (transaction_id, resource_type, resource_id, lock_state, lock_mode) -> lock
	uint64_t tx_index_root_page_id;
	bplus_tree_tuple_defs* tx_index_td;

	// index that stores (resource_type, resource_id, lock_state, lock_mode) -> lock
	uint64_t rt_index_root_page_id;
	bplus_tree_tuple_defs* rt_index_td;

	// below is the volatile non-ACID rage_engine that powers the transaction_table
	// preferrably an implementation of the VolatilePageStore based rage_engine
	rage_engine* ltbl_engine;
};

typedef enum lock_state lock_state;
enum lock_state
{
	WAITING = 0,
	LOCK_HELD = 1,
};

// maximum number of bytes to be allocated for the resource_id of the resource to be locked
#define MAX_RESOURCE_ID_SIZE 16

typedef struct lock_entry lock_entry;
struct lock_entry
{
	// transaction_id that has locked or is waiting for the lock
	uint256 transaction_id;

	// this is the resource_type for the below resource_id
	// this dictates what lock_modes can be used for this resource_type
	// this implies it is same as the type of the lock to be used on this resource
	// i.e. resource_type = lock_type, resulting into fixed usable number of lock_mode-s that you can acquire on it
	uint32_t resource_type;

	// the resource that is locked it is atmost 16 bytes wide, i.e. 128 bits
	uint8_t resource_id_size;
	uint8_t resource_id[MAX_RESOURCE_ID_SIZE];

	// lock_state is a 1-bit field that suggests if the lock_entry belongs to the waiting members or the lock is held
	lock_state lock_state;

	// lock mode is the mode of the lock, for instance a (resource_type ==) reader_writer_lock has 2 modes, READ_MODE and WRITE_MODE
	uint32_t lock_mode;

	// this is the time when this lock_entry was created for lock_state == WAITING,
	// or if the lock_state == LOCK_HELD, this is the time when the lock_state changed to LOCK_HELD
	uint64_t updated_at;
};

void initialize_lock_manager(lock_manager* lckmgr_p, uint256 overflow_transaction_id, rage_engine* ltbl_engine);

// registering a lock_type is same as registering a resource_type
// both of them dictate what lock_mode-s you can use with them
uint32_t register_lock_type_with_lock_manager(lock_manager* lckmgr_p, glock_matrix lock_matrix);

typedef enum lock_result lock_result;
enum lock_result
{
	ACQUIRED, // success
	FAILED,   // happens when timeout_in_seconds = NON_BLOCKING
	TIMEOUT,  // happens when timeout_in_seconds = BLOCKING or some non-zero value
	DEADLOCK, // you must abort
};

// timeout value can also be BLOCKING and NON_BLOCKING
lock_result acquire_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint16_t lock_mode, uint64_t timeout_in_seconds);

// timeout value can also be BLOCKING and NON_BLOCKING
lock_result modify_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint16_t old_lock_mode, uint16_t new_lock_mode, uint64_t timeout_in_seconds);

void release_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint16_t lock_mode);

void release_all_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id);

#endif