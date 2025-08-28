#ifndef LOCK_MANAGER_H
#define LOCK_MANAGER_H

#include<lockking/rwlock.h>

#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/bplus_tree/bplus_tree.h>

#include<rondb/rage_engine.h>

typedef struct lock_manager lock_manager;
struct lock_manager
{
	// main mutex controlling everything inside the lock_manager
	pthread_mutex_t manager_lock;

	// since the lock_table required a volatile non-ACID rage_engine
	// we need this lock to protect the volatile contents of the lock_table
	// that includes the lock_table and it's volatile indices
	rwlock table_lock;

	// this is the record_def for the lock_table's records
	// this tuple_def is similar to the lock_entry given below
	tuple_def* lock_record_def;

	// root of the lock_table and it's heap_table's tuple_defs
	uint64_t lock_table_root_page_id;
	heap_table_tuple_defs lock_table_td;

	// index that stores transaction_id -> lock
	uint64_t tx_index_root_page_id;
	bplus_tree_tuple_defs tx_index_td;

	// index that stores (resource_id, lock_type) -> lock
	uint64_t rt_index_root_page_id;
	bplus_tree_tuple_defs rt_index_td;

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
	uint32_t resource_type;

	// the resource that is locked it is atmost 16 bytes wide, i.e. 128 bits
	uint8_t resource_id_size;
	uint8_t resource_id[MAX_RESOURCE_ID_SIZE];

	// lock mode is the mode of the lock, for instance a (resource_type ==) reader_writer_lock has 2 modes, READ_MODE and WRITE_MODE
	uint16_t lock_mode;

	// lock_state is a 1-bit field that suggests if the lock_entry belongs to the waiting members or the lock is held
	lock_state lock_state;

	// this is the time when this lock_entry was created for lock_state == WAITING,
	// or if the lock_state == LOCK_HELD, this is the time when the lock_state changed to LOCK_HELD
	uint64_t updated_at;
};

#endif