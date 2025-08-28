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

#endif