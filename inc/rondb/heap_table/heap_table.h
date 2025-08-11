#ifndef HEAP_TABLE_H
#define HEAP_TABLE_H

#include<lockking/rwlock.h>

#include<cutlery/singlylist.h>

#include<boompar/periodic_job.h>

#include<tupleindexer/bplus_tree/bplus_tree.h>
#include<tupleindexer/heap_page/heap_page.h>

#include<rondb/rage_engine.h>

/*
	heap table consists of two primary things
	1. free_space_tree
	  * volatile b+tree powered by volatile_page_store, possibly,
	  * protected by free_space_tree_lock (LockKing/rwlock),
	  * only hints at the amount of free_space taken up by any of the heap_pages in the heap_table
	  * it is a b+tree of (free_space, page_id)
	2. heap_pages_tree
	  * ACID-persistent b+tree powered by min_tx_engine, possibly,
	  * protected by latch-crabbing rules of b+tree and the page locks of min_tx_engine,
	  * only stores page_ids of all the persistent heap_pages in ascending order
	  * it is a b+tree of (page_id)
*/

typedef struct heap_table heap_table;
struct heap_table
{
	rwlock free_space_tree_lock;
	uint64_t free_space_tree_root_page_id;
	bplus_tree_tuple_defs* free_space_tree_defs;

	rage_engine* free_space_tree_engine; // volatile non-acid engine, preferrably volatilepagestore

	uint64_t heap_pages_tree_root_page_id;
	bplus_tree_tuple_defs* heap_pages_tree_defs;

	rage_engine* heap_pages_tree_engine; // persistent acid engine, preferrably mintxengine

	// only this job is allowed to asynchronously insert/remove entries from the free_space_tree
	periodic_job* free_space_tree_modifier_job;
	singlylist free_space_tree_modifying_params_list; // for this params are to be inserted into this singlylist and they act as a queue, push at tail and pop from head
	pthread_mutex_t free_space_tree_modifying_params_lock; // finally we need a chlidmost lock to manage concurrency for inserting and removing elements from the free_space_tree_modifying_params_list
};

// for all the below functions the transaction_id must belong to the persistent ACID engine instance that is used for the heap_pages and the heap_pages_tree
// the volatile engine used for the free_space tree is not 

// free_space_tree functions

#include<rondb/heap_table/free_space_tree.h>

// heap_table-iterator and functions

#include<rondb/heap_table/heap_table_iterator.h>

#endif