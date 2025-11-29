#ifndef TRANSACTION_TABLE_H
#define TRANSACTION_TABLE_H

#include<cutlery/bst.h>
#include<cutlery/cachemap.h>

#include<serint/large_uints.h>

#include<lockking/rwlock.h>

#include<tupleindexer/page_table/page_table.h>
#include<tupleindexer/bitmap_page/bitmap_page.h>
#include<tupleindexer/interface/page_access_methods.h>

#include<rhendb/rage_engine.h>

#include<rhendb/transaction_status.h>
#include<rhendb/mvcc_snapshot.h>

typedef struct transaction_table transaction_table;
struct transaction_table
{
	// next transaction_id that was assignable at the beginning right after initialization of the transaction_table
	// any transaction_id in TX_IN_PROGRESS state (in persistent memory) before this transaction_id is and will be set as TX_ABORTED state, after any of it's access
	// this is read-only attribute
	uint256 next_assignable_transaction_id_at_boot;

	// locks the next_assignable_transaction_id, currently_active_transaction_ids and transaction_table_cache
	// transaction_table_cache bumps on even every read of its entry, so you will need an exclusive lock to even read it
	rwlock transaction_table_cache_lock;

	// next transaction id to be assigned
	uint256 next_assignable_transaction_id;

	// no transction_id gretaer than or equal to overflow_transaction_id will ever be assigned
	uint256 overflow_transaction_id;

	// this bst stores the ids of all the transaction that are IN_PROGRESS state and could be making progress
	// it is maintaned continuously (as new transaction_ids are assigned) and is used to generate mvcc_snapshot
	bst currently_active_transaction_ids;

	// cache to quickly access frequently accessed transaction_ids and their statuses
	// only holds transaction_id -> transaction_status mappings for TX_COMMITTED or TX_ABORTED transactions (not for TX_IN_PROGRESS transactions)
	cy_uint transaction_table_cache_capacity; // this is the number of elements that the transaction_table_cache is allowed to hold
	cachemap transaction_table_cache;

	// below attributes only work with the transaction_table that is persistently stored on the disk

	// lock to protect the disk resident transaction table
	// take it with the transaction_table_cache_lock still held and then release the transaction_table_cache_lock
	// you may hold only a read lock, while reading its entries
	// but you must hold a write lock for an update, this allows the cache to be consistently holding the right values, even while writing a new one to the disk
	rwlock transaction_table_lock;

	/*
		transaction table on the disk is maintained as a page_table pointing to bitmap pages
		no lock is actually needed to access it, as MinTxEngine takes care of it's ACID-compliant access, (unless ofcourse while assigning a brand new transaction_id)
		each bitmap page is composed of transaction_statuses_per_bitmap_page number of bit_fields, each bitfield is 2 bits wide
		the status values being
		0b00 -> UNASSIGNED
		remaining are assigned as per the same rules as transaction_status.h
		TX_IN_PROGRESS = 0b01,
		TX_ABORTED     = 0b10,
		TX_COMMITTED   = 0b11,
		this specific assignment helps, because bitmap fields start by value of 0 only
	*/

	uint64_t transaction_table_root_page_id;

	uint32_t transaction_statuses_per_bitmap_page;

	// below two attributes will be used to access the transaction_table on the disk
	page_table_tuple_defs* pttd_p; // actual page_table definition
	tuple_def* bitmap_page_tuple_def_p; // actual tuple def of the bitmap_page to access them bit_field at a time

	// below is the persistent ACID rage_engine that powers the transaction_table
	// preferrably an implementation of the MinTxEngine based rage_engine
	rage_engine* ttbl_engine;
};

// here the root_page_id is an in-out parameter, pass it as NULL_PAGE_ID to create a new transaction table, or an existing one to open that particular transaction_table
void initialize_transaction_table(transaction_table* ttbl, uint64_t* root_page_id, rage_engine* ttbl_engine, uint32_t transaction_table_cache_capacity);

// gives you a new unassiged transaction_id wrappin it in a mvcc_snapshot
// this transaction will be in TX_IN_RPOGRESS status
void get_new_transaction_id(transaction_table* ttbl, mvcc_snapshot* snp);

// for read committed isolation level you may later call this, this will not modify the existing self_transaction_id of the snapshot
void revise_mvcc_snapshot(transaction_table* ttbl, mvcc_snapshot* snp);

// in both the above 2 function it is assumed that the snp struct is in initialized condition

// exit(-1) if you pass in an unassigned transaction_id
// else returns the status
transaction_status get_transaction_status(transaction_table* ttbl, uint256 transaction_id);

// updates transaction_status of the transaction from TX_IN_PROGRESS to either TX_COMMITTED or TX_ABORTED status
// this implies that the transaction_id provided must be in the currently_active_transaction_ids bst
int update_transaction_status(transaction_table* ttbl, uint256 transaction_id, transaction_status status);

// returns horizon transaction id, changes of all transactions under this value are visible to one or other future transaction
uint256 get_vaccum_horizon_transaction_id(transaction_table* ttbl);

#endif