#ifndef TRANSACTION_TABLE_H
#define TRANSACTION_TABLE_H

#include<cutlery/bst.h>
#include<cutlery/cachemap.h>

#include<serint/large_uints.h>

#include<lockking/rwlock.h>

#include<tupleindexer/page_table/page_table.h>
#include<tupleindexer/bitmap_page/bitmap_page.h>

#include<rondb/transaction_status.h>
#include<rondb/mvcc_snapshot.h>

typedef struct transaction_table transaction_table;
struct transaction_table
{
	// locks the completed transaction table under a read/write lock
	rwlock transaction_table_lock;

	// next transaction_id that was assignable at the beginning right after initialization of the transaction_table
	// any transaction_id in TX_IN_PROGRESS state (in persistent memory) before this transaction_id is and will be set as TX_ABORTED state, after any of it's access
	uint256 next_assignable_transaction_id_at_boot;

	// next transaction id to be assigned
	uint256 next_assignable_transaction_id;

	// this bst stores the ids of all the transaction that are IN_PROGRESS state and could be making progress
	// it is maintaned continuously (as new transaction_ids are assigned) and is used to generate mvcc_snapshot
	bst currently_active_transaction_ids;

	// cache to quickly access frequently accessed transaction_ids and their statuses
	// only holds transaction_id -> transaction_status mappings for TX_COMMITTED or TX_ABORTED transactions (not for TX_IN_PROGRESS transactions)
	cy_uint transaction_table_cache_capacity; // this is the number of elements that the transaction_table_cache is allowed to hold
	cachemap transaction_table_cache;

	// below attributes only work with the transaction_table that is persistently stored on the disk

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
	const page_table_tuple_defs* pttd_p; // actual page_table definition
	const tuple_def* bitmap_page_tuple_def_p; // actual tuple def of the bitmap_page to access them bit_field at a time
};

// gives you a new unassiged transaction_id wrappin it in a mvcc_snapshot
// this transaction will be in TX_IN_RPOGRESS status
mvcc_snapshot* get_new_transaction_id(transaction_table* ttbl);

// exit(-1) if you pass in an unassigned transaction_id
// else returns the status
transaction_status get_transaction_status(transaction_table* ttbl, uint256 transaction_id);

// updates transaction_status of the transaction from TX_IN_PROGRESS to either TX_COMMITTED or TX_ABORTED status
int update_transaction_status(transaction_table* ttbl, uint256 transaction_id, transaction_status status);

#endif