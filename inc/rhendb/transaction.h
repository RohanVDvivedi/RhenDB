#ifndef TRANSACTION_H
#define TRANSACTION_H

#include<rhendb/rhendb.h>
#include<rhendb/mvcc_snapshot.h>
#include<rhendb/query_plan_interface.h>

typedef struct query_plan query_plan;

// the transaction struct only consists of pointers to already created structs, and need to be managed by you
// it is basically a place holder for you (the user), and noone else, the application will not be handling or initializing it for you

// this struct must be 0 initiialized, before trying to use it

typedef struct transaction transaction;
struct transaction
{
	// database that this transaction belongs to
	rhendb* db;

	// mvcc snapshot, for snapshot isolation
	mvcc_snapshot snapshot;

	// transaction_id points to the transaction_id in the snapshot for this transaction
	uint256* transaction_id;

	// actual query plan, for the current query being executed
	query_plan* curr_query;
};

// helper compare and hash functions to insert the transaction in your data structures

int compare_transaction_by_transaction_id(const void* tx1, const void* tx2);

cy_uint hash_transaction_by_transaction_id(const void* tx);

#endif