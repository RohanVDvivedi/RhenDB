#ifndef TRANSACTION_H
#define TRANSACTION_H

#include<rhendb/rhendb.h>
#include<rhendb/mvcc_snapshot.h>
#include<rhendb/query_plan_interface.h>

typedef struct query_plan query_plan;

typedef struct transaction transaction;
struct transaction
{
	// database that this transaction belongs to
	rhendb* db;

	// mvcc snapshot, for snapshot isolation
	mvcc_snapshot* snapshot;

	// transaction_id points to the transaction_id in the snapshot
	uint256* transaction_id;

	// actual query plan, for the current query being executed
	query_plan* curr_query;
};

#endif