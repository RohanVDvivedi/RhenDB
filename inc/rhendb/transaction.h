#ifndef TRANSACTION_H
#define TRANSACTION_H

#include<rhendb/rhendb.h>
#include<rhendb/mvcc_snapshot.h>
#include<rhendb/operator_interface.h>

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

struct query_plan
{
	// operators scans, writers, and also the joins, sorts and groupbys
	uint64_t operators_count;
	operator* operators;

	// operator outputs including the intermediate ones
	uint64_t result_buffers_count;
	operator_buffer* result_buffers;

	// output tuples of the query
	operator_buffer* output;
};

#endif