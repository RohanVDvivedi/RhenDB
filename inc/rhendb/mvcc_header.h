#ifndef MVCC_HEADER_H
#define MVCC_HEADER_H

#include<tuplestore/tuple_def.h>

// below function generates a mvcc_header tuple_def to be used by all of the database based on what transaction_id_width [1,32] it is using
// it consists of
/*
	is_xmin_committed BIT_FIELD[1] (non_nullable)
	is_xmin_aborted   BIT_FIELD[1] (non_nullable)

	xmin              LARGE_UINT[transaction_id_width] (non_nullable)

	is_xmax_committed BIT_FIELD[1] (non_nullable)
	is_xmax_aborted   BIT_FIELD[1] (non_nullable)

	xmax              LARGE_UINT[transaction_id_width] (nullable, tuple is not yet marked deleted if this field is NULL)
*/
tuple_def* get_mvcc_header_tuple_definition(uint8_t transaction_id_width);

/*
	a funny thing to note before moving forward
	the values is_xm**_committed and is_xm**_aborted are advisory fields, and atmost only one of them can be and will be set suggesting if the xm** was commited or aborted
	if none of them are set then, the status of xm** transaction_id is unclear and you need to refer to the transaction table to figure it out
*/

typedef struct transaction_id_with_hints transaction_id_with_hints;
struct transaction_id_with_hints
{
	// is_committed and is_aborted must have atmost 1 of them set
	// if both of them are unset then the status of the transaction in context is unclear and must be fetched from transaction table
	int is_committed:1;
	int is_aborted:1;

	uint256 transaction_id;
};

// in-memory representation of the mvcc header
typedef struct mvcc_header mvcc_header;
struct mvcc_header
{
	transaction_id_with_hints xmin;

	int is_xmax_NULL:1; // xmax makes sense if this bit is 0

	transaction_id_with_hints xmax; // does not make sense if is_xmax_NULL is set
};

void read_mvcc_header(mvcc_header* mvcchdr_p, const void* mvcchdr_tup, const tuple_def* mvcchdr_def);

void write_mvcc_header(void* mvcchdr_tup, const tuple_def* mvcchdr_def, const mvcc_header* mvcchdr_p);

void print_mvcc_header(const mvcc_header* mvcchdr_p);

#include<rhendb/transaction_status.h>

// below functions updates the hints if it is not uptodate, and will set the were_hints_updated if hints were updated
transaction_status fetch_status_for_transaction_id_with_hints(transaction_id_with_hints* transaction_id, transaction_status (*get_transaction_status)(uint256 transaction_id), int* were_hints_updated);

#endif