#ifndef DML_H
#define DML_H

#include<rhendb/interim_tuple_store.h>
#include<rhendb/fetched_table.h>
#include<rhendb/transaction.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/positional_accessor.h>

// dml error

#define DML_SUCCESS                           0

// dml_insert errors
#define DML_INSERT_MATERIALIZATION_FAILED  (-6001) // an extended (text/blob/numeric/jsonb) value failed to materialize
#define DML_INSERT_RECORD_TOO_BIG          (-6002) // the built heap record does not fit PAGE_FILL_PER_TUPLE * page_size
#define DML_INSERT_FAILED                  (-6003) // insertion aborted and could not be retried

// dml_delete errors
#define DML_DELETE_NULL_PARTITION_ID       (-6101) // the source tuple had a NULL partition_id
#define DML_DELETE_NULL_TUPLE_POINTER      (-6102) // the source tuple had a NULL tuple_pointer
#define DML_DELETE_MISSING_PARTITION       (-6103) // partition_id of a row to delete is not a partition of ftabl
#define DML_DELETE_MISSING_TUPLE           (-6104) // the tuple_pointer of a row to delete does not resolve to a live tuple
#define DML_DELETE_FAILED                  (-6105) // deletion aborted and could not be retried

// ===================================================================================================
// the two DML functions
// ===================================================================================================

// Inserts every tuple of its_p into the newest partition of ftabl.
//   ots_p                                     : optional output store; if non-NULL and output_flags != 0,
//                                               one context tuple is appended per inserted row
//   its_p                                     : input store, holds the already materialized rows to insert
//   input_tuple_def                           : tuple_def of the rows in its_p
//   insertion_from_source_positional_accessors: for each non-mvcc column i (1..n) of the partition record,
//                                               where to read it from inside an its_p tuple; length = (columns - 1)
//   ftabl                                     : target table
//   tx                                        : transaction (must own a writable snapshot with a self_transaction_id)
//   output_flags                              : table_operator_output_type.h flags selecting the ots_p tuple shape
//   additional_flags                          : transaction.h flags (SAVEPOINT_LOGGING_ENABLED honoured)
//   dml_error                                 : out, set to DML_SUCCESS or one of the DML_INSERT_* codes
// returns the number of tuples inserted (rows written before an error, if one occurs).
uint64_t dml_insert(
	interim_tuple_store* ots_p,
	interim_tuple_store* its_p,
	const tuple_def* input_tuple_def,
	const positional_accessor* insertion_from_source_positional_accessors,
	const fetched_table* ftabl,
	transaction* tx,
	int output_flags,
	int additional_flags,
	int* dml_error);

// Marks deleted (writes xmax = self_transaction_id) every tuple whose (partition_id, tuple_pointer)
// is read from its_p. Rows are buffered and sorted so all deletions on a single page happen inside a
// single mini transaction.
//   ots_p                                      : optional output store; if non-NULL and output_flags != 0,
//                                                one context tuple is appended per deleted row
//   its_p                                      : input store, holds the already materialized rows to delete
//   input_tuple_def                            : tuple_def of the rows in its_p
//   partition_id_from_source_positional_accessor : where to read partition_id inside an its_p tuple
//   tuple_pointer_from_source_positional_accessor: where to read the tuple_pointer inside an its_p tuple
//   ftabl, tx, output_flags, additional_flags, dml_error : as for dml_insert
// returns the number of tuples marked deleted.
uint64_t dml_delete(
	interim_tuple_store* ots_p,
	interim_tuple_store* its_p,
	const tuple_def* input_tuple_def,
	const positional_accessor* partition_id_from_source_positional_accessor,
	const positional_accessor* tuple_pointer_from_source_positional_accessor,
	const fetched_table* ftabl,
	transaction* tx,
	int output_flags,
	int additional_flags,
	int* dml_error);

#endif