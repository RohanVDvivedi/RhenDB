#ifndef MVCC_SNAPSHOT_H
#define MVCC_SNAPSHOT_H

#include<serint/large_uints.h>

#include<cutlery/value_arraylist.h>

data_definitions_value_arraylist(sorted_transaction_list, uint256)

typedef struct mvcc_snapshot mvcc_snapshot;
struct mvcc_snapshot
{
	uint256 transaction_id; // id of the transaction that this snapshot belongs to

	sorted_transaction_list in_progress_transaction_ids; // list of in_progress_transactions ( < transaction_id) at the time of taking this snapshot
};

void initialize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 transaction_id);

// below function fails on the following conditions
// transaction_id >= in_progress_transaction_id
// in_progress_transaction_id <= last inserted in_progress_transaction_id
// if the memory allocation fails
// in all the three conditions you can do nothing except exit(-1);
// else it returns 1 on successfull insertion
int insert_in_progress_transaction_in_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 in_progress_transaction_id);

void finalize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p);

/*
	The usage of this functions (to fully initialize a mvcc_snapshot) is as follows

	initialize_mvcc_snapshot(...);
	insert_in_progress_transaction_in_mvcc_snapshot(...);
	insert_in_progress_transaction_in_mvcc_snapshot(...);
	insert_in_progress_transaction_in_mvcc_snapshot(...);
	.
	.
	.
	finalize_mvcc_snapshot(...);

	the finalize_mvcc_snapshot() is required only if you plan to reduce the memory utilization for the snapshot, it releases the unsed memory of in_progress_transaction_ids back to the system
*/

// below function returns true, only if (transaction_id == mvccsnp_p->transaction_id)
int is_self_transaction_for_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id);

// below function returns true, only if the transaction_id provided was committed or aborted at the time of taking this snapshot
// returns (transaction_id < mvccsnp_p->transaction_id) && (transaction_id not in mvccsnp_p->in_progress_transaction_ids)
int was_completed_transaction_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id);

void deinitialize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p);

#include<rondb/mvcc_header.h>
#include<rondb/transaction_status.h>

// returns true if is_self() || (was_completed() && committed)
int are_changes_for_transaction_id_visible_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id, transaction_status (*get_transaction_status)(uint256 transaction_id));

// checks if a tuple with the provided mvcchdr_p is visible for the mvccsnp_p in context
// mvcchdr_p's is_x**_committed or is_x**_aborted, may be set accordingly if they both are unset and a concrete information is available at the time of evaluation of this function using the provided callback
// this function returns true, if the xmin of the header is visible (is_self OR (was_completed AND committed)) and the xmax is (not visible) i.e. (is_NULL OR (!is_self AND (!was_completed OR aborted)))
// can_delete will be set if the mvcc_snapshot possessing transaction is allowed to delete or update (delete + insert_new_version) this tuple, it would only be set if mvcc_header is_visible and xmax is_NULL OR (was_completed and aborted)
int is_mvcc_header_visible_to_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, mvcc_header* mvcchdr_p, transaction_status (*get_transaction_status)(uint256 transaction_id), int* can_delete, int* was_mvcc_header_updated);

#endif