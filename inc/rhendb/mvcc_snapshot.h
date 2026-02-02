#ifndef MVCC_SNAPSHOT_H
#define MVCC_SNAPSHOT_H

#include<serint/large_uints.h>

#include<cutlery/value_arraylist.h>
#include<cutlery/linkedlist.h>

data_definitions_value_arraylist(sorted_transaction_ids_list, uint256)

typedef struct mvcc_snapshot mvcc_snapshot;
struct mvcc_snapshot
{
	sorted_transaction_ids_list in_progress_transaction_ids; // list of in_progress_transactions ( < least_unassigned_transaction_id) at the time of taking this snapshot

	uint256 least_unassigned_transaction_id; // this is the next_assignable_transaction_id from transaction table, at the time of taking this snapshot

	int has_self_transaction_id; // this boolean suggests if a transaction_id was assigned for this snapshot, if false, transaction_id attribiute is expected to be absent
	uint256 self_transaction_id; // id of the transaction that this snapshot belongs to

	// must condition => (self_transaction_id <= least_unassigned_transaction_id)
	// and for any in_progress_transaction_ids <= least_unassigned_transaction_id

	// below is an embed_node for maintaining a global linkedlist of all active snapshots in the transaction_table
	// having pointer to mvcc_snapshot does not mean you can access this embed_node
	// it is protected by the locks/latches internal to the transaction_table
	llnode embed_node;
};

mvcc_snapshot* get_new_mvcc_snapshot();

// this function marks the start of taking the snapshot, it will set the least_unassigned_transaction_id, and also clears the in_progress_transaction_ids
void begin_taking_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 least_unassigned_transaction_id);

// below function fails on the following conditions
// least_unassigned_transaction_id >= in_progress_transaction_id
// in_progress_transaction_id <= last inserted in_progress_transaction_id
// if the memory allocation fails
// in all the three conditions you can do nothing except exit(-1);
// else it returns 1 on successfull insertion
int insert_in_progress_transaction_in_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 in_progress_transaction_id);

void finalize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p);

// set self_transaction_id for the mvcc snapshot
// fails if it already has a self_transaction_id
int set_self_transaction_id_in_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 self_transaction_id);

// avoid using this function.
const uint256* get_in_progress_transaction_ids_for_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, cy_uint index);

/*
	The usage of this functions (to fully initialize a mvcc_snapshot) is as follows

	initialize_mvcc_snapshot(...);

	// the for taking the snapshot run the below lines of code
	begin_taking_mvcc_snapshot(...);
	insert_in_progress_transaction_in_mvcc_snapshot(...);
	insert_in_progress_transaction_in_mvcc_snapshot(...);
	insert_in_progress_transaction_in_mvcc_snapshot(...);
	.
	.
	.
	finalize_mvcc_snapshot(...);

	// then anytime later and only once, we can do
	// when are about to make a write query. assign a transaction_id for the snapshot
	set_transaction_id_in_mvcc_snapshot(...);
*/

// below function returns true, only if (transaction_id == mvccsnp_p->transaction_id)
int is_self_transaction_for_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id);

// below function returns true, only if the transaction_id provided was committed or aborted at the time of taking this snapshot
// returns (transaction_id < mvccsnp_p->transaction_id) && (transaction_id not in mvccsnp_p->in_progress_transaction_ids)
int was_completed_transaction_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id);

void delete_mvcc_snapshot(mvcc_snapshot* mvccsnp_p);

#include<rhendb/mvcc_header.h>
#include<rhendb/transaction_status.h>

// returns true if is_self() || (was_completed() && committed)
int are_changes_for_transaction_id_visible_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, transaction_id_with_hints* transaction_id, transaction_status_getter* tsg_p, int* were_hints_updated);

// checks if a tuple with the provided mvcchdr_p is visible for the mvccsnp_p in context
// returns true, if the xmin is visible and the xmax is NULL or (not visible)
int is_tuple_visible_to_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, mvcc_header* mvcchdr_p, transaction_status_getter* tsg_p, int* were_hints_updated);

typedef enum can_delete_result can_delete_result;
enum can_delete_result
{
	IN_VISIBLE_TUPLE, // an invisible tuple, should not even be read, far from deleting it, so just skip and continue the scan
	CAN_DELETE, // delete immediately, take lock if your system requires it
	WAIT_FOR_XMAX_TO_ABORT, // tuple in context deleted by logically future in_progress transaction, so either wait for it to abort or abort your own transaction
	MUST_ABORT, // tuple in context deleted by logically future committed transaction, so you have no choice other than to abort
};
extern char const * const can_delete_result_string[];

// checks if a tuple with the provided mvcchdr_p can be deleted (or updated via delete + new insert) for the mvccsnp_p in context
// returns true, if the tuple is_visible and xmax is_NULL OR (was_completed and aborted)
can_delete_result can_delete_tuple_for_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, mvcc_header* mvcchdr_p, transaction_status_getter* tsg_p, int* were_hints_updated);

// debug print mvcc snapshot
void print_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p);

// test if a tuple can be vaccummed
int can_vaccum_tuple_for_mvcc(mvcc_header* mvcchdr_p, transaction_status_getter* tsg_p, uint256 vaccum_horizon_transaction_id, int* were_hints_updated);

#endif