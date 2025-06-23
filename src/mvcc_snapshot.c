#include<rondb/mvcc_snapshot.h>

#include<stdlib.h>

declarations_value_arraylist(sorted_transaction_list, uint256, static inline)
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(sorted_transaction_list, uint256, static inline)

void initialize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 transaction_id)
{
	mvccsnp_p->transaction_id = transaction_id;

	if(!initialize_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids), 4))
		exit(-1);
}

int insert_in_progress_transaction_in_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 in_progress_transaction_id)
{
	if(compare_uint256(in_progress_transaction_id, mvccsnp_p->transaction_id) >= 0) // if the new in_progress_transaction_id >= mvccsnp_p->transaction_id, then fail
		return 0;

	// in_progress_transaction_id must be > last inserted one
	if(!is_empty_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids)) && compare_uint256(in_progress_transaction_id, *get_back_of_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids))) <= 0)
		return 0;

	// if the container is full and we can not insert, then crash, we are out of memory
	if(is_full_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids)) && !expand_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids)))
		exit(-1);

	// now, this must succeed
	return push_back_to_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids), &in_progress_transaction_id);
}

void finalize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p)
{
	// jsut shrink to fit
	shrink_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids));
}

int is_self_transaction_for_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id)
{
	// only if transaction_id == mvccsnp_p->transaction_id
	return are_equal_uint256(transaction_id, mvccsnp_p->transaction_id);
}

int was_completed_transaction_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id);

void deinitialize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p)
{
	deinitialize_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids));
}

int are_changes_for_transaction_id_visible_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id, transaction_status (*get_transaction_status)(uint256 transaction_id));

int is_mvcc_header_visible_to_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, mvcc_header* mvcchdr_p, transaction_status (*get_transaction_status)(uint256 transaction_id), int* can_delete, int* was_mvcc_header_updated);