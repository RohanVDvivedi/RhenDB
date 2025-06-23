#include<rondb/mvcc_snapshot.h>

#include<cutlery/index_accessed_search_sort.h>

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
	// just shrink to fit
	shrink_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids));
}

int is_self_transaction_for_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id)
{
	// only if transaction_id == mvccsnp_p->transaction_id
	return are_equal_uint256(transaction_id, mvccsnp_p->transaction_id);
}

static int compare_uint256_by_ptrs(const void* u1, const void* u2)
{
	return compare_uint256(*((const uint256*)u1), *((const uint256*)u2));
}

int was_completed_transaction_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id)
{
	index_accessed_interface iai = get_index_accessed_interface_for_front_of_sorted_transaction_list((sorted_transaction_list*)(&(mvccsnp_p->in_progress_transaction_ids)));

	// (transaction_id < mvccsnp_p->transaction_id) && (transaction_id not in mvccsnp_p->in_progress_transaction_ids)
	return (compare_uint256(transaction_id, mvccsnp_p->transaction_id) < 0) && (is_empty_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids)) ||
		INVALID_INDEX == binary_search_in_sorted_iai(&iai, 0, get_element_count_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids)) - 1, &transaction_id, &simple_comparator(compare_uint256_by_ptrs), FIRST_OCCURENCE));
}

void deinitialize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p)
{
	deinitialize_sorted_transaction_list(&(mvccsnp_p->in_progress_transaction_ids));
}

int are_changes_for_transaction_id_visible_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, transaction_id_with_hints* transaction_id, transaction_status (*get_transaction_status)(uint256 transaction_id), int* were_hints_updated)
{
	// you can only see your changes OR changes of transactions that committed before you
	return is_self_transaction_for_mvcc_snapshot(mvccsnp_p, transaction_id->transaction_id) ||
		(was_completed_transaction_at_mvcc_snapshot(mvccsnp_p, transaction_id->transaction_id) &&
		fetch_status_for_transaction_id_with_hints(transaction_id, get_transaction_status, were_hints_updated) == TX_COMMITTED);
}

int is_mvcc_header_visible_to_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, mvcc_header* mvcchdr_p, transaction_status (*get_transaction_status)(uint256 transaction_id), int* can_delete, int* were_hints_updated)
{
	(*can_delete) = 0;

	// if xmin is not visible the tuple is not visible
	if(!are_changes_for_transaction_id_visible_at_mvcc_snapshot(mvccsnp_p, &(mvcchdr_p->xmin), get_transaction_status, were_hints_updated))
		return 0;

	// if xmax is null, then tuple is alive until infinity and is visible
	if(mvcchdr_p->is_xmax_NULL)
		return 1;

	// if the changes from xmax is visible, then the tuple is dead for you
	if(are_changes_for_transaction_id_visible_at_mvcc_snapshot(mvccsnp_p, &(mvcchdr_p->xmax), get_transaction_status, were_hints_updated))
		return 0;

	// now we are sure that the tuple is visible to you

	// lets check if it could be deleted or updated (delete then a new insert) by you
	// this is possible only if the tuple was not deleted by you AND the deletor of the tuple aborted
	(*can_delete) = (mvcchdr_p->is_xmax_NULL) || (
		!is_self_transaction_for_mvcc_snapshot(mvccsnp_p, mvcchdr_p->xmax.transaction_id) &&
		fetch_status_for_transaction_id_with_hints(&(mvcchdr_p->xmax), get_transaction_status, were_hints_updated) == TX_ABORTED
		);

	return 1;
}