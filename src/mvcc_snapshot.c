#include<rhendb/mvcc_snapshot.h>

#include<cutlery/index_accessed_search_sort.h>

#include<stdlib.h>

declarations_value_arraylist(sorted_transaction_ids_list, uint256, static inline)
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(sorted_transaction_ids_list, uint256, static inline)

void initialize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p)
{
	if(!initialize_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids), 4))
		exit(-1);

	mvccsnp_p->has_self_transaction_id = 0;

	initialize_llnode(&(mvccsnp_p->embed_node));
}

void begin_taking_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 least_unassigned_transaction_id)
{
	mvccsnp_p->least_unassigned_transaction_id = least_unassigned_transaction_id;
	remove_all_from_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids));
}

int insert_in_progress_transaction_in_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 in_progress_transaction_id)
{
	if(compare_uint256(in_progress_transaction_id, mvccsnp_p->least_unassigned_transaction_id) >= 0) // if the new in_progress_transaction_id >= mvccsnp_p->least_unassigned_transaction_id, then fail
		return 0;

	// in_progress_transaction_id must be > last inserted one
	if((!is_empty_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids))) && compare_uint256(in_progress_transaction_id, *get_back_of_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids))) <= 0)
		return 0;

	// if the container is full and we can not insert, then crash, we are out of memory
	if(is_full_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids)) && !expand_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids)))
		exit(-1);

	// now, this must succeed
	return push_back_to_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids), &in_progress_transaction_id);
}

void finalize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p)
{
	// just shrink to fit
	shrink_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids));
}

int set_self_transaction_id_in_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 self_transaction_id)
{
	if(mvccsnp_p->has_self_transaction_id)
		return 0;

	mvccsnp_p->has_self_transaction_id = 1;
	mvccsnp_p->self_transaction_id = self_transaction_id;

	return 1;
}

const uint256* get_in_progress_transaction_ids_for_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, cy_uint index)
{
	return get_from_front_of_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids), index);
}

int is_self_transaction_for_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id)
{
	// only if transaction_id == mvccsnp_p->self_transaction_id
	return (mvccsnp_p->has_self_transaction_id) && are_equal_uint256(transaction_id, mvccsnp_p->self_transaction_id);
}

int was_completed_transaction_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id)
{
	// self_transaction_id of the snapshot is never completed
	if(is_self_transaction_for_mvcc_snapshot(mvccsnp_p, transaction_id))
		return 0;

	index_accessed_interface iai = get_index_accessed_interface_for_front_of_sorted_transaction_ids_list((sorted_transaction_ids_list*)(&(mvccsnp_p->in_progress_transaction_ids)));

	// (transaction_id < mvccsnp_p->least_unassigned_transaction_id) && (transaction_id not in mvccsnp_p->in_progress_transaction_ids)
	return (compare_uint256(transaction_id, mvccsnp_p->least_unassigned_transaction_id) < 0) && (is_empty_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids)) ||
		(INVALID_INDEX == binary_search_in_sorted_iai(&iai, 0, get_element_count_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids)) - 1, &transaction_id, &simple_comparator(compare_uint256_with_ptrs), FIRST_OCCURENCE)));
}

void deinitialize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p)
{
	deinitialize_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids));
}

int are_changes_for_transaction_id_visible_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, transaction_id_with_hints* transaction_id, transaction_status_getter* tsg_p, int* were_hints_updated)
{
	// you can only see your changes OR changes of transactions that committed before you
	return is_self_transaction_for_mvcc_snapshot(mvccsnp_p, transaction_id->transaction_id) ||
		(was_completed_transaction_at_mvcc_snapshot(mvccsnp_p, transaction_id->transaction_id) &&
		(fetch_status_for_transaction_id_with_hints(transaction_id, tsg_p, were_hints_updated) == TX_COMMITTED));
}

int is_tuple_visible_to_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, mvcc_header* mvcchdr_p, transaction_status_getter* tsg_p, int* were_hints_updated)
{
	// if xmin is NULL, this tuple is not visible to any snapshot
	if(mvcchdr_p->is_xmin_NULL)
		return 0;

	// if xmin is not visible the tuple is not visible
	if(!are_changes_for_transaction_id_visible_at_mvcc_snapshot(mvccsnp_p, &(mvcchdr_p->xmin), tsg_p, were_hints_updated))
		return 0;

	// if xmax is null, then tuple is alive until infinity and is visible
	if(mvcchdr_p->is_xmax_NULL)
		return 1;

	// if the changes from xmax is visible, then the tuple is dead for you, so not visible
	if(are_changes_for_transaction_id_visible_at_mvcc_snapshot(mvccsnp_p, &(mvcchdr_p->xmax), tsg_p, were_hints_updated))
		return 0;

	return 1;
}

char const * const can_delete_result_string[] = {
	[IN_VISIBLE_TUPLE] = "IN_VISIBLE_TUPLE",
	[CAN_DELETE] = "CAN_DELETE",
	[WAIT_FOR_XMAX_TO_ABORT] = "WAIT_FOR_XMAX_TO_ABORT",
	[MUST_ABORT] = "MUST_ABORT",
};

can_delete_result can_delete_tuple_for_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, mvcc_header* mvcchdr_p, transaction_status_getter* tsg_p, int* were_hints_updated)
{
	// if xmin is NULL, this tuple is not visible to anyone and can not be deleted by anyone, it can though only be vaccummed
	if(mvcchdr_p->is_xmin_NULL)
		return IN_VISIBLE_TUPLE;

	// you can not delete a tuple not visible to you
	if(!is_tuple_visible_to_mvcc_snapshot(mvccsnp_p, mvcchdr_p, tsg_p, were_hints_updated))
		return IN_VISIBLE_TUPLE;

	// if xmax is null, then tuple can be immediately deleted by writing a new xmax to it
	if(mvcchdr_p->is_xmax_NULL)
		return CAN_DELETE;

	// if you yourself deleted the tuple, you can not delete it (not again)
	if(is_self_transaction_for_mvcc_snapshot(mvccsnp_p, mvcchdr_p->xmax.transaction_id))
		return IN_VISIBLE_TUPLE;

	switch (fetch_status_for_transaction_id_with_hints(&(mvcchdr_p->xmax), tsg_p, were_hints_updated))
	{
		case TX_ABORTED : // the status of xmax must be ABORTED to delete it
			return CAN_DELETE;
		case TX_IN_PROGRESS : // if (logically future) xmax is in_progress -> then wait for it to abort, or abort yourself
			return WAIT_FOR_XMAX_TO_ABORT;
		case TX_COMMITTED : // if (logically future) xmax is committed -> then abort the current transaction
			return MUST_ABORT;
	}

	return MUST_ABORT;
}

void print_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p)
{
	if(mvccsnp_p->has_self_transaction_id)
	{
		char temp[80] = {};
		serialize_to_decimal_uint256(temp, mvccsnp_p->self_transaction_id);
		printf("self : %s\n\n", temp);
	}
	else
		printf("self : UNASSIGNED\n\n");

	printf("in_progress_transactions : [\n");
	for(cy_uint i = 0; i < get_element_count_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids)); i++)
	{
		if(i % 8 == 0)
			printf("\t");
		char temp[80] = {};
		serialize_to_decimal_uint256(temp, *get_from_front_of_sorted_transaction_ids_list(&(mvccsnp_p->in_progress_transaction_ids), i));
		printf(" %s,", temp);
		if((i+1) % 8 == 0)
			printf("\n");
	}
	printf("]\n\n");

	{
		char temp[80] = {};
		serialize_to_decimal_uint256(temp, mvccsnp_p->least_unassigned_transaction_id);
		printf("least_unassigned_transaction_id : %s\n\n", temp);
	}
}

int can_vaccum_tuple_for_mvcc(mvcc_header* mvcchdr_p, transaction_status_getter* tsg_p, uint256 vaccum_horizon_transaction_id, int* were_hints_updated)
{
	// if xmin is NULL, this tuple is not visible to anyone and can be vaccummed rightaway
	if(mvcchdr_p->is_xmin_NULL)
		return 1;

	// tuple created by an aborted transaction, vaccum immediately
	if(fetch_status_for_transaction_id_with_hints(&(mvcchdr_p->xmin), tsg_p, were_hints_updated) == TX_ABORTED)
		return 1;

	// if xmax is not NULL, and xmax is committed before the vaccum_horizon_transaction_id, then this tuple is not visible to any transaction, hence you may vaccum it
	if((!mvcchdr_p->is_xmax_NULL)
		&& (fetch_status_for_transaction_id_with_hints(&(mvcchdr_p->xmax), tsg_p, were_hints_updated) == TX_COMMITTED)
		&& (compare_uint256(mvcchdr_p->xmax.transaction_id, vaccum_horizon_transaction_id) < 0))
		return 1;

	return 0;
}