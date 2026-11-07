#include<rhendb/query_plan.h>

#include<rhendb/query_plan_descriptor.h>

#include<rhendb/transaction.h>

#include<posixutils/pthread_cond_utils.h>

/*

lockign mechanism only required for isolation levels above snapshot isolation - to be handled later

int acquire_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, uint64_t timeout_in_microseconds)
{
	int result = 0;
	int latches_released = 0;
	int non_blocking = (timeout_in_microseconds == NON_BLOCKING);

	pthread_mutex_lock(&(o->self_query_plan->desc->curr_tx->rdb->lock_manager_external_lock));

	int wait_error = 0;
	while(!can_not_proceed_for_execution_operator(o) && !(wait_error))
	{
		lock_result locking_result = acquire_lock_with_lock_manager(&(o->self_query_plan->desc->curr_tx->rdb->lck_table), o->self_query_plan->desc->curr_tx, o, resource_type, resource_id, resource_id_size, new_lock_mode, non_blocking);

		if(locking_result == LOCK_ACQUIRED || locking_result == LOCK_TRANSITIONED || locking_result == LOCK_ALREADY_HELD)
		{
			result = 1;
			break;
		}
		else if(locking_result == LOCKING_FAILED)
		{
			result = 0;
			break;
		}
		else if(locking_result == MUST_BLOCK_FOR_LOCK)
		{
			if(timeout_in_microseconds == NON_BLOCKING)
			{
				printf("BUG: in operator for query_plan, lock manager asked us to wait for lock when requesting for a lock non-blockingly\n");
				exit(-1);
			}

			// release latches before going into wait
			if(!latches_released)
			{
				o->operator_release_latches_and_store_context(o);
				latches_released = 1;
			}

			wait_error = pthread_cond_timedwait_for_microseconds(&(o->self_query_plan->desc->curr_tx->wait_on_lock_table_for_lock), &(o->self_query_plan->desc->curr_tx->rdb->lock_manager_external_lock), &timeout_in_microseconds);

			// we just came out from wait, so make the lock_manager discard our wait entries
			discard_all_wait_entries_for_task_in_lock_manager(&(o->self_query_plan->desc->curr_tx->rdb->lck_table), o->self_query_plan->desc->curr_tx, o);

			// if a kill signal was sent while we were waiting then break, and return -1
			if(can_not_proceed_for_execution_operator(o))
			{
				result = -1;
				break;
			}
			else // else just continue
				continue;
		}
	}

	pthread_mutex_unlock(&(o->self_query_plan->desc->curr_tx->rdb->lock_manager_external_lock));

	return result;
}

void release_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size)
{
	pthread_mutex_lock(&(o->self_query_plan->desc->curr_tx->rdb->lock_manager_external_lock));

	release_lock_with_lock_manager(&(o->self_query_plan->desc->curr_tx->rdb->lck_table), o->self_query_plan->desc->curr_tx, o, resource_type, resource_id, resource_id_size);

	pthread_mutex_unlock(&(o->self_query_plan->desc->curr_tx->rdb->lock_manager_external_lock));
}
*/

void OPERATOR_DESTROY_STATE_NO_OP_FUNCTION(operator* o)
{
	if(o->state)
	{
		free(o->state);
		o->state = NULL;
	}
}

#include<rhendb/operators.h>

query_plan* get_new_query_plan(const query_plan_desc* desc)
{
	query_plan* qp = malloc(sizeof(query_plan));
	if(qp == NULL)
		exit(-1);

	qp->desc = desc;

	uint64_t operators_count = get_element_count_arraylist(&(desc->operator_descriptors));

	if(!initialize_arraylist(&(qp->operators), operators_count))
		exit(-1);

	for(uint64_t i = 0; i < operators_count; i++)
	{
		operator* o = malloc(sizeof(operator));

		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(desc->operator_descriptors), i);

		o->self_query_plan = qp;
		o->index = i;
		o->desc = get_from_front_of_arraylist(&(desc->operator_descriptors), i);
		o->state = NULL;
		o->next_tuple = NULL;
		o->destroy_state = NULL;

		switch(d->type)
		{
			case OP_CONSTANT_DATASET :
				open_constant_dataset_operator(o);
				break;
			case OP_GENERATOR :
				open_generator_operator(o);
				break;
			case OP_STREAM_INPUT :
				open_stream_input_operator(o);
				d->params.stream_input.in_strm = NULL; // consumed : must be set again by the user before the next create_query_plan()
				break;
			case OP_SCAN :
				open_scan_operator(o);
				break;
			case OP_IDENTITY :
				open_identity_operator(o);
				break;
			case OP_ROW_RENAME :
				open_row_rename_operator(o);
				break;
			case OP_OFFSET_LIMIT :
				open_offset_limit_operator(o);
				break;
			case OP_SELECTION :
				open_selection_operator(o);
				break;
			case OP_SORT :
				open_sort_operator(o);
				break;
			case OP_PROJECTION :
				open_projection_operator(o);
				break;
			case OP_SIMPLE_AGGREGATION :
				open_simple_aggregation_operator(o);
				break;
			case OP_SORTED_AGGREGATION :
				open_sorted_aggregation_operator(o);
				break;
			case OP_HASH_AGGREGATION :
				open_hash_aggregation_operator(o);
				break;
			case OP_CONSUMER :
				open_consumer_operator(o);
				break;
			case OP_STREAM_OUTPUT :
				open_stream_output_operator(o);
				d->params.stream_output.out_strm = NULL; // consumed : must be set again by the user before the next create_query_plan()
				break;
			case OP_SUBQUERY_OUTPUT :
				open_subquery_output_operator(o);
				d->params.subquery_output.out_pipe = NULL; // consumed : must be set again by the user before the next create_query_plan()
				break;
			case OP_UNION :
				open_union_operator(o);
				break;
			case OP_MERGE_SORTED_INPUTS :
				open_merge_sorted_inputs_operator(o);
				break;
			case OP_RESULT_MATCH :
				open_result_match_operator(o);
				break;
			case OP_BLOCK_NESTED_LOOP_JOIN :
				open_block_nested_loop_join_operator(o);
				break;
			case OP_SORT_MERGE_JOIN :
				open_sort_merge_join_operator(o);
				break;
			case OP_HASH_JOIN :
				open_hash_join_operator(o);
				break;
			case OP_BLOCK_NESTED_LOOP_SEMI_JOIN :
				open_block_nested_loop_semi_join_operator(o);
				break;
			case OP_SORT_MERGE_SEMI_JOIN :
				open_sort_merge_semi_join_operator(o);
				break;
			case OP_HASH_SEMI_JOIN :
				open_hash_semi_join_operator(o);
				break;
			case OP_INSERTION :
				open_insertion_operator(o);
				break;
			case OP_DELETION :
				open_deletion_operator(o);
				break;
			case OP_POINTER_LOOKUP :
				open_pointer_lookup_operator(o);
				break;
		}

		if(!push_back_to_arraylist(&(qp->operators), o))
			exit(-1);
	}

	return qp;
}

void* next_tuple_from_query_plan(query_plan* qp, int* error_code)
{
	operator* o = (operator*) get_back_of_arraylist(&(qp->operators));
	return o->next_tuple(o, error_code);
}

void destroy_query_plan(query_plan* qp)
{
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operators)); i++)
	{
		operator* o = (operator*) get_from_arraylist(&(qp->operators), i);

		if(o->destroy_state)
			o->destroy_state(o);

		free(o);
	}

	deinitialize_arraylist(&(qp->operators));
	free(qp);
}