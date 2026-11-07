#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

#include<posixutils/pthread_cond_utils.h>

int acquire_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, uint64_t timeout_in_microseconds)
{
	int result = 0;
	int latches_released = 0;
	int non_blocking = (timeout_in_microseconds == NON_BLOCKING);

	pthread_mutex_lock(&(o->self_query_plan->curr_tx->rdb->lock_manager_external_lock));

	int wait_error = 0;
	while(!can_not_proceed_for_execution_operator(o) && !(wait_error))
	{
		lock_result locking_result = acquire_lock_with_lock_manager(&(o->self_query_plan->curr_tx->rdb->lck_table), o->self_query_plan->curr_tx, o, resource_type, resource_id, resource_id_size, new_lock_mode, non_blocking);

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

			wait_error = pthread_cond_timedwait_for_microseconds(&(o->self_query_plan->curr_tx->wait_on_lock_table_for_lock), &(o->self_query_plan->curr_tx->rdb->lock_manager_external_lock), &timeout_in_microseconds);

			// we just came out from wait, so make the lock_manager discard our wait entries
			discard_all_wait_entries_for_task_in_lock_manager(&(o->self_query_plan->curr_tx->rdb->lck_table), o->self_query_plan->curr_tx, o);

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

	pthread_mutex_unlock(&(o->self_query_plan->curr_tx->rdb->lock_manager_external_lock));

	return result;
}

void release_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size)
{
	pthread_mutex_lock(&(o->self_query_plan->curr_tx->rdb->lock_manager_external_lock));

	release_lock_with_lock_manager(&(o->self_query_plan->curr_tx->rdb->lck_table), o->self_query_plan->curr_tx, o, resource_type, resource_id, resource_id_size);

	pthread_mutex_unlock(&(o->self_query_plan->curr_tx->rdb->lock_manager_external_lock));
}

void OPERATOR_DESTROY_STATE_NO_OP_FUNCTION(operator* o)
{
	if(o->state)
	{
		free(o->state);
		o->state = NULL;
	}
}

// query plan functions

query_plan* get_new_query_plan(const query_plan_desc* desc);
{
	query_plan* qp = malloc(sizeof(query_plan));
	if(qp == NULL)
		exit(-1);

	qp->curr_tx = curr_tx;

	qp->this_sub_query_expr_eval_context = this_sub_query_expr_eval_context;

	if(!initialize_arraylist(&(qp->operators), operators_count))
		exit(-1);

	// query initially never gets this flag set, it is set when an unknown operator reason is encountered
	qp->query_errored = 0;

	return qp;
}

void* next_tuple_from_query_plan(query_plan* qp);

void destroy_query_plan(query_plan* qp)
{
	// release all of the operator resources
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operators)); i++)
	{
		operator* o = (operator*) get_from_arraylist(&(qp->operators), i);

		o->free_resources(o);

		o->self_query_plan = NULL;

		o->inputs = NULL;

		o->execute = NULL;
		o->operator_release_latches_and_store_context = NULL;
		o->clean_up_resources = NULL;
		o->free_resources = NULL;

		pthread_cond_destroy(&(o->wait_on_lock_table_for_lock));

		while(NULL != get_head_of_linkedlist(&(o->output_consumers)))
		{
			consumption_iterator* cit_p = (consumption_iterator*) get_head_of_linkedlist(&(o->output_consumers));
			remove_head_from_linkedlist(&(o->output_consumers));
			unmap_for_interim_tuple_region(&(cit_p->curr_region));
			free(cit_p);
		}

		while(NULL != get_head_of_singlylist(&(o->output_buffers)))
		{
			interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&(o->output_buffers));
			remove_head_from_singlylist(&(o->output_buffers));
			delete_interim_tuple_store(its_p);
		}

		o->output_buffers_count = 0;
		o->output_buffer_bytes_unnotified = 0;

		while(NULL != get_head_of_singlylist(&(o->free_output_buffers)))
		{
			interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&(o->free_output_buffers));
			remove_head_from_singlylist(&(o->free_output_buffers));
			delete_interim_tuple_store(its_p);
		}

		o->output_tuple_def = NULL;

		pthread_mutex_destroy(&(o->output_lock));

		pthread_mutex_destroy(&(o->state_lock));
		pthread_cond_destroy(&(o->wait_until_completion));
		o->state = OPERATOR_KILLED;
		o->queued_jobs_count = 0;
		o->running_jobs_count = 0;
		o->is_kill_signal_sent = 0;
		o->is_trigger_signaled_on_running = 0;

		// concatenate the kill reason passed
		if(kill_reasons != NULL)
		{
			if(!concatenate_dstring(kill_reasons, &(o->kill_reason)))
				exit(-1);
			if(!concatenate_char(kill_reasons, '\n'))
				exit(-1);
		}

		deinit_dstring(&(o->kill_reason));

		free(o);
	}

	deinitialize_arraylist(&(qp->operators));
	free(qp);
}