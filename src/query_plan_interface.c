#include<rhendb/query_plan_interface.h>

#include<rhendb/transaction.h>

#include<posixutils/pthread_cond_utils.h>

// operator functions

int is_killed_operator(operator* o)
{
	pthread_mutex_lock(&(o->kill_lock));

	int result = (!!(o->is_killed));

	pthread_mutex_unlock(&(o->kill_lock));

	return result;
}

int can_not_proceed_for_execution_operator(operator* o)
{
	pthread_mutex_lock(&(o->kill_lock));

	int result = (!!(o->is_kill_signal_sent)) || (!!(o->is_killed));

	pthread_mutex_unlock(&(o->kill_lock));

	return result;
}

void mark_operator_self_killed(operator* o, dstring kill_reason)
{
	pthread_mutex_lock(&(o->kill_lock));

	// concatenate the kill reason passed
	if(!is_empty_dstring(&kill_reason))
	{
		if(!is_empty_dstring(&(o->kill_reason)))
			if(!concatenate_char(&(o->kill_reason), '$'))
				exit(-1);
		if(!concatenate_dstring(&(o->kill_reason), &kill_reason))
			exit(-1);
	}

	o->is_killed = 1;
	pthread_cond_broadcast(&(o->wait_until_killed));

	pthread_mutex_unlock(&(o->kill_lock));

	// force the consumer to read through all that has been left to be consumed
	if(o->consumer_operator != NULL)
		o->consumer_operator->trigger_execution(o->consumer_operator);
}

// called by the query_plan to send kill to an operator
static void send_kill_signal_to_operator(operator* o, dstring kill_reason)
{
	pthread_mutex_lock(&(o->kill_lock));

	// concatenate the kill reason passed
	if(!is_empty_dstring(&kill_reason))
	{
		if(!is_empty_dstring(&(o->kill_reason)))
			if(!concatenate_char(&(o->kill_reason), '$'))
				exit(-1);
		if(!concatenate_dstring(&(o->kill_reason), &kill_reason))
			exit(-1);
	}

	o->is_kill_signal_sent = 1;

	pthread_mutex_unlock(&(o->kill_lock));
}

// wait here after you send the operators a kill signal, waiting for them to die
static void wait_for_operator_to_die(operator* o)
{
	pthread_mutex_lock(&(o->kill_lock));

	while(!(o->is_killed))
		pthread_cond_wait(&(o->wait_until_killed), &(o->kill_lock));

	pthread_mutex_unlock(&(o->kill_lock));
}

static void spurious_wake_up_operator(operator* o)
{
	pthread_cond_broadcast(&(o->wait_on_lock_table_for_lock));
}

int acquire_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, uint64_t timeout_in_microseconds)
{
	int result = 0;
	int latches_released = 0;
	int non_blocking = (timeout_in_microseconds == NON_BLOCKING);

	pthread_mutex_lock(&(o->self_query_plan->curr_tx->db->lock_manager_external_lock));

	int wait_error = 0;
	while(!can_not_proceed_for_execution_operator(o) && !(wait_error))
	{
		lock_result locking_result = acquire_lock_with_lock_manager(&(o->self_query_plan->curr_tx->db->lck_table), o->self_query_plan->curr_tx, o, resource_type, resource_id, resource_id_size, new_lock_mode, non_blocking);

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
				printf("BUG: in operator for query_plan_interface, lock manager asked us to wait for lock when requesting for a lock non-blockingly\n");
				exit(-1);
			}

			// release latches before going into wait
			if(!latches_released)
			{
				o->operator_release_latches_and_store_context(o);
				latches_released = 1;
			}

			wait_error = pthread_cond_timedwait_for_microseconds(&(o->wait_on_lock_table_for_lock), &(o->self_query_plan->curr_tx->db->lock_manager_external_lock), &timeout_in_microseconds);

			// we just came out from wait, so make the lock_manager discard our wait entries
			discard_all_wait_entries_for_task_in_lock_manager(&(o->self_query_plan->curr_tx->db->lck_table), o->self_query_plan->curr_tx, o);

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

	pthread_mutex_unlock(&(o->self_query_plan->curr_tx->db->lock_manager_external_lock));

	return result;
}

void release_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size)
{
	pthread_mutex_lock(&(o->self_query_plan->curr_tx->db->lock_manager_external_lock));

	release_lock_with_lock_manager(&(o->self_query_plan->curr_tx->db->lck_table), o->self_query_plan->curr_tx, o, resource_type, resource_id, resource_id_size);

	pthread_mutex_unlock(&(o->self_query_plan->curr_tx->db->lock_manager_external_lock));
}

void OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION(operator* o){}

void OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION(operator* o)
{
	if(o->inputs)
	{
		free(o->inputs);
		o->inputs = NULL;
	}

	if(o->context)
	{
		free(o->context);
		o->context = NULL;
	}
}

static int need_to_wake_up_consumer_UNSAFE(operator* o)
{
	// no wake up, if no consumer_operator
	if(o->consumer_operator == NULL)
		return 0;

	// no wake up, if the consumer may not be alive
	if(can_not_proceed_for_execution_operator(o->consumer_operator))
		return 0;

	// no wake up, if there is nothing to consume
	if(is_empty_singlylist(&(o->output_buffers)))
		return 0;

	// surely wake up if the output_buffers have more than 1 interim_tuple_stores
	if(get_head_of_singlylist(&(o->output_buffers)) != get_tail_of_singlylist(&(o->output_buffers)))
		return 1;

	const interim_tuple_store* its_p = get_head_of_singlylist(&(o->output_buffers));
	// wake up if there is more than consumer_trigger_on_bytes_accumulated bytes
	if(o->consumer_trigger_on_bytes_accumulated > 0 && its_p->next_tuple_offset >= o->consumer_trigger_on_bytes_accumulated)
		return 1;

	return 0;
}

int produce_tuple_from_operator(operator* o, void* tuple)
{
	int pushed = 0;

	uint32_t tuple_size = get_tuple_size(o->output_tuple_def, tuple);

	pthread_mutex_lock(&(o->output_lock));

	// proceed only if the consumer is alive
	if((o->consumer_operator != NULL) && !can_not_proceed_for_execution_operator(o->consumer_operator))
	{
		pushed = 1;

		// fetch tail, create one and insert if it does not exists
		interim_tuple_store* its_p = (interim_tuple_store*) get_tail_of_singlylist(&(o->output_buffers));
		if(its_p != NULL)
		{
			its_p = get_new_interim_tuple_store(".");

			if(!insert_tail_in_singlylist(&(o->output_buffers), its_p))
				exit(-1);
		}

		// append the tuple in this tail interim_tuple_store
		{
			interim_tuple_region tr = INIT_INTERIM_TUPLE_REGION;
			mmap_for_writing_tuple(its_p, &tr, (tuple_size_def*)(&(o->output_tuple_def->size_def)), tuple_size);
			memory_move(tr.tuple, tuple, tuple_size);
			finalize_written_tuple(its_p, &tr);
			unmap_for_interim_tuple_region(&tr);
		}
	}

	int need_to_wake_up_consumer = pushed && need_to_wake_up_consumer_UNSAFE(o);

	pthread_mutex_unlock(&(o->output_lock));

	if(need_to_wake_up_consumer)
		o->consumer_operator->trigger_execution(o->consumer_operator);

	return pushed;
}

int produce_tuples_from_operator(operator* o, interim_tuple_store* its_p)
{
	int pushed = 0;

	pthread_mutex_lock(&(o->output_lock));

	// proceed only if the consumer_operator is alive
	if((o->consumer_operator != NULL) && !can_not_proceed_for_execution_operator(o->consumer_operator))
	{
		pushed = 1;

		if(!insert_tail_in_singlylist(&(o->output_buffers), its_p))
			exit(-1);
	}

	int need_to_wake_up_consumer = pushed && need_to_wake_up_consumer_UNSAFE(o);

	pthread_mutex_unlock(&(o->output_lock));

	if(need_to_wake_up_consumer)
		o->consumer_operator->trigger_execution(o->consumer_operator);

	return pushed;
}

interim_tuple_store* consume_from_operator(operator* producer, uint64_t min_bytes_to_consume, int* no_more_data)
{
	interim_tuple_store* its_p = NULL;

	pthread_mutex_lock(&(producer->output_lock));

	// only if the operator is killed and ther is nothing in output_buffers then no_more_data will be produced
	if(is_killed_operator(producer) && is_empty_singlylist(&(producer->output_buffers)))
		(*no_more_data) = 1;

	// proceed only if the consumer_operator is alive
	if((!(*no_more_data)) && (producer->consumer_operator != NULL) && !can_not_proceed_for_execution_operator(producer->consumer_operator))
	{
		its_p = (interim_tuple_store*) get_head_of_singlylist(&(producer->output_buffers));
		if(its_p != NULL)
			if(!remove_head_from_singlylist(&(producer->output_buffers))) // remove must not fail
				exit(-1);
	}

	pthread_mutex_unlock(&(producer->output_lock));

	return its_p;
}

// query plan functions

query_plan* get_new_query_plan(transaction* curr_tx, uint32_t operators_count)
{
	query_plan* qp = malloc(sizeof(query_plan));
	if(qp == NULL)
		exit(-1);

	qp->curr_tx = curr_tx;
	if(!initialize_arraylist(&(qp->operators), operators_count))
		exit(-1);

	return qp;
}

operator* get_new_registered_operator_for_query_plan(query_plan* qp)
{
	operator* o = malloc(sizeof(operator));
	if(o == NULL)
		exit(-1);

	o->self_query_plan = qp;

	o->inputs = NULL;
	o->context = NULL;

	o->trigger_execution = NULL;
	o->operator_release_latches_and_store_context = NULL;
	o->free_resources = NULL;

	pthread_cond_init_with_monotonic_clock(&(o->wait_on_lock_table_for_lock));

	pthread_mutex_init(&(o->output_lock), NULL);
	initialize_singlylist(&(o->output_buffers), offsetof(interim_tuple_store, embed_node_sl));
	o->consumer_operator = NULL;
	o->consumer_trigger_on_bytes_accumulated = 8192;
	o->output_tuple_def = NULL;
	o->output_key_element_ids = NULL;
	o->output_key_compare_direction = NULL;
	o->output_key_element_count = 0;

	pthread_mutex_init(&(o->kill_lock), NULL);
	pthread_cond_init_with_monotonic_clock(&(o->wait_until_killed));
	o->is_killed = 0;
	o->is_kill_signal_sent = 0;
	init_empty_dstring(&(o->kill_reason), 0);

	if(is_full_arraylist(&(qp->operators)) && !expand_arraylist(&(qp->operators)))
		exit(-1);
	push_back_to_arraylist(&(qp->operators), o);

	return o;
}

void start_all_operators_for_query_plan(query_plan* qp)
{
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operators)); i++)
	{
		operator* o = (operator*) get_from_arraylist(&(qp->operators), i);
		o->trigger_execution(o);
	}
}

void shutdown_query_plan(query_plan* qp, dstring kill_reason)
{
	// send kill signal to all the operators
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operators)); i++)
	{
		operator* o = (operator*) get_from_arraylist(&(qp->operators), i);
		send_kill_signal_to_operator(o, kill_reason);
	}

	// spurious wake up all operators waiting on the lock tables
	pthread_mutex_lock(&(qp->curr_tx->db->lock_manager_external_lock));
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operators)); i++)
	{
		operator* o = (operator*) get_from_arraylist(&(qp->operators), i);
		spurious_wake_up_operator(o);
	}
	pthread_mutex_unlock(&(qp->curr_tx->db->lock_manager_external_lock));
}

void shutdown_query_plan_LOCK_TABLE_UNSAFE(query_plan* qp, dstring kill_reason)
{
	// send kill signal to all the operators
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operators)); i++)
	{
		operator* o = (operator*) get_from_arraylist(&(qp->operators), i);
		send_kill_signal_to_operator(o, kill_reason);
	}

	// spurious wake up all operators waiting on the lock tables
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operators)); i++)
	{
		operator* o = (operator*) get_from_arraylist(&(qp->operators), i);
		spurious_wake_up_operator(o);
	}
}

void wait_for_shutdown_of_query_plan(query_plan* qp)
{
	// wait for the operator to die
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operators)); i++)
	{
		operator* o = (operator*) get_from_arraylist(&(qp->operators), i);
		wait_for_operator_to_die(o);
	}
}

void destroy_query_plan(query_plan* qp, dstring* kill_reasons)
{
	// release all of the operator resources
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operators)); i++)
	{
		operator* o = (operator*) get_from_arraylist(&(qp->operators), i);

		o->free_resources(o);

		o->self_query_plan = NULL;

		o->inputs = NULL;
		o->context = NULL;

		o->trigger_execution = NULL;
		o->operator_release_latches_and_store_context = NULL;
		o->free_resources = NULL;

		pthread_cond_destroy(&(o->wait_on_lock_table_for_lock));

		while(NULL != get_head_of_singlylist(&(o->output_buffers)))
		{
			interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&(o->output_buffers));
			remove_head_from_singlylist(&(o->output_buffers));
			delete_interim_tuple_store(its_p);
		}

		pthread_mutex_destroy(&(o->output_lock));
		o->consumer_operator = NULL;
		o->consumer_trigger_on_bytes_accumulated = 8192;
		o->output_tuple_def = NULL;
		o->output_key_element_ids = NULL;
		o->output_key_compare_direction = NULL;
		o->output_key_element_count = 0;

		pthread_mutex_destroy(&(o->kill_lock));
		pthread_cond_destroy(&(o->wait_until_killed));
		o->is_killed = 0;
		o->is_kill_signal_sent = 0;

		// concatenate the kill reason passed
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

void notify_unblocked(void* context_p, void* transaction_vp, void* task_vp)
{
	// rhendb provided the callback so we are the context
	rhendb* rdb = context_p;

	// wake up the right operator, for the corresponding transaction
	if(((uintptr_t)transaction_vp) >= 1024)
	{
		pthread_mutex_lock(&(rdb->lock_manager_external_lock));

		transaction* tx = transaction_vp;
		operator* o = task_vp;
		if(o->self_query_plan->curr_tx == tx)
			pthread_cond_broadcast(&(o->wait_on_lock_table_for_lock));

		pthread_mutex_unlock(&(rdb->lock_manager_external_lock));
	}
	else
		printf("notify_unblocked( trx_id = %"PRIuPTR" , task_id = %"PRIuPTR" )\n\n", ((uintptr_t)transaction_vp), ((uintptr_t)task_vp));
}

void notify_deadlocked(void* context_p, void* transaction_vp)
{
	// rhendb provided the callback so we are the context
	rhendb* rdb = context_p;

	// notify the right transaction's curr_query, for the deadlock
	if(((uintptr_t)transaction_vp) >= 1024)
	{
		pthread_mutex_lock(&(rdb->lock_manager_external_lock));

		transaction* tx = transaction_vp;
		shutdown_query_plan_LOCK_TABLE_UNSAFE(tx->curr_query, get_dstring_pointing_to_literal_cstring("DEADLOCK DETECTED"));

		pthread_mutex_unlock(&(rdb->lock_manager_external_lock));
	}
	else
		printf("notify_deadlocked( trx_id = %"PRIuPTR" )\n\n",  ((uintptr_t)transaction_vp));
}