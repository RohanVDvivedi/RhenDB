#include<rhendb/query_plan_interface.h>

#include<rhendb/transaction.h>

#include<posixutils/pthread_cond_utils.h>

// operator functions

int is_kill_signal_sent(operator* o)
{
	pthread_mutex_lock(&(o->kill_lock));

	int is_kill_signal_sent = !!(o->is_kill_signal_sent);

	pthread_mutex_unlock(&(o->kill_lock));

	return is_kill_signal_sent;
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
	while(!is_kill_signal_sent(o) && !(wait_error))
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
			if(is_kill_signal_sent(o))
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

// operator buffer functions

int increment_operator_buffer_producers_count(operator_buffer* ob, uint32_t change_amount)
{
	if(change_amount == 0)
		return 1;

	int result = 0;

	pthread_mutex_lock(&(ob->lock));

	// we can not update producers_count once it reaches 0
	if(ob->producers_count == 0)
		goto EXIT;

	// you can not add more producers, if the consumers_count has reached 0
	if(ob->consumers_count == 0)
		goto EXIT;

	if(!will_unsigned_sum_overflow(uint32_t, ob->producers_count, change_amount))
	{
		ob->producers_count += change_amount;
		result = 1;
	}

	EXIT:;
	pthread_mutex_unlock(&(ob->lock));

	return result;
}

int decrement_operator_buffer_producers_count(operator_buffer* ob, uint32_t change_amount)
{
	if(change_amount == 0)
		return 1;

	int result = 0;

	pthread_mutex_lock(&(ob->lock));

	// we can not update producers_count once it reaches 0
	if(ob->producers_count == 0)
		goto EXIT;

	// you can still decrement producers_count, if there are no consumers

	if(ob->producers_count >= change_amount)
	{
		ob->producers_count -= change_amount;
		result = 1;
	}

	if(result && ob->producers_count == 0)
	{
		// producers count just reached 0, no new data will be available so wake up all consumers
		pthread_cond_broadcast(&(ob->wait));
	}

	EXIT:;
	pthread_mutex_unlock(&(ob->lock));

	return result;
}

int increment_operator_buffer_consumers_count(operator_buffer* ob, uint32_t change_amount)
{
	if(change_amount == 0)
		return 1;

	int result = 0;

	pthread_mutex_lock(&(ob->lock));

	// we can not update consumers_count once it reaches 0
	if(ob->consumers_count == 0)
		goto EXIT;

	// you can not add more consumers, even if the producers_count has reached 0, in order to consume everything quickly

	// only possible to increment consumers, while there still are some consumers
	if(!will_unsigned_sum_overflow(uint32_t, ob->consumers_count, change_amount))
	{
		ob->consumers_count += change_amount;
		result = 1;
	}

	EXIT:;
	pthread_mutex_unlock(&(ob->lock));

	return result;
}

int decrement_operator_buffer_consumers_count(operator_buffer* ob, uint32_t change_amount)
{
	if(change_amount == 0)
		return 1;

	int result = 0;

	pthread_mutex_lock(&(ob->lock));

	// we can not update consumers_count once it reaches 0
	if(ob->consumers_count == 0)
		goto EXIT;

	// you can still decrement consumers_count, if there are no producers

	if(ob->consumers_count >= change_amount)
	{
		ob->consumers_count -= change_amount;
		result = 1;
	}

	EXIT:;
	pthread_mutex_unlock(&(ob->lock));

	return result;
}

int push_to_operator_buffer(operator_buffer* ob, operator* callee, temp_tuple_store* tts)
{
	int pushed = 0;

	pthread_mutex_lock(&(ob->lock));

	// proceed only if there are both producers and consumers on this operator_buffer, and callee has not received kill_signal yet
	if(ob->producers_count > 0 && ob->consumers_count > 0 && !is_kill_signal_sent(callee))
	{
		pushed = insert_tail_in_linkedlist(&(ob->tuple_stores), tts);
		if(pushed)
		{
			ob->tuple_stores_count += 1;
			ob->tuples_count += tts->tuples_count;

			// wake up some blocked waiter
			pthread_cond_signal(&(ob->wait));
		}
	}

	pthread_mutex_unlock(&(ob->lock));

	return pushed;
}

temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, operator* callee, uint64_t timeout_in_microseconds, int* no_more_data)
{
	(*no_more_data) = 0;
	temp_tuple_store* tts = NULL;

	int latches_released = 0;

	pthread_mutex_lock(&(ob->lock));

	// if there is no data and there are producers producing and there is atleast 1 consumer and the callee has not received kill signal yet
	// only then we are allowed to wait
	if(timeout_in_microseconds != NON_BLOCKING)
	{
		int wait_error = 0;
		while((get_head_of_linkedlist(&(ob->tuple_stores)) == NULL) && ob->producers_count > 0 && ob->consumers_count > 0 && !is_kill_signal_sent(callee) && (!wait_error))
		{
			// if latches had not been released up until now, then do it
			if(!latches_released)
			{
				callee->operator_release_latches_and_store_context(callee);
				latches_released = 1;
			}

			wait_error = pthread_cond_timedwait_for_microseconds(&(ob->wait), &(ob->lock), &timeout_in_microseconds);
		}
	}

	if(ob->consumers_count == 0)
	{
		(*no_more_data) = 1;
		goto EXIT;
	}

	if(is_kill_signal_sent(callee))
	{
		(*no_more_data) = 1;
		goto EXIT;
	}

	// now if there is data, remove it from the queue and exit
	tts = (temp_tuple_store*) get_head_of_linkedlist(&(ob->tuple_stores));
	if(tts != NULL)
	{
		(*no_more_data) = 0;

		remove_from_linkedlist(&(ob->tuple_stores), tts);

		ob->tuple_stores_count -= 1;
		ob->tuples_count -= tts->tuples_count;
	}
	else // if there is no more data
	{
		// if there are no more producers then there won't be any more pushes, i.e. end of stream
		if(ob->producers_count == 0)
			(*no_more_data) = 1;
		else
			(*no_more_data) = 0;
	}

	EXIT:;
	pthread_mutex_unlock(&(ob->lock));

	return tts;
}

static void spurious_wake_up_all_for_operator_buffer(operator_buffer* ob)
{
	pthread_mutex_lock(&(ob->lock));
		pthread_cond_broadcast(&(ob->wait));
	pthread_mutex_unlock(&(ob->lock));
}

// query plan functions

query_plan* get_new_query_plan(transaction* curr_tx, uint32_t operators_count, uint32_t operator_buffers_count)
{
	query_plan* qp = malloc(sizeof(query_plan));
	if(qp == NULL)
		exit(-1);

	qp->curr_tx = curr_tx;
	if(!initialize_arraylist(&(qp->operators), operators_count))
		exit(-1);
	if(!initialize_arraylist(&(qp->operator_buffers), operator_buffers_count))
		exit(-1);

	return qp;
}

operator_buffer* get_new_registered_operator_buffer_for_query_plan(query_plan* qp)
{
	operator_buffer* ob = malloc(sizeof(operator_buffer));
	if(ob == NULL)
		exit(-1);

	pthread_mutex_init(&(ob->lock), NULL);
	pthread_cond_init_with_monotonic_clock(&(ob->wait));
	ob->tuple_stores_count = 0;
	ob->tuples_count = 0;
	initialize_linkedlist(&(ob->tuple_stores), offsetof(temp_tuple_store, embed_node_ll));
	ob->producers_count = 1;
	ob->consumers_count = 1;

	if(is_full_arraylist(&(qp->operator_buffers)) && !expand_arraylist(&(qp->operator_buffers)))
		exit(-1);
	push_back_to_arraylist(&(qp->operator_buffers), ob);

	return ob;
}

operator* get_new_registered_operator_for_query_plan(query_plan* qp)
{
	operator* o = malloc(sizeof(operator));
	if(o == NULL)
		exit(-1);

	// ith created operator has i as it's operator_id, and is as ith index in the qp->operators
	o->operator_id = get_element_count_arraylist(&(qp->operators));
	o->self_query_plan = qp;

	o->inputs = NULL;
	o->context = NULL;

	o->start_execution = NULL;
	o->operator_release_latches_and_store_context = NULL;
	o->free_resources = NULL;

	pthread_cond_init_with_monotonic_clock(&(o->wait_on_lock_table_for_lock));

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
		o->start_execution(o);
	}
}

operator* get_operator_for_query_plan(query_plan* qp, uint32_t operator_id)
{
	// using the fact that the operator with given operator_id will always be at the ith index
	if(operator_id < get_element_count_arraylist(&(qp->operators)))
	{
		operator* o = (operator*) get_from_arraylist(&(qp->operators), operator_id);
		if(o->operator_id == operator_id)
			return o;
	}
	return NULL;
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

	// spurious wake up all operator_buffer waiters
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operator_buffers)); i++)
	{
		operator_buffer* ob = (operator_buffer*) get_from_arraylist(&(qp->operator_buffers), i);
		spurious_wake_up_all_for_operator_buffer(ob);
	}
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

	// spurious wake up all operator_buffer waiters
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operator_buffers)); i++)
	{
		operator_buffer* ob = (operator_buffer*) get_from_arraylist(&(qp->operator_buffers), i);
		spurious_wake_up_all_for_operator_buffer(ob);
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

		o->operator_id = 0;
		o->self_query_plan = NULL;

		o->inputs = NULL;
		o->context = NULL;

		o->start_execution = NULL;
		o->operator_release_latches_and_store_context = NULL;
		o->free_resources = NULL;

		pthread_cond_destroy(&(o->wait_on_lock_table_for_lock));

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

		init_empty_dstring(&(o->kill_reason), 0);

		free(o);
	}

	// release all resources for all operator buffers
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operator_buffers)); i++)
	{
		operator_buffer* ob = (operator_buffer*) get_from_arraylist(&(qp->operator_buffers), i);

		while(NULL != get_head_of_linkedlist(&(ob->tuple_stores)))
		{
			temp_tuple_store* tts = (temp_tuple_store*) get_head_of_linkedlist(&(ob->tuple_stores));
			remove_from_linkedlist(&(ob->tuple_stores), tts);

			delete_temp_tuple_store(tts);
		}

		ob->tuples_count = 0;
		ob->tuple_stores_count = 0;

		ob->consumers_count = 0;
		ob->consumers_count = 0;

		pthread_mutex_destroy(&(ob->lock));
		pthread_cond_destroy(&(ob->wait));

		free(ob);
	}

	deinitialize_arraylist(&(qp->operators));
	deinitialize_arraylist(&(qp->operator_buffers));
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

	// debug print if the operator was not found
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

	// debug print if the transaction was not found
	printf("notify_deadlocked( trx_id = %"PRIuPTR" )\n\n",  ((uintptr_t)transaction_vp));
}