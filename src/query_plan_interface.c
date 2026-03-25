#include<rhendb/query_plan_interface.h>

#include<rhendb/transaction.h>

#include<posixutils/pthread_cond_utils.h>

// operator functions

int is_killed_operator(operator* o)
{
	pthread_mutex_lock(&(o->state_lock));

	int result = (o->state == OPERATOR_KILLED);

	pthread_mutex_unlock(&(o->state_lock));

	return result;
}

int can_not_proceed_for_execution_operator(operator* o)
{
	pthread_mutex_lock(&(o->state_lock));

	int result = (!!(o->is_kill_signal_sent)) || (o->state == OPERATOR_KILLED);

	pthread_mutex_unlock(&(o->state_lock));

	return result;
}

void mark_operator_self_killed(operator* o, dstring kill_reason)
{
	pthread_mutex_lock(&(o->state_lock));

	// concatenate the kill reason passed
	if(!is_empty_dstring(&kill_reason))
	{
		if(!is_empty_dstring(&(o->kill_reason)))
			if(!concatenate_char(&(o->kill_reason), '$'))
				exit(-1);
		if(!concatenate_dstring(&(o->kill_reason), &kill_reason))
			exit(-1);
	}

	o->state = OPERATOR_KILLED;
	pthread_cond_broadcast(&(o->wait_until_killed));

	pthread_mutex_unlock(&(o->state_lock));

	// force the consumer to read through all that has been left to be consumed
	if(o->consumer_operator != NULL)
		trigger_execution_on_operator(o->consumer_operator);
}

static void* internal_execute(void* o_vp)
{
	operator* o = o_vp;

	while(1)
	{
		int was_killed = 0;

		pthread_mutex_lock(&(o->state_lock));
		if(o->is_kill_signal_sent)
		{
			o->state = OPERATOR_KILLED;
			pthread_cond_broadcast(&(o->wait_until_killed));
			was_killed = 1;
		}
		else
			o->state = OPERATOR_RUNNING;
		pthread_mutex_unlock(&(o->state_lock));

		if(was_killed)
			goto WAS_KILLED;

		o->execute(o);

		int was_triggered_while_we_were_running = 0;

		pthread_mutex_lock(&(o->state_lock));
		if(o->is_kill_signal_sent) // if the user exited while running, then mark it now to be waiting
		{
			o->state = OPERATOR_KILLED;
			pthread_cond_broadcast(&(o->wait_until_killed));
			was_killed = 1;
		}
		else if(o->state == OPERATOR_RUNNING)
		{
			if(o->is_trigger_signaled_on_running)
			{
				was_triggered_while_we_were_running = 1;
				o->is_trigger_signaled_on_running = 0; // reset this flag
				o->state = OPERATOR_QUEUED; // mark the operator to be queued, but we will just loop again
			}
			else
				o->state = OPERATOR_WAITING; // else put it into waiting state
		}
		pthread_mutex_unlock(&(o->state_lock));

		if(was_killed)
			goto WAS_KILLED;

		if(was_triggered_while_we_were_running)
			continue;
		else
			break;
	}

	return NULL;

	WAS_KILLED:
	// force the consumer to read through all that has been left to be consumed, as we just killed the operator
	if(o->consumer_operator != NULL)
		trigger_execution_on_operator(o->consumer_operator);
	return NULL;
}

void trigger_execution_on_operator(operator* o)
{
	int should_queue = 0;

	pthread_mutex_lock(&(o->state_lock));
	if(o->state == OPERATOR_RUNNING) // if the operator is already in running state, mark to let the operator know that a trigger was signalled
	{
		o->is_trigger_signaled_on_running = 1;
		should_queue = 0;
	}
	else if(o->state == OPERATOR_WAITING) // only a WAITING operator can be queued
	{
		o->state = OPERATOR_QUEUED; // change the state to OPERATOR_QUEUED, right away to prevent double queuing
		should_queue = 1;
	}
	pthread_mutex_unlock(&(o->state_lock));

	if(should_queue)
	{
		if(!submit_job_executor(o->self_query_plan->curr_tx->db->operator_thread_pool, (void* (*)(void*))(internal_execute), o, NULL, NULL, BLOCKING))
		{
			printf("ISSUE in query_plan_interface : COULD NOT PUSH A PAUSED OPERATOR'S JOB TO QUEUE IT\n");
			exit(-1);
		}
	}
}

typedef struct operator_job_wrapper_params operator_job_wrapper_params;
struct operator_job_wrapper_params
{
	operator* o;
	void* param;
	void (*operator_job_function)(operator* o, void* param);
};

static void* operator_job_wrapper_function(void* ojwp_vp)
{
	operator_job_wrapper_params ojwp = *((operator_job_wrapper_params*)(ojwp_vp));
	free(ojwp_vp);

	// do not run the operator's job, if it is not allowed to proceed
	// if it is killed or marked to be killed
	if(can_not_proceed_for_execution_operator(ojwp.o))
		return NULL;

	ojwp.operator_job_function(ojwp.o, ojwp.param);

	return NULL;
}

void run_concurrent_job_for_operator(operator* o, void* param, void (*operator_job_function)(operator* o, void* param))
{
	// allocate the parameter for the operator_job_wrapper_function
	operator_job_wrapper_params* ojwp_p = malloc(sizeof(operator_job_wrapper_params));
	(*ojwp_p) = (operator_job_wrapper_params){
		.o = o,
		.param = param,
		.operator_job_function = operator_job_function,
	};

	// and push it to thread pool for execution in a concurrent threadpool
	if(!submit_job_executor(o->self_query_plan->curr_tx->db->operator_thread_pool, (void* (*)(void*))(operator_job_wrapper_function), ojwp_p, NULL, NULL, BLOCKING))
	{
		printf("ISSUE in query_plan_interface : COULD NOT PUSH A PAUSED OPERATOR'S CURRENT JOB TO RUN IT\n");
		exit(-1);
	}
}

// called by the query_plan to send kill to an operator
static void send_kill_signal_to_operator(operator* o, dstring kill_reason)
{
	pthread_mutex_lock(&(o->state_lock));

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

	pthread_mutex_unlock(&(o->state_lock));
}

// wait here after you send the operators a kill signal, waiting for them to die
static void wait_for_operator_to_die(operator* o)
{
	pthread_mutex_lock(&(o->state_lock));

	while(o->state != OPERATOR_KILLED)
		pthread_cond_wait(&(o->wait_until_killed), &(o->state_lock));

	pthread_mutex_unlock(&(o->state_lock));
}

static void spurious_wake_up_operator(operator* o)
{
	pthread_cond_broadcast(&(o->wait_on_lock_table_for_lock));
	trigger_execution_on_operator(o);
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

	// perform the output transformations
	int need_to_free_output_tuple = 0;
	tuple = process_tuple_transformers(&(o->output_tuple_transformers), tuple, &need_to_free_output_tuple);

	pthread_mutex_lock(&(o->output_lock));

	// proceed only if the consumer is alive
	if((o->consumer_operator != NULL) && !can_not_proceed_for_execution_operator(o->consumer_operator))
	{
		pushed = 1;

		// fetch tail, create one and insert if it does not exists
		interim_tuple_store* its_p = (interim_tuple_store*) get_tail_of_singlylist(&(o->output_buffers));
		if(its_p == NULL)
		{
			its_p = get_new_interim_tuple_store(".");

			if(!insert_tail_in_singlylist(&(o->output_buffers), its_p))
				exit(-1);
		}

		// append the tuple in this tail interim_tuple_store
		append_tuple_to_interim_tuple_store(its_p, tuple, &(get_output_def_for_tuple_transformers(&(o->output_tuple_transformers))->size_def));
	}

	int need_to_wake_up_consumer = pushed && need_to_wake_up_consumer_UNSAFE(o);

	pthread_mutex_unlock(&(o->output_lock));

	if(need_to_free_output_tuple)
		free(tuple);

	if(need_to_wake_up_consumer)
		trigger_execution_on_operator(o->consumer_operator);

	return pushed;
}

#define MERGE_THRESHOLD (16 * 1024)

int produce_tuples_from_operator(operator* o, interim_tuple_store* its_p)
{
	int pushed = 0;

	if(has_no_tuple_transformers(&(o->output_tuple_transformers))) // if there are no output transformations, then this is optimzation path
	{
		pthread_mutex_lock(&(o->output_lock));

		// proceed only if the consumer_operator is alive
		if((o->consumer_operator != NULL) && !can_not_proceed_for_execution_operator(o->consumer_operator))
		{
			pushed = 1;

			interim_tuple_store* tail_its_p = (interim_tuple_store*) get_tail_of_singlylist(&(o->output_buffers));

			if(tail_its_p == NULL || get_total_bytes_in_interim_tuple_store(its_p) > MERGE_THRESHOLD)
			{
				if(!insert_tail_in_singlylist(&(o->output_buffers), its_p))
					exit(-1);
			}
			else
			{
				append_all_from_another_interim_tuple_store(tail_its_p, its_p);
				delete_interim_tuple_store(its_p);
			}
		}

		int need_to_wake_up_consumer = pushed && need_to_wake_up_consumer_UNSAFE(o);

		pthread_mutex_unlock(&(o->output_lock));

		if(need_to_wake_up_consumer)
			trigger_execution_on_operator(o->consumer_operator);
	}
	else
	{
		// else loop and produce 1 tuple at a time
		FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(tuple, tuple_index, tuple_offset, &(get_input_def_for_tuple_transformers(&(o->output_tuple_transformers))->size_def), its_p, MERGE_THRESHOLD, {
			pushed = produce_tuple_from_operator(o, tuple);
			if(!pushed)
				break;
		});
	}

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
		{
			if((its_p->next_tuple_offset >= min_bytes_to_consume) || (get_head_of_singlylist(&(producer->output_buffers)) != get_tail_of_singlylist(&(producer->output_buffers))) || is_killed_operator(producer))
			{
				if(!remove_head_from_singlylist(&(producer->output_buffers))) // remove must not fail
					exit(-1);
			}
			else
				its_p = NULL;
		}
	}

	pthread_mutex_unlock(&(producer->output_lock));

	return its_p;
}

const tuple_def* get_tuple_def_for_tuples_to_be_consumed_from(operator* o)
{
	return get_output_def_for_tuple_transformers(&(o->output_tuple_transformers));
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

	o->execute = NULL;
	o->operator_release_latches_and_store_context = NULL;
	o->free_resources = NULL;

	pthread_cond_init_with_monotonic_clock(&(o->wait_on_lock_table_for_lock));

	pthread_mutex_init(&(o->output_lock), NULL);
	initialize_singlylist(&(o->output_buffers), offsetof(interim_tuple_store, embed_node_sl));
	init_tuple_transformers(&(o->output_tuple_transformers), NULL); // must initialize it again, unless it is the sink operator
	o->consumer_operator = NULL;
	o->consumer_trigger_on_bytes_accumulated = 8192;
	o->output_key_element_ids = NULL;
	o->output_key_compare_direction = NULL;
	o->output_key_element_count = 0;

	pthread_mutex_init(&(o->state_lock), NULL);
	pthread_cond_init_with_monotonic_clock(&(o->wait_until_killed));
	o->state = OPERATOR_WAITING;
	o->is_kill_signal_sent = 0;
	o->is_trigger_signaled_on_running = 0;
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
		trigger_execution_on_operator(o);
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

static void shutdown_query_plan_LOCK_TABLE_UNSAFE(query_plan* qp, dstring kill_reason)
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

		o->execute = NULL;
		o->operator_release_latches_and_store_context = NULL;
		o->free_resources = NULL;

		pthread_cond_destroy(&(o->wait_on_lock_table_for_lock));

		while(NULL != get_head_of_singlylist(&(o->output_buffers)))
		{
			interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&(o->output_buffers));
			remove_head_from_singlylist(&(o->output_buffers));
			delete_interim_tuple_store(its_p);
		}

		destroy_tuple_transformers(&(o->output_tuple_transformers));

		pthread_mutex_destroy(&(o->output_lock));
		o->consumer_operator = NULL;
		o->consumer_trigger_on_bytes_accumulated = 8192;
		o->output_key_element_ids = NULL;
		o->output_key_compare_direction = NULL;
		o->output_key_element_count = 0;

		pthread_mutex_destroy(&(o->state_lock));
		pthread_cond_destroy(&(o->wait_until_killed));
		o->state = OPERATOR_KILLED;
		o->is_kill_signal_sent = 0;
		o->is_trigger_signaled_on_running = 0;

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
			spurious_wake_up_operator(o);

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
		shutdown_query_plan_LOCK_TABLE_UNSAFE(tx->curr_query, get_dstring_pointing_to_literal_cstring("DEADLOCK_DETECTED"));

		pthread_mutex_unlock(&(rdb->lock_manager_external_lock));
	}
	else
		printf("notify_deadlocked( trx_id = %"PRIuPTR" )\n\n",  ((uintptr_t)transaction_vp));
}