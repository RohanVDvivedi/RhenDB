#include<rhendb/query_plan_interface.h>

#include<rhendb/transaction.h>

#include<posixutils/pthread_cond_utils.h>

// operator functions

static int is_killed_operator(operator* o)
{
	pthread_mutex_lock(&(o->state_lock));

	int result = (o->state == OPERATOR_KILLED);

	pthread_mutex_unlock(&(o->state_lock));

	return result;
}

int can_not_proceed_for_execution_operator(operator* o)
{
	pthread_mutex_lock(&(o->state_lock));

	int result = (!!(o->is_kill_signal_sent));

	pthread_mutex_unlock(&(o->state_lock));

	return result;
}

void send_kill_signal_to_operator(operator* o, dstring kill_reason)
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

// returns true, if the operator as killed
// if this call succeeds please call trigger_execution_on_operator(o->consumer_operator);
static int process_kill_signal_if_received_for_operator_UNSAFE(operator* o)
{
	// already killed return 1
	if(o->state == OPERATOR_KILLED)
		return 1;

	// only condition for the operator to be placed in OPERATOR_KILLED state
	if(o->is_kill_signal_sent && (o->state == OPERATOR_WAITING || o->state == OPERATOR_QUEUED) && o->queued_jobs_count == 0 && o->running_jobs_count == 0)
	{
		// mark it and put it in killed state
		o->state = OPERATOR_KILLED;

		// wake up anyone aiting for this operator to get killed
		pthread_cond_broadcast(&(o->wait_until_killed));

		return 1;
	}

	// the kill signal as not sent OR there are active jobs queued/running
	return 0;
}

static void trigger_all_consumers_for_operator_UNSAFE(operator* o, int force_trigger)
{
	if(is_empty_linkedlist(&(o->output_consumers)))
		return ;

	consumption_iterator* cit_p = (consumption_iterator*) get_head_of_linkedlist(&(o->output_consumers));
	do
	{
		if(force_trigger || (cit_p->was_consumer_triggered == 0)) // either on force trigger OR the consumer was not priorly triggered, only then trigger
		{
			trigger_execution_on_operator(cit_p->consumer);
			cit_p->was_consumer_triggered = 1; // mark it as already triggered, so the next consecutive produce need not trigger it
		}
		cit_p = (consumption_iterator*) get_next_of_in_linkedlist(&(o->output_consumers), cit_p);
	}
	while(cit_p != get_head_of_linkedlist(&(o->output_consumers)));
}

static void* internal_execute(void* o_vp)
{
	operator* o = o_vp;

	while(1)
	{
		int was_killed = 0;
		int should_run = 0;

		pthread_mutex_lock(&(o->state_lock));
		was_killed = process_kill_signal_if_received_for_operator_UNSAFE(o);
		if(!was_killed)
		{
			if(!(o->is_kill_signal_sent)) // current state has to be OPERATOR_QUEUED
			{
				should_run = 1;
				o->state = OPERATOR_RUNNING;
			}
			else
				o->state = OPERATOR_WAITING;
		}
		pthread_mutex_unlock(&(o->state_lock));

		if(was_killed)
		{
			pthread_mutex_lock(&(o->output_lock));
			trigger_all_consumers_for_operator_UNSAFE(o, 1); // force trigger all consumers
			pthread_mutex_unlock(&(o->output_lock));
			return NULL;
		}

		if(!should_run)
			return NULL;

		o->execute(o);

		int was_triggered_while_we_were_running = 0;

		pthread_mutex_lock(&(o->state_lock));
		if(!(o->is_kill_signal_sent)) // current state has to be OPERATOR_RUNNING
		{
			if(o->is_trigger_signaled_on_running)
			{
				was_triggered_while_we_were_running = 1;
				o->is_trigger_signaled_on_running = 0; // reset this flag
				o->state = OPERATOR_QUEUED; // mark the operator to be queued, but we will just loop again
			}
			else
				o->state = OPERATOR_WAITING;
		}
		else
			o->state = OPERATOR_WAITING;
		was_killed = process_kill_signal_if_received_for_operator_UNSAFE(o);
		pthread_mutex_unlock(&(o->state_lock));

		if(was_killed)
		{
			pthread_mutex_lock(&(o->output_lock));
			trigger_all_consumers_for_operator_UNSAFE(o, 1); // force trigger all consumers
			pthread_mutex_unlock(&(o->output_lock));
			return NULL;
		}

		if(!was_triggered_while_we_were_running)
			return NULL;
	}

	return NULL;
}

void trigger_execution_on_operator(operator* o)
{
	int should_queue = 0;
	int was_killed = 0;

	pthread_mutex_lock(&(o->state_lock));
	was_killed = process_kill_signal_if_received_for_operator_UNSAFE(o);
	if(!was_killed)
	{
		if(!(o->is_kill_signal_sent))
		{
			if(o->state == OPERATOR_RUNNING)
			{
				o->is_trigger_signaled_on_running = 1;
				should_queue = 0;
			}
			else if(o->state == OPERATOR_WAITING)
			{
				o->state = OPERATOR_QUEUED;
				should_queue = 1;
			}
		}
		else
			o->state = OPERATOR_WAITING;
	}
	pthread_mutex_unlock(&(o->state_lock));

	if(was_killed)
	{
		pthread_mutex_lock(&(o->output_lock));
		trigger_all_consumers_for_operator_UNSAFE(o, 1); // force trigger all consumers
		pthread_mutex_unlock(&(o->output_lock));
		return ;
	}

	if(!should_queue)
		return;

	if(!submit_job_executor(o->self_query_plan->curr_tx->db->operator_thread_pool, (void* (*)(void*))(internal_execute), o, NULL, NULL, BLOCKING))
	{
		printf("ISSUE in query_plan_interface : COULD NOT PUSH A PAUSED OPERATOR'S JOB TO QUEUE IT\n");
		exit(-1);
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

	operator* o = ojwp.o;

	int was_killed = 0;
	int should_run = 0;

	pthread_mutex_lock(&(o->state_lock));
	o->queued_jobs_count--;
	was_killed = process_kill_signal_if_received_for_operator_UNSAFE(o);
	if(!was_killed)
	{
		if(!(o->is_kill_signal_sent))
		{
			should_run = 1;
			o->running_jobs_count++;
		}
	}
	pthread_mutex_unlock(&(o->state_lock));

	if(was_killed)
	{
		pthread_mutex_lock(&(o->output_lock));
		trigger_all_consumers_for_operator_UNSAFE(o, 1); // force trigger all consumers
		pthread_mutex_unlock(&(o->output_lock));
		return NULL;
	}

	if(!should_run)
		return NULL;

	ojwp.operator_job_function(ojwp.o, ojwp.param);

	pthread_mutex_lock(&(o->state_lock));
	o->running_jobs_count--;
	was_killed = process_kill_signal_if_received_for_operator_UNSAFE(o);
	pthread_mutex_unlock(&(o->state_lock));

	if(was_killed)
	{
		pthread_mutex_lock(&(o->output_lock));
		trigger_all_consumers_for_operator_UNSAFE(o, 1); // force trigger all consumers
		pthread_mutex_unlock(&(o->output_lock));
		return NULL;
	}

	return NULL;
}

int run_concurrent_job_for_operator(operator* o, void* param, void (*operator_job_function)(operator* o, void* param))
{
	int was_killed = 0;
	int should_queue = 0;

	pthread_mutex_lock(&(o->state_lock));
	was_killed = process_kill_signal_if_received_for_operator_UNSAFE(o);
	if(!was_killed)
	{
		if(!o->is_kill_signal_sent)
		{
			should_queue = 1;
			o->queued_jobs_count++;
		}
	}
	pthread_mutex_unlock(&(o->state_lock));

	if(was_killed)
	{
		pthread_mutex_lock(&(o->output_lock));
		trigger_all_consumers_for_operator_UNSAFE(o, 1); // force trigger all consumers
		pthread_mutex_unlock(&(o->output_lock));
		return 0;
	}

	if(!should_queue)
		return 0;

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

	return 1;
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

#define MIN_OUTPUT_BUFFER_STORE_SIZE (128 * 1024)
#define MAX_OUTPUT_BUFFER_COUNT 3
#define MIN_BYTES_TO_MMAP (16 * 1024)

int produce_tuple_from_operator(operator* o, void* tuple)
{
	int pushed = 0;

	// perform the output transformations
	int need_to_free_output_tuple = 0;
	tuple = process_tuple_transformers(&(o->output_tuple_transformers), tuple, &need_to_free_output_tuple);
	if(tuple == NULL) // this implies selection/filter failed, so just return success for the produce operation
		return 1;

	pthread_mutex_lock(&(o->output_lock));

	// make sure that there is some alive consumer
	int there_are_consumers = 0;
	// loop while there are no consumers, we are supposed to find one
	if(!is_empty_linkedlist(&(o->output_consumers)))
	{
		const consumption_iterator* cit_p = get_head_of_linkedlist(&(o->output_consumers));
		do
		{
			there_are_consumers = (there_are_consumers || (!can_not_proceed_for_execution_operator(cit_p->consumer)));
			cit_p = get_next_of_in_linkedlist(&(o->output_consumers), cit_p);
		}
		while((!there_are_consumers) && (cit_p != get_head_of_linkedlist(&(o->output_consumers))));
	}

	// proceed only if some consumer is alive
	if(there_are_consumers)
	{
		pushed = 1;

		// fetch tail, create one and insert if it does not exists
		interim_tuple_store* its_p = (interim_tuple_store*) get_tail_of_singlylist(&(o->output_buffers));

		int produce_new_tuple_store = 0;

		if(its_p == NULL) // no output_buffers, e surely need to make 1
			produce_new_tuple_store = 1;
		else if(o->output_buffers_count < MAX_OUTPUT_BUFFER_COUNT) // there are some, so make one only if the tail has more than minimum bytes
			produce_new_tuple_store = (get_total_bytes_in_interim_tuple_store(its_p) >= MIN_OUTPUT_BUFFER_STORE_SIZE);
		else // else do not make one
			produce_new_tuple_store = 0;

		if(produce_new_tuple_store)
		{
			// we are producing a new buffer/chunk for tuples, so unmap the regions for the previous tail
			if(its_p != NULL)
				unmap_all_embed_regions_in_interim_tuple_store(its_p);

			its_p = get_new_interim_tuple_store(".");
			extend_interim_tuple_store(its_p, MIN_OUTPUT_BUFFER_STORE_SIZE);

			if(!insert_tail_in_singlylist(&(o->output_buffers), its_p))
				exit(-1);
			o->output_buffers_count++;
		}

		// append the tuple in this tail interim_tuple_store
		append_tuple_to_interim_tuple_store2(its_p, &(its_p->embed_regions[0]), tuple, &(get_output_def_for_tuple_transformers(&(o->output_tuple_transformers))->size_def), MIN_BYTES_TO_MMAP);
	}

	// wake up all consumers, only if we pushed
	if(pushed)
		trigger_all_consumers_for_operator_UNSAFE(o, 0); // do not force trigger all consumers, trigger only the ones that were not triggered in the past

	pthread_mutex_unlock(&(o->output_lock));

	if(need_to_free_output_tuple)
		free(tuple);

	return pushed;
}

consumption_iterator* create_consumption_iterator(operator* producer, operator* consumer, consumption_iterator* clone_cit_p)
{
	if(clone_cit_p != NULL)
		if(clone_cit_p->producer != producer || clone_cit_p->consumer != consumer)
			return NULL;

	consumption_iterator* cit_p = malloc(sizeof(consumption_iterator));
	cit_p->producer = producer;
	cit_p->was_consumer_triggered = 0;
	cit_p->consumer = consumer;
	initialize_llnode(&(cit_p->embed_node_for_output_consumers));

	cit_p->curr_store = NULL;
	cit_p->curr_region = INIT_INTERIM_TUPLE_REGION;

	pthread_mutex_lock(&(producer->output_lock));

	if(clone_cit_p != NULL)
		cit_p->curr_store = clone_cit_p->curr_store;
	if(cit_p->curr_store == NULL)
		cit_p->curr_store = (interim_tuple_store*) get_head_of_singlylist(&(producer->output_buffers));

	if(cit_p->curr_store != NULL)
	{
		uint64_t offset = 0;
		if(clone_cit_p != NULL && !is_empty_interim_tuple_region(&(clone_cit_p->curr_region)))
			offset = curr_tuple_offset_for_interim_tuple_region(&(clone_cit_p->curr_region));
		mmap_for_reading_tuple(cit_p->curr_store, &(cit_p->curr_region), offset, &(get_tuple_def_for_tuples_to_be_consumed_from(producer)->size_def), MIN_BYTES_TO_MMAP);
	}
	else
		cit_p->curr_region = INIT_INTERIM_TUPLE_REGION;

	insert_tail_in_linkedlist(&(producer->output_consumers), cit_p);

	pthread_mutex_unlock(&(producer->output_lock));

	return cit_p;
}

static void destroy_all_un_referenced_output_buffers_UNSAFE(operator* o)
{
	// iterate while there are output_buffers
	while(o->output_buffers_count > 0)
	{
		// fetch the head, the one with the oldest tuples in the list
		interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&(o->output_buffers));

		// preserve a flag suggesting if its_p is referenced
		int is_referenced = 0;
		if(!is_empty_linkedlist(&(o->output_consumers)))
		{
			// iterate over all the output_consumers
			const consumption_iterator* cit_p = get_head_of_linkedlist(&(o->output_consumers));
			do
			{
				// a consumption_iterator pointing to being NULL (referencing the oldest one) or its_p
				// is said to be refernecing its_p
				if(cit_p->curr_store == NULL || cit_p->curr_store == its_p)
				{
					is_referenced = 1;
					break;
				}
				cit_p = get_next_of_in_linkedlist(&(o->output_consumers), cit_p);
			}
			while(cit_p != get_head_of_linkedlist(&(o->output_consumers)));
		}

		// if its_p is referenced, then break out of the loop
		if(is_referenced)
			break;

		// else it becomes safe to discard the its_p, the head of the output_buffers
		remove_head_from_singlylist(&(o->output_buffers));
		delete_interim_tuple_store(its_p);
		o->output_buffers_count--;
	}
}

void destroy_consumption_iterator(consumption_iterator* cit_p)
{
	pthread_mutex_lock(&(cit_p->producer->output_lock));

	// check if cit_p points to head
	int points_to_head = ((cit_p->curr_store == NULL) || (cit_p->curr_store == get_head_of_singlylist(&(cit_p->producer->output_buffers))));

	// remove cit_p from existence
	remove_from_linkedlist(&(cit_p->producer->output_consumers), cit_p);
	unmap_for_interim_tuple_region(&(cit_p->curr_region));

	// if the cit_p pointed to head, then it might just have become safe to delete older output_buffers
	if(points_to_head)
		destroy_all_un_referenced_output_buffers_UNSAFE(cit_p->producer);

	pthread_mutex_unlock(&(cit_p->producer->output_lock));

	// safe to free
	free(cit_p);
}

int points_to_same_tuple_for_consumtion_iterators(const consumption_iterator* cit1_p, const consumption_iterator* cit2_p)
{
	if(cit1_p->producer != cit2_p->producer)
		return 0;

	int points_to_same_tuple = 0;

	pthread_mutex_lock(&(cit1_p->producer->output_lock));

	// if both are null they are point pointing to nothing, so same-same
	if(cit1_p->curr_store == NULL && cit2_p->curr_store == NULL)
		points_to_same_tuple = 1;
	else if(cit1_p->curr_store != cit2_p->curr_store) // if they are not same fail
		points_to_same_tuple = 0;
	else
	{
		// in this case the curr_store are same, and not NULL
		points_to_same_tuple = (curr_tuple_offset_for_interim_tuple_region(&(cit1_p->curr_region)) == curr_tuple_offset_for_interim_tuple_region(&(cit2_p->curr_region)));
	}

	pthread_mutex_unlock(&(cit1_p->producer->output_lock));

	return points_to_same_tuple;
}

const void* consume_for_consumption_iterator(consumption_iterator* cit_p, int* no_more_data)
{
	const void* tuple = NULL;
	(*no_more_data) = 0;

	pthread_mutex_lock(&(cit_p->producer->output_lock));

	// only if the operator is killed and there is nothing in output_buffers then no_more_data will be produced
	if(is_killed_operator(cit_p->producer) && is_empty_singlylist(&(cit_p->producer->output_buffers)))
		(*no_more_data) = 1;

	int clean_up_oldest_buffer = 0;
	if(!(*no_more_data))
	{
		if(!is_empty_singlylist(&(cit_p->producer->output_buffers)))
		{
			if(cit_p->curr_store == NULL)
				cit_p->curr_store = (interim_tuple_store*) get_head_of_singlylist(&(cit_p->producer->output_buffers));

			uint64_t offset = 0;
			if(!is_empty_interim_tuple_region(&(cit_p->curr_region)))
				offset = next_tuple_offset_for_interim_tuple_region(&(cit_p->curr_region));

			if(offset == cit_p->curr_store->next_tuple_offset) // change the buffer
			{
				offset = 0;

				if(cit_p->curr_store == get_head_of_singlylist(&(cit_p->producer->output_buffers)))
					clean_up_oldest_buffer = 1;

				if(cit_p->curr_store == get_tail_of_singlylist(&(cit_p->producer->output_buffers)))
				{
					clean_up_oldest_buffer = 0; // we are not going next, so can not clean up for sure
					if(is_killed_operator(cit_p->producer))
						(*no_more_data) = 1;
				}
				else
				{
					// but first unmap the old mapped region
					unmap_for_interim_tuple_region(&(cit_p->curr_region));

					cit_p->curr_store = (interim_tuple_store*) get_next_of_in_singlylist(&(cit_p->producer->output_buffers), cit_p->curr_store);
					mmap_for_reading_tuple(cit_p->curr_store, &(cit_p->curr_region), offset, &(get_tuple_def_for_tuples_to_be_consumed_from(cit_p->producer)->size_def), MIN_BYTES_TO_MMAP);
					tuple = cit_p->curr_region.tuple;
				}
			}
			else
			{
				mmap_for_reading_tuple(cit_p->curr_store, &(cit_p->curr_region), offset, &(get_tuple_def_for_tuples_to_be_consumed_from(cit_p->producer)->size_def), MIN_BYTES_TO_MMAP);
				tuple = cit_p->curr_region.tuple;
			}
		}
	}

	if(clean_up_oldest_buffer)
		destroy_all_un_referenced_output_buffers_UNSAFE(cit_p->producer);

	// if something was consumed by this consumer then clear its was_triggered flag, so that we would receive the next trigger
	if(tuple != NULL)
		cit_p->was_consumer_triggered = 0;

	pthread_mutex_unlock(&(cit_p->producer->output_lock));

	return tuple;
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
	o->output_buffers_count = 0;
	initialize_linkedlist(&(o->output_consumers), offsetof(consumption_iterator, embed_node_for_output_consumers));
	init_tuple_transformers(&(o->output_tuple_transformers), NULL); // must initialize it again, unless it is the sink operator

	pthread_mutex_init(&(o->state_lock), NULL);
	pthread_cond_init_with_monotonic_clock(&(o->wait_until_killed));
	o->state = OPERATOR_WAITING;
	o->queued_jobs_count = 0;
	o->running_jobs_count = 0;
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

		destroy_tuple_transformers(&(o->output_tuple_transformers));

		pthread_mutex_destroy(&(o->state_lock));
		pthread_cond_destroy(&(o->wait_until_killed));
		o->state = OPERATOR_KILLED;
		o->queued_jobs_count = 0;
		o->running_jobs_count = 0;
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