#include<rhendb/query_plan_interface.h>

#include<posixutils/pthread_cond_utils.h>

// operator functions

int is_kill_signal_sent(operator* o)
{
	pthread_mutex_lock(&(o->kill_lock));

	int is_kill_signal_sent = !!(o->is_kill_signal_sent);

	pthread_mutex_unlock(&(o->kill_lock));

	return is_kill_signal_sent;
}

int is_kill_signal_sent_OR_is_killed(operator* o)
{
	pthread_mutex_lock(&(o->kill_lock));

	int is_kill_signal_sent_OR_is_killed = (o->is_kill_signal_sent) || (o->is_killed);

	pthread_mutex_unlock(&(o->kill_lock));

	return is_kill_signal_sent_OR_is_killed;
}

void mark_operator_self_killed(operator* o)
{
	pthread_mutex_lock(&(o->kill_lock));

	o->is_killed = 1;
	pthread_cond_broadcast(&(o->wait_until_killed));

	pthread_mutex_unlock(&(o->kill_lock));
}

void send_kill_and_wait_for_operator_to_die(operator* o)
{
	pthread_mutex_lock(&(o->kill_lock));

	o->is_kill_signal_sent = 1;

	while(!(o->is_killed))
		pthread_cond_wait(&(o->wait_until_killed), &(o->kill_lock));

	pthread_mutex_unlock(&(o->kill_lock));
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

	// you can not add more consumers, if the producers_count has reached 0
	if(ob->producers_count == 0)
		goto EXIT;

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

			// wake up blocking waiters
			pthread_cond_signal(&(ob->wait));
		}
	}

	pthread_mutex_unlock(&(ob->lock));

	return pushed;
}

temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, operator* callee)
{
	temp_tuple_store* tts = NULL;

	pthread_mutex_lock(&(ob->lock));

	// if there is not data and there are producers producing and there is atleast 1 consumer and the callee has not received kill signal yet
	// only then we are allowed to wait
	while((get_head_of_linkedlist(&(ob->tuple_stores)) == NULL) && ob->producers_count > 0 && ob->consumers_count > 0 && !is_kill_signal_sent(callee))
	{
		callee->operator_release_latches_and_store_context(callee);
		pthread_cond_wait(&(ob->wait), &(ob->lock));
	}

	if(ob->consumers_count == 0)
		goto EXIT;

	if(is_kill_signal_sent(callee))
		goto EXIT;

	// now if there is data, remove it from the queue and exit
	tts = (temp_tuple_store*) get_head_of_linkedlist(&(ob->tuple_stores));
	if(tts != NULL)
	{
		remove_from_linkedlist(&(ob->tuple_stores), tts);

		ob->tuple_stores_count -= 1;
		ob->tuples_count -= tts->tuples_count;
	}

	EXIT:;
	pthread_mutex_unlock(&(ob->lock));

	return tts;
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

	if(is_full_arraylist(&(qp->operator_buffers)) && !expand_arraylist(&(qp->operator_buffers)))
		exit(-1);
	push_back_to_arraylist(&(qp->operator_buffers), ob);

	pthread_mutex_init(&(ob->lock), NULL);
	pthread_cond_init_with_monotonic_clock(&(ob->wait));
	ob->tuple_stores_count = 0;
	ob->tuples_count = 0;
	initialize_linkedlist(&(ob->tuple_stores), offsetof(temp_tuple_store, embed_node_ll));
	ob->producers_count = 1;
	ob->consumers_count = 1;

	return ob;
}

void register_operator_for_query_plan(query_plan* qp, operator* o)
{
	if(is_full_arraylist(&(qp->operators)) && !expand_arraylist(&(qp->operators)))
		exit(-1);
	push_back_to_arraylist(&(qp->operators), o);
}

void shutdown_and_destroy_query_plan(query_plan* qp)
{
	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operators)); i++)
	{
		operator* o = (operator*) get_from_arraylist(&(qp->operators), i);
		send_kill_and_wait_for_operator_to_die(o);
		o->free_resources(o);
		free(o);
	}

	for(cy_uint i = 0; i < get_element_count_arraylist(&(qp->operator_buffers)); i++)
	{
		operator_buffer* ob = (operator_buffer*) get_from_arraylist(&(qp->operator_buffers), i);

		pthread_mutex_lock(&(ob->lock));

		while(NULL != get_head_of_linkedlist(&(ob->tuple_stores)))
		{
			temp_tuple_store* tts = (temp_tuple_store*) get_head_of_linkedlist(&(ob->tuple_stores));
			remove_from_linkedlist(&(ob->tuple_stores), tts);

			delete_temp_tuple_store(tts);
		}

		pthread_mutex_unlock(&(ob->lock));

		pthread_mutex_destroy(&(ob->lock));
		pthread_cond_destroy(&(ob->wait));

		free(ob);
	}

	deinitialize_arraylist(&(qp->operators));
	deinitialize_arraylist(&(qp->operator_buffers));
	free(qp);
}