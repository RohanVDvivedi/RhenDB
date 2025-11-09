#include<rhendb/query_plan_interface.h>

// operator functions

int is_kill_signal_sent(operator* o)
{
	pthread_mutex_lock(&(o->kill_lock));

	int is_kill_signal_sent = !!(o->is_kill_signal_sent);

	pthread_mutex_unlock(&(o->kill_lock));

	return is_kill_signal_sent
}

void mark_operator_self_killed(operator* o)
{
	pthread_mutex_lock(&(o->kill_lock));

	o->is_killed = 1;
	pthread_cond_broadcast(&(o->kill_lock));

	pthread_mutex_unlock(&(o->kill_lock));
}

void send_kill_and_wait_for_operator_to_die_FROM_NON_OPERATOR(operator* o)
{
	pthread_mutex_lock(&(o->kill_lock));

	o->is_kill_signal_sent = 1;

	while(!(o->is_killed))
		pthread_cond_wait(&(o->kill_lock), &(o->wait_until_killed));

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

	if(!will_unsigned_sub_underflow(uint32_t, ob->producers_count, change_amount))
	{
		ob->producers_count -= change_amount;
		result = 1;
	}

	if(result && ob->producers_count == 0)
	{
		// producers count just reached 0, no new data will be available so wake up all consumers
		tiber_cond_broadcast(&(ob->wait));
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

	if(!will_unsigned_sub_underflow(uint32_t, ob->consumers_count, change_amount))
	{
		ob->consumers_count -= change_amount;
		result = 1;
	}

	EXIT:;
	pthread_mutex_unlock(&(ob->lock));

	return result;
}

int push_to_operator_buffer(operator_buffer* ob, temp_tuple_store* tts)
{
	int pushed = 0;

	pthread_mutex_lock(&(ob->lock));

	// proceed only if there are both producers and consumers on this operator_buffer
	if(ob->producers_count > 0 && ob->consumers_count > 0)
	{
		pushed = insert_tail_in_linkedlist(&(ob->tuple_stores), tts);
		if(pushed)
		{
			ob->tuple_stores_count += 1;
			ob->tuples_count += tts->tuples_count;

			// wake up blocking waiters
			pthread_cond_broadcast(&(ob->wait));
		}
	}

	// wake up atleast 1 operator, that is waiting for data, if all of them are in waiting state
	operator* to_wake_up = NULL;
	if(ob->waiting_consumers_count == ob->consumers_count)
	{
		while(!is_empty_linkedlist(&(ob->waiting_consumers)))
		{
			to_wake_up = (operator*) get_head_of_linkedlist(&(ob->waiting_consumers));
			remove_from_linkedlist(&(ob->waiting_consumers), to_wake_up);

			if(set_operator_state(to_wake_up, OPERATOR_QUEUED))
				break;

			to_wake_up = NULL;
		}
	}


	pthread_mutex_unlock(&(ob->lock));

	// enqueue the operator that we just woke up
	if(to_wake_up)
		enqueue_operator(to_wake_up);

	return pushed;
}

temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, uint64_t timeout_in_microseconds)
{
	pthread_mutex_lock(&(ob->lock));

	if(timeout_in_microseconds != NON_BLOCKING)
	{
		int wait_error = 0;
		while((get_head_of_linkedlist(&(ob->tuple_stores)) == NULL) && !is_operator_killed_or_to_be_killed(ob->producer) && !(ob->prohibit_usage) && !wait_error)
		{
			if(timeout_in_microseconds == BLOCKING)
				wait_error = pthread_cond_wait(&(ob->wait), &(ob->lock));
			else
				wait_error = pthread_cond_timedwait_for_microseconds(&(ob->wait), &(ob->lock), &timeout_in_microseconds);
		}
	}

	temp_tuple_store* tts = NULL;

	// if there is prohibit_usage on the operator_buffer
	// OR it is empty with the producer in OPERATOR_KILLED state, then we kill the consumer
	if((ob->prohibit_usage) || ((get_head_of_linkedlist(&(ob->tuple_stores)) == NULL) && is_operator_killed_or_to_be_killed(ob->producer)))
	{
		set_operator_state(ob->consumer, OPERATOR_TO_BE_KILLED);
	}
	else
	{
		// fetch the head
		tts = (temp_tuple_store*) get_head_of_linkedlist(&(ob->tuple_stores));
		if(tts != NULL)
		{
			// if it exists, remove it as the return value and return it
			remove_from_linkedlist(&(ob->tuple_stores), tts);
		}
		else
		{
			// else the set the operator in OPERATOR_WAITING_TO_BE_NOTIFIED state
			set_operator_state(ob->consumer, OPERATOR_WAITING_TO_BE_NOTIFIED);
		}
	}

	pthread_mutex_unlock(&(ob->lock));

	return tts;
}