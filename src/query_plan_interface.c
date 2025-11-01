#include<rhendb/query_plan_interface.h>

// operator functions

operator_state get_operator_state(operator* o)
{
	pthread_mutex_lock(&(o->lock));

	operator_state state = o->state;

	pthread_mutex_unlock(&(o->lock));

	return state;
}

int wait_until_operator_is_killed(operator* o, uint64_t timeout_in_microseconds)
{
	pthread_mutex_lock(&(o->lock));

	if(timeout_in_microseconds != NON_BLOCKING)
	{
		int wait_error = 0;
		while((o->state != OPERATOR_KILLED) && !wait_error)
		{
			if(timeout_in_microseconds == BLOCKING)
				wait_error = pthread_cond_wait(&(o->wait_until_killed), &(o->lock));
			else
				wait_error = pthread_cond_timedwait_for_microseconds(&(o->wait_until_killed), &(o->lock), &timeout_in_microseconds);
		}
	}

	int is_killed = (o->state == OPERATOR_KILLED);

	pthread_mutex_unlock(&(o->lock));

	return is_killed;
}

int set_operator_state(operator* o, operator_state state)
{
	int state_changed = 0;

	pthread_mutex_lock(&(o->lock));

	// a killed operator can not be revived
	if(state != OPERATOR_KILLED)
	{
		o->state = state;
		state_changed = 1;
		if(state == OPERATOR_KILLED)
			pthread_cond_broadcast(&(o->wait_until_killed));
	}

	pthread_mutex_unlock(&(o->lock));

	return state_changed;
}

// operator buffer functions

void prohibit_usage_for_operator_buffer(operator_buffer* ob)
{
	pthread_mutex_lock(&(ob->lock));

	if(ob->prohibit_usage == 0)
	{
		ob->prohibit_usage = 1;

		temp_tuple_store* tts = NULL;
		while((tts = (temp_tuple_store*) get_head_of_linkedlist(&(ob->tuple_stores))))
		{
			remove_from_linkedlist(&(ob->tuple_stores), tts);
			delete_temp_tuple_store(tts);
		}

		// wake up waiters so that they can know about the prohibition of the usage of the operator_buffer
		pthread_cond_broadcast(&(ob->wait));
	}

	pthread_mutex_unlock(&(ob->lock));
}

int push_to_operator_buffer(operator_buffer* ob, temp_tuple_store* tts)
{
	int pushed = 0;

	pthread_mutex_lock(&(ob->lock));

	// proceed only if there is no prohibit_usage and the consumer is alive
	if(!(ob->prohibit_usage) && (OPERATOR_KILLED != get_operator_state(ob->consumer)))
	{
		pushed = insert_tail_in_linkedlist(&(ob->tuple_stores), tts);
		if(pushed)
		{
			ob->tuple_stores_count += 1;

			// wake up blocking waiters
			pthread_cond_broadcast(&(ob->wait));
		}
	}
	else // kill the producer_operator
	{
		set_operator_state(ob->producer, OPERATOR_KILLED);
	}

	pthread_mutex_unlock(&(ob->lock));

	// if we pushed and there is a consumer that is waiting to be notified then wake it up
	if(pushed && get_operator_state(ob->consumer) == OPERATOR_WAITING_TO_BE_NOTIFIED)
	{
		ob->consumer->notify_wake_up(ob->consumer, ob->consumer->operator_id);
	}

	return pushed;
}

temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, uint64_t timeout_in_microseconds)
{
	pthread_mutex_lock(&(ob->lock));

	if(timeout_in_microseconds != NON_BLOCKING)
	{
		int wait_error = 0;
		while((get_head_of_linkedlist(&(ob->tuple_stores)) == NULL) && (OPERATOR_KILLED != get_operator_state(ob->producer)) && !(ob->prohibit_usage) && !wait_error)
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
	if((ob->prohibit_usage) || ((get_head_of_linkedlist(&(ob->tuple_stores)) == NULL) && (OPERATOR_KILLED == get_operator_state(ob->producer))))
	{
		set_operator_state(ob->consumer, OPERATOR_KILLED);
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