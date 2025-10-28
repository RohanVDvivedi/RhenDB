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
	}

	pthread_mutex_unlock(&(ob->lock));
}

int push_to_operator_buffer(operator_buffer* ob, temp_tuple_store* tts)
{
	int pushed = 0;

	pthread_mutex_lock(&(ob->lock));

	if(!(ob->prohibit_usage))
	{
		pushed = insert_tail_in_linkedlist(&(ob->tuple_stores), tts);
		if(pushed)
		{
			ob->tuple_stores_count += 1;

			// wake up blocking waiters
			pthread_cond_broadcast(&(ob->wait));
		}
	}

	pthread_mutex_unlock(&(ob->lock));

	// if we pushed and there is a consumer that is waiting to be notified them wake it up
	if(pushed && ob->consumer != NULL && get_operator_state(ob->consumer) == OPERATOR_WAITING_TO_BE_NOTIFIED)
	{
		ob->consumer->notify_wake_up(ob->consumer, ob->consumer->operator_id);
	}

	return pushed;
}

temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, uint64_t timeout_in_microseconds, int* prohibit_usage, int* operator_paused)
{
	if(operator_paused != NULL)
		(*operator_paused) = 0;

	pthread_mutex_lock(&(ob->lock));

	if(timeout_in_microseconds != NON_BLOCKING)
	{
		int wait_error = 0;
		while((get_head_of_linkedlist(&(ob->tuple_stores)) == NULL) && !(ob->prohibit_usage) && !wait_error)
		{
			if(timeout_in_microseconds == BLOCKING)
				wait_error = pthread_cond_wait(&(ob->wait), &(ob->lock));
			else
				wait_error = pthread_cond_timedwait_for_microseconds(&(ob->wait), &(ob->lock), &timeout_in_microseconds);
		}
	}

	temp_tuple_store* tts = NULL;

	if(!(ob->prohibit_usage))
	{
		tts = (temp_tuple_store*) get_head_of_linkedlist(&(ob->tuple_stores));
		if(tts != NULL)
		{
			remove_from_linkedlist(&(ob->tuple_stores), tts);
		}
		else
		{
			if(operator_paused != NULL)
				(*operator_paused) = set_operator_state(ob->consumer, OPERATOR_WAITING_TO_BE_NOTIFIED);
		}
	}
	else
		(*prohibit_usage) = (ob->prohibit_usage);

	pthread_mutex_unlock(&(ob->lock));

	return tts;
}