#include<rhendb/operator_interface.h>

int push_to_operator_buffer(operator_buffer* ob, temp_tuple_store* tts)
{
	int pushed = 0;

	pthread_mutex_lock(&(ob->lock));

	if(!(ob->prohibit_pushing))
	{
		ob->tuple_stores_count += insert_tail_in_linkedlist(&(ob->tuple_stores), tts);
		pushed = 1;

		// wake up blocking waiters
		pthread_cond_broadcast(&(ob->wait));

		// wake up any waiters (operators)
	}

	pthread_mutex_unlock(&(ob->lock));

	return pushed;
}

temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, uint64_t timeout_in_microseconds)
{
	pthread_mutex_lock(&(ob->lock));

	temp_tuple_store* tts = NULL;
	int wait_error = 0;
	while(tts == NULL && !wait_error)
	{
		wait_error = pthread_cond_timedwait_for_microseconds(&(ob->wait), &(ob->lock), &timeout_in_microseconds);
		tts = (temp_tuple_store*) get_head_of_linkedlist(&(ob->tuple_stores));
	}

	if(tts != NULL)
		remove_from_linkedlist(&(ob->tuple_stores), tts);

	pthread_mutex_unlock(&(ob->lock));

	return tts;
}

temp_tuple_store* pop_from_operator_buffer_nonblocking(operator_buffer* ob, operator_state* state_next)
{
	pthread_mutex_lock(&(ob->lock));

	temp_tuple_store* tts = (temp_tuple_store*) get_head_of_linkedlist(&(ob->tuple_stores));
	if(tts != NULL)
		remove_from_linkedlist(&(ob->tuple_stores), tts);

	if(tts == NULL)
	{
		// compute the state_next for the caller of this function
	}

	pthread_mutex_unlock(&(ob->lock));

	return tts;
}

void insert_pusher_to_operator_buffer(operator_buffer* ob, operator* o)
{
	pthread_mutex_lock(&(ob->lock));

	insert_tail_in_linkedlist(&(ob->pushers), o);

	pthread_mutex_unlock(&(ob->lock));
}

void remove_pusher_to_operator_buffer(operator_buffer* ob, operator* o)
{
	pthread_mutex_lock(&(ob->lock));

	remove_from_linkedlist(&(ob->pushers), o);

	pthread_mutex_unlock(&(ob->lock));
}

void insert_waiter_to_operator_buffer(operator_buffer* ob, operator* o)
{
	pthread_mutex_lock(&(ob->lock));

	insert_tail_in_linkedlist(&(ob->waiters), o);

	pthread_mutex_unlock(&(ob->lock));
}

void remove_waiter_to_operator_buffer(operator_buffer* ob, operator* o)
{
	pthread_mutex_lock(&(ob->lock));

	remove_from_linkedlist(&(ob->waiters), o);

	pthread_mutex_unlock(&(ob->lock));
}