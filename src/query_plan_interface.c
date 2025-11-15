#include<rhendb/query_plan_interface.h>

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
		callee->operator_release_latches_and_store_contexts(callee);
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