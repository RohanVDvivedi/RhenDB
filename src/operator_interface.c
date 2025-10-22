#include<rhendb/operator_interface.h>

int push_to_operator_buffer(operator_buffer* ob, temp_tuple_store* tts)
{
	int pushed = 0;

	pthread_mutex_lock(&(ob->lock));

	if(!(ob->prohibit_pushing))
	{
		ob->tuple_stores_count += insert_tail_in_linkedlist(&(ob->tuple_stores), tts);
		pushed = 1;
	}

	pthread_mutex_unlock(&(ob->lock));

	return pushed;
}

temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, uint64_t timeout_in_microseconds);

temp_tuple_store* pop_from_operator_buffer_unblockingly(operator_buffer* ob, operator_state* state_next);

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