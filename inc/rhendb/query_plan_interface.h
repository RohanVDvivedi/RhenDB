#ifndef OPERATOR_INTERFACE_H
#define OPERATOR_INTERFACE_H

#include<stdlib.h>

#include<pthread.h>
#include<boompar/tiber.h>

#include<rhendb/temp_tuple_store.h>

typedef struct query_plan query_plan;

// operator is actually a task in the pipeline of the query_plan, it must be implemented single threadedly to pull resources
// for multithreading make other compute threads or clone new operators into the query_plan
typedef struct operator operator;
struct operator
{
	// below attributes are static and you do not need lock to access them

	// operator_id is the id of this operator
	uint32_t operator_id;

	// pointer to the query_plan that this operator is part of
	query_plan* self_query_plan;

	void* inputs;			// pointer for the operator to store input params

	void* context;			// to store positions of the operator in the scans

	tiber opertor_tiber;	// the fiber that executes the operator, along with it's thread_pool, and all of it's state context

	void (*initialize_operator_tiber)(operator* o); // this function will be called from outside the operator, you may not initialize your tiber on your own

	// this function get's called before operator goes into waiting on lock_table or the operator_buffer
	void (*operator_release_latches_and_store_contexts)(operator* o);

	// below variables are only necessary if you are interested in killing the operator OR waiting for it to be killed

	pthread_mutex_t kill_lock;		// global lock for the operator
	pthread_cond_t wait_until_killed; // wait for the operator to get into OPERATOR_KILLED state

	int is_killed:1;
	int is_kill_signal_sent:1;
};

// keep on checking this signal from the operator and kill your self once it returns true
int is_kill_signal_sent(operator* o);

// to be called from inside the operator once it is killed
void mark_operator_self_killed(operator* o);

// call this function from user threads of the cached threadpool, never from the operator
// this is a non-tiber function
void send_kill_and_wait_for_operator_to_die_FROM_NON_OPERATOR(operator* o);

typedef struct operator_buffer operator_buffer;
struct operator_buffer
{
	pthread_mutex_t lock;	// global lock for the operator_buffer

	tiber_cond wait;		// wait for the data here blockingly

	uint64_t tuple_stores_count;
	uint64_t tuples_count;

	linkedlist tuple_stores;	// temp_tuple_store produced by the operator gets stored here

	// number of operator tasks producing to this operator buffer
	uint32_t producers_count;

	// number of operator tasks consuming from this operator buffer
	uint32_t consumers_count;
};

int increment_operator_buffer_producers_count(operator_buffer* ob, uint32_t change_amount);

int decrement_operator_buffer_producers_count(operator_buffer* ob, uint32_t change_amount);

int increment_operator_buffer_consumers_count(operator_buffer* ob, uint32_t change_amount);

int decrement_operator_buffer_consumers_count(operator_buffer* ob, uint32_t change_amount);

// there should be only 1 operator execution context calling these operator_bufffer functions

int push_to_operator_buffer(operator_buffer* ob, operator* callee, temp_tuple_store* tts);

temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, operator* callee);

typedef struct transaction transaction;

typedef struct query_plan query_plan;
struct query_plan
{
	// the transaction that this query belongs to
	transaction* curr_tx;

	// operators scans, writers, and also the joins, sorts and groupbys
	uint32_t operators_count;
	operator** operators;

	// operator outputs including the intermediate ones
	uint32_t operator_buffers_count;
	operator_buffer** operator_buffers;
};

#endif