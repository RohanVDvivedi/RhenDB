#ifndef OPERATOR_INTERFACE_H
#define OPERATOR_INTERFACE_H

#include<stdlib.h>

#include<pthread.h>
#include<boompar/executor.h>

#include<rhendb/temp_tuple_store.h>

typedef struct operator_buffer operator_buffer;
struct operator_buffer
{
	pthread_mutex_t lock;		// global lock for the operator_buffer

	pthread_cond_t wait;		// wait for the data here blockingly

	uint64_t tuple_stores_count;
	linkedlist tuple_stores;	// temp_tuple_store produced by the operator gets stored here

	linkedlist pushers;			// all the operators that are meant to push data to this operator_buffer must be registered here
	// if there are no pushers we ask the popping function to kill itself
	// if there are psuhers and are all in paused state we make the caller of the pop function to go into paused state

	linkedlist waiters;			// all operators that went into waiting state waiting for data from this operator_buffer
	// as soom as something new gets pushed and we find a operator here, we call queue_operator() on it and pop it

	int prohibit_pushing;		// if this flag gets set, then operator_buffer stops letting the pushers push their produce here
};

typedef enum operator_state operator_state;
enum operator_state
{
	OPERATOR_QUEUED,	// queued in thread pool but not executing as of now
	OPERATOR_RUNNING,	// executing right now
	OPERATOR_PAUSED,	// paused because the source operators are paused OR waiting for a lock to be granted
	OPERATOR_KILLED,	// operator is not functional and has quit, this implies all the context is released before setting to this state
};

typedef struct transaction transaction;

typedef struct operator operator;
struct operator
{
	// transaction handle and the index of the operator in it, helps to identify who waits on what, and wake the respective operator only
	transaction* txn;
	uint64_t operator_id;		

	operator_buffer* output;	// operator must write its output here, not protected by the operator's global lock

	operator_buffer* input;		// operator must read its input here, not protected by the operator's global lock
	// it will be NULL, if it is a scan

	pthread_mutex_t lock;		// global lock for the operator

	operator_state state;		// access the current state of the operator here

	void* input_params;			// pointer for the operator to store input params

	void* context;				// context to be used by the operators, when they are paused, this hels them stay paused and then restart again when data is again available
	// for easy usability, keep all your operator local variables here, and no need to remember the nstate of the operator

	executor* thread_pool;		// thread_pool to be used by the operator, it would be a global cached thread pool for the scans and writers, but for joins, sorts and group_bys we will use a compute intensive fixed sized thread pool

	void (*execute)(operator* o); // the function that gets called inside the thread_pool

	// below are the 
	llnode embed_node_pushers;
	llnode embed_node_waiters;
};

#include<rhendb/transaction.h>

// global function, does not do the same on the child operators
// queues the operator to run immediately
int queue_operator(operator* o); // fails if the operator is in OPERATOR_KILLED state, NO-OP in OPERATOR_QUEUED/OPERATOR_RUNNING state, gets to OPERATOR_QUEUED state if it was in OPERATOR_PAUSED

int push_to_operator_buffer(operator_buffer* ob, temp_tuple_store* tts);

// timeout can be BLOCKING or a positive value in microseconds
// to be used by the user's io thread to send data to user
temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, uint64_t timeout_in_microseconds);

// to be used by the intermediate operators to retrieve data from their source
temp_tuple_store* pop_from_operator_buffer_nonblocking(operator_buffer* ob, operator_state* state_next); // state_next must be used only if return valus is NULL, it gets set to OPERATOR_PAUSED, if all pushers are in paused state, and OPERATOR_KILLED if all pushers are dead

// insert pusher when building the query tree, and remove when killed
void insert_pusher_to_operator_buffer(operator_buffer* ob, operator* o);
void remove_pusher_to_operator_buffer(operator_buffer* ob, operator* o);

// insert waiter when going into paused state over input buffer being empty, and remove on being queued
void insert_waiter_to_operator_buffer(operator_buffer* ob, operator* o);
void remove_waiter_to_operator_buffer(operator_buffer* ob, operator* o);

#endif