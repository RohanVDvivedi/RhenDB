#ifndef OPERATOR_INTERFACE_H
#define OPERATOR_INTERFACE_H

#include<stdlib.h>

#include<pthread.h>
#include<boompar/executor.h>

#include<rhendb/temp_tuple_store.h>

typedef enum operator_state operator_state;
enum operator_state
{
	OPERATOR_QUEUED,	// queued in thread pool but not executing as of now
	OPERATOR_RUNNING,	// executing right now

	// notify_wake_up() for an operator only does something if the operator is in this state, this implies it is still running or paused completely
	// but is waiting for someone to notify it, once an operator (or one of it's thread) is in this state, it can not come back without being notified
	OPERATOR_WAITING_TO_BE_NOTIFIED,	// some thread of this operator is paused, because the source operators are paused OR waiting for a lock to be granted

	OPERATOR_KILLED,	// operator is not functional and has quit, this implies all the context is released before setting to this state
};

typedef struct operator operator;
struct operator
{
	// below attributes are static and you do not need lock to access them

	// operator_id is the id of the first worker in the group of threads of this operator
	// you get 10000 such ids, it is believed you may never need more than 10000 threads for any one of your operators
	uint64_t operator_id;

	// below attributes are not static and you need a lock to access them

	pthread_mutex_t lock;		// global lock for the operator

	pthread_cond_t wait_until_killed; // wait for any one to wait for the operator state to change to OPERATOR_KILLED state

	operator_state state;		// access the current state of the operator here
	// you can not have different states for each of the threads of the operator
	// for let's say a join operator joining two tables atmost can assign one operator_id to each of its input readers and only service wakeups when the correct operator_id is called for the notify_wake_up() call
	// it is all upto you to design this
	// for any external user an operator being in OPERATOR_WAITING_TO_BE_NOTIFIED, just means that it is desperate to run and needs someone to just give it a push

	void* input_params;			// pointer for the operator to store input params

	void* context;				// context to be used by the operators, when they are paused, this helps them stay paused and then restart again when data is again available
	// this could possibly be a local variable store for all your threads of this operators, so that these operator threads could sleep and wake up as they desire

	executor* thread_pool;		// thread_pool to be used by the operator, it would be a global cached thread pool for the scans and writers, but for joins, sorts and group_bys we will use a compute intensive fixed sized thread pool

	void (*notify_wake_up)(operator* o, uint64_t operator_id); // the function that gets called to wake up a paused operator when new data is available OR when it could possibly have been unblocked for a lock
	// here if operator_id is operator->operator_id if it is a broadcast to all the operators, else we are talking about one specific thread of this operator
	// you can also have some operator that does it's entire work in this function itself (only shuffles between RUNNING and TO_BE_NOTIFIED states)
	// OR an operator that just pushes it's execute function into the thread_pool upon notified to be called asynchronously
	// each operator design is you to you to decide, how it should run synchronously or asynchronously

	// execute function will never be called by the external users, it is up to the operator designers to design how to use this function pointer
	// it is essentially private to thethe operator just like the input_param and the context pointers
	void (*execute)(operator* o); // the function that gets called inside the thread_pool

	void (*clean_up)(operator* o); // the function must be called only after an operator reaches OPERATOR_KILLED state, OR 
};

// public
operator_state get_operator_state(operator* o);

// public
// timeout can be BLOCKING or some positive value in microseconds
// returns 1 if the operator is killed
int wait_until_operator_is_killed(operator* o, uint64_t timeout_in_microseconds);

// private
// to be only used from inside the operator
// it wakes up any thread waiting on operator to get into OPERATOR_KILLED state
// returns 1 if the state was changed
int set_operator_state(operator* o, operator_state state);

typedef struct operator_buffer operator_buffer;
struct operator_buffer
{
	// this is the operator that pushes its produce into this operator buffer, this is static
	operator* producer;

	// this is the operator that pops/consumes from this operator buffer, this is static
	operator* consumer;

	// there can be only 1 producer and 1 consumer for the operator_buffer, but these operators may be using multiple threads

	pthread_mutex_t lock;		// global lock for the operator_buffer

	pthread_cond_t wait;		// wait for the data here blockingly

	uint64_t tuple_stores_count;
	linkedlist tuple_stores;	// temp_tuple_store produced by the operator gets stored here

	int prohibit_usage;
	// this implies an error in the system OR a request that the whole pipeline must collapse
};

// public
// this will also clear all the temp_tuple_stores accumulated upuntil now
void prohibit_usage_for_operator_buffer(operator_buffer* ob);

// private -> only for the operators to use them
// failure only implies that the prohibit_usage request was sent
int push_to_operator_buffer(operator_buffer* ob, temp_tuple_store* tts);

// public, operators must use this call NON_BLOCKING
// user level functions must use BLOCKING or with a timeout, to ensure that we have data immediately
// if returned NULL, prohibit_usage will be set if the pipeline was asked to be collapsed
temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, uint64_t timeout_in_microseconds, int* prohibit_usage);

typedef struct query_plan query_plan;
struct query_plan
{
	// operators scans, writers, and also the joins, sorts and groupbys
	uint64_t operators_count;
	operator** operators;

	// operator outputs including the intermediate ones
	uint64_t operator_buffers_count;
	operator_buffer** operator_buffers;

	// output tuples of the query, come here
	operator_buffer* output;
};

#endif