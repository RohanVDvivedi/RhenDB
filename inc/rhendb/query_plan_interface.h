#ifndef OPERATOR_INTERFACE_H
#define OPERATOR_INTERFACE_H

#include<stdlib.h>

#include<pthread.h>
#include<boompar/executor.h>

#include<rhendb/temp_tuple_store.h>

typedef enum operator_state operator_state;
enum operator_state
{
	OPERATOR_QUEUED, // operator queued to thread pool

	OPERATOR_RUNNING, // executing right now

	OPERATOR_WAITING, // operator waiting for some resource to be available

	OPERATOR_KILLED, // no state transitions can occur for the operator after this state is reached, operator has released all the resources
};

typedef struct operator operator;
struct operator
{
	// below attributes are static and you do not need lock to access them

	// operator_id is the id of this operator
	uint64_t operator_id;

	// below attributes are not static and you need a lock to access them

	pthread_mutex_t lock;		// global lock for the operator

	pthread_cond_t wait_until_state_changes; // wait for the operator to change it's state

	operator_state state;	// access the current state of the operator tasks here

	void* input;			// pointer for the operator to store input params

	void* context;			// context to be used by the operators, when they are waiting, this helps then restart again when data is again available
							// this must hold the last position you were scanning at, so as to restore from that same old position, upon being relieved of the blocking from the resource (being locks in lock_table OR operator_buffer-s)

	void* locals;			// local variables for use by the operator tasks, this may hold your scan structures like heap_table_iterator and bplus_tree_iterator

	executor* thread_pool;		// thread_pool to be used by the operator, it would be a global cached thread pool for the scans and writers, but for joins, sorts and group_bys we will use a compute intensive fixed sized thread pool

	// called to put the operator in waiting state
	// hold lock while calling this function and atomically transitioning from OPERATOR_RUNNING to OPERATOR_WAITING state
	void (*store_locals_to_context)(operator* o);

	// called to put the operator in running from queued state
	// hold lock while calling this function and atomically transitioning from OPERATOR_QUEUED to OPERATOR_RUNNING state
	void (*restore_locals_from_context)(operator* o);

	// this is the function that runs for the operator, after it is set to OPERATOR_RUNNING state and atomically making calling it's restore_locals_from_context() function
	void (*execute)(operator* o);
};

// public
operator_state get_operator_state(operator* o);

// public
// timeout can be BLOCKING or some positive value in microseconds
// returns 1 if the operator is killed, and you may call cleanup() on that operator
int wait_until_operator_is_killed(operator* o, uint64_t timeout_in_microseconds);

// private
// to be only used from inside the operator
// it wakes up any thread waiting on operator to get into OPERATOR_KILLED state
// returns 1 if the state was changed
int set_operator_state(operator* o, operator_state state);

typedef struct operator_buffer operator_buffer;
struct operator_buffer
{
	pthread_mutex_t lock;		// global lock for the operator_buffer

	pthread_cond_t wait;		// wait for the data here blockingly

	uint64_t tuple_stores_count;
	uint64_t tuples_count;

	linkedlist tuple_stores;	// temp_tuple_store produced by the operator gets stored here

	// number of operator tasks producing to this operator buffer
	uint64_t producers_count;

	// number of operator tasks consuming from this operator buffer
	uint64_t consumers_count;

	// this counters can be incremented or decremented at will
	// but once any of these numbers reach zero then they can not change, they are both initialized to 1
};

// public
// this will also clear all the temp_tuple_stores accumulated upuntil now
// returns 1 if the prohibit usage for the operator_buffer was done, (it only fails if done again)
int prohibit_usage_for_operator_buffer(operator_buffer* ob);

// private -> only for the operators to use them
// failure only implies that the prohibit_usage request was sent OR that the consumer operator is in OPERATOR_KILLED state and will never come back
// in both these failure states the producer operator get's it's state set to OPERATOR_KILLED and must shutdown
int push_to_operator_buffer(operator_buffer* ob, temp_tuple_store* tts);

// public, operators must use this call NON_BLOCKING
// user level functions must use BLOCKING or with a timeout, to ensure that we have data immediately
// failure implies that the operator_buffer is empty
// another failure condition comes from the operator_buffer getting it's prohibit_usage set OR if the operator_buffer is empty and the producer is OPERATOR_KILLED
// in the second failure case the consumer also get's OPERATOR_KILLED if it exists and the consumer must shutdown
// in the first case the consumer is instead placed in the OPERATOR_WAITING_TO_BE_NOTIFIED state and asked to wait
temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, uint64_t timeout_in_microseconds);

typedef struct query_plan query_plan;
struct query_plan
{
	// the transaction that this query belongs to
	transaction* curr_tx;

	// operators scans, writers, and also the joins, sorts and groupbys
	uint64_t operators_count;
	operator** operators;

	// operator outputs including the intermediate ones
	uint64_t operator_buffers_count;
	operator_buffer** operator_buffers;
};

// prohibit usage of all the operator_buffers
// then wait for all the operators to get killed in a loop, calling their cleanups one by one
void shutdown_query_plan(query_plan* qp);

operator* find_right_operator_for_query_plan(query_plan* qp, uint64_t operator_task_id);

#endif