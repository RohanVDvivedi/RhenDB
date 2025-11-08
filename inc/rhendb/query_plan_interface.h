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

	OPERATOR_WAITING, // operator waiting for some resource to be available, the operator starts in this state

	OPERATOR_KILLED, // no state transitions can occur for the operator after this state is reached, operator has released all the resources
};

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

	// below attributes are not static and you need a lock to access them

	pthread_mutex_t lock;		// global lock for the operator

	pthread_cond_t wait_until_killed; // wait for the operator to get into OPERATOR_KILLED state

	operator_state state;	// access the current state of the operator tasks here

	int kill_signal_sent;	// this flag will be set if a kill signal sent was done from the outside of the operator, predominantly while it was still running

	void* inputs;			// pointer for the operator to store input params

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

	// to be called when operator state is to be changed to OPERATOR_KILLED
	// this method must be idempotent and must set all the 3, local, context and input to NULLs
	void (*destroy_locals_context_inputs)(operator* o);

	// this is the function that runs for the operator, after it is set to OPERATOR_RUNNING state and atomically making calling it's restore_locals_from_context() function
	// an operator will enter this function always in OPERATOR_RUNNING state but must exit in any of the 3 other states, please keep this in mind
	void (*execute)(operator* o);

	// embedded node for the operator to get stacked onto the waiters of the operator_buffer
	llnode embed_node_waiting_on_operator_buffer;
};

operator_state get_operator_state(operator* o, int* kill_signal_sent);

/*
	state transitions allowed

	OPERATOR_QUEUED <-> OPERATOR_RUNNING
		   ^                /
			\              \/
			 OPERATOR_WAITING

	OPERATOR_KILLED can come after any of the states
*/

// fails only when the state transition is not possible OR when the the operator is in OPERATOR_KILLED state
// if you state change the operator using this function to OPERATOR_QUEUED, you still need to queue the operator itself into the threadpool it get's dedicated to
int set_operator_state(operator* o, operator_state state);

// ask operator to be killed from outside the operator, from another operator or some global thread
// the locals of the operator are not protected by the lock, so you need to send kill signal instead for killing the operator, and let the operator itself kill itself
void kill_OR_send_kill_to_operator(operator* o);

// you must call this function after putting the operator in OPERATOR_QUEUED state, form OPERATOR_WAITING state, after using the set_operator_state function
// this function will not change the state of the operator for you
void enqueue_operator(operator* o);
// enqueue_operator executes the operator in the current thread if the operator does not register thread_pool, in this case enqueue operator must not be called from the inside of the operator itself as it may cause infinite recursion

typedef struct operator_buffer operator_buffer;
struct operator_buffer
{
	pthread_mutex_t lock;		// global lock for the operator_buffer

	pthread_cond_t wait;		// wait for the data here blockingly

	uint64_t tuple_stores_count;
	uint64_t tuples_count;

	linkedlist tuple_stores;	// temp_tuple_store produced by the operator gets stored here

	// number of operator tasks producing to this operator buffer
	uint32_t producers_count;

	// number of operator tasks consuming from this operator buffer
	uint32_t consumers_count;

	// this counters can be incremented or decremented at will
	// but once any of these numbers reach zero then they can not change, they are both initialized to 1

	// list of consumer operators that went to waiting state for this operator_buffer not having data
	linkedlist waiting_consumers;
};

int increment_operator_buffer_producers_count(operator_buffer* ob, uint32_t change_amount);

int decrement_operator_buffer_producers_count(operator_buffer* ob, uint32_t change_amount);

int increment_operator_buffer_consumers_count(operator_buffer* ob, uint32_t change_amount);

int decrement_operator_buffer_consumers_count(operator_buffer* ob, uint32_t change_amount);

// there should be only 1 operator execution context calling these operator_bufffer functions

// fails if the operator is in OPERATOR_KILLED state
int push_to_operator_buffer(operator_buffer* ob, operator* producer, temp_tuple_store* tts);

// fails if the operator is in OPERATOR_KILLED state
temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, operator* consumer, uint64_t timeout_in_microseconds);

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