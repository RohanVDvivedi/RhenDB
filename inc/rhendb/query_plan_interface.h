#ifndef OPERATOR_INTERFACE_H
#define OPERATOR_INTERFACE_H

#include<pthread.h>

#include<rhendb/temp_tuple_store.h>

#include<cutlery/arraylist.h>

typedef struct query_plan query_plan;

// operator is actually a task in the pipeline of the query_plan, it must be implemented single threadedly to pull resources
// and for multithreading clone new operators from your current one into the query_plan
typedef struct operator operator;
struct operator
{
	// pointer to the query_plan that this operator is part of
	query_plan* self_query_plan;

	void* inputs;			// pointer for the operator to store input params

	void* context;			// to store positions of the scans for the operator

	void (*execute)(operator* o);	// this will be the function that will get pushed into the operator_thread_pool for execution

	// this function get's called before operator goes into waiting on lock_table or the operator_buffer
	void (*operator_release_latches_and_store_context)(operator* o);

	// the free_resources function gets called after the operator is killed
	// it should only be responsible for cleaning up inputs and contexts used by the operator
	void (*free_resources)(operator* o);	// only thing left to be done after this call must be to call free on the operator

	// below is the condition variable that this operator will wait on, when it needs to acquire locks on the database entities in the lock table
	// only scans/writers on indexes and heap tables will need this
	// the mutex to be used with this condition variable must be operator * o->self_query_plan->curr_tx->db->lock_manager_external_lock
	pthread_cond_t wait_on_lock_table_for_lock;

	// below variables are only necessary if you are interested in killing the operator OR waiting for it to be killed

	pthread_mutex_t kill_lock;			// kill lock for the operator
	pthread_cond_t wait_until_killed; 	// wait for the operator to be killed

	int is_killed:1;

	int is_kill_signal_sent:1;

	// this kill_reason will be set while sending a kill signal to the operator
	// kill_reason is valid only if a kill signal was sent
	// kill_reasons only get appended here
	dstring kill_reason;
};

// keep on checking this signal from the operator and kill your self once it returns true
int is_kill_signal_sent(operator* o);

// to be called from inside the operator once it is killed
void mark_operator_self_killed(operator* o, dstring kill_reason);

// returns 0, if lock not acquired 1 if acquired, and -1 if an abort must be performed
int acquire_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, uint64_t timeout_in_microseconds);

void release_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size);

// below is a no-op function to be used with operator_release_latches_and_store_context
void OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION(operator* o);

// below is a simple flat free_resource function to be used with simple operators
void OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION(operator* o);

typedef struct operator_buffer operator_buffer;
struct operator_buffer
{
	pthread_mutex_t lock;	// global lock for the operator_buffer

	pthread_cond_t wait;		// wait for the data here blockingly

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

temp_tuple_store* pop_from_operator_buffer(operator_buffer* ob, operator* callee, uint64_t timeout_in_microseconds, int* no_more_data);

typedef struct transaction transaction;

typedef struct query_plan query_plan;
struct query_plan
{
	// the transaction that this query belongs to
	transaction* curr_tx;

	// operators like scans, writers, and also the joins, sorts and groupbys
	arraylist operators;

	// operator outputs including the intermediate ones
	arraylist operator_buffers;
};

query_plan* get_new_query_plan(transaction* curr_tx, uint32_t operators_count, uint32_t operator_buffers_count);

operator_buffer* get_new_registered_operator_buffer_for_query_plan(query_plan* qp);

operator* get_new_registered_operator_for_query_plan(query_plan* qp);

void start_all_operators_for_query_plan(query_plan* qp);

// may be called as many times as you desire
void shutdown_query_plan(query_plan* qp, dstring kill_reasons);

// below function is the identical version of the function above that needs to be called with lock_manager_external_lock held
void shutdown_query_plan_LOCK_TABLE_UNSAFE(query_plan* qp, dstring kill_reasons);

void wait_for_shutdown_of_query_plan(query_plan* qp);

// must be called only after the query_plan is shutdown
void destroy_query_plan(query_plan* qp, dstring* kill_reasons);

/*
	callbacks for lock_manager
*/

void notify_unblocked(void* context_p, void* transaction, void* task);

void notify_deadlocked(void* context_p, void* transaction);

#endif