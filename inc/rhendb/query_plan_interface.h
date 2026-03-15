#ifndef OPERATOR_INTERFACE_H
#define OPERATOR_INTERFACE_H

#include<pthread.h>

#include<rhendb/interim_tuple_store.h>

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

	void (*trigger_execution)(operator* o);
	// this trigger_execution() is called when ever you want this operator to start doing some work
	// you may even make it force call trigger_execution() on the operators that it consumes from
	// you can even make it synchronous and make it use the thread of the calling function to do some work
	/*
		you can make it
		 * submit a long running thread (completes when the operator finished)
		 * submit a short job to just complete processing on the currently accumulated threads
		 * execute and process the current set fo tuples synchronously in the same call
		just do anything that makes the pipeline progress
		 * including calling the trigger_execution() on the operators that this operator is meant to consume from
	*/

	// this function get's called before operator goes into waiting on lock_table or the operator_buffer
	void (*operator_release_latches_and_store_context)(operator* o);

	// the free_resources function gets called after the operator is killed
	// it should only be responsible for cleaning up inputs and contexts used by the operator
	void (*free_resources)(operator* o);	// only thing left to be done after this call must be to call free on the operator

	// below is the condition variable that this operator will wait on, when it needs to acquire locks on the database entities in the lock table
	// only scans/writers on indexes and heap tables will need this
	// the mutex to be used with this condition variable must be operator * o->self_query_plan->curr_tx->db->lock_manager_external_lock
	pthread_cond_t wait_on_lock_table_for_lock;

	// -----------------------------------------------------------------------------------------------------

	// for the produced results from this operator use the following
	// you may not need these below attributes if it is a sink operator

	// lock protecting the output_buffers
	pthread_mutex_t output_lock;

	// singlylist of interim_tuple_store
	// new tuple is always appended to last interim_tuple_store in this list else a new interim_tuple_store is created
	singlylist output_buffers;

	// the operator that is meant to consume the output of this operator must be registered here
	// every insert to the output_buffers forces a trigger_execution() for the consumer_operator, driving the data in the pipeline forward
	// the design permits for only 1 consumer for every 1 producer operator
	// but there may be many producer operators for any 1 consumer operator
	operator* consumer_operator; // [MUST BE SET DURING THE SETUP PHASE]

	// after these many bytes are accumulated the consumer_operator will receive a trigger_execution() call
	// we check after every produce call and trigger_execution() on the consumer, if (consumer_trigger_on_bytes_accumulated >= next_tuple_offset)
	// this check is done after every tuple produced, and is skipped if this value is set to 0
	uint64_t consumer_trigger_on_bytes_accumulated; // [MUST BE SET DURING THE SETUP PHASE]

	// definition of the produced output tuples

	// these are most likely populated and then cleaned up by the operator itself
	// they can be used by the operators that are meant to consume the output of this operator

	// to be used by the consumers of the output of this operator
	const tuple_def* output_tuple_def; // [MUST BE SET DURING THE SETUP PHASE]

	// if the output is sorted, then the following attributes are set by the operator itself
	const positional_accessor* output_key_element_ids; // [MUST BE SET DURING THE SETUP PHASE]
	const compare_direction* output_key_compare_direction; // [MUST BE SET DURING THE SETUP PHASE]
	uint32_t output_key_element_count; // [MUST BE SET DURING THE SETUP PHASE]

	// -----------------------------------------------------------------------------------------------------

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

// appends tuple to the latest output_buffers
int produce_tuple_from_operator(operator* o, void* tuple);

// appends tuples at the end of the output_buffers
int produce_tuples_from_operator(operator* o, interim_tuple_store* tuples);

// consmes the interim_tuple_store from the procuder, the consumer is already assigned in the producer
// no_more_data flag will be set if the producer is killed for sure
// we also have a min_bytes_to_consume, the successfull consume happens only if there are excess of these many bytes or excess interim_tuple_stores in the output_buffers
interim_tuple_store* consume_from_operator(operator* producer, uint64_t min_bytes_to_consume, int* no_more_data);

typedef struct transaction transaction;

typedef struct query_plan query_plan;
struct query_plan
{
	// the transaction that this query belongs to
	transaction* curr_tx;

	// operators like scans, writers, and also the joins, sorts and groupbys
	arraylist operators;
};

query_plan* get_new_query_plan(transaction* curr_tx, uint32_t operators_count, uint32_t operator_buffers_count);

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