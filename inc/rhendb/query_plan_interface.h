#ifndef OPERATOR_INTERFACE_H
#define OPERATOR_INTERFACE_H

#include<pthread.h>

#include<rhendb/interim_tuple_store.h>
#include<rhendb/tuple_transformer_interface.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<cutlery/arraylist.h>

typedef struct query_plan query_plan;

typedef enum operator_state operator_state;
enum operator_state
{
	OPERATOR_WAITING, // (begin state) -> then always goes to OPERATOR_RUNNING on being queued
	OPERATOR_QUEUED,
	OPERATOR_RUNNING, // -> always goes first to OPERATOR_PAUSED,
	OPERATOR_KILLED, // (termianl state)
};

// operator is actually a task in the pipeline of the query_plan, it must be implemented single threadedly to pull resources
// and for multithreading clone new operators from your current one into the query_plan
typedef struct operator operator;
struct operator
{
	// pointer to the query_plan that this operator is part of
	query_plan* self_query_plan;

	void* inputs;			// pointer for the operator to store input params

	void* context;			// to store positions of the scans for the operator

	void (*execute)(operator* o); // the operator's main function that produces tuples until it dies or has nothing in it's input
	// a source operator is always the thread hoarding operator in most simple cases
	// other operators like join and group_by and sort are job based and exit when nothing is found in their input
	// they get triggered again by the source operator when it pushes in the pipeline

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
	uint32_t output_buffers_count; // this number does never cross 3

	// list of consumption_iterator-s, pointing into tuple_regions in output_buffers
	linkedlist output_consumers;

	// this transformations will be applicable to all the tuples produced by this operator
	// as soon as it call produce_tuple/s_from_operator
	// remember output_lock will not be held while calling any of the transformers
	tuple_transformers output_tuple_transformers; // [MUST BE SET DURING THE SETUP PHASE]

	// -----------------------------------------------------------------------------------------------------

	// below variables are only needed for the internal function of the state machine of the operator

	pthread_mutex_t state_lock;			// lock for all the operator's attributes given below, related to only states
	pthread_cond_t wait_until_killed; 	// wait for the operator to be killed

	operator_state state;

	// count of the concurrent jobs currently in active state spaned by the operator
	// using the function run_concurrent_job_for_operator()
	uint64_t queued_jobs_count;
	uint64_t running_jobs_count;

	int is_kill_signal_sent:1;
	int is_trigger_signaled_on_running:1; // this flag ill be set, if a trigger as signalled while the operator was in running state

	// this kill_reason will be set while sending a kill signal to the operator
	// kill_reason is valid only if a kill signal was sent
	// kill_reasons only get appended here
	dstring kill_reason;
};

typedef struct consumption_iterator consumption_iterator;
struct consumption_iterator
{
	operator* producer;

	operator* consumer;

	// this flag initializes to 0
	// once set, it is on cleared on a successfull consume_for_consumption_iterator()
	// only if this flag is 0, is the next trgger will be done for the consumer
	int was_consumer_triggered;

	interim_tuple_store* curr_store;
	interim_tuple_region curr_region;

	llnode embed_node_for_output_consumers;
};

// check if the operator is allowed to do it's execution
// returns 1, if the operator is_killed or is_kill_signal_sent
int can_not_proceed_for_execution_operator(operator* o);

// called by any one to put the operator in shutting down state
void send_kill_signal_to_operator(operator* o, dstring kill_reason);

#define kill_signal_for_self_operator send_kill_signal_to_operator

// force an OPERATOR_WAITING stated operator into OPERATOR_QUEUED state, and push a corresponding job into the thread_pool for it's execution
void trigger_execution_on_operator(operator* o);

// to be called only from the inside of the operator or an operator's concurrently running job
// used by the operator to schedule a concurrent job from the inside of an operator, no state is maintained for this job
// returns 1 only if the job gets queued, if this function fails check if the operator you are working for can proceed
int run_concurrent_job_for_operator(operator* o, void* param, void (*operator_job_function)(operator* o, void* param));

// returns 0, if lock not acquired 1 if acquired, and -1 if an abort must be performed
int acquire_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, uint64_t timeout_in_microseconds);

void release_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size);

// below is a no-op function to be used with operator_release_latches_and_store_context
void OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION(operator* o);

// below is a simple flat free_resource function to be used with simple operators
void OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION(operator* o);

// produce a tuple from the operator o
int produce_tuple_from_operator(operator* o, void* tuple);

// clone_cti_p may be NULL
consumption_iterator* create_consumption_iterator(operator* producer, operator* consumer, consumption_iterator* clone_cit_p);
void destroy_consumption_iterator(consumption_iterator* cit_p);

// consume the tuple using the iterator provided that has the producer consumer pair
// it presents the pointer from the tuple_region itself
const void* consume_for_consumption_iterator(consumption_iterator* cit_p, int* no_more_data);

// this is the tuple_def that the consumer of the operator should be ready to consume
// this is the tuple_def that is required to be used to read tuples returned from consume_from_operator()
const tuple_def* get_tuple_def_for_tuples_to_be_consumed_from(operator* o);

typedef struct transaction transaction;

typedef struct query_plan query_plan;
struct query_plan
{
	// the transaction that this query belongs to
	transaction* curr_tx;

	// operators like scans, writers, and also the joins, sorts and groupbys
	arraylist operators;
};

query_plan* get_new_query_plan(transaction* curr_tx, uint32_t operators_count);

operator* get_new_registered_operator_for_query_plan(query_plan* qp);

void start_all_operators_for_query_plan(query_plan* qp);

// may be called as many times as you desire
void shutdown_query_plan(query_plan* qp, dstring kill_reasons);

void wait_for_shutdown_of_query_plan(query_plan* qp);

// must be called only after the query_plan is shutdown
void destroy_query_plan(query_plan* qp, dstring* kill_reasons);

/*
	callbacks for lock_manager
*/

void notify_unblocked(void* context_p, void* transaction, void* task);

void notify_deadlocked(void* context_p, void* transaction);

#endif