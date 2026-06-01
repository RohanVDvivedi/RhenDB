#ifndef QUERY_PLAN_H
#define QUERY_PLAN_H

#include<pthread.h>

#include<rhendb/interim_tuple_store.h>
#include<rhendb/tuple_transformer_interface.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<cutlery/arraylist.h>

#define MAX_INTERMEDIATE_TUPLE_SIZE 3.9e9 // set to 3.9 billion (4.2 is max value for this) bytes

typedef struct query_plan query_plan;

typedef enum operator_state operator_state;
enum operator_state
{
	OPERATOR_WAITING, // (begin state) -> then always goes to OPERATOR_RUNNING on being queued
	OPERATOR_QUEUED,
	OPERATOR_RUNNING, // -> never goes to OPERATOR_KILLED from here

	// on suicide or kill signal from above state the next state will always be OPERATOR_KILLED

	// dead states are below

	OPERATOR_KILLED, // -> always goes to OPERATOR_CLEANED_UP from here
	OPERATOR_CLEANED_UP, // (terminal state)
};

// operator is actually a multithreadable task in the pipeline of the query_plan,
// it must be implemented as a single threaded execute function to pull resources from it's producer operator, and produce tuples for it's consuming operators
// and for multithreading operator needs to queue jobs using run_concurrent_job_for_operator()
typedef struct operator operator;
struct operator
{
	// pointer to the query_plan that this operator is part of
	query_plan* self_query_plan;

	void* inputs;			// pointer for the operator to store input params and it's state

	void (*execute)(operator* o); // the operator's main function that produces tuples until it dies or has nothing in it's input operators
	// they get triggered again by the source operator when it pushes in the pipeline

	// this function get's called before operator goes into waiting on lock_table
	void (*operator_release_latches_and_store_context)(operator* o);

	// called while the operator gets into OPERATOR_KILLED state, after this function is called asynchronously and after execution gets placed into OPERATOR_CLEANED_UP state
	void (*clean_up_resources)(operator* o);

	// the free_resources function gets called after the operator is in OPERATOR_CLEANED_UP state, i.e after terminal state
	// it should only be responsible for cleaning up inputs used by the operator
	void (*free_resources)(operator* o);

	// below is the condition variable that this operator will wait on, when it needs to acquire locks on the database entities in the lock table
	// only scans/writers on indexes and heap tables will need this
	// the mutex to be used with this condition variable must be o->self_query_plan->curr_tx->db->lock_manager_external_lock
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

	// freelist for the above output_buffers
	singlylist free_output_buffers;

	// list of consumption_iterator-s, pointing into tuple_regions in output_buffers
	linkedlist output_consumers;

	// this transformations will be applicable to all the tuples produced by this operator
	// as soon as it call produce_tuple/s_from_operator
	// remember output_lock will not be held while calling any of the transformers
	tuple_transformers output_tuple_transformers; // [MUST BE SET DURING THE SETUP PHASE]

	// -----------------------------------------------------------------------------------------------------

	// below variables are only needed for the internal function of the state machine of the operator

	pthread_mutex_t state_lock;				// lock for all the operator's attributes given below, related to only states
	pthread_cond_t wait_until_completion; 	// wait for the operator to be in it's terminal state

	operator_state state;

	// count of the concurrent jobs currently in active state spawned by the operator
	// using the function run_concurrent_job_for_operator()
	uint64_t queued_jobs_count;
	uint64_t running_jobs_count;

	int is_kill_signal_sent:1;
	int is_trigger_signaled_on_running:1; // this flag will be set, if a trigger is signalled while the operator was in running state

	// this kill_reason will be set/appended while sending a kill signal to the operator
	dstring kill_reason;
};

typedef struct consumption_iterator consumption_iterator;
struct consumption_iterator
{
	// stays constant for the lifetime of the consumption_iterator
	operator* producer;

	// stays constant for the lifetime of the consumption_iterator
	operator* consumer;

	// this flag initializes to 0
	// once set, it is on cleared on a successfull consume_for_consumption_iterator()
	// only if this flag is 0, is the next trgger will be done for the consumer
	// it is protected by the output_lock above
	int was_consumer_triggered;

	// this attribute may be NULL, if unused
	// this callback will be called right before consumer operator is triggered
	// use this callback to mark this cit_p ready to be used, and not to perform the actual read
	void (*notify_callback)(operator* consumer, consumption_iterator* cit_p);

	// it is protected (the pointer) by the output_lock above
	interim_tuple_store* curr_store;

	// this attribute of the consumtion_iterator is not protected by the output_lock
	// it not necessary to hold output_lock to access it
	interim_tuple_region curr_region;

	// it is protected by the output_lock above
	llnode embed_node_for_output_consumers;

	// for use outside the query_plan only
	union
	{
		llnode embed_node_ll;
		slnode embed_node_sl;
		bstnode embed_node_bst;
		rbhnode embed_node_rbh;
		hpnode embed_node_hp;
		phpnode embed_node_php;
	};
	uint64_t embed_uints[4];
	void* embed_ptrs[4];
};

// check if the operator is allowed to do it's execution
// returns 1, if the operator is in state OPERATOR_KILLED or OPERATOR_CLEANED_UP states or is_kill_signal_sent
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

// below is a simple flat clean_up_resource function to be used with simple operators
void OPERATOR_CLEAN_UP_RESOURCE_NO_OP_FUNCTION(operator* o);

// below is a simple flat free_resource function to be used with simple operators
void OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION(operator* o);

// produce a tuple from the operator o
int produce_tuple_from_operator(operator* o, void* tuple);

// clone_cti_p may be NULL
// notify_callback may be NULL
consumption_iterator* create_consumption_iterator(operator* producer, operator* consumer, void (*notify_callback)(operator* consumer, consumption_iterator* cit_p), consumption_iterator* clone_cit_p);
void destroy_consumption_iterator(consumption_iterator* cit_p);

// to be used with cutlery containers only
void delete_on_notify_for_consumption_iterator(void* resource_p, const void* data_p);
#define DELETE_ON_NOTIFY_FOR_CONSUMPTION_ITERATOR (&((notifier_interface){NULL, delete_on_notify_for_consumption_iterator}))

int points_to_same_tuple_for_consumtion_iterators(const consumption_iterator* cit1_p, const consumption_iterator* cit2_p);

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

// may be called as many times as you desire, from the inside of the operators or from outside
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