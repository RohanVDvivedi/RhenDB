#ifndef QUERY_PLAN_H
#define QUERY_PLAN_H

#include<pthread.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<cutlery/arraylist.h>

typedef struct query_plan query_plan;

typedef struct operator_desc operator_desc;

typedef struct operator operator;
struct operator
{
	// pointer to the query_plan that this operator is part of
	query_plan* self_query_plan;

	// index of this operator in the query plan (in the self_query_plan)
	uint64_t index;

	// operator descriptor that this operator got created from
	const operator_desc* desc;

	// pointer for the operator to store state
	// this includes the spilling state for the sort, hash joins, keyed-aggregations
	void* state;

	// the operator's main function that produces tuples until it dies or has nothing in it's input operators 
	void* (*next_tuple)(operator* o, int* error_code);

	// destroy state of this operator
	void (*destroy_state)(operator* o);
};

// returns 0, if lock not acquired 1 if acquired, and -1 if an abort must be performed
int acquire_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, uint64_t timeout_in_microseconds);
void release_lock_on_resource_from_operator(operator* o, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size);

typedef struct query_plan_desc query_plan_desc;

typedef struct query_plan query_plan;
struct query_plan
{
	// query_plan descriptor that this query_plan got created from
	const query_plan_desc* desc;

	// anything that implements the operator interface for this query_plan
	arraylist operators;
};

// creates query_plan from the descriptor
query_plan* get_new_query_plan(const query_plan_desc* desc);

// calls next on the last operator of the query_plan
void* next_tuple_from_query_plan(query_plan* qp, int* error_code);

// calls destroy state on all and destroys the query_plan
void destroy_query_plan(query_plan* qp);

#endif