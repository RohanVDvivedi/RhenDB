#ifndef AGGREGATE_FUNCTIONS_H
#define AGGREGATE_FUNCTIONS_H

#include<tuplestore/data_type_info.h>
#include<tuplestore/datum.h>

#include<rhendb/rhendb.h>

typedef struct rhendb_function rhendb_function;
struct rhendb_function
{
	const void* context_p;

	// to be set to 1, if it is an aggregate function
	int is_aggregate_function;

	// returns 0 on failure
	int (*process_input)(const rhendb_function* rf_p, void** state_p, const datum inputs[]);

	// returns 0 on failure
	int (*produce_output)(const rhendb_function* rf_p, datum* output, void** state_p);

	// destroys state object, must be NO-OP if NULL
	// this function will set the state to NULL, so this function stays idempotent
	void (*destroy_state)(const rhendb_function* rf_p, void** state_p);

	// destroys this object
	// must not be called again
	void (*destroy_rhendb_function)(rhendb_function* rf_p);

	// single value storing the type_info for the rhendb_function's output
	const data_type_info* output_type_info;

	// number of buffers from bufferpool required for the execution of the function for its inputs
	// per live state of this rhendb_function
	uint64_t buffers_resource_count;

	// number of elements in input_type_infos[]
	uint32_t input_type_infos_count;

	// type_infos for the elements that go in as input to the process_input() function
	const data_type_info* input_type_infos[];
};

#define size_of_rhendb_function(input_params_count) (sizeof(rhendb_function) + ((input_params_count) * sizeof(data_type_info*)))

/*
	usage guide (not exactly C)

	void* state = NULLl;
	datum ouput = (*NULL_DATUM);

	rf_p->process_input(rf_p, &state, inputs_array_0[]);
	rf_p->process_input(rf_p, &state, inputs_array_1[]);
	rf_p->process_input(rf_p, &state, inputs_array_2[]);
	rf_p->process_input(rf_p, &state, inputs_array_3[]);
	.
	.
	.

	// then when all input tuples are finished
	// call process_input() only once if this is not an aggregate function (is_aggregate_function == 0)
	// only for is_aggregate_function = 1, should this fucntion be called more than once

	rf_p->produce_output(rf_p, &output, state);

	// give output to the expecting user

	rf_p->destroy_state(rf_p, state); // destroy state always at the last
*/

rhendb_function* get_count_aggregate_function(const data_type_info* input_type_info);

rhendb_function* get_min_max_aggregate_function(rhendb* rdb, const data_type_info* input_type_info, int is_min);

rhendb_function* get_sum_aggregate_function(const data_type_info* input_type_info);

static inline uint64_t get_max_buffers_count_for_all_rhendb_functions(uint32_t rfs_count, rhendb_function const * const * rfs_p)
{
	uint64_t buffers_resource_count = 0;
	for(uint32_t i = 0; i < rfs_count; i++)
		buffers_resource_count = max(buffers_resource_count, rfs_p[i]->buffers_resource_count);
	return buffers_resource_count;
}

#endif