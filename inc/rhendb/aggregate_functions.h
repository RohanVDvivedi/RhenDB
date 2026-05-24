#ifndef AGGREGATE_FUNCTIONS_H
#define AGGREGATE_FUNCTIONS_H

#include<tuplestore/data_type_info.h>
#include<tuplestore/datum.h>

#include<rhendb/rhendb.h>

#define AGGREGATE_FUNCTION_PARAM_TYPE_INFO_HOLDER_CAPACITY 4

typedef struct aggregate_function aggregate_function;
struct aggregate_function
{
	const void* context_p;

	// returns 0 on failure
	int (*process_input)(const aggregate_function* af_p, void** state_p, const datum inputs[]);

	// returns 0 on failure
	int (*produce_output)(const aggregate_function* af_p, datum* output, void** state_p);

	// destroys state object, must be NO-OP if NULL
	// this function will set the state to NULL, so this function stays idempotent
	void (*destroy_state)(const aggregate_function* af_p, void** state_p);

	// destroys this object
	// must not be called again
	void (*destroy_aggregate_function)(aggregate_function* af_p);

	// single value storing the type_info for the aggregate_function's output
	const data_type_info* output_type_info;

	// number of buffers from bufferpool required for the execution of the aggregate function for its inputs
	// per live state of this aggregate_function
	uint64_t buffers_resource_count;

	// number of elements in input_type_infos[]
	uint32_t input_type_infos_count;

	// type_infos for the elements that go in as input to the process_input() function
	const data_type_info* input_type_infos[];
};

#define size_of_aggregate_function(input_params_count) (sizeof(aggregate_function) + ((input_params_count) * sizeof(data_type_info*)))

/*
	usage guide (not exactly C)

	void* state = NULLl;
	datum ouput = (*NULL_DATUM);

	af_p->process_input(af_p, &state, inputs_array_0[]);
	af_p->process_input(af_p, &state, inputs_array_1[]);
	af_p->process_input(af_p, &state, inputs_array_2[]);
	af_p->process_input(af_p, &state, inputs_array_3[]);
	.
	.
	.

	// then when all input tuples are finished

	af_p->produce_output(af_p, &output, state);

	// give output to the expecting user

	af_p->destroy_state(af_p, state); // destroy state always at the last
*/

aggregate_function* get_count_aggregate_function(const data_type_info* input_type_info);

aggregate_function* get_min_max_aggregate_function(rhendb* rdb, const data_type_info* input_type_info, int is_min);

static inline uint64_t get_max_buffers_count_for_all_aggregate_functions(uint32_t afs_count, aggregate_function const * const * afs_p)
{
	uint64_t buffers_resource_count = 0;
	for(uint32_t i = 0; i < afs_count; i++)
		buffers_resource_count = max(buffers_resource_count, afs_p[i]->buffers_resource_count);
	return buffers_resource_count;
}

#endif