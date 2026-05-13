#ifndef AGGREGATE_FUNCTIONS_H
#define AGGREGATE_FUNCTIONS_H

#include<tuplestore/data_type_info.h>
#include<tuplestore/datum.h>

#define AGGREGATE_FUNCTION_PARAM_TYPE_INFO_HOLDER_CAPACITY 4

typedef struct aggregate_function aggregate_function;
struct aggregate_function
{
	const void* context_p;

	// returns 0 on failure
	int (*process_input)(const aggregate_function* af_p, void** state_p, const datum inputs[]);

	// destroys state object, must be NO-OP if NULL
	// this function will set the state to NULL, so this function stays idempotent
	void (*destroy_state)(const aggregate_function* af_p, void** state);

	// returns 0 on failure
	int (*produce_output)(const aggregate_function* af_p, datum* output, const void* state);

	// use this function to destroy the output produced, NO-OP on NULL_DATUM
	// this function will set the output to NULL_DATUM, so this function stays idempotent
	void (*destroy_output)(const aggregate_function* af_p, datum* output);

	// destroys this object
	// must not be called again
	void (*destroy_aggregate_function)(aggregate_function* af_p);

	// single value storing the type_info for the aggregate_function's output
	const data_type_info* output_type_info;

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

	af_p->destroy_output(af_p, output);
	af_p->destroy_state(af_p, state); // destroy state always at the last
*/

aggregate_function* get_count_aggregate_function(const data_type_info* input_type_info);

#endif