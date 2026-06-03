#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<tuplestore/tuple.h>

#include<cutlery/stream.h>

typedef struct input_values input_values;
struct input_values
{
	const tuple_def* input_tuple_def;

	stream* in_strm;
};

fail_build_on(sizeof(uint32_t) > sizeof(cy_uint))

static uint32_t read_tuple_prefix_from_stream(void* context_p, void* data, uint32_t data_size)
{
	int strm_error = 0;
	stream* in_strm = context_p;

	cy_uint bytes_read = 0;
	while(bytes_read < data_size)
	{
		cy_uint bytes_read_this_iteration = read_from_stream(in_strm, data + bytes_read, data_size - bytes_read, &strm_error);
		if(strm_error)
			break;
		if(bytes_read_this_iteration == 0)
			break;
		bytes_read += bytes_read_this_iteration;
	}

	if(!strm_error)
		unread_from_stream(in_strm, data, bytes_read, &strm_error);

	return bytes_read;
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	while(1)
	{
		uint32_t tuple_size = get_tuple_size_using_tuple_size_def2(&(inputs->input_tuple_def->size_def), inputs->in_strm, read_tuple_prefix_from_stream);
		if(tuple_size == 0)
		{
			if(inputs->in_strm->end_of_stream_received)
				break;
			else
			{
				kill_reason = get_dstring_pointing_to_literal_cstring("in_strm_corrupted");
				break;
			}
		}

		void* curr_tuple = malloc(tuple_size);
		int strm_error = 0;
		cy_uint bytes_read = 0;
		while(bytes_read < tuple_size)
		{
			cy_uint bytes_read_this_iteration = read_from_stream(inputs->in_strm, curr_tuple + bytes_read, tuple_size - bytes_read, &strm_error);
			if(strm_error)
				break;
			if(bytes_read_this_iteration == 0)
				break;
			bytes_read += bytes_read_this_iteration;
		}
		if(tuple_size != bytes_read)
		{
			free(curr_tuple);
			kill_reason = get_dstring_pointing_to_literal_cstring("in_strm_corrupted");
			break;
		}

		int produced = produce_tuple_from_operator(o, curr_tuple);
		if(!produced)
		{
			kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
			free(curr_tuple);
			break;
		}

		free(curr_tuple);
	}

	kill_signal_for_self_operator(o, kill_reason);
	return ;
}

static void clean_up_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->in_strm != NULL)
	{
		int strm_error = 0;
		close_stream(inputs->in_strm, &strm_error);
		deinitialize_stream(inputs->in_strm);
		inputs->in_strm = NULL;
	}
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	free(inputs);
}

operator_resource_counter setup_stream_input_operator(operator* o, stream* in_strm, const tuple_def* input_tuple_def)
{
	operator_resource_counter result = {.thread_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = free_resources;

	// generator's output is what we produce
	init_tuple_transformers(&(o->output_tuple_transformers), input_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.in_strm = in_strm,
		.input_tuple_def = input_tuple_def,
	};

	return result;
}