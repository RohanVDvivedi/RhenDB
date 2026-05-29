#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<tuplestore/tuple.h>

#include<cutlery/stream.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* input_iterator;
	const tuple_def* input_tuple_def;

	stream* out_strm;
	int is_out_strm_closed;
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data)
		{
			int strm_error = 0;
			flush_all_from_stream(inputs->out_strm, &strm_error);

			inputs->is_out_strm_closed = 1;
			close_stream(inputs->out_strm, &strm_error);

			destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

			kill_signal_for_self_operator(o, kill_reason); return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			int strm_error = 0;
			inputs->is_out_strm_closed = 1;
			close_stream(inputs->out_strm, &strm_error);

			destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		if(tuple != NULL)
		{
			int strm_error = 0;
			write_to_stream(inputs->out_strm, tuple, get_tuple_size(inputs->input_tuple_def, tuple), &strm_error);
			if(strm_error)
			{
				int strm_error = 0;
				inputs->is_out_strm_closed = 1;
				close_stream(inputs->out_strm, &strm_error);

				destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

				kill_reason = get_dstring_pointing_to_literal_cstring("error_output_stream_operator");
				kill_signal_for_self_operator(o, kill_reason); return ;
			}
		}
		else
			break;
	}

	return ;
}

static void clean_up_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(!(inputs->is_out_strm_closed))
	{
		int strm_error = 0;
		inputs->is_out_strm_closed = 1;
		close_stream(inputs->out_strm, &strm_error);
	}

	deinitialize_stream(inputs->out_strm);
	free(inputs);
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	free(inputs);
}

operator_resource_counter setup_stream_output_operator(operator* o, operator* input_operator, stream* out_strm)
{
	operator_resource_counter result = {.job_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = free_resources;

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator),
		.out_strm = out_strm,
		.is_out_strm_closed = 0,
	};

	return result;
}