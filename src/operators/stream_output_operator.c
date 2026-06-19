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
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data)
		{
			int strm_error = 0;
			flush_all_from_stream(inputs->out_strm, &strm_error);

			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
			return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
			return ;
		}

		if(tuple != NULL)
		{
			int strm_error = 0;
			write_to_stream(inputs->out_strm, tuple, get_tuple_size(inputs->input_tuple_def, tuple), &strm_error);
			if(strm_error)
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("error_output_stream_operator"));
				return ;
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

	if(inputs->input_iterator != NULL)
	{
		destroy_consumption_iterator(inputs->input_iterator);
		inputs->input_iterator = NULL;
	}

	if(inputs->out_strm != NULL)
	{
		int strm_error = 0;
		close_stream(inputs->out_strm, &strm_error);
		deinitialize_stream(inputs->out_strm);
		inputs->out_strm = NULL;
	}
}

operator_resource_counter setup_stream_output_operator(operator* o, operator* input_operator, stream* out_strm)
{
	operator_resource_counter result = {.job_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator),
		.out_strm = out_strm,
	};

	return result;
}