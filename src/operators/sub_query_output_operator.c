#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<tuplestore/tuple.h>

#include<boompar/sync_pipe.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* input_iterator;
	const tuple_def* input_tuple_def;

	sync_pipe* out_pipe;
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
			kill_signal_for_self_operator(o, OPERATOR_SUCCESS_REASON);
			return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
			return ;
		}

		if(tuple != NULL)
		{
			cy_uint bytes_to_write = get_tuple_size(inputs->input_tuple_def, tuple);
			while(bytes_to_write > 0)
			{
				cy_uint bytes_written = write_to_sync_pipe(inputs->out_pipe, tuple + bytes_to_write, bytes_to_write);
				if(bytes_written == 0)
				{
					kill_signal_for_self_operator(o, OPERATOR_SUCCESS_REASON);
					return ;
				}
				bytes_to_write -= bytes_written;
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

	if(inputs->out_pipe != NULL)
	{
		close_sync_pipe(inputs->out_pipe);
		inputs->out_pipe = NULL;
	}
}

operator_resource_counter setup_subquery_output_operator(operator* o, operator* input_operator, sync_pipe* out_pipe)
{
	operator_resource_counter result = {.thread_counter = 1};
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
		.out_pipe = out_pipe,
	};

	return result;
}

typedef struct subquery_pipe_size_reader_context subquery_pipe_size_reader_context;
struct subquery_pipe_size_reader_context
{
	void* buffer;
	uint32_t bytes_capacity;
	uint32_t bytes_read;

	sync_pipe* spipe;
};

static uint32_t read_tuple_prefix_from_sync_pipe(void* context_p, void* data, uint32_t data_size)
{
	if(data_size == 0)
		return 0;

	subquery_pipe_size_reader_context* cntxt = context_p;

	cy_uint bytes_read = 0;
	while(bytes_read < data_size)
	{
		cy_uint bytes_read_this_iteration = read_from_sync_pipe(cntxt->spipe, data + bytes_read, data_size - bytes_read);
		if(bytes_read_this_iteration == 0)
			break;
		bytes_read += bytes_read_this_iteration;
	}

	if(cntxt->bytes_capacity - cntxt->bytes_read < bytes_read)
	{
		cntxt->buffer = realloc(cntxt->buffer, cntxt->bytes_read + bytes_read);
		cntxt->bytes_capacity = cntxt->bytes_read + bytes_read;
		memmove(cntxt->buffer + cntxt->bytes_read, data, bytes_read);
	}

	return bytes_read;
}

void* get_tuple_from_subquery_pipe(sync_pipe* spipe, tuple_def* tpl_d)
{
	subquery_pipe_size_reader_context cntxt = {
		.buffer = NULL,
		.bytes_capacity = 0,
		.bytes_read = 0,
		.spipe = spipe,
	};

	uint32_t tuple_size = get_tuple_size2(tpl_d, &cntxt, read_tuple_prefix_from_sync_pipe);
	if(tuple_size == 0)
	{
		free(cntxt.buffer);
		return NULL;
	}

	cntxt.buffer = realloc(cntxt.buffer, tuple_size);
	while(cntxt.bytes_read < tuple_size)
	{
		cy_uint bytes_read_this_iteration = read_from_sync_pipe(spipe, cntxt.buffer + cntxt.bytes_read, tuple_size - cntxt.bytes_read);
		if(bytes_read_this_iteration == 0)
		{
			free(cntxt.buffer);
			return NULL;
		}
		cntxt.bytes_read += bytes_read_this_iteration;
	}

	return cntxt.buffer;
}