#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

#include<tuplestore/tuple.h>

#include<cutlery/stream.h>

fail_build_on(sizeof(uint32_t) > sizeof(cy_uint))

typedef struct op_state op_state;
struct op_state
{
	void* curr_tuple;

	// return this from next_tuple
	rhendb_datum rtuple;
};

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

static const rhendb_datum* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	uint32_t tuple_size = get_tuple_size_using_tuple_size_def2(&(o->desc->params.stream_input.input_tuple_def->size_def), o->desc->params.stream_input.in_strm, read_tuple_prefix_from_stream);
	if(tuple_size == 0)
	{
		if(o->desc->params.stream_input.in_strm->end_of_stream_received)
		{
			o->destroy_state(o);
			return NULL;
		}
		else
		{
			o->destroy_state(o);
			(*error_code) = STREAM_ERROR_IN_OPERATOR;
			return NULL;
		}
	}

	int strm_error = 0;
	cy_uint bytes_read = 0;
	while(bytes_read < tuple_size)
	{
		cy_uint bytes_read_this_iteration = read_from_stream(o->desc->params.stream_input.in_strm, this_state->curr_tuple + bytes_read, tuple_size - bytes_read, &strm_error);
		if(strm_error)
		{
			o->destroy_state(o);
			(*error_code) = STREAM_ERROR_IN_OPERATOR;
			return NULL;
		}
		if(bytes_read_this_iteration == 0)
			break;
		bytes_read += bytes_read_this_iteration;
	}
	if(bytes_read == 0)
	{
		o->destroy_state(o);
		return NULL;
	}
	if(tuple_size != bytes_read)
	{
		o->destroy_state(o);
		(*error_code) = STREAM_ERROR_IN_OPERATOR;
		return NULL;
	}

	return &(this_state->rtuple);
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		if(this_state->curr_tuple)
			free(this_state->curr_tuple);

		free(this_state);
	}
	o->state = NULL;
}

void open_stream_input_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->curr_tuple = malloc(get_maximum_tuple_size(o->desc->params.stream_input.input_tuple_def));

	this_state->rtuple.type = RAW_DATUM;
	this_state->rtuple.as_raw_datum.value.tuple_value = this_state->curr_tuple;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}