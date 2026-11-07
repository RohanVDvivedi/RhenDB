#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

#include<tuplestore/tuple.h>

#include<cutlery/stream.h>

fail_build_on(sizeof(uint32_t) > sizeof(cy_uint))

// pointer to this byte means continue generation, and a NULL in the state means completed
char GENERATE_DATA;

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

static void* next_tuple(operator* o, int* error_code)
{
	if(o->state == NULL)
		return NULL;

	uint32_t tuple_size = get_tuple_size_using_tuple_size_def2(&(o->desc->params.stream_input.input_tuple_def->size_def), o->desc->params.stream_input.in_strm, read_tuple_prefix_from_stream);
	if(tuple_size == 0)
	{
		if(o->desc->params.stream_input.in_strm->end_of_stream_received)
		{
			o->state = NULL;
			return NULL;
		}
		else
		{
			o->state = NULL;
			(*error_code) = STREAM_ERROR_IN_OPERATOR;
			return NULL;
		}
	}

	void* curr_tuple = malloc(tuple_size);
	int strm_error = 0;
	cy_uint bytes_read = 0;
	while(bytes_read < tuple_size)
	{
		cy_uint bytes_read_this_iteration = read_from_stream(o->desc->params.stream_input.in_strm, curr_tuple + bytes_read, tuple_size - bytes_read, &strm_error);
		if(strm_error)
		{
			free(curr_tuple);
			o->state = NULL;
			(*error_code) = STREAM_ERROR_IN_OPERATOR;
			return NULL;
		}
		if(bytes_read_this_iteration == 0)
			break;
		bytes_read += bytes_read_this_iteration;
	}
	if(tuple_size != bytes_read)
	{
		free(curr_tuple);
		o->state = NULL;
		(*error_code) = STREAM_ERROR_IN_OPERATOR;
		return NULL;
	}

	return curr_tuple;
}

void open_stream_input_operator(operator* o, stream* in_strm, const tuple_def* input_tuple_def)
{
	o->state = &GENERATE_DATA;
	o->next_tuple = next_tuple;
	o->destroy_state = NULL;
}