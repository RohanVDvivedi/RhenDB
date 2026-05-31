#include<rhendb/aggregate_functions.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<serint/large_uints.h>
#include<serint/large_ints.h>

#include<stdlib.h>

/*
	for unsigned integers output_type_info = LARGE_UINT_NULLABLE[32]
	for integers output_type_info = LARGE_INT_NULLABLE[32]
	for float output_type_info = FLOAT_double_NULLABLE
*/

static data_type_info* get_sum_output_type_info(const data_type_info* input_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		case UINT :
			return LARGE_UINT_NULLABLE[9];
		case LARGE_UINT:
			return LARGE_UINT_NULLABLE[32];

		case INT :
			return LARGE_INT_NULLABLE[9];
		case LARGE_INT:
			return LARGE_INT_NULLABLE[32];

		case FLOAT :
			return FLOAT_double_NULLABLE;

		default :
			return NULL;
	}
}

static void* create_sum_state(const data_type_info* input_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		case UINT :
		case LARGE_UINT:
		{
			uint256* sum_state = malloc(sizeof(uint256));
			(*sum_state) = get_0_uint256();
			return sum_state;
		}

		case INT :
		case LARGE_INT:
		{
			int256* sum_state = malloc(sizeof(int256));
			(*sum_state) = get_0_int256();
			return sum_state;
		}

		case FLOAT :
		{
			double* sum_state = malloc(sizeof(double));
			(*sum_state) = 0.0;
			return sum_state;
		}

		default :
			return NULL;
	}
}

static datum get_sum_from_sum_state(const void* state, const data_type_info* input_type_info)
{
	if(state == NULL)
		return (*NULL_DATUM);

	switch(input_type_info->type)
	{
		case BIT_FIELD :
		case UINT :
		case LARGE_UINT:
		{
			return (datum){.large_uint_value = *((const uint256*)state)};
		}

		case INT :
		case LARGE_INT:
		{
			return (datum){.large_int_value = *((const int256*)state)};
		}

		case FLOAT :
		{
			return (datum){.double_value = *((const double*)state)};
		}

		default :
			return (*NULL_DATUM);
	}
}

// return of 0 implies overflow or underflow
typedef int (*update_sum_state)(void** state_p, const datum input);

static int BIT_FIELD_and_UINT_update_sum_state(void** state_p, const datum input)
{
	add_uint256(*state_p, **(const uint256**)state_p, get_uint256(input.uint_value));
	return 1;
}

static int LARGE_UINT_update_sum_state(void** state_p, const datum input)
{
	add_uint256(*state_p, **(const uint256**)state_p, input.large_uint_value);
	return 1;
}

static int INT_update_sum_state(void** state_p, const datum input)
{
	add_int256(*state_p, **(const int256**)state_p, get_int256(input.int_value));
	return 1;
}

static int LARGE_INT_update_sum_state(void** state_p, const datum input)
{
	add_int256(*state_p, **(const int256**)state_p, input.large_int_value);
	return 1;
}

static int FLOAT_update_sum_state(void** state_p, const datum input)
{
	(**((double**)state_p)) += input.float_value;
	return 1;
}

static int DOUBLE_update_sum_state(void** state_p, const datum input)
{
	(**((double**)state_p)) += input.double_value;
	return 1;
}

update_sum_state get_dedicated_update_sum_state_function(const data_type_info* input_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		case UINT :
			return BIT_FIELD_and_UINT_update_sum_state;
		case LARGE_UINT:
			return LARGE_UINT_update_sum_state;

		case INT :
			return INT_update_sum_state;
		case LARGE_INT:
			return LARGE_INT_update_sum_state;

		case FLOAT :
		{
			if(input_type_info->size == sizeof(float))
				return FLOAT_update_sum_state;
			else if(input_type_info->size == sizeof(double))
				return DOUBLE_update_sum_state;
			else
				return NULL;
		}

		default :
			return NULL;
	}
}

static int process_input(const aggregate_function* af_p, void** state_p, const datum inputs[])
{
	if((*state_p) == NULL)
		(*state_p) = create_sum_state(af_p->input_type_infos[0]);

	if(!is_datum_NULL(&(inputs[0])))
	{
		if(!((update_sum_state)(af_p->context_p))(state_p, inputs[0]))
			return 0;
	}

	return 1;
}

static int produce_output(const aggregate_function* af_p, datum* output, void** state_p)
{
	(*output) = get_sum_from_sum_state((*state_p), af_p->input_type_infos[0]);
	return 1;
}

static void destroy_state(const aggregate_function* af_p, void** state_p)
{
	// NOP if the state_p is already NULL
	if((*state_p) == NULL)
		return;

	free(*state_p);
	(*state_p) = NULL;
}

static void destroy_aggregate_function(aggregate_function* af_p)
{
	free(af_p);
}

aggregate_function* get_sum_aggregate_function(const data_type_info* input_type_info)
{
	aggregate_function* af_p = malloc(size_of_aggregate_function(1));

	af_p->context_p = get_dedicated_update_sum_state_function(input_type_info);
	if(af_p->context_p == NULL)
	{
		printf("incompatible input_type_info for sum_aggregate_function\n");
		exit(-1);
	}

	af_p->process_input = process_input;

	af_p->produce_output = produce_output;

	af_p->destroy_state = destroy_state;

	af_p->destroy_aggregate_function = destroy_aggregate_function;

	af_p->output_type_info = get_sum_output_type_info(input_type_info);
	if(af_p->output_type_info == NULL)
	{
		printf("incompatible input_type_info for sum_aggregate_function\n");
		exit(-1);
	}

	af_p->buffers_resource_count = 0;

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}