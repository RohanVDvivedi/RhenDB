#include<rhendb/aggregate_functions.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<serint/large_uints.h>
#include<serint/large_ints.h>

#include<stdlib.h>

static data_type_info* get_logical_output_type_info(const data_type_info* input_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		case UINT :
		case INT :
		case LARGE_UINT:
		case LARGE_INT:
			return BIT_FIELD_NULLABLE[1]; // output is always a single bit, true or false OR NULL
		default :
			return NULL;
	}
}

static void* create_logical_state(const data_type_info* input_type_info, const datum input)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		{
			int* logical_value = malloc(sizeof(int));
			(*logical_value) = !!(input.bit_field_value);
			return logical_value;
		}
		case UINT :
		{
			int* logical_value = malloc(sizeof(int));
			(*logical_value) = !!(input.uint_value);
			return logical_value;
		}
		case INT :
		{
			int* logical_value = malloc(sizeof(int));
			(*logical_value) = !!(input.int_value);
			return logical_value;
		}
		case LARGE_UINT:
		{
			int* logical_value = malloc(sizeof(int));
			(*logical_value) = !is_zero_uint256(input.large_uint_value);
			return logical_value;
		}
		case LARGE_INT:
		{
			int* logical_value = malloc(sizeof(int));
			(*logical_value) = !is_zero_int256(input.large_int_value);
			return logical_value;
		}

		default :
			return NULL;
	}
}

static datum get_logical_from_logical_state(void* state, const data_type_info* input_type_info, const data_type_info* output_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		case UINT :
		case INT :
		case LARGE_UINT:
		case LARGE_INT:
		{
			return (datum){.bit_field_value = *((const int*)state)};
		}

		default :
			return (*NULL_DATUM);
	}
}

// return of 0 implies overflow or underflow
typedef int (*update_logical_state)(void** state_p, const datum input, const aggregate_function* af_p);

// AND

static int BIT_FIELD_update_logical_and_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) && input.bit_field_value;
	return 1;
}

static int UINT_update_logical_and_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) && input.uint_value;
	return 1;
}

static int INT_update_logical_and_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) && input.int_value;
	return 1;
}

static int LARGE_UINT_update_logical_and_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) && (!is_zero_uint256(input.large_uint_value));
	return 1;
}

static int LARGE_INT_update_logical_and_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) && (!is_zero_int256(input.large_int_value));
	return 1;
}

// OR

static int BIT_FIELD_update_logical_or_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) || input.bit_field_value;
	return 1;
}

static int UINT_update_logical_or_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) || input.uint_value;
	return 1;
}

static int INT_update_logical_or_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) || input.int_value;
	return 1;
}

static int LARGE_UINT_update_logical_or_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) || (!is_zero_uint256(input.large_uint_value));
	return 1;
}

static int LARGE_INT_update_logical_or_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) || (!is_zero_int256(input.large_int_value));
	return 1;
}

// XOR

static int BIT_FIELD_update_logical_xor_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) ^ (!!(input.bit_field_value));
	return 1;
}

static int UINT_update_logical_xor_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) ^ (!!(input.uint_value));
	return 1;
}

static int INT_update_logical_xor_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) ^ (!!(input.int_value));
	return 1;
}

static int LARGE_UINT_update_logical_xor_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) ^ (!is_zero_uint256(input.large_uint_value));
	return 1;
}

static int LARGE_INT_update_logical_xor_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	int* state = *((int**)state_p);
	(*state) = (*state) ^ (!is_zero_int256(input.large_int_value));
	return 1;
}

// if is_and is set, it is AND, else if is_or is set, it is OR, else it is XOR
static update_logical_state get_dedicated_update_logical_state_function(const data_type_info* input_type_info, int is_and, int is_or)
{
	if(is_and)
	{
		switch(input_type_info->type)
		{
			case BIT_FIELD :
				return BIT_FIELD_update_logical_and_state;
			case UINT :
				return UINT_update_logical_and_state;
			case INT :
				return INT_update_logical_and_state;
			case LARGE_UINT:
				return LARGE_UINT_update_logical_and_state;
			case LARGE_INT:
				return LARGE_INT_update_logical_and_state;
			default :
				return NULL;
		}
	}
	else if(is_or)
	{
		switch(input_type_info->type)
		{
			case BIT_FIELD :
				return BIT_FIELD_update_logical_or_state;
			case UINT :
				return UINT_update_logical_or_state;
			case INT :
				return INT_update_logical_or_state;
			case LARGE_UINT:
				return LARGE_UINT_update_logical_or_state;
			case LARGE_INT:
				return LARGE_INT_update_logical_or_state;
			default :
				return NULL;
		}
	}
	else
	{
		switch(input_type_info->type)
		{
			case BIT_FIELD :
				return BIT_FIELD_update_logical_xor_state;
			case UINT :
				return UINT_update_logical_xor_state;
			case INT :
				return INT_update_logical_xor_state;
			case LARGE_UINT:
				return LARGE_UINT_update_logical_xor_state;
			case LARGE_INT:
				return LARGE_INT_update_logical_xor_state;
			default :
				return NULL;
		}
	}
}

static int process_input(const aggregate_function* af_p, void** state_p, const datum inputs[])
{
	if(!is_datum_NULL(&(inputs[0])))
	{
		// initialize state from the first non-NULL input
		if((*state_p) == NULL)
		{
			(*state_p) = create_logical_state(af_p->input_type_infos[0], inputs[0]);
			if((*state_p) == NULL)
				return 0;
		}
		else
		{
			if(!((update_logical_state)(af_p->context_p))(state_p, inputs[0], af_p))
				return 0;
		}
	}

	return 1;
}

static int produce_output(const aggregate_function* af_p, datum* output, void** state_p)
{
	// just return NULL_DATUM, no tuple/row was seen
	if((*state_p) == NULL)
	{
		(*output) = (*NULL_DATUM);
		return 1;
	}

	(*output) = get_logical_from_logical_state((*state_p), af_p->input_type_infos[0], af_p->output_type_info);
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

aggregate_function* get_logical_aggregate_function(const data_type_info* input_type_info, int is_and, int is_or)
{
	aggregate_function* af_p = malloc(size_of_aggregate_function(1));

	if(get_dedicated_update_logical_state_function(input_type_info, is_and, is_or) == NULL)
	{
		printf("incompatible input_type_info for logical_aggregate_function, is_and = %d, is_or = %d\n", is_and, is_or);
		exit(-1);
	}

	af_p->context_p = (update_logical_state) get_dedicated_update_logical_state_function(input_type_info, is_and, is_or);

	af_p->process_input = process_input;

	af_p->produce_output = produce_output;

	af_p->destroy_state = destroy_state;

	af_p->destroy_aggregate_function = destroy_aggregate_function;

	af_p->output_type_info = get_logical_output_type_info(input_type_info);
	if(af_p->output_type_info == NULL)
	{
		printf("incompatible input_type_info for logical_aggregate_function, is_and = %d, is_or = %d\n", is_and, is_or);
		exit(-1);
	}

	af_p->buffers_resource_count = 0;

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}