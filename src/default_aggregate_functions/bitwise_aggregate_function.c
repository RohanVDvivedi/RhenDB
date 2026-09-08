#include<rhendb/aggregate_functions.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<serint/large_uints.h>
#include<serint/large_ints.h>

#include<stdlib.h>

static data_type_info* get_bitwise_output_type_info(const data_type_info* input_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
			return BIT_FIELD_NULLABLE[input_type_info->bit_field_size];
		case UINT :
			return UINT_NULLABLE[input_type_info->size];
		case INT :
			return INT_NULLABLE[input_type_info->size];
		case LARGE_UINT:
			return LARGE_UINT_NULLABLE[input_type_info->size];
		case LARGE_INT:
			return LARGE_INT_NULLABLE[input_type_info->size];
		default :
			return NULL;
	}
}

static void* create_bitwise_state(const data_type_info* input_type_info, const datum input)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		case UINT :
		case INT :
		case LARGE_UINT:
		case LARGE_INT:
		{
			datum* bitwise_state = malloc(sizeof(datum));
			(*bitwise_state) = input;
			return bitwise_state;
		}

		default :
			return NULL;
	}
}

static datum get_bitwise_from_bitwise_state(void* state, const data_type_info* input_type_info, const data_type_info* output_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		case UINT :
		case INT :
		case LARGE_UINT:
		case LARGE_INT:
		{
			return *((const datum*)state);
		}

		default :
			return (*NULL_DATUM);
	}
}

// return of 0 implies overflow or underflow
typedef int (*update_bitwise_state)(void** state_p, const datum input, const aggregate_function* af_p);

// AND

static int BIT_FIELD_update_bitwise_and_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->bit_field_value = state->bit_field_value & input.bit_field_value;
	return 1;
}

static int UINT_update_bitwise_and_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->uint_value = state->uint_value & input.uint_value;
	return 1;
}

static int INT_update_bitwise_and_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->int_value = state->int_value & input.int_value;
	return 1;
}

static int LARGE_UINT_update_bitwise_and_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->large_uint_value = bitwise_and_uint256(state->large_uint_value, input.large_uint_value);
	return 1;
}

static int LARGE_INT_update_bitwise_and_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->large_int_value = bitwise_and_int256(state->large_int_value, input.large_int_value);
	return 1;
}

// OR

static int BIT_FIELD_update_bitwise_or_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->bit_field_value = state->bit_field_value | input.bit_field_value;
	return 1;
}

static int UINT_update_bitwise_or_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->uint_value = state->uint_value | input.uint_value;
	return 1;
}

static int INT_update_bitwise_or_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->int_value = state->int_value | input.int_value;
	return 1;
}

static int LARGE_UINT_update_bitwise_or_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->large_uint_value = bitwise_or_uint256(state->large_uint_value, input.large_uint_value);
	return 1;
}

static int LARGE_INT_update_bitwise_or_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->large_int_value = bitwise_or_int256(state->large_int_value, input.large_int_value);
	return 1;
}

// XOR

static int BIT_FIELD_update_bitwise_xor_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->bit_field_value = state->bit_field_value ^ input.bit_field_value;
	return 1;
}

static int UINT_update_bitwise_xor_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->uint_value = state->uint_value ^ input.uint_value;
	return 1;
}

static int INT_update_bitwise_xor_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->int_value = state->int_value ^ input.int_value;
	return 1;
}

static int LARGE_UINT_update_bitwise_xor_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->large_uint_value = bitwise_xor_uint256(state->large_uint_value, input.large_uint_value);
	return 1;
}

static int LARGE_INT_update_bitwise_xor_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	datum* state = *((datum**)state_p);
	state->large_int_value = bitwise_xor_int256(state->large_int_value, input.large_int_value);
	return 1;
}

// if is_and is set, it is AND, else if is_or is set, it is OR, else it is XOR
static update_bitwise_state get_dedicated_update_bitwise_state_function(const data_type_info* input_type_info, int is_and, int is_or)
{
	if(is_and)
	{
		switch(input_type_info->type)
		{
			case BIT_FIELD :
				return BIT_FIELD_update_bitwise_and_state;
			case UINT :
				return UINT_update_bitwise_and_state;
			case INT :
				return INT_update_bitwise_and_state;
			case LARGE_UINT:
				return LARGE_UINT_update_bitwise_and_state;
			case LARGE_INT:
				return LARGE_INT_update_bitwise_and_state;
			default :
				return NULL;
		}
	}
	else if(is_or)
	{
		switch(input_type_info->type)
		{
			case BIT_FIELD :
				return BIT_FIELD_update_bitwise_or_state;
			case UINT :
				return UINT_update_bitwise_or_state;
			case INT :
				return INT_update_bitwise_or_state;
			case LARGE_UINT:
				return LARGE_UINT_update_bitwise_or_state;
			case LARGE_INT:
				return LARGE_INT_update_bitwise_or_state;
			default :
				return NULL;
		}
	}
	else
	{
		switch(input_type_info->type)
		{
			case BIT_FIELD :
				return BIT_FIELD_update_bitwise_xor_state;
			case UINT :
				return UINT_update_bitwise_xor_state;
			case INT :
				return INT_update_bitwise_xor_state;
			case LARGE_UINT:
				return LARGE_UINT_update_bitwise_xor_state;
			case LARGE_INT:
				return LARGE_INT_update_bitwise_xor_state;
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
			(*state_p) = create_bitwise_state(af_p->input_type_infos[0], inputs[0]);
			if((*state_p) == NULL)
				return 0;
		}
		else
		{
			if(!((update_bitwise_state)(af_p->context_p))(state_p, inputs[0], af_p))
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

	(*output) = get_bitwise_from_bitwise_state((*state_p), af_p->input_type_infos[0], af_p->output_type_info);
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

aggregate_function* get_bitwise_aggregate_function(const data_type_info* input_type_info, int is_and, int is_or)
{
	aggregate_function* af_p = malloc(size_of_aggregate_function(1));

	if(get_dedicated_update_bitwise_state_function(input_type_info, is_and, is_or) == NULL)
	{
		printf("incompatible input_type_info for bitwise_aggregate_function, is_and = %d, is_or = %d\n", is_and, is_or);
		exit(-1);
	}

	af_p->context_p = (update_bitwise_state) get_dedicated_update_bitwise_state_function(input_type_info, is_and, is_or);

	af_p->process_input = process_input;

	af_p->produce_output = produce_output;

	af_p->destroy_state = destroy_state;

	af_p->destroy_aggregate_function = destroy_aggregate_function;

	af_p->output_type_info = get_bitwise_output_type_info(input_type_info);
	if(af_p->output_type_info == NULL)
	{
		printf("incompatible input_type_info for bitwise_aggregate_function, is_and = %d, is_or = %d\n", is_and, is_or);
		exit(-1);
	}

	af_p->buffers_resource_count = 0;

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}