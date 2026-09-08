#include<rhendb/aggregate_functions.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<serint/large_uints.h>
#include<serint/large_ints.h>

#include<stdlib.h>

static data_type_info* get_gcd_output_type_info(const data_type_info* input_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
			return BIT_FIELD_NULLABLE[input_type_info->bit_field_size];

		case UINT :
		case INT :
			return UINT_NULLABLE[input_type_info->size];

		case LARGE_UINT:
		case LARGE_INT:
			return LARGE_UINT_NULLABLE[input_type_info->size];

		default :
			return NULL;
	}
}

static void* create_gcd_state(const data_type_info* input_type_info, const datum input)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		{
			uint64_t* gcd_state = malloc(sizeof(uint64_t));
			(*gcd_state) = input.bit_field_value;
			return gcd_state;
		}

		case UINT :
		{
			uint64_t* gcd_state = malloc(sizeof(uint64_t));
			(*gcd_state) = input.uint_value;
			return gcd_state;
		}

		case INT :
		{
			uint64_t* gcd_state = malloc(sizeof(uint64_t));
			(*gcd_state) = (input.int_value >= 0) ? input.int_value : (((uint64_t)(-(input.int_value+1)))+1); // taking it's absolute value in uint64_t
			return gcd_state;
		}

		case LARGE_UINT:
		{
			uint256* gcd_state = malloc(sizeof(uint256));
			(*gcd_state) = input.large_uint_value;
			return gcd_state;
		}

		case LARGE_INT:
		{
			uint256* gcd_state = malloc(sizeof(uint256));
			(*gcd_state) = get_absolute_int256(input.large_int_value);
			return gcd_state;
		}

		default :
			return NULL;
	}
}

static datum get_gcd_from_gcd_state(void* state, const data_type_info* input_type_info, const data_type_info* output_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		{
			return (datum){.bit_field_value = *((const uint64_t*)state)};
		}

		case UINT :
		case INT :
		{
			return (datum){.uint_value = *((const uint64_t*)state)};
		}

		case LARGE_UINT:
		case LARGE_INT:
		{
			return (datum){.large_uint_value = *((const uint256*)state)};
		}

		default :
			return (*NULL_DATUM);
	}
}

// return of 0 implies overflow or underflow
typedef int (*update_gcd_state)(void** state_p, const datum input, const aggregate_function* af_p);

static uint64_t gcd_for_uint64_t(uint64_t a, uint64_t b)
{
	while(b != 0)
	{
		uint64_t temp = b;
		b = a % b;
		a = temp;
	}
	return a;
}

static int BIT_FIELD_update_gcd_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	uint64_t* gcd_val = *((uint64_t**)state_p);
	(*gcd_val) = gcd_for_uint64_t((*gcd_val), input.bit_field_value);
	return 1;
}

static int UINT_update_gcd_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	uint64_t* gcd_val = *((uint64_t**)state_p);
	(*gcd_val) = gcd_for_uint64_t((*gcd_val), input.uint_value);
	return 1;
}

static int INT_update_gcd_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	uint64_t* gcd_val = *((uint64_t**)state_p);
	(*gcd_val) = gcd_for_uint64_t((*gcd_val), ((input.int_value >= 0) ? input.int_value : (((uint64_t)(-(input.int_value+1)))+1))); // taking it's absolute value in uint64_t, then gcd
	return 1;
}

static int LARGE_UINT_update_gcd_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	uint256* gcd_val = *((uint256**)state_p);
	(*gcd_val) = gcd_uint256((*gcd_val), input.large_uint_value);
	return 1;
}

static int LARGE_INT_update_gcd_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	uint256* gcd_val = *((uint256**)state_p);
	(*gcd_val) = gcd_uint256((*gcd_val), get_absolute_int256(input.large_int_value));
	return 1;
}

static update_gcd_state get_dedicated_update_gcd_state_function(const data_type_info* input_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
			return BIT_FIELD_update_gcd_state;
		case UINT :
			return UINT_update_gcd_state;
		case INT :
			return INT_update_gcd_state;
		case LARGE_UINT:
			return LARGE_UINT_update_gcd_state;
		case LARGE_INT:
			return LARGE_INT_update_gcd_state;
		default :
			return NULL;
	}
}

static int process_input(const aggregate_function* af_p, void** state_p, const datum inputs[])
{
	if(!is_datum_NULL(&(inputs[0])))
	{
		// always create an empty state if one does not exist yet
		if((*state_p) == NULL)
		{
			(*state_p) = create_gcd_state(af_p->input_type_infos[0], inputs[0]);
			if((*state_p) == NULL)
				return 0;
		}
		else
		{
			if(!((update_gcd_state)(af_p->context_p))(state_p, inputs[0], af_p))
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

	(*output) = get_gcd_from_gcd_state((*state_p), af_p->input_type_infos[0], af_p->output_type_info);
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

aggregate_function* get_gcd_aggregate_function(const data_type_info* input_type_info)
{
	aggregate_function* af_p = malloc(size_of_aggregate_function(1));

	if(get_dedicated_update_gcd_state_function(input_type_info) == NULL)
	{
		printf("incompatible input_type_info for gcd_aggregate_function\n");
		exit(-1);
	}

	af_p->context_p = (update_gcd_state) get_dedicated_update_gcd_state_function(input_type_info);

	af_p->process_input = process_input;

	af_p->produce_output = produce_output;

	af_p->destroy_state = destroy_state;

	af_p->destroy_aggregate_function = destroy_aggregate_function;

	af_p->output_type_info = get_gcd_output_type_info(input_type_info);
	if(af_p->output_type_info == NULL)
	{
		printf("incompatible input_type_info for gcd_aggregate_function\n");
		exit(-1);
	}

	af_p->buffers_resource_count = 0;

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}