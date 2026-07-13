#include<rhendb/aggregate_functions.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<serint/large_uints.h>
#include<serint/large_ints.h>

#include<mpdecimal.h>
#include<tuplelargetypes/numeric_extended.h>

#include<stdlib.h>

#define MAX_NUMERIC_DIGITS_IN_BASE_10_pow_12     6500 // number of digits in base 12 number
#define MAX_NUMERIC_DIGITS_IN_BASE_10           (MAX_NUMERIC_DIGITS_IN_BASE_10_pow_12 * 12)
#define MAX_NUMERIC_BYTES                       (MAX_NUMERIC_DIGITS_IN_BASE_10_pow_12 * 5)

static data_type_info* get_sum_output_type_info(const data_type_info* input_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		case UINT :
			return LARGE_UINT_NULLABLE[20];
		case LARGE_UINT:
			return LARGE_UINT_NULLABLE[32];

		case INT :
			return LARGE_INT_NULLABLE[20];
		case LARGE_INT:
			return LARGE_INT_NULLABLE[32];

		case FLOAT :
			return FLOAT_double_NULLABLE;

		case TUPLE :
		{
			if(is_numeric_type_info(input_type_info))
				return get_numeric_inline_type_info(MAX_NUMERIC_BYTES + 12);
			return NULL;
		}

		default :
			return NULL;
	}
}

typedef struct numeric_sum_state numeric_sum_state;
struct numeric_sum_state
{
	mpd_context_t ctx;

	mpd_t sum;
	void* output_buffer;
};

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

		case TUPLE :
		{
			if(is_numeric_type_info(input_type_info))
			{
				numeric_sum_state* sum_state = malloc(sizeof(numeric_sum_state));
				mpd_maxcontext(&(sum_state->ctx));
				sum_state->ctx.traps = 0;

				mpd_init(&(sum_state->sum));

				uint32_t status = 0;
				mpd_qresize(&(sum_state->sum), 1, &status);
				mpd_set_i32(&(sum_state->sum), 0, &(sum_state->ctx));

				sum_state->output_buffer = NULL;

				return sum_state;
			}
			return NULL;
		}

		default :
			return NULL;
	}
}

static void destroy_sum_state(void** state_p, const data_type_info* input_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		case UINT :
		case LARGE_UINT :
		case INT :
		case LARGE_INT:
		case FLOAT :
		{
			free(*state_p);
			(*state_p) = NULL;
			return ;
		}

		case TUPLE :
		{
			if(is_numeric_type_info(input_type_info))
			{
				numeric_sum_state* sum_state = (*state_p);

				mpd_del(&(sum_state->sum));

				if(sum_state->output_buffer)
					free(sum_state->output_buffer);

				free(*state_p);
				(*state_p) = NULL;
				return ;
			}
		}

		default :
			return ;
	}
}

static datum get_sum_from_sum_state(void* state, const data_type_info* input_type_info, const data_type_info* output_type_info)
{
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

		case TUPLE :
		{
			if(is_numeric_type_info(input_type_info))
			{
				numeric_sum_state* sum_state = state;

				if(sum_state->output_buffer == NULL) // it must be NULL here
				{
					// convert sum_state->sum to numeric tuple in buffer, with enough bytes allocated as an output_type_info
				}

				return (datum){.tuple_value = sum_state->output_buffer};
			}

			return (*NULL_DATUM);
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

static int NUMERIC_update_sum_state(void** state_p, const datum input)
{
	numeric_sum_state* sum_state = (*state_p);

	// convert innput to input_mpd_t, i.e. materialize it

	uint32_t status = 0;
	mpd_qadd(&(sum_state->sum), &(sum_state->sum), &input_mpd_t, &(sum_state->ctx), &status);

	mpd_del(&input_mpd_t);

	if(status & MPD_Malloc_error)
		return 0;

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

		case TUPLE :
		{
			if(is_numeric_type_info(input_type_info))
				return NUMERIC_update_sum_state;
		}

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
			(*state_p) = create_sum_state(af_p->input_type_infos[0]);

		if(!((update_sum_state)(af_p->context_p))(state_p, inputs[0]))
			return 0;
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

	(*output) = get_sum_from_sum_state((*state_p), af_p->input_type_infos[0], af_p->output_type_info);
	return 1;
}

static void destroy_state(const aggregate_function* af_p, void** state_p)
{
	// NOP if the state_p is already NULL
	if((*state_p) == NULL)
		return;

	destroy_sum_state(state_p, af_p->input_type_infos[0]);
}

static void destroy_aggregate_function(aggregate_function* af_p)
{
	if(is_numeric_type_info(af_p->output_type_info))
		destroy_type_info_recursively((void*)(af_p->output_type_info), NULL);

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

	af_p->buffers_resource_count = has_extended_type_info(input_type_info); // only the case with extended numeric to be added

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}