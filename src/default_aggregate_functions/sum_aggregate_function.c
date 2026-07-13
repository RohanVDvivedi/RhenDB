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
				// init context to maxcontext
				numeric_sum_state* sum_state = malloc(sizeof(numeric_sum_state));
				mpd_maxcontext(&(sum_state->ctx));
				sum_state->ctx.traps = 0;

				// initialize sum to 0
				{
					sum_state->sum.flags = MPD_STATIC;
					sum_state->sum.exp = 0;
					sum_state->sum.digits = 0;
					sum_state->sum.len = MPD_MINALLOC;
					sum_state->sum.alloc = MPD_MINALLOC;
					sum_state->sum.data = mpd_alloc(MPD_MINALLOC, sizeof(mpd_uint_t));
					if(sum_state->sum.data == NULL)
						exit(-1);
				}

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
					int exponent_too_big = 0;
					materialized_numeric mn = decimal_to_materialized_numeric(&(sum_state->sum), &exponent_too_big);

					tuple_def output_tuple_def;
					initialize_tuple_def(&output_tuple_def, (data_type_info*)output_type_info);
					uint32_t output_tuple_size = get_minimum_tuple_size(&output_tuple_def);
					uint32_t output_buffer_capacity = output_tuple_size;
					sum_state->output_buffer = malloc(output_buffer_capacity);
					init_tuple(&output_tuple_def, sum_state->output_buffer);

					set_sign_bits_and_exponent_for_numeric(mn.sign_bits, mn.exponent, sum_state->output_buffer, &output_tuple_def, SELF);
					if(mn.sign_bits == POSITIVE_NUMERIC || mn.sign_bits == NEGATIVE_NUMERIC)
					{
						uint32_t digits_to_be_written = min(MAX_NUMERIC_DIGITS_IN_BASE_10_pow_12, get_digits_count_for_materialized_numeric(&mn));
						output_buffer_capacity += digits_to_be_written * 5 + 4;
						sum_state->output_buffer = realloc(sum_state->output_buffer, output_buffer_capacity);

						set_element_in_tuple(&output_tuple_def, STATIC_POSITION(2), sum_state->output_buffer, EMPTY_DATUM, output_buffer_capacity - output_tuple_size);
						output_tuple_size = get_tuple_size(&output_tuple_def, sum_state->output_buffer);
						expand_element_count_for_element_in_tuple(&output_tuple_def, STATIC_POSITION(2), sum_state->output_buffer, 0, digits_to_be_written, output_buffer_capacity - output_tuple_size);
						output_tuple_size = get_tuple_size(&output_tuple_def, sum_state->output_buffer);

						for(uint32_t i = 0; i < digits_to_be_written; i++)
						{
							uint64_t digit = get_nth_digit_from_materialized_numeric(&mn, i);
							set_element_in_tuple(&output_tuple_def, STATIC_POSITION(2, i), sum_state->output_buffer, &((datum){.uint_value = digit}), output_buffer_capacity - output_tuple_size);
						}
					}

					deinitialize_materialized_numeric(&mn);
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
typedef int (*update_sum_state)(void** state_p, const datum input, const aggregate_function* af_p);

static int BIT_FIELD_and_UINT_update_sum_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	add_uint256(*state_p, **(const uint256**)state_p, get_uint256(input.uint_value));
	return 1;
}

static int LARGE_UINT_update_sum_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	add_uint256(*state_p, **(const uint256**)state_p, input.large_uint_value);
	return 1;
}

static int INT_update_sum_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	add_int256(*state_p, **(const int256**)state_p, get_int256(input.int_value));
	return 1;
}

static int LARGE_INT_update_sum_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	add_int256(*state_p, **(const int256**)state_p, input.large_int_value);
	return 1;
}

static int FLOAT_update_sum_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	(**((double**)state_p)) += input.float_value;
	return 1;
}

static int DOUBLE_update_sum_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	(**((double**)state_p)) += input.double_value;
	return 1;
}

typedef struct sum_context sum_context;
struct sum_context
{
	rage_engine* persistent_acid_rage_engine;

	update_sum_state update_sum_state_callback;
};

static int NUMERIC_update_sum_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	rage_engine* eng = ((const sum_context*)(af_p->context_p))->persistent_acid_rage_engine;
	numeric_sum_state* sum_state = (*state_p);

	// convert innput to input_mpd_t, i.e. materialize it
	mpd_t input_mpd_t;
	{
		const void* transaction_id = NULL;
		int abort_error = 0;
		numeric_reader_interface nri = init_intuple_numeric_reader_interface(input, af_p->input_type_infos[0], &(eng->bstd), eng->pam_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while sum aggregating extended numeric type\n");
			exit(-1);
		}

		numeric_sign_bits sb; int16_t exp;
		nri.extract_sign_bits_and_exponent(&nri, &sb, &exp);

		materialized_numeric mn;
		if(!initialize_materialized_numeric(&mn, 8))
			exit(-1);
		set_sign_bits_and_exponent_for_materialized_numeric(&mn, sb, exp);

		if(sb == POSITIVE_NUMERIC || sb == NEGATIVE_NUMERIC)   /* only finite non-zero values carry digits */
		{
			uint64_t buf[128];
			while(1)
			{
				int err = 0;
				uint32_t got = nri.read_digits_as_stream(&nri, buf, 128, &err);
				if(err)
				{
					printf("experienced abort_error while sum aggregating extended numeric type\n");
					exit(-1);
				}
				if(got == 0)
					break;
				/* digits stream MSD-first; push_lsd appends, keeping the MSD at the front */
				for(uint32_t i = 0; i < got; i++)
					push_lsd_in_materialized_numeric(&mn, buf[i]);
			}
		}
		nri.close_digits_stream(&nri);

		input_mpd_t = decimal_from_materialized_numeric(&mn);
		deinitialize_materialized_numeric(&mn);
	}

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

		if(!(((sum_context*)(af_p->context_p))->update_sum_state_callback)(state_p, inputs[0], af_p))
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

	free((void*)af_p->context_p);
	free(af_p);
}

aggregate_function* get_sum_aggregate_function(rhendb* rdb, const data_type_info* input_type_info)
{
	aggregate_function* af_p = malloc(size_of_aggregate_function(1));

	if(get_dedicated_update_sum_state_function(input_type_info) == NULL)
	{
		printf("incompatible input_type_info for sum_aggregate_function\n");
		exit(-1);
	}

	// context stores persistent_rage_engine here, for this aggregate function
	af_p->context_p = malloc(sizeof(sum_context));
	((sum_context*)(af_p->context_p))->persistent_acid_rage_engine = &(rdb->persistent_acid_rage_engine);
	((sum_context*)(af_p->context_p))->update_sum_state_callback = get_dedicated_update_sum_state_function(input_type_info);

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