#include<rhendb/aggregate_functions.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<rhendb/transaction.h>

#include<serint/large_uints.h>
#include<serint/large_ints.h>

#include<mpdecimal.h>
#include<tuplelargetypes/numeric_extended.h>

#include<stdlib.h>

#define RESULT_PREFIX_BYTES        90
#define RESULT_MAX_SIZE           128

static data_type_info* get_sum_output_type_info(const data_type_info* input_type_info, transaction* tx)
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
			{
				data_type_info* output_type_info = get_numeric_extended_type_info(VOLATILE_EXT_SUB_TYPE, RESULT_MAX_SIZE, get_numeric_inline_type_info(RESULT_MAX_SIZE), &(tx->rdb->volatile_rage_engine.pam_p->pas));
				finalize_type_info(output_type_info);
				return output_type_info;
			}
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
					sum_state->sum.alloc = MPD_MINALLOC;
					sum_state->sum.data = mpd_alloc(MPD_MINALLOC, sizeof(mpd_uint_t));
					if(sum_state->sum.data == NULL)
						exit(-1);

					sum_state->sum.digits = 1;
					sum_state->sum.len = 1;
					sum_state->sum.data[0] = 0;
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

static datum get_sum_from_sum_state(void* state, const data_type_info* input_type_info, const data_type_info* output_type_info, transaction* tx)
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
					sum_state->output_buffer = malloc(RESULT_MAX_SIZE);
					init_tuple(&output_tuple_def, sum_state->output_buffer);

					set_sign_bits_and_exponent_for_numeric(mn.sign_bits, mn.exponent, sum_state->output_buffer, &output_tuple_def, SELF);

					if(mn.sign_bits == POSITIVE_NUMERIC || mn.sign_bits == NEGATIVE_NUMERIC)
					{
						uint64_t digits_count = get_digits_count_for_materialized_numeric(&mn);
						uint64_t* digits = malloc(digits_count * sizeof(uint64_t));

						for(uint64_t i = 0; i < digits_count; i++)
							digits[i] = get_nth_digit_from_materialized_numeric(&mn, i);

						deinitialize_materialized_numeric(&mn);

						{
							int abort_error_dummy = 0;
							rage_engine* ex_engine = &(tx->rdb->volatile_rage_engine);

							digit_write_iterator* wr = get_new_digit_write_iterator(sum_state->output_buffer, &output_tuple_def, SELF, 0 /*dummy root*/, get_NULL_tuple_pointer(&(ex_engine->pam_p->pas)), RESULT_PREFIX_BYTES / BYTES_PER_NUMERIC_DIGIT, &(ex_engine->bstd), ex_engine->pam_p, ex_engine->pmm_p);

							temporary_extension_store* temp_ext_store = NULL;
							uint64_t digits_written = 0;

							// write just the prefix
							while(digits_written < digits_count && wr->digits_written_to_prefix < wr->digits_to_be_written_to_prefix)
							{
								uint32_t digits_to_write_this_iteration = min(wr->digits_to_be_written_to_prefix - wr->digits_written_to_prefix, digits_count - digits_written);
								uint32_t digits_written_this_iteration = append_to_digit_write_iterator(wr, digits + digits_written, digits_to_write_this_iteration, NULL, NULL, &abort_error_dummy);
								if(digits_written_this_iteration == 0)
									break;
								digits_written += digits_written_this_iteration;
							}

							// write the extension
							if(digits_written < digits_count)
							{
								temp_ext_store = &(tx->temp_ext_stores[hash_element_within_tuple(sum_state->output_buffer, &output_tuple_def, EXTENDED_PREFIX_POS_ACC, FNV_64_TUPLE_HASHER) % TEMPORARY_EXTENSION_STORE_COUNT]);

								// set the write iterator with new root_page_id and take the lock before any further access
								write_lock(&(temp_ext_store->blob_store_lock), BLOCKING);
								wr->blob_store_root_page_id = temp_ext_store->blob_store_root_page_id;

								const heap_table_notifier* htan_p = &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(temp_ext_store->htan));

								while(digits_written < digits_count)
								{
									uint32_t digits_written_this_iteration = append_to_digit_write_iterator(wr, digits + digits_written, min(1000000ULL, digits_count - digits_written), htan_p, NULL, &abort_error_dummy);
									if(digits_written_this_iteration == 0)
										break;
									digits_written += digits_written_this_iteration;

									fix_unused_space_entries_in_store(tx, temp_ext_store);
								}
							}

							delete_digit_write_iterator(wr, NULL, &abort_error_dummy);
							if(temp_ext_store != NULL)
							{
								fix_unused_space_entries_in_store(tx, temp_ext_store);
								write_unlock(&(temp_ext_store->blob_store_lock));
							}
						}

						free(digits);
					}
					else // only positive number have digits
					{
						deinitialize_materialized_numeric(&mn);
					}
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
	transaction* tx;

	update_sum_state update_sum_state_callback;
};

static int NUMERIC_update_sum_state(void** state_p, const datum input, const aggregate_function* af_p)
{
	transaction* tx = ((const sum_context*)(af_p->context_p))->tx;
	numeric_sum_state* sum_state = (*state_p);

	// convert innput to input_mpd_t, i.e. materialize it
	mpd_t input_mpd_t;
	{
		const void* transaction_id = NULL;
		int abort_error = 0;

		extension_reader_iterator_callback temp;
		rage_engine* ex_engine;
		extension_reader_iterator_callback* callbacks = get_callback_and_engine_for_extended_type(tx, af_p->input_type_infos[0], &ex_engine, &temp);

		numeric_reader_interface nri = init_intuple_numeric_reader_interface(input, af_p->input_type_infos[0], ex_engine ? &(ex_engine->bstd) : NULL, ex_engine ? ex_engine->pam_p : NULL, callbacks, transaction_id, &abort_error);
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

	(*output) = get_sum_from_sum_state((*state_p), af_p->input_type_infos[0], af_p->output_type_info, ((const sum_context*)(af_p->context_p))->tx);
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

aggregate_function* get_sum_aggregate_function(transaction* tx, const data_type_info* input_type_info)
{
	aggregate_function* af_p = malloc(size_of_aggregate_function(1));

	if(get_dedicated_update_sum_state_function(input_type_info) == NULL)
	{
		printf("incompatible input_type_info for sum_aggregate_function\n");
		exit(-1);
	}

	// context stores persistent_rage_engine here, for this aggregate function
	af_p->context_p = malloc(sizeof(sum_context));
	((sum_context*)(af_p->context_p))->tx = tx;
	((sum_context*)(af_p->context_p))->update_sum_state_callback = get_dedicated_update_sum_state_function(input_type_info);

	af_p->process_input = process_input;

	af_p->produce_output = produce_output;

	af_p->destroy_state = destroy_state;

	af_p->destroy_aggregate_function = destroy_aggregate_function;

	af_p->output_type_info = get_sum_output_type_info(input_type_info, tx);
	if(af_p->output_type_info == NULL)
	{
		printf("incompatible input_type_info for sum_aggregate_function\n");
		exit(-1);
	}

	af_p->buffers_resource_count = has_extended_type_info(input_type_info, PERSISTENT_EXT_SUB_TYPE); // only the case with extended numeric to be added

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}