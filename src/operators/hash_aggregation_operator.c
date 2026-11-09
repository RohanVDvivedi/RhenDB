#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

#include<rhendb/rash_table.h>

#include<rhendb/aggregate_functions.h>

#define INIT_BUCKET_COUNT 64
#define MAX_LOAD_FACTOR 1.3

typedef struct op_state op_state;
struct op_state
{
	operator* input_operator;

	rash_table_handle rth;

	int has_active_rti; // valid only if the build step is completed
	rash_table_iterator rti;

	// maximum number of input_datums required by any aggregate function
	// 0, if there are no aggregate functions to be evaluated
	uint32_t max_input_datums_count;
};

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	if(!this_state->has_active_rti)
	{
		while(1)
		{
			void* tuple = this_state->input_operator->next_tuple(this_state->input_operator, error_code);
			if(tuple == NULL)
			{
				if(*error_code)
				{
					o->destroy_state(o);
					return NULL;
				}
				break;
			}

			// insert into the rth, if the aggregate_functions count == 0, no need to insert the value, just insert the key
			{
				// create rash table key
				rash_table_key rtk = get_new_rash_table_key(tuple, this_state->input_operator->desc->output_tuple_def, o->desc->params.hash_aggregation.key_element_ids, o->desc->params.hash_aggregation.key_element_count, o->self_query_plan->desc->curr_tx);

				// open rash table iterator for insertion/appending
				rash_table_iterator rti = find_equals_in_rash_table(&(this_state->rth), &rtk, 0);

				// open write iterator on this key
				binary_write_iterator* bwi_p = open_for_writing_value_in_rash_table_iterator(&rti);

				// store the value in the rash_table, only if we need any inputs for the aggregation
				if(this_state->max_input_datums_count > 0)
				{
					// perform append into this iterator storing in the complete tuple
					int abort_error_dummy = 0;
					append_to_binary_write_iterator(bwi_p, tuple, get_tuple_size(this_state->input_operator->desc->output_tuple_def, tuple), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(this_state->rth.htan)), NULL, &abort_error_dummy);
				}

				// close the bwi_p
				close_and_write_value_in_hash_table_iterator(&rti, bwi_p);

				// delete the insertion iterator
				delete_rash_table_iterator(&rti);

				// if the load factor went too high then expand the bucket_count for the partition
				if(get_load_factor_for_rash_table(&(this_state->rth)) > MAX_LOAD_FACTOR)
					expand_rash_table(&(this_state->rth));

				// destroy rash table key
				destroy_rash_table_key(&rtk);
			}

			free(tuple);
		}

		// create a rti to iterate over all the entries
		this_state->rti = find_all_in_rash_table(&(this_state->rth), 1);
		this_state->has_active_rti = 1;
	}

	// if no more entries in the build table, we are done
	if(!exists_in_rash_table_iterator(&(this_state->rti)))
	{
		o->destroy_state(o);
		return NULL;
	}

	// get memory for states and input_datums
	void** states = calloc(sizeof(void*), o->desc->params.hash_aggregation.aggregate_functions_count);
	datum* input_datums = malloc(sizeof(datum) * this_state->max_input_datums_count);

	// allocate for the output_tuple
	void* output_tuple = malloc(get_maximum_tuple_size(o->desc->output_tuple_def));
	init_tuple(o->desc->output_tuple_def, output_tuple);

	// iterate over the current entry in rti, set the key
	{
		// read key of this entry
		void* key_tuple = read_key_in_rash_table_iterator(&(this_state->rti));

		for(uint32_t i = 0; i < o->desc->params.hash_aggregation.key_element_count; i++)
		{
			const data_type_info* dti_temp;
			datum key_val;
			if(get_value_from_element_from_tuple(&key_val, &dti_temp, &(this_state->rth.key_tuple_def), STATIC_POSITION(i), key_tuple))
				set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(i), output_tuple, &key_val, UINT32_MAX);
		}

		free(key_tuple);
	}

	// if it has any aggregate funtion, read all tuples and do the process_input and then produce_output, and set the output
	if(o->desc->params.hash_aggregation.aggregate_functions_count > 0)
	{
		{
			// open an iterator to read values of this entry
			binary_read_iterator* value_bri_p = read_value_in_rash_table_iterator(&(this_state->rti));

			int abort_error_dummy = 0;
			while(!(*error_code))
			{
				int finish = 0;
				consume_tuple_from_tuple_list(tuple, this_state->input_operator->desc->output_tuple_def, value_bri_p, NULL, &abort_error_dummy,
				{
					if(tuple != NULL)
					{
						// process the aggregate function calls for this tuple
						for(uint32_t i = 0; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++)
						{
							// generate input params to the i-th udaf
							for(uint32_t j = 0; j < o->desc->params.hash_aggregation.aggregate_functions[i]->input_type_infos_count; j++)
							{
								const data_type_info* dti_temp;
								if(!get_value_from_element_from_tuple(&(input_datums[j]), &dti_temp, this_state->input_operator->desc->output_tuple_def, o->desc->params.hash_aggregation.aggregate_input_element_ids[i][j], tuple))
									input_datums[j] = (*NULL_DATUM);
							}

							// process_input for the udaf, if it fails kill the operator
							if(!o->desc->params.hash_aggregation.aggregate_functions[i]->process_input(o->desc->params.hash_aggregation.aggregate_functions[i], &(states[i]), input_datums))
							{
								(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
								finish = 1;
								goto FAILED_EXIT;
							}
						}
					}
					else
					{
						finish = 1;
					}

					FAILED_EXIT:;
				});
				if(finish)
					break;
			}

			// after read all tuples close the read iterator in value
			delete_binary_read_iterator(value_bri_p, NULL, &abort_error_dummy);
		}

		if(*error_code)
		{
			free(output_tuple);
			for(uint32_t i = 0; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++)
				o->desc->params.hash_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.hash_aggregation.aggregate_functions[i], &(states[i]));
			free(states);
			free(input_datums);
			o->destroy_state(o);
			return NULL;
		}
		else
		{
			for(uint32_t i = 0, j = o->desc->params.hash_aggregation.key_element_count; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++, j++)
			{
				// produce the output_uval the output of the i-th aggregate function
				datum output_uval;
				if(!o->desc->params.hash_aggregation.aggregate_functions[i]->produce_output(o->desc->params.hash_aggregation.aggregate_functions[i], &output_uval, &(states[i])))
				{
					(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
					break;
				}

				set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(j), output_tuple, &output_uval, UINT32_MAX);
			}
		}
	}

	// iterate next in rti, and if we can not go next break out, this might just be the last entry
	if(!next_in_rash_table_iterator(&(this_state->rti)))
	{
		for(uint32_t i = 0; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++)
			o->desc->params.hash_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.hash_aggregation.aggregate_functions[i], &(states[i]));
		free(states);
		free(input_datums);
		o->destroy_state(o);
	}

	output_tuple = realloc(output_tuple, get_tuple_size(o->desc->output_tuple_def, output_tuple));
	return output_tuple;
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		this_state->input_operator->destroy_state(this_state->input_operator);

		destroy_rash_table(&(this_state->rth));

		if(this_state->has_active_rti)
			delete_rash_table_iterator(&(this_state->rti));

		free(this_state);
	}
	o->state = NULL;
}

void open_hash_aggregation_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.hash_aggregation.input_operator_index);
	this_state->rth = get_new_rash_table(INIT_BUCKET_COUNT, this_state->input_operator->desc->output_tuple_def, o->desc->params.hash_aggregation.key_element_ids, o->desc->params.hash_aggregation.key_element_count, o->self_query_plan->desc->curr_tx, o->self_query_plan->desc->curr_tx->rdb);
	this_state->has_active_rti = 0;
	this_state->max_input_datums_count = 0;
	for(uint32_t i = 0; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++)
		this_state->max_input_datums_count = max(this_state->max_input_datums_count, o->desc->params.hash_aggregation.aggregate_functions[i]->input_type_infos_count);

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}