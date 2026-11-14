#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

#include<rhendb/aggregate_functions.h>

#include<rhendb/function_compare.h>

typedef struct op_state op_state;
struct op_state
{
	operator* input_operator;

	void* first_tuple;

	datum* first_tuple_key_datums;
	datum* subsequent_tuple_key_datums;

	void** states;
	datum* input_datums; // enough datums to accomodate max inputs from any aggregate function 

	// derieved information
	const data_type_info** key_dtis;
};

static void revise_key_datums_from_tuple(datum* key_datums, const void* tuple, const tuple_def* tpl_d, const positional_accessor* key_element_ids, uint32_t key_element_count)
{
	for(uint32_t i = 0; i < key_element_count; i++)
	{
		const data_type_info* dti_temp;
		if(!get_value_from_element_from_tuple(&(key_datums[i]), &dti_temp, tpl_d, key_element_ids[i], tuple))
			key_datums[i] = (*NULL_DATUM);
	}
}

static void revise_key_datums_for_input_tuple(operator* o, datum* key_datums, const void* tuple)
{
	op_state* this_state = o->state;
	revise_key_datums_from_tuple(key_datums, tuple, this_state->input_operator->desc->output_tuple_def, o->desc->params.sorted_aggregation.key_element_ids, o->desc->params.sorted_aggregation.key_element_count);
}

static void revise_aggregate_input_datums_for_input_tuple(operator* o, datum* input_datums, const void* tuple, uint32_t agg_func_index)
{
	op_state* this_state = o->state;
	revise_key_datums_from_tuple(input_datums, tuple, this_state->input_operator->desc->output_tuple_def, o->desc->params.sorted_aggregation.aggregate_input_element_ids[agg_func_index], o->desc->params.sorted_aggregation.aggregate_functions[agg_func_index]->input_type_infos_count);
}

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	if(this_state->first_tuple == NULL) // if there has not been any first tuple, then grab one
	{
		void* tuple = this_state->input_operator->next_tuple(this_state->input_operator, error_code);
		if(tuple == NULL) // no more tuples nothing to produce, error_code propogated as is
		{
			o->destroy_state(o);
			return NULL;
		}
		else
		{
			this_state->first_tuple = tuple;
			revise_key_datums_for_input_tuple(o, this_state->first_tuple_key_datums, this_state->first_tuple);
		}
	}

	// process the first tuple of this group, giving it to the input of the aggregate functions
	for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
	{
		// generate input params to the i-th udaf
		revise_aggregate_input_datums_for_input_tuple(o, this_state->input_datums, this_state->first_tuple, i);

		// process_input for the udaf, if it fails kill the operator
		if(!o->desc->params.sorted_aggregation.aggregate_functions[i]->process_input(o->desc->params.sorted_aggregation.aggregate_functions[i], &(this_state->states[i]), this_state->input_datums))
		{
			for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
				o->desc->params.sorted_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.sorted_aggregation.aggregate_functions[i], &(this_state->states[i]));
			o->destroy_state(o);
			(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
			return NULL;
		}
	}

	void* next_first_tuple = NULL;
	while(1)
	{
		void* tuple = this_state->input_operator->next_tuple(this_state->input_operator, error_code);
		if(tuple == NULL)
		{
			if(*error_code)
			{
				for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
					o->desc->params.sorted_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.sorted_aggregation.aggregate_functions[i], &(this_state->states[i]));
				o->destroy_state(o);
				return NULL;
			}
			break;
		}
		else
		{
			revise_key_datums_for_input_tuple(o, this_state->subsequent_tuple_key_datums, tuple);
		}

		int same_group = (0 == compare_datums3_rhendb(this_state->subsequent_tuple_key_datums, this_state->first_tuple_key_datums, this_state->key_dtis, NULL, o->desc->params.sorted_aggregation.key_element_count, o->self_query_plan->desc->curr_tx));

		if(same_group) // process the tuple if it falls in the same group
		{
			for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
			{
				// generate input params to the i-th udaf
				revise_aggregate_input_datums_for_input_tuple(o, this_state->input_datums, tuple, i);

				// process_input for the udaf, if it fails kill the operator
				if(!o->desc->params.sorted_aggregation.aggregate_functions[i]->process_input(o->desc->params.sorted_aggregation.aggregate_functions[i], &(this_state->states[i]), this_state->input_datums))
				{
					free(tuple);
					for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
						o->desc->params.sorted_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.sorted_aggregation.aggregate_functions[i], &(this_state->states[i]));
					o->destroy_state(o);
					(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
					return NULL;
				}
			}

			free(tuple);
			continue;
		}
		else // if tuple belongs to the new group, this group is done
		{
			next_first_tuple = tuple;
			break;
		}
	}

	// produce output and set the next_first tuple

	void* output_tuple = malloc(get_maximum_tuple_size(o->desc->output_tuple_def));
	init_tuple(o->desc->output_tuple_def, output_tuple);

	// set keys in the output
	for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.key_element_count; i++)
	{
		const data_type_info* dti_temp;
		datum key_val;
		if(get_value_from_element_from_tuple(&key_val, &dti_temp, this_state->input_operator->desc->output_tuple_def, o->desc->params.sorted_aggregation.key_element_ids[i], this_state->first_tuple))
			set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(i), output_tuple, &key_val, UINT32_MAX);
	}

	// set values from aggregate function in output_tuple
	for(uint32_t i = 0, j = o->desc->params.sorted_aggregation.key_element_count; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++, j++)
	{
		// produce the output_uval the output of the i-th aggregate function
		datum output_uval;
		if(!o->desc->params.sorted_aggregation.aggregate_functions[i]->produce_output(o->desc->params.sorted_aggregation.aggregate_functions[i], &output_uval, &(this_state->states[i])))
		{
			free(output_tuple);
			if(next_first_tuple != NULL)
				free(next_first_tuple);
			for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
				o->desc->params.sorted_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.sorted_aggregation.aggregate_functions[i], &(this_state->states[i]));
			o->destroy_state(o);
			(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
			return NULL;
		}

		set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(j), output_tuple, &output_uval, UINT32_MAX);
	}

	// destroy states, we do not need them now
	for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
		o->desc->params.sorted_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.sorted_aggregation.aggregate_functions[i], &(this_state->states[i]));

	// reset the first_tuple to it's next value
	free(this_state->first_tuple);
	this_state->first_tuple = next_first_tuple;
	{
		void* temp = this_state->first_tuple_key_datums;
		this_state->first_tuple_key_datums = this_state->subsequent_tuple_key_datums;
		this_state->subsequent_tuple_key_datums = temp;
	}
	if(this_state->first_tuple == NULL) // if first_tuple becomes NULL, we have no more groups
		o->destroy_state(o);

	output_tuple = realloc(output_tuple, get_tuple_size(o->desc->output_tuple_def, output_tuple));
	return output_tuple;
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		this_state->input_operator->destroy_state(this_state->input_operator);

		if(this_state->first_tuple != NULL)
			free(this_state->first_tuple);

		if(this_state->first_tuple_key_datums != NULL)
			free(this_state->first_tuple_key_datums);

		if(this_state->subsequent_tuple_key_datums != NULL)
			free(this_state->subsequent_tuple_key_datums);

		if(this_state->states != NULL)
			free(this_state->states);

		if(this_state->input_datums != NULL)
			free(this_state->input_datums);

		free(this_state->key_dtis);

		free(this_state);
	}
	o->state = NULL;
}

void open_sorted_aggregation_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.sorted_aggregation.input_operator_index);
	this_state->first_tuple = NULL;

	this_state->first_tuple_key_datums = malloc(o->desc->params.sorted_aggregation.key_element_count * sizeof(datum));
	this_state->subsequent_tuple_key_datums = malloc(o->desc->params.sorted_aggregation.key_element_count * sizeof(datum));

	uint32_t max_input_datums_count = 0;
	for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
		max_input_datums_count = max(max_input_datums_count, o->desc->params.sorted_aggregation.aggregate_functions[i]->input_type_infos_count);

	this_state->states = calloc(sizeof(void*), o->desc->params.sorted_aggregation.aggregate_functions_count);
	this_state->input_datums = malloc(sizeof(datum) * max_input_datums_count);

	this_state->key_dtis = malloc(sizeof(data_type_info*) * o->desc->params.sorted_aggregation.key_element_count);
	for(uint32_t j = 0; j < o->desc->params.sorted_aggregation.key_element_count; j++)
		this_state->key_dtis[j] = get_type_info_for_element_from_tuple_def(this_state->input_operator->desc->output_tuple_def, o->desc->params.sorted_aggregation.key_element_ids[j]);

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}