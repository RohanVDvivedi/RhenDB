#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

#include<rhendb/aggregate_functions.h>

#include<rhendb/function_compare.h>

typedef struct op_state op_state;
struct op_state
{
	operator* input_operator;

	void* first_tuple;

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

	if(this_state->first_tuple == NULL) // if there has not been any first tuple, then grab one
	{
		void* tuple = this_state->input_operator->next_tuple(this_state->input_operator, error_code);
		if(tuple == NULL) // no more tuples nothing to produce, error_code propogated as is
		{
			o->destroy_state(o);
			return NULL;
		}
		this_state->first_tuple = tuple;
	}

	// now we have the first tuple, get memory for states and input_datums
	void** states = calloc(sizeof(void*), o->desc->params.sorted_aggregation.aggregate_functions_count);
	datum* input_datums = malloc(sizeof(datum) * this_state->max_input_datums_count);

	// process the first tuple of this group, giving it to the input of the aggregate functions
	for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
	{
		// generate input params to the i-th udaf
		for(uint32_t j = 0; j < o->desc->params.sorted_aggregation.aggregate_functions[i]->input_type_infos_count; j++)
		{
			const data_type_info* dti_temp;
			if(!get_value_from_element_from_tuple(&(input_datums[j]), &dti_temp, this_state->input_operator->desc->output_tuple_def, o->desc->params.sorted_aggregation.aggregate_input_element_ids[i][j], this_state->first_tuple))
				input_datums[j] = (*NULL_DATUM);
		}

		// process_input for the udaf, if it fails kill the operator
		if(!o->desc->params.sorted_aggregation.aggregate_functions[i]->process_input(o->desc->params.sorted_aggregation.aggregate_functions[i], &(states[i]), input_datums))
		{
			for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
				o->desc->params.sorted_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.sorted_aggregation.aggregate_functions[i], &(states[i]));
			free(states);
			free(input_datums);
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
					o->desc->params.sorted_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.sorted_aggregation.aggregate_functions[i], &(states[i]));
				free(states);
				free(input_datums);
				o->destroy_state(o);
				return NULL;
			}
			break;
		}

		int same_group = (0 == compare_tuples_rhendb(tuple, this_state->input_operator->desc->output_tuple_def, o->desc->params.sorted_aggregation.key_element_ids, this_state->first_tuple, this_state->input_operator->desc->output_tuple_def, o->desc->params.sorted_aggregation.key_element_ids, NULL, o->desc->params.sorted_aggregation.key_element_count, o->self_query_plan->desc->curr_tx));

		if(same_group) // process the tuple if it falls in the same group
		{
			for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
			{
				// generate input params to the i-th udaf
				for(uint32_t j = 0; j < o->desc->params.sorted_aggregation.aggregate_functions[i]->input_type_infos_count; j++)
				{
					const data_type_info* dti_temp;
					if(!get_value_from_element_from_tuple(&(input_datums[j]), &dti_temp, this_state->input_operator->desc->output_tuple_def, o->desc->params.sorted_aggregation.aggregate_input_element_ids[i][j], tuple))
						input_datums[j] = (*NULL_DATUM);
				}

				// process_input for the udaf, if it fails kill the operator
				if(!o->desc->params.sorted_aggregation.aggregate_functions[i]->process_input(o->desc->params.sorted_aggregation.aggregate_functions[i], &(states[i]), input_datums))
				{
					free(tuple);
					for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
						o->desc->params.sorted_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.sorted_aggregation.aggregate_functions[i], &(states[i]));
					free(states);
					free(input_datums);
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

	// we will not need input datums any more
	free(input_datums);

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
		if(!o->desc->params.sorted_aggregation.aggregate_functions[i]->produce_output(o->desc->params.sorted_aggregation.aggregate_functions[i], &output_uval, &(states[i])))
		{
			free(output_tuple);
			if(next_first_tuple != NULL)
				free(next_first_tuple);
			for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
				o->desc->params.sorted_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.sorted_aggregation.aggregate_functions[i], &(states[i]));
			free(states);
			o->destroy_state(o);
			(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
			return NULL;
		}

		set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(j), output_tuple, &output_uval, UINT32_MAX);
	}

	// destroy states, we do not need them noe
	for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
		o->desc->params.sorted_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.sorted_aggregation.aggregate_functions[i], &(states[i]));
	free(states);

	// reset the first_tuple to it's next value
	free(this_state->first_tuple);
	this_state->first_tuple = next_first_tuple;
	if(this_state->first_tuple == NULL) // if first_tuple becomes NULL, we have no more groups
		o->destroy_state(o);

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
	this_state->max_input_datums_count = 0;
	for(uint32_t i = 0; i < o->desc->params.sorted_aggregation.aggregate_functions_count; i++)
		this_state->max_input_datums_count = max(this_state->max_input_datums_count, o->desc->params.sorted_aggregation.aggregate_functions[i]->input_type_infos_count);

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}