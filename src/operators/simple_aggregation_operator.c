#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	operator* input_operator = o->state;

	void** states = calloc(sizeof(void*), o->desc->params.simple_aggregation.aggregate_functions_count);

	uint32_t max_input_datums_count = 0;
	for(uint32_t i = 0; i < o->desc->params.simple_aggregation.aggregate_functions_count; i++)
		max_input_datums_count = max(max_input_datums_count, o->desc->params.simple_aggregation.aggregate_functions[i]->input_type_infos_count);
	datum* input_datums = malloc(sizeof(datum) * max_input_datums_count);

	while(1)
	{
		void* tuple = input_operator->next_tuple(input_operator, error_code);
		if(tuple == NULL)
		{
			if(*error_code)
			{
				for(uint32_t i = 0; i < o->desc->params.simple_aggregation.aggregate_functions_count; i++)
					o->desc->params.simple_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.simple_aggregation.aggregate_functions[i], &(states[i]));
				free(states);
				free(input_datums);
				o->destroy_state(o);
				return NULL;
			}
			else // the task of aggregation is completed
				break;
		}

		// aggregate tuple
		for(uint32_t i = 0; i < o->desc->params.simple_aggregation.aggregate_functions_count; i++)
		{
			// generate input params to the i-th udaf
			for(uint32_t j = 0; j < o->desc->params.simple_aggregation.aggregate_functions[i]->input_type_infos_count; j++)
			{
				const data_type_info* dti_temp;
				if(!get_value_from_element_from_tuple(&(input_datums[j]), &dti_temp, input_operator->desc->output_tuple_def, o->desc->params.simple_aggregation.aggregate_input_element_ids[i][j], tuple))
					input_datums[j] = (*NULL_DATUM);
			}

			// process_input for the udaf, if it fails kill the operator
			if(!o->desc->params.simple_aggregation.aggregate_functions[i]->process_input(o->desc->params.simple_aggregation.aggregate_functions[i], &(states[i]), input_datums))
			{
				free(tuple);
				for(uint32_t i = 0; i < o->desc->params.simple_aggregation.aggregate_functions_count; i++)
					o->desc->params.simple_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.simple_aggregation.aggregate_functions[i], &(states[i]));
				free(states);
				free(input_datums);
				o->destroy_state(o);
				(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
				return NULL;
			}
		}

		free(tuple);
	}

	void* output_tuple = malloc(get_maximum_tuple_size(o->desc->output_tuple_def));
	init_tuple(o->desc->output_tuple_def, output_tuple);

	for(uint32_t i = 0; i < o->desc->params.simple_aggregation.aggregate_functions_count; i++)
	{
		datum output_uval;
		if(!o->desc->params.simple_aggregation.aggregate_functions[i]->produce_output(o->desc->params.simple_aggregation.aggregate_functions[i], &output_uval, &(states[i])))
		{
			free(output_tuple);
			for(uint32_t i = 0; i < o->desc->params.simple_aggregation.aggregate_functions_count; i++)
				o->desc->params.simple_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.simple_aggregation.aggregate_functions[i], &(states[i]));
			free(states);
			free(input_datums);
			o->destroy_state(o);
			(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
			return NULL;
		}

		set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(i), output_tuple, &output_uval, UINT32_MAX);
	}

	for(uint32_t i = 0; i < o->desc->params.simple_aggregation.aggregate_functions_count; i++)
		o->desc->params.simple_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.simple_aggregation.aggregate_functions[i], &(states[i]));
	free(states);
	free(input_datums);
	o->destroy_state(o);

	output_tuple = realloc(output_tuple, get_tuple_size(o->desc->output_tuple_def, output_tuple));
	return output_tuple;
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		operator* input_operator = o->state;

		input_operator->destroy_state(input_operator);
	}
	o->state = NULL;
}

void open_simple_aggregation_operator(operator* o)
{
	o->state = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.simple_aggregation.input_operator_index);
	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}