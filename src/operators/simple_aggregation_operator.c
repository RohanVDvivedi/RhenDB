#include<rhendb/query_plan.h>

#include<rhendb/aggregate_functions.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	// input_tuple_def for the aggregate functions
	const tuple_def* input_tuple_def;

	consumption_iterator* input_iterator;

	uint32_t aggregate_functions_count;
	aggregate_function** aggregate_functions;

	// array of void pointers, of size aggregate_functions_count, one for each aggregate functions
	void** states;

	// array of datums of size = max(aggregate_functions->input_type_infos_count)
	datum* input_datums;

	// 2D array of positonal_accessors
	// aggregate_input_element_ids[i][j]; -> gives the position of the j-th input_param of the i-th aggregate functions
	// its size is [aggregate_functions_count] and [i][aggregate_functions[i]->input_type_infos_count]
	const positional_accessor** aggregate_input_element_ids;

	// consists of all the output_type_infos of all aggregate_functions
	const tuple_def* output_tuple_def;
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data) // if no_more_data, produce the final output
		{
			// destroy the input_iterator we will not be needing it
			destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

			// produce output tuple and return it
			{
				// generate the smallest possible tuple
				uint32_t tuple_size = get_minimum_tuple_size(inputs->output_tuple_def);
				uint32_t tuple_capacity = tuple_size;
				void* output_tuple = malloc(tuple_capacity);
				init_tuple(inputs->output_tuple_def, output_tuple);

				for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
				{
					// produce the output_uval the output of the i-th aggregate function
					datum output_uval;
					if(!inputs->aggregate_functions[i]->produce_output(inputs->aggregate_functions[i], &output_uval, &(inputs->states[i])))
					{
						free(output_tuple);
						kill_reason = get_dstring_pointing_to_literal_cstring("produce_output_of_udaf_failed");
						kill_signal_for_self_operator(o, kill_reason); return ;
					}

					// ensure there are enopugh bytes in the output_tuple
					while(!can_set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(i), output_tuple, &output_uval, tuple_capacity - tuple_size))
					{
						tuple_capacity = min(tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
						output_tuple = realloc(output_tuple, tuple_capacity);
					}

					// set output_uval in output_tuple
					set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(i), output_tuple, &output_uval, tuple_capacity - tuple_size);

					// recompute tuple_size
					tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);
				}

				// produce output_tuple
				int produced = produce_tuple_from_operator(o, output_tuple);
				free(output_tuple);
				if(!produced)
				{
					kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
					kill_signal_for_self_operator(o, kill_reason); return ;
				}
			}

			// destroy aggregate states, right before we quit
			{
				for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
					inputs->aggregate_functions[i]->destroy_state(inputs->aggregate_functions[i], &(inputs->states[i]));
			}

			kill_signal_for_self_operator(o, kill_reason); return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		if(tuple != NULL)
		{
			// iterate for process_input of each udaf
			for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
			{
				// generate input params to the i-th udaf
				for(uint32_t j = 0; j < inputs->aggregate_functions[i]->input_type_infos_count; j++)
				{
					if(!get_value_from_element_from_tuple(&(inputs->input_datums[j]), inputs->input_tuple_def, inputs->aggregate_input_element_ids[i][j], tuple))
						inputs->input_datums[j] = (*NULL_DATUM);
				}

				// process_input for the udaf, if it fails kill the operator
				if(!inputs->aggregate_functions[i]->process_input(inputs->aggregate_functions[i], &(inputs->states[i]), inputs->input_datums))
				{
					kill_reason = get_dstring_pointing_to_literal_cstring("process_input_of_udaf_failed");
					kill_signal_for_self_operator(o, kill_reason); return ;
				}
			}
		}
		else
			break;
	}

	return ;
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
		inputs->aggregate_functions[i]->destroy_state(inputs->aggregate_functions[i], &(inputs->states[i]));

	free(inputs->states);

	for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
		inputs->aggregate_functions[i]->destroy_aggregate_function(inputs->aggregate_functions[i]);

	free(inputs->aggregate_functions);

	free(inputs->input_datums);

	free(inputs->aggregate_input_element_ids);

	free((data_type_info*)(inputs->output_tuple_def->type_info));
	free((tuple_def*)(inputs->output_tuple_def));

	free(inputs);
}

void setup_simple_aggregation_operator(operator* o, operator* input_operator, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids)
{
	if(aggregate_functions_count == 0)
	{
		printf("aggregate_functions_count can not be 0 for simple aggregation operator\n");
		exit(-1);
	}

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = free_resources;

	uint32_t input_datums_count = 0;
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(aggregate_functions_count));
	uint32_t max_output_tuple_size = 0;

	for(uint32_t i = 0; i < aggregate_functions_count; i++)
	{
		input_datums_count = max(input_datums_count, aggregate_functions[i]->input_type_infos_count);

		if(aggregate_functions[i]->output_type_info->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += aggregate_functions[i]->output_type_info->is_variable_sized ? (8 + aggregate_functions[i]->output_type_info->max_size) : (1 + aggregate_functions[i]->output_type_info->size);

		sprintf(output_dti->containees[i].field_name, "agg_%u", i);
		output_dti->containees[i].al.type_info = (data_type_info*) aggregate_functions[i]->output_type_info;
	}

	initialize_tuple_data_type_info(output_dti, "aggregates", 0, max_output_tuple_size, aggregate_functions_count);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	// it is an identity operator, produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	input_values* inputs = o->inputs;
	*inputs = (input_values){
		.input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator),
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.aggregate_functions_count = aggregate_functions_count,
		.aggregate_functions = malloc(sizeof(aggregate_function*) * aggregate_functions_count),
		.states = calloc(sizeof(void*), aggregate_functions_count),
		.input_datums = malloc(sizeof(datum) * input_datums_count),
		.aggregate_input_element_ids = malloc(sizeof(aggregate_function*) * aggregate_functions_count),
		.output_tuple_def = output_tuple_def,
	};

	memory_move(inputs->aggregate_functions, aggregate_functions, sizeof(aggregate_function*) * aggregate_functions_count);

	memory_move(inputs->aggregate_input_element_ids, aggregate_input_element_ids, sizeof(aggregate_function*) * aggregate_functions_count);
}