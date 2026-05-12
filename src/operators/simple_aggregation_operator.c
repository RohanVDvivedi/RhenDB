#include<rhendb/query_plan.h>

#include<rhendb/aggregate_functions.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* input_iterator;

	uint32_t aggregate_functions_count;
	aggregate_function** aggregate_functions;

	// array of void pointers, of size aggregate_functions_count, one for each aggregate functions
	void** states;

	// array of datums of size = max(aggregate_functions->input_type_infos_count)
	datum* input_datums;

	// consists of all the output_type_infos of all aggregate_functions
	tuple_def* output_tuple_def;
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data)
		{
			destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

			kill_signal_for_self_operator(o, kill_reason); return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		if(tuple != NULL)
		{
			int produced = produce_tuple_from_operator(o, (void*)tuple);
			if(!produced)
			{
				destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

				kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
				kill_signal_for_self_operator(o, kill_reason); return ;
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
		inputs->aggregate_functions[i]->destroy_state(inputs->aggregate_functions[i], inputs->states[i]);

	free(inputs->states);

	for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
		inputs->aggregate_functions[i]->destroy_aggregate_function(inputs->aggregate_functions[i]);

	free(inputs->aggregate_functions);

	free(inputs->input_datums);

	free(inputs->output_tuple_def->type_info);
	free(inputs->output_tuple_def);

	free(inputs);
}

void setup_simple_aggregation_operator(operator* o, operator* input_operator, uint32_t aggregate_functions_count, aggregate_function** aggregate_functions)
{
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
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.aggregate_functions_count = aggregate_functions_count,
		.aggregate_functions = malloc(sizeof(aggregate_function*) * aggregate_functions_count),
		.states = calloc(sizeof(void*), aggregate_functions_count),
		.input_datums = malloc(sizeof(datum) * input_datums_count),
		.output_tuple_def = output_tuple_def,
	};

	memory_move(inputs->aggregate_functions, aggregate_functions, sizeof(aggregate_function*) * aggregate_functions_count);
}