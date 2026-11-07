#include<rhendb/query_plan.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* input_iterator;

	const tuple_def* output_tuple_def;
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data)
		{
			kill_signal_for_self_operator(o, OPERATOR_SUCCESS_REASON);
			return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
			return ;
		}

		if(tuple != NULL)
		{
			int produced = produce_tuple_from_operator(o, (void*)tuple);
			if(!produced)
			{
				kill_signal_for_self_operator(o, OPERATOR_COULD_NOT_PRODUCE_REASON);
				return ;
			}
		}
		else
			break;
	}

	return ;
}

static void clean_up_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->input_iterator != NULL)
	{
		destroy_consumption_iterator(inputs->input_iterator);
		inputs->input_iterator = NULL;
	}
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->output_tuple_def != NULL)
	{
		free((void*)(inputs->output_tuple_def->type_info));
		free((void*)(inputs->output_tuple_def));
		inputs->output_tuple_def = NULL;
	}

	free(inputs);
}

void setup_row_rename_operator(operator* o, operator* input_operator, const char* output_name, const char** column_names, uint64_t columns_count)
{
	const tuple_def* input_tuple_def = input_operator->output_tuple_def;

	if(input_tuple_def->type_info->type != TUPLE)
	{
		printf("input must be a tuple for row_rename_operator\n");
		exit(-1);
	}

	if(columns_count > input_tuple_def->type_info->element_count || columns_count == 0)
	{
		printf("can not have more or 0 renames than input tuple columns for row_rename_operator\n");
		exit(-1);
	}

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = free_resources;

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(input_tuple_def->type_info->element_count));
	for(uint32_t i = 0; i < input_tuple_def->type_info->element_count; i++)
	{
		if(i < columns_count && column_names[i] != NULL)
			sprintf(output_dti->containees[i].field_name, column_names[i], i);
		else
			sprintf(output_dti->containees[i].field_name, input_tuple_def->type_info->containees[i].field_name, i);

		output_dti->containees[i].al.type_info = input_tuple_def->type_info->containees[i].al.type_info;
	}
	initialize_tuple_data_type_info(output_dti, (output_name == NULL) ? input_tuple_def->type_info->type_name : output_name, 0, get_maximum_tuple_size(input_tuple_def), input_tuple_def->type_info->element_count);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	o->output_tuple_def = output_tuple_def;

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.output_tuple_def = output_tuple_def,
	};
}