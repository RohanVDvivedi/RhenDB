#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<stdlib.h>

/*
	identity operator, primarily used for adding tuple_transformers to its produce
*/

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* input_iterator;
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
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
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
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
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

operator_resource_counter setup_identity_operator(operator* o, operator* input_operator)
{
	operator_resource_counter result = {.job_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	// it is an identity operator, produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), get_tuple_def_for_tuples_to_be_consumed_from(input_operator));

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
	};

	return result;
}