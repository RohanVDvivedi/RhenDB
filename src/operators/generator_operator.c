#include<rhendb/query_plan_interface.h>

#include<boompar/executor.h>

typedef struct input_values input_values;
struct input_values
{
	void* (*generator)(void* generator_context, tuple_def* generator_tuple_def);
	void* generator_context;
	tuple_def* generator_tuple_def;
};

static void trigger_execution(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	while(1)
	{
		if(can_not_proceed_for_execution_operator(o))
			break;

		void* curr_tuple = inputs->generator(inputs->generator_context, inputs->generator_tuple_def);
		if(curr_tuple == NULL)
			break;

		int produced = produce_tuple_from_operator(o, curr_tuple);
		if(!produced)
		{
			kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
			free(curr_tuple);
			break;
		}

		free(curr_tuple);
	}

	mark_operator_self_killed(o, kill_reason);
}

void setup_generator_operator(operator* o, void* (*generator)(void* generator_context, tuple_def* generator_tuple_def), void* generator_context, tuple_def* generator_tuple_def)
{
	o->trigger_execution = trigger_execution;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	o->output_tuple_def = generator_tuple_def;

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.generator = generator,
		.generator_context = generator_context,
		.generator_tuple_def = generator_tuple_def,
	};
}