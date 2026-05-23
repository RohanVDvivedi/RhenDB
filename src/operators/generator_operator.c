#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

/*
	source operator like the scans, but it uses generator() to generate tuple by tuple (each malloc-ed) and produce it
*/

typedef struct input_values input_values;
struct input_values
{
	void* (*generator)(void* generator_context, const tuple_def* generator_tuple_def);
	void* generator_context;
	const tuple_def* generator_tuple_def;
};

static void execute(operator* o)
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
		free(curr_tuple);
		if(!produced)
		{
			kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
			break;
		}
	}

	kill_signal_for_self_operator(o, kill_reason); return ;
}

operator_resource_counter setup_generator_operator(operator* o, void* (*generator)(void* generator_context, const tuple_def* generator_tuple_def), void* generator_context, const tuple_def* generator_tuple_def)
{
	operator_resource_counter result = {.thread_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	// generator's output is what we produce
	init_tuple_transformers(&(o->output_tuple_transformers), generator_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.generator = generator,
		.generator_context = generator_context,
		.generator_tuple_def = generator_tuple_def,
	};

	return result;
}