#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/interim_tuple_store.h>

/*
	source operator like the scans, but it iterates over an interim_tuple_store, tuple by tuple and then produces each tuple from it
*/

typedef struct input_values input_values;
struct input_values
{
	interim_tuple_store* constant_dataset_p;
	const tuple_def* record_def;
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(curr_tuple, tuple_index, tuple_offset, (&(inputs->record_def->size_def)), (inputs->constant_dataset_p), (1024 * 1024), {
		int produced = produce_tuple_from_operator(o, curr_tuple);
		if(!produced)
		{
			kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
			break;
		}
	});

	kill_signal_for_self_operator(o, kill_reason); return ;
}

void clean_up_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->constant_dataset_p != NULL)
	{
		delete_interim_tuple_store(inputs->constant_dataset_p);
		inputs->constant_dataset_p = NULL;
	}
}

operator_resource_counter setup_constant_dataset_operator(operator* o, interim_tuple_store* constant_dataset_p, const tuple_def* record_def)
{
	operator_resource_counter result = {.thread_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	// this record's record_def, this constant_dataset_p is what we produce
	init_tuple_transformers(&(o->output_tuple_transformers), record_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.constant_dataset_p = constant_dataset_p,
		.record_def = record_def,
	};

	return result;
}