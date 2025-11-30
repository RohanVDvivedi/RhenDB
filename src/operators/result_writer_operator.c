#include<rhendb/query_plan_interface.h>

#include<boompar/executor.h>
#include<cutlery/stream.h>

typedef struct input_values input_values;
struct input_values
{
	operator_buffer* input;
	tuple_def* input_tuple_def;
};

static void execute(operator* o)
{
	// TODO
}

void setup_printf_operator(operator* o, operator_buffer* input, tuple_def* input_tuple_def)
{
	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input = input,
		.input_tuple_def = input_tuple_def,
	};
}