#include<rhendb/query_plan_interface.h>

#include<tuplestore/tuple.h>

#include<stdlib.h>

/*
	TEMPLATE FOR SINK OPERATORS (writers to file descriptors)
*/

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* input_iterator;
	const tuple_def* input_tuple_def;

	int do_print;
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
			kill_signal_for_self_operator(o, kill_reason); return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		if(tuple != NULL)
		{
			if(inputs->do_print)
				print_tuple(tuple, inputs->input_tuple_def);
		}
		else
			break;
	}

	return ;
}

void setup_printf_operator(operator* o, operator* input_operator, int do_print)
{
	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_iterator = create_consumption_iterator(input_operator, o, NULL),
		.input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator),
		.do_print = do_print,
	};
}