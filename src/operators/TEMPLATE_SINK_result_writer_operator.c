#include<rhendb/query_plan_interface.h>

#include<rhendb/transaction.h>

#include<tuplestore/tuple.h>

#include<boompar/executor.h>
#include<cutlery/stream.h>

/*
	TEMPLATE FOR SINK OPERATORS (writers to file descriptors)
*/

typedef struct input_values input_values;
struct input_values
{
	operator* input_operator;
	tuple_def* input_tuple_def;
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	while(1)
	{
		int no_more_data = 0;
		interim_tuple_store* its_p = consume_from_operator(inputs->input_operator, 300, &no_more_data);
		if(no_more_data)
		{
			if(is_killed_operator(inputs->input_operator))
			{
				mark_operator_self_killed(o, kill_reason); return ;
			}
			else
				break;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			mark_operator_self_killed(o, kill_reason); return ;
		}

		if(its_p != NULL)
		{
			printf("\n\nprinting interim_tuple_store with %"PRIu64" tuples, and filled upto %"PRIu64"/%"PRIu64"\n\n", its_p->tuples_count, its_p->next_tuple_offset, its_p->total_size);

			FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(tuple, tuple_index, tuple_offset, &(inputs->input_tuple_def->size_def), its_p, 0, {
				printf("tuple_index = %"PRIu64", tuple_offset = %"PRIu64", tuple_size = %"PRIu32"\n", tuple_index, tuple_offset, get_tuple_size(inputs->input_tuple_def, tuple));
				print_tuple(tuple, inputs->input_tuple_def);
				printf("\n\n");
			});

			printf("\n\n");

			delete_interim_tuple_store(its_p);
			its_p = NULL;
		}
		else
			break;
	}

	return ;
}

void setup_printf_operator(operator* o, operator* input_operator, tuple_def* input_tuple_def)
{
	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_operator = input_operator,
		.input_tuple_def = input_tuple_def,
	};

	input_operator->consumer_operator = o;
	input_operator->consumer_trigger_on_bytes_accumulated = 300;
}