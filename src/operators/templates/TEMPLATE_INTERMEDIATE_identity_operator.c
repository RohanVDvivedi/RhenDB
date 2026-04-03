#include<rhendb/query_plan_interface.h>

#include<stdlib.h>

/*
	TEMPLATE FOR INTERMEDIATE OPERATORS (sorting(ordering), joins(hash_joins), aggregations(groupby->aggregates))
*/

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* input_iterator;
	uint64_t consume_only_after_bytes_count;
};

void print_job(operator* o, void* param);

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
			int produced = produce_tuple_from_operator(o, (void*)tuple);
			if(!produced)
			{
				kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
				kill_signal_for_self_operator(o, kill_reason); return ;
			}
		}
		else
			break;
	}

	return ;
}

void setup_identity_operator(operator* o, operator* input_operator, uint64_t consume_only_after_bytes_count)
{
	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	// it is an identity operator, produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), get_tuple_def_for_tuples_to_be_consumed_from(input_operator));

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_iterator = create_consumption_iterator(input_operator, o, NULL),
		.consume_only_after_bytes_count = consume_only_after_bytes_count,
	};
}