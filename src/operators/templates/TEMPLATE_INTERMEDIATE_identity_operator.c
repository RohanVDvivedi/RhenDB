#include<rhendb/query_plan_interface.h>

#include<rhendb/transaction.h>

/*
	TEMPLATE FOR INTERMEDIATE OPERATORS (sorting(ordering), joins(hash_joins), aggregations(groupby->aggregates))
*/

typedef struct input_values input_values;
struct input_values
{
	operator* input_operator;
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
		interim_tuple_store* its_p = consume_from_operator(inputs->input_operator, inputs->consume_only_after_bytes_count, &no_more_data);
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
			run_concurrent_job_for_operator(o, "Hello world, from TEMPLATE identity operator, just received data", print_job);

			int produced = produce_tuples_from_operator(o, its_p);
			if(!produced)
			{
				kill_reason = get_dstring_pointing_to_literal_cstring("pushed_failed_from_identity_oerator_and_so_killed");
				delete_interim_tuple_store(its_p);
				mark_operator_self_killed(o, kill_reason); return ;
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

	o->output_tuple_def = NULL; // not necessary to know this

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_operator = input_operator,
		.consume_only_after_bytes_count = consume_only_after_bytes_count,
	};

	input_operator->consumer_operator = o;
	input_operator->consumer_trigger_on_bytes_accumulated = consume_only_after_bytes_count;
}