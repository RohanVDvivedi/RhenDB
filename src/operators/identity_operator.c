#include<rhendb/query_plan_interface.h>

#include<rhendb/transaction.h>

#include<boompar/executor.h>

typedef struct input_values input_values;
struct input_values
{
	operator* input_operator;
};

static void* execute(void* o_vp)
{
	operator* o = o_vp;
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
				mark_operator_self_killed(o, kill_reason);
				return NULL;
			}
			else
				break;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			mark_operator_self_killed(o, kill_reason);
			return NULL;
		}

		if(its_p != NULL)
		{
			int produced = produce_tuples_from_operator(o, its_p);
			if(!produced)
			{
				kill_reason = get_dstring_pointing_to_literal_cstring("pushed_failed_from_identity_oerator_and_so_killed");
				delete_interim_tuple_store(its_p);
				mark_operator_self_killed(o, kill_reason);
				return NULL;
			}
		}
		else
			break;
	}

	return NULL;
}

static void trigger_execution(operator* o)
{
	if(!submit_job_executor(o->self_query_plan->curr_tx->db->operator_thread_pool, (void* (*)(void*))(execute), o, NULL, NULL, BLOCKING))
	{
		exit(-1);
	}
}

void setup_identity_operator(operator* o, operator* input_operator)
{
	o->trigger_execution = trigger_execution;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	o->output_tuple_def = NULL; // not necessary to know this

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_operator = input_operator,
	};

	input_operator->consumer_operator = o;
	input_operator->consumer_trigger_on_bytes_accumulated = 300;
}