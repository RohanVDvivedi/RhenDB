#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/tuples_down_counter.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* input_iterator;

	tuples_down_counter offset_counter;

	tuples_down_counter limit_counter;
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
			destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

			kill_signal_for_self_operator(o, kill_reason); return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		if(tuple != NULL)
		{
			// make sure that offse counter reaches 0 first, before e could produce any thing
			if(can_decrement_tuples_down_counter(&(inputs->offset_counter)))
			{
				decrement_tuples_down_counter(&(inputs->offset_counter));
			}
			else
			{
				// if we could decrement limit counter, decrement it and produce output
				if(can_decrement_tuples_down_counter(&(inputs->limit_counter)))
				{
					decrement_tuples_down_counter(&(inputs->limit_counter));

					int produced = produce_tuple_from_operator(o, (void*)tuple);
					if(!produced)
					{
						destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

						kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
						kill_signal_for_self_operator(o, kill_reason); return ;
					}
				}

				// after every produce if this counter reached 0, then kill your self
				if(is_zero_tuples_down_counter(&(inputs->limit_counter)))
				{
					destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

					kill_signal_for_self_operator(o, kill_reason); return ;
				}
			}
		}
		else
			break;
	}

	return ;
}

operator_resource_counter setup_offset_limit_operator(operator* o, operator* input_operator, tuples_down_counter offset_counter, tuples_down_counter limit_counter)
{
	if(is_inf_tuples_down_counter(&offset_counter))
	{
		printf("offset can not be infinity for offset_limit_operator\n");
		exit(-1);
	}
	else if(is_zero_tuples_down_counter(&limit_counter))
	{
		printf("limit can not be 0 for offset_limit_operator\n");
		exit(-1);
	}

	operator_resource_counter result = {.job_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	// it is an identity operator, produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), get_tuple_def_for_tuples_to_be_consumed_from(input_operator));

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.offset_counter = offset_counter,
		.limit_counter = limit_counter,
	};

	return result;
}