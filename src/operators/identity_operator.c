#include<rhendb/query_plan_interface.h>

#include<boompar/executor.h>

typedef struct input_values input_values;
struct input_values
{
	operator_buffer* output;
	operator_buffer* input;
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	temp_tuple_store* tts = NULL;

	while(1)
	{
		if(is_kill_signal_sent(o))
			goto EXIT;

		int no_more_data = 0;
		tts = pop_from_operator_buffer(inputs->input, 0, 1000000, &no_more_data);

		if(is_kill_signal_sent(o))
			goto EXIT;

		if(no_more_data)
			goto EXIT;

		if(tts != NULL)
		{
			int pushed = push_to_operator_buffer(inputs->output, o, tts);

			if(is_kill_signal_sent(o))
				goto EXIT;

			if(!pushed)
			{
				kill_reason = get_dstring_pointing_to_literal_cstring("pushed_failed_from_identity_oerator_and_so_killed");
				goto EXIT;
			}

			tts = NULL;
		}
	}

	EXIT:
	if(tts != NULL)
	{
		delete_temp_tuple_store(tts);
		tts = NULL;
	}

	decrement_operator_buffer_consumers_count(inputs->input, 1);
	decrement_operator_buffer_producers_count(inputs->output, 1);

	mark_operator_self_killed(o, kill_reason);
}

void setup_identity_operator(operator* o, operator_buffer* output, operator_buffer* input)
{
	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.output = output,
		.input = input,
	};
}