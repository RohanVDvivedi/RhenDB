#include<rhendb/query_plan_interface.h>

#include<boompar/executor.h>

typedef struct input_values input_values;
struct input_values
{
	operator_buffer* output;
	void* (*generator)(void* generator_context, tuple_def* generator_tuple_def);
	void* generator_context;
	tuple_def* generator_tuple_def;
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

		tts = get_new_temp_tuple_store(".");
		tuple_region tr = INIT_TUPLE_REGION;
		while(tts->next_tuple_offset < 128)
		{
			void* curr_tuple = inputs->generator(inputs->generator_context, inputs->generator_tuple_def);
			if(curr_tuple == NULL)
				break;
			mmap_for_writing_tuple(tts, &tr, &(inputs->generator_tuple_def->size_def), get_tuple_size(inputs->generator_tuple_def, curr_tuple));
			memcpy(tr.tuple, curr_tuple, get_tuple_size(inputs->generator_tuple_def, curr_tuple));
			finalize_written_tuple(tts, &tr);
			free(curr_tuple);
		}
		unmap_for_tuple_region(&tr);

		if(is_kill_signal_sent(o))
			goto EXIT;

		// if no tuples were generated, we exit
		if(tts->tuples_count == 0)
			goto EXIT;

		if(tts != NULL)
		{
			int pushed = push_to_operator_buffer(inputs->output, o, tts);

			if(is_kill_signal_sent(o))
				goto EXIT;

			if(!pushed)
			{
				kill_reason = get_dstring_pointing_to_literal_cstring("pushed_failed_from_generator_oerator_and_so_killed");
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

	decrement_operator_buffer_producers_count(inputs->output, 1);

	mark_operator_self_killed(o, kill_reason);
}

void setup_generator_operator(operator* o, operator_buffer* output, void* (*generator)(void* generator_context, tuple_def* generator_tuple_def), void* generator_context, tuple_def* generator_tuple_def)
{
	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.output = output,
		.generator = generator,
		.generator_context = generator_context,
		.generator_tuple_def = generator_tuple_def,
	};
}