#include<rhendb/query_plan_interface.h>

#include<tuplestore/tuple.h>

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
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	temp_tuple_store* tts = NULL;

	while(1)
	{
		int no_more_data = 0;
		tts = pop_from_operator_buffer(inputs->input, o, 100000, &no_more_data);

		if(no_more_data)
			goto EXIT;

		if(tts != NULL)
		{
			printf("\n\nprinting temp_tuple_store with %"PRIu64" tuples, and filled upto %"PRIu64"/%"PRIu64"\n\n", tts->tuples_count, tts->next_tuple_offset, tts->total_size);
			uint64_t index = 0;
			uint64_t offset = 0;
			tuple_region tr = INIT_TUPLE_REGION;
			while(mmap_for_reading_tuple(tts, &tr, offset, &(inputs->input_tuple_def->size_def)))
			{
				printf("tuple_index = %"PRIu64", tuple_offset = %"PRIu64", tuple_size = %"PRIu32"\n", index, offset, curr_tuple_size_for_tuple_region(&tr));
				print_tuple(tr.tuple, inputs->input_tuple_def);
				printf("\n\n");
				offset = next_tuple_offset_for_tuple_region(&tr);
				index++;
			}
			unmap_for_tuple_region(&tr);
			printf("\n\n");

			delete_temp_tuple_store(tts);
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

	mark_operator_self_killed(o, kill_reason);
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