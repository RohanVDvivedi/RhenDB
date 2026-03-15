#include<rhendb/query_plan_interface.h>

#include<rhendb/transaction.h>

#include<tuplestore/tuple.h>

#include<boompar/executor.h>
#include<cutlery/stream.h>

typedef struct input_values input_values;
struct input_values
{
	operator* input_operator;
	tuple_def* input_tuple_def;
};

static void* execute(void* o_vp)
{
	operator* o = o_vp;
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	int no_more_data = 0;

	interim_tuple_store* its_p = consume_from_operator(inputs->input_operator, 300, &no_more_data);
	if(no_more_data)
		mark_operator_self_killed(o, kill_reason);
	if(can_not_proceed_for_execution_operator(o))
		mark_operator_self_killed(o, kill_reason);

	if(its_p != NULL)
	{
		printf("\n\nprinting interim_tuple_store with %"PRIu64" tuples, and filled upto %"PRIu64"/%"PRIu64"\n\n", its_p->tuples_count, its_p->next_tuple_offset, its_p->total_size);
		uint64_t index = 0;
		uint64_t offset = 0;
		interim_tuple_region tr = INIT_INTERIM_TUPLE_REGION;
		while(mmap_for_reading_tuple(its_p, &tr, offset, &(inputs->input_tuple_def->size_def), 0))
		{
			printf("tuple_index = %"PRIu64", tuple_offset = %"PRIu64", tuple_size = %"PRIu32"\n", index, offset, curr_tuple_size_for_interim_tuple_region(&tr));
			print_tuple(tr.tuple, inputs->input_tuple_def);
			printf("\n\n");
			offset = next_tuple_offset_for_interim_tuple_region(&tr);
			index++;
		}
		unmap_for_interim_tuple_region(&tr);
		printf("\n\n");

		delete_interim_tuple_store(its_p);
		its_p = NULL;
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

void setup_printf_operator(operator* o, operator* input_operator, tuple_def* input_tuple_def)
{
	o->trigger_execution = trigger_execution;
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