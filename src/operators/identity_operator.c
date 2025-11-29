#include<rhendb/query_plan_interface.h>

#include<boompar/executor.h>

typedef struct input_values input_values;
struct input_values
{
	executor* thread_pool;
	operator_buffer* output;
	operator_buffer* input;
};

static void* execute(void* o_v);

static void start_execution(operator* o)
{
	if(!submit_job_executor(((input_values*)(o->inputs))->thread_pool, execute, o, NULL, NULL, BLOCKING))
	{
		printf("FAILED TO SUBMIT INDENTITY OPERATOR\n");
		exit(-1);
	}
}

void setup_identity_operator(operator* o, executor* thread_pool, operator_buffer* output, operator_buffer* input)
{
	o->start_execution = start_execution;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.thread_pool = thread_pool,
		.output = output,
		.input = input,
	};
}