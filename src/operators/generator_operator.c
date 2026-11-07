#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

// pointer to this byte means continue generation, and a NULL in the state means completed
static char GENERATE_DATA;

static void* next_tuple(operator* o, int* error_code)
{
	if(o->state == NULL)
		return NULL;

	void* tuple = o->desc->params.generator.generator(o->desc->params.generator.generator_context, o->desc->params.generator.generator_tuple_def);
	if(tuple == NULL)
	{
		o->state = NULL;
		return NULL;
	}

	return tuple;
}

void open_generator_operator(operator* o)
{
	o->state = &GENERATE_DATA;
	o->next_tuple = next_tuple;
	o->destroy_state = NULL;
}