#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

// pointer to this byte means continue generation, and a NULL in the state means completed
static char GENERATE_DATA;

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	void* tuple = o->desc->params.generator.generator(o->desc->params.generator.generator_context, o->desc->params.generator.generator_tuple_def);
	if(tuple == NULL)
	{
		o->destroy_state(o);
		return NULL;
	}

	return tuple;
}

static void destroy_state(operator* o)
{
	o->state = NULL;
}

void open_generator_operator(operator* o)
{
	o->state = &GENERATE_DATA;
	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}