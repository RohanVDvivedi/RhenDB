#include<rhendb/query_plan.h>

static void* next_tuple(operator* o, int* error_code)
{
	if(o->state == NULL)
		return NULL;

	operator* input_operator = o->state;

	void* tuple = input_operator->next_tuple(input_operator, error_code);
	if(tuple == NULL)
		o->state = NULL;

	return tuple;
}

void setup_identity_operator(operator* o, operator* input_operator)
{
	o->state = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.identity.input_operator_index);
	o->next_tuple = next_tuple;
	o->destroy_state = NULL;
}