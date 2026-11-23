#include<rhendb/query_plan.h>

static const rhendb_datum* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	operator* input_operator = o->state;

	const rhendb_datum* rtuple = input_operator->next_tuple(input_operator, error_code);
	if(rtuple == NULL)
		o->destroy_state(o);

	return rtuple;
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		operator* input_operator = o->state;

		input_operator->destroy_state(input_operator);
	}
	o->state = NULL;
}

void open_identity_operator(operator* o)
{
	o->state = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.identity.input_operator_index);
	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}