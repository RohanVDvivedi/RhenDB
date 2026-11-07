#include<rhendb/query_plan.h>

static void* next_tuple(operator* o, int* error_code)
{
	if(o->state == NULL)
		return NULL;

	operator* p = o->state;

	void* tuple = p->next_tuple(p, error_code);
	if(tuple == NULL)
		o->state = NULL;

	return tuple;
}

void open_row_rename_operator(operator* o)
{
	o->state = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.row_rename.input_operator_index);
	o->next_tuple = next_tuple;
	o->destroy_state = NULL;
}