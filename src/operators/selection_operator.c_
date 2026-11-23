#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	operator* input_operator = o->state;

	while(1)
	{
		void* tuple = input_operator->next_tuple(input_operator, error_code);
		if(tuple == NULL)
		{
			o->destroy_state(o);
			return NULL;
		}

		set_input_tuples_in_context_for_rhendb_v(o->desc->expr_eval_context, 1, tuple);

		int expr_error_code = 0;
		int selection_match = select_using_evaluate_sql_expr_for_rhendb(o->desc->params.selection.expr, o->desc->expr_eval_context, &expr_error_code);
		if(expr_error_code)
		{
			free(tuple);
			o->destroy_state(o);
			(*error_code) = EXPRESSION_ERRORED_IN_OPERATOR;
			return NULL;
		}

		if(selection_match)
			return tuple;
		else
			free(tuple);
	}

	return NULL;
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

void open_selection_operator(operator* o)
{
	o->state = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.selection.input_operator_index);
	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}