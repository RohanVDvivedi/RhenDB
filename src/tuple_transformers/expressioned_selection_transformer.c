#include<rhendb/tuple_transformer_interface.h>

#include<rhendb/expression_evaluator.h>

typedef struct selection_context selection_context;
struct selection_context
{
	sql_expr_eval_context ec;

	sql_expression* expr; // this is not owned by the transformer
};

static void* process(tuple_transformer* tt_p, void* tuple)
{
	selection_context* sc_p = tt_p->context;

	int selection_match = 0;
	int error_code = 0;

	set_input_tuples_in_context_for_rhendb_v(&(sc_p->ec), 1, tuple);

	selection_match = select_using_evaluate_sql_expr_for_rhendb(sc_p->expr, &(sc_p->ec), &error_code);
	if(error_code)
		return NULL;

	if(selection_match)
		return tuple;
	else
		return NULL;
}

static void destroy(tuple_transformer* tt_p)
{
	selection_context* sc_p = tt_p->context;
	delete_context_p_for_sql_expr_eval_context_for_rhendb(sc_p->ec.context_p);
	free(sc_p);
}

tuple_transformer* get_new_expressioned_selection_transformer(const tuple_def* input_def, rhendb* rdb, sql_expression* expr)
{
	if(has_sub_query_in_sql_exp(expr))
		return NULL;

	selection_context* sc_p = malloc(sizeof(selection_context));
	sc_p->expr = expr;

	sc_p->ec = get_sql_expr_eval_context_for_rhendb((tuple_def**)(&input_def), 1, rdb);

	int error_code = 0;
	void* res_type = infer_type_sql_expr(sc_p->expr, &(sc_p->ec), &error_code);
	if(error_code)
	{
		delete_context_p_for_sql_expr_eval_context_for_rhendb(sc_p->ec.context_p);
		free(sc_p);
		return NULL;
	}
	delete_type(res_type, &(sc_p->ec));

	if(has_reference_to_extended_type_from_expression(sc_p->ec.context_p))
	{
		delete_context_p_for_sql_expr_eval_context_for_rhendb(sc_p->ec.context_p);
		free(sc_p);
		return NULL;
	}

	return get_new_tuple_transformer(sc_p, input_def, input_def, process, destroy);
}