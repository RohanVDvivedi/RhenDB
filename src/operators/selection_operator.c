#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<rhendb/expression_evaluator.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* input_iterator;

	sql_expr_eval_context ec;

	sql_expression* expr; // this is not owned by the transformer
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data)
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
			return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
			return ;
		}

		if(tuple != NULL)
		{
			int selection_match = 0;
			int error_code = 0;

			// set the input tuples
			set_input_tuples_in_context_for_rhendb_v(&(inputs->ec), 1, tuple);

			// evaluate the selection/filter expression
			selection_match = select_using_evaluate_sql_expr_for_rhendb(inputs->expr, &(inputs->ec), &error_code);
			if(error_code)
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("errored_from_selection_booling_expression"));
				return ;
			}

			// if bool os result says true, produce the tuple
			if(selection_match)
			{
				int produced = produce_tuple_from_operator(o, (void*)tuple);
				if(!produced)
				{
					kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
					return ;
				}
			}
		}
		else
			break;
	}

	return ;
}

static void clean_up_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->input_iterator != NULL)
	{
		destroy_consumption_iterator(inputs->input_iterator);
		inputs->input_iterator = NULL;
	}

	delete_context_p_for_sql_expr_eval_context_for_rhendb(inputs->ec.context_p);
}

operator_resource_counter setup_selection_operator(operator* o, operator* input_operator, sql_expression* expr)
{
	const tuple_def* input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator);

	sql_expr_eval_context ec = get_sql_expr_eval_context_for_rhendb((tuple_def**)(&input_tuple_def), 1, input_operator->self_query_plan->curr_tx);

	int error_code = 0;
	if(!is_valid_using_infer_sql_expr_for_rhendb(expr, &ec, &error_code))
	{
		printf("type validation errored for selection_operator : %d\n", error_code);
		exit(-1);
	}
	int has_reference_to_extended_type = has_reference_to_persistent_extended_type_from_expression(ec.context_p); // must be called only after validation/inference of type of the expr

	operator_resource_counter result = {.buffer_counter = has_reference_to_extended_type * 2, .job_counter = 1}; // * 2 if the expression compares 2 extended types
	if(o == NULL)
	{
		delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p);
		return result;
	}

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	// it is an identity operator, produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), input_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	input_values* inputs = o->inputs;
	*inputs = (input_values){
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.ec = ec,
		.expr = expr,
	};

	return result;
}