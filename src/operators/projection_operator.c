#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	operator* input_operator = o->state;

	void* tuple = input_operator->next_tuple(input_operator, error_code);
	if(tuple == NULL)
	{
		o->destroy_state(o);
		return NULL;
	}

	set_input_tuples_in_context_for_rhendb_v(o->desc->expr_eval_context, 1, tuple);

	void* output_tuple = malloc(get_maximum_tuple_size(o->desc->output_tuple_def));
	init_tuple(o->desc->output_tuple_def, output_tuple);

	for(uint32_t i = 0; i < o->desc->params.projection.projection_descriptions_count; i++)
	{
		datum output_uval;

		projected_value temp_proj_uval;
		if(o->desc->params.projection.projection_descriptions[i].type == PROJECT_EXPRESSION)
		{
			int expr_error_code = 0;
			temp_proj_uval = project_using_evaluate_sql_expr_for_rhendb(o->desc->params.projection.projection_descriptions[i].expr, o->desc->expr_eval_context, o->desc->output_tuple_def->type_info->containees[i].al.type_info, &expr_error_code);
			if(expr_error_code)
			{
				free(tuple);
				free(output_tuple);
				o->destroy_state(o);
				return NULL;
			}
			output_uval = temp_proj_uval.value;
		}
		else
		{
			const data_type_info* dti_temp;
			if(!get_value_from_element_from_tuple(&output_uval, &dti_temp, input_operator->desc->output_tuple_def, o->desc->params.projection.projection_descriptions[i].pa, tuple))
				output_uval = (*NULL_DATUM);
		}

		set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(i), output_tuple, &output_uval, UINT32_MAX);

		if(o->desc->params.projection.projection_descriptions[i].type == PROJECT_EXPRESSION)
			destroy_projected_value(temp_proj_uval);
	}

	free(tuple);
	output_tuple = realloc(output_tuple, get_tuple_size(o->desc->output_tuple_def, output_tuple));
	return output_tuple;
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

void open_projection_operator(operator* o)
{
	o->state = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.projection.input_operator_index);
	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}