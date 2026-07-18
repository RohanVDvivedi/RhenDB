#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/projection_type.h>

#include<rhendb/transaction.h>

#include<rhendb/expression_evaluator.h>

#include<rhendb/nullable_type_info_maker.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	const tuple_def* input_tuple_def;

	consumption_iterator* input_iterator;

	sql_expr_eval_context ec;

	uint32_t projection_descriptions_count;

	projection_description* projection_descriptions;

	// will have only as many entries as count(projection_descriptions[i].type == PROJECT_IDENTITY)
	projected_type_info* projected_expression_type_infos;

	const tuple_def* output_tuple_def;
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
			// set the input tuples
			set_input_tuples_in_context_for_rhendb_v(&(inputs->ec), 1, tuple);

			// generate the smallest possible tuple
			uint32_t output_tuple_size = get_minimum_tuple_size(inputs->output_tuple_def);
			uint64_t output_tuple_capacity = output_tuple_size;
			void* output_tuple = malloc(output_tuple_capacity);
			init_tuple(inputs->output_tuple_def, output_tuple);

			for(uint32_t i = 0, e = 0; i < inputs->projection_descriptions_count; i++)
			{
				datum output_uval;

				projected_value temp_proj_uval;
				if(inputs->projection_descriptions[i].type == PROJECT_EXPRESSION)
				{
					int error_code = 0;
					temp_proj_uval = project_using_evaluate_sql_expr_for_rhendb(inputs->projection_descriptions[i].expr, &(inputs->ec), inputs->projected_expression_type_infos[e], &error_code);
					if(error_code)
					{
						free(output_tuple);
						kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("errored_from_projection_expression"));
						return ;
					}
					output_uval = temp_proj_uval.value;
				}
				else
				{
					if(!get_value_from_element_from_tuple(&output_uval, inputs->input_tuple_def, inputs->projection_descriptions[i].pa, tuple))
						output_uval = (*NULL_DATUM);
				}

				// ensure there are enough bytes in the output_tuple, as we try to insert this datum
				while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(i), output_tuple, &output_uval, output_tuple_capacity - output_tuple_size))
				{
					output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
					output_tuple = realloc(output_tuple, output_tuple_capacity);
				}

				// recompute tuple_size
				output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

				if(inputs->projection_descriptions[i].type == PROJECT_EXPRESSION)
				{
					destroy_projected_value(temp_proj_uval);
					e++;
				}
			}

			int produced = produce_tuple_from_operator(o, output_tuple);
			free(output_tuple);
			if(!produced)
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
				return ;
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

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	for(uint32_t i = 0, e = 0; i < inputs->projection_descriptions_count; i++)
	{
		if(inputs->projection_descriptions[i].type == PROJECT_EXPRESSION)
			destroy_projected_type_info(inputs->projected_expression_type_infos[e++]);
		else
			free((data_type_info*)(inputs->output_tuple_def->type_info->containees[i].al.type_info));
	}
	free((data_type_info*)(inputs->output_tuple_def->type_info));
	free((tuple_def*)(inputs->output_tuple_def));

	free(inputs->projection_descriptions);
	free(inputs->projected_expression_type_infos);

	free(inputs);
}

operator_resource_counter setup_projection_operator(operator* o, operator* input_operator, projection_description* projection_descriptions, uint32_t projection_descriptions_count)
{
	const tuple_def* input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator);

	sql_expr_eval_context ec = get_sql_expr_eval_context_for_rhendb((tuple_def**)(&input_tuple_def), 1, input_operator->self_query_plan->curr_tx);

	uint32_t expression_projections_count = 0;
	for(uint32_t i = 0; i < projection_descriptions_count; i++)
	{
		if(projection_descriptions[i].type == PROJECT_EXPRESSION)
		{
			expression_projections_count++;

			int error_code = 0;
			if(!is_valid_using_infer_sql_expr_for_rhendb(projection_descriptions[i].expr, &ec, &error_code))
			{
				printf("type validation errored for projection_operator : %d @ index : %u\n", error_code, i);
				exit(-1);
			}
		}
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
	o->free_resources = free_resources;

	projection_description* projection_descriptions_cloned = malloc(sizeof(projection_description) * projection_descriptions_count);

	projected_type_info* projected_expression_type_infos = malloc(sizeof(projected_type_info) * expression_projections_count);

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(projection_descriptions_count));
	uint64_t max_output_tuple_size = 8;

	for(uint32_t i = 0, e = 0; i < projection_descriptions_count; i++)
	{
		data_type_info* col_dti = NULL;

		if(projection_descriptions[i].type == PROJECT_EXPRESSION)
		{
			int error_code = 0;
			projected_expression_type_infos[e] = infer_projected_type_sql_expr_for_rhendb(projection_descriptions[i].expr, &ec, &error_code);
			col_dti = projected_expression_type_infos[e++].projected_type_info;
		}
		else
			col_dti = shallow_clone_into_nullable_type((data_type_info*) get_type_info_for_element_from_tuple_def(input_tuple_def, projection_descriptions[i].pa)); // some nested key could be out-of-bounds so making it into a nullable type info is required

		if(col_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += col_dti->is_variable_sized ? (8 + col_dti->max_size) : (1 + col_dti->size);

		sprintf(output_dti->containees[i].field_name, "proj_%u", i);
		output_dti->containees[i].al.type_info = col_dti; // some nested key could be out-of-bounds so making it into a nullable type info is required

		projection_descriptions_cloned[i] = projection_descriptions[i];
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for projection_operator\n");
		exit(-1);
	}

	initialize_tuple_data_type_info(output_dti, "projection", 0, max_output_tuple_size, projection_descriptions_count);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	input_values* inputs = o->inputs;
	*inputs = (input_values){
		.input_tuple_def = input_tuple_def,
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.ec = ec,
		.projection_descriptions_count = projection_descriptions_count,
		.projection_descriptions = projection_descriptions_cloned,
		.projected_expression_type_infos = projected_expression_type_infos,
		.output_tuple_def = output_tuple_def,
	};

	return result;
}