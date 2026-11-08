#include<rhendb/query_plan_descriptor.h>

#include<rhendb/rhendb.h>
#include<rhendb/nullable_type_info_maker.h>

#include<rhendb/function_compare.h>
#include<rhendb/function_hash.h>

#include<rhendb/max_intermediate_tuple_size.h>

#include<tuplestore/data_type_info.h>
#include<tuplestore/data_type_info_defaults.h>
#include<tuplestore/tuple.h>

#include<tuplelargetypes/common_extended.h>

#include<cutlery/cutlery_math.h>

#include<stdlib.h>
#include<stdio.h>
#include<string.h>

static operator_desc* get_new_registered_operator_descriptor_for_query_plan_descriptor(query_plan_desc* qpd, operator_descriptor_type type)
{
	operator_desc* d = malloc(sizeof(operator_desc));
	if(d == NULL)
		exit(-1);

	d->type = type;
	d->index = get_element_count_arraylist(&(qpd->operator_descriptors));
	d->output_tuple_def = NULL;
	d->resource_counter = (operator_resource_counter){};
	d->expr_eval_context = NULL;
	d->consumers_count = 0; // every operator descripttor starts with no other operator consuming from it

	if(is_full_arraylist(&(qpd->operator_descriptors)) && !expand_arraylist(&(qpd->operator_descriptors)))
		exit(-1);
	push_back_to_arraylist(&(qpd->operator_descriptors), d);

	return d;
}

query_plan_desc* get_new_query_plan_descriptor(transaction* curr_tx, const sql_expr_eval_context* this_sub_query_expr_eval_context)
{
	query_plan_desc* qpd = malloc(sizeof(query_plan_desc));
	if(qpd == NULL)
		exit(-1);

	qpd->curr_tx = curr_tx;
	qpd->this_sub_query_expr_eval_context = this_sub_query_expr_eval_context;

	initialize_arraylist(&(qpd->operator_descriptors), 16);

	return qpd;
}

int does_query_plan_descriptor_contains_operator_descriptor(const query_plan_desc* qpd, const operator_desc* d)
{
	return d == get_from_front_of_arraylist(&(qpd->operator_descriptors), d->index);
}

// ---------------------------------------------------------------------------------------------------
// deep clone / free helpers for the borrowed arrays and strings the descriptor must own
// (a positional_accessor itself owns a positions array, so cloning one clones that array too)
// ---------------------------------------------------------------------------------------------------

// deep clones a contiguous array of `count` positional_accessors (key_element_ids style), or a single
// positional_accessor with count == 1
static positional_accessor* deep_clone_positional_accessors(const positional_accessor* src, uint32_t count)
{
	if(src == NULL || count == 0)
		return NULL;

	positional_accessor* cloned = malloc(sizeof(positional_accessor) * count);
	for(uint32_t i = 0; i < count; i++)
	{
		cloned[i].positions_length = src[i].positions_length;
		cloned[i].positions = (src[i].positions_length == 0) ? NULL : malloc(sizeof(uint32_t) * src[i].positions_length);
		for(uint32_t j = 0; j < src[i].positions_length; j++)
			cloned[i].positions[j] = src[i].positions[j];
	}
	return cloned;
}

static void free_positional_accessors(positional_accessor* arr, uint32_t count)
{
	if(arr == NULL)
		return;

	for(uint32_t i = 0; i < count; i++)
		free(arr[i].positions);
	free(arr);
}

// deep clones a contiguous array of `count` compare_directions
static compare_direction* deep_clone_compare_directions(const compare_direction* src, uint32_t count)
{
	if(src == NULL || count == 0)
		return NULL;

	compare_direction* cloned = malloc(sizeof(compare_direction) * count);
	for(uint32_t i = 0; i < count; i++)
		cloned[i] = src[i];
	return cloned;
}

static aggregate_function** shallow_clone_aggregate_functions(aggregate_function* const * src, uint32_t count)
{
	if(src == NULL || count == 0)
		return NULL;

	aggregate_function** cloned = malloc(sizeof(aggregate_function*) * count);
	for(uint32_t i = 0; i < count; i++)
		cloned[i] = src[i];
	return cloned;
}

// deep clones aggregate_input_element_ids : an array of `aggregate_functions_count` pointers, whose i-th entry
// points to a contiguous array of aggregate_functions[i]->input_type_infos_count positional_accessors
static positional_accessor** deep_clone_aggregate_input_element_ids(const positional_accessor** src, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions)
{
	if(src == NULL || aggregate_functions_count == 0)
		return NULL;

	positional_accessor** cloned = malloc(sizeof(positional_accessor*) * aggregate_functions_count);
	for(uint32_t i = 0; i < aggregate_functions_count; i++)
		cloned[i] = deep_clone_positional_accessors(src[i], aggregate_functions[i]->input_type_infos_count);
	return cloned;
}

static void free_aggregate_input_element_ids(positional_accessor** arr, uint32_t aggregate_functions_count, aggregate_function** aggregate_functions)
{
	if(arr == NULL)
		return;

	for(uint32_t i = 0; i < aggregate_functions_count; i++)
		free_positional_accessors(arr[i], aggregate_functions[i]->input_type_infos_count);
	free(arr);
}

static char* deep_clone_string(const char* src)
{
	if(src == NULL)
		return NULL;

	uint32_t length = strlen(src);
	char* cloned = malloc(length + 1);
	memcpy(cloned, src, length + 1);
	return cloned;
}

// deep clones a projection_descriptions array : the PROJECT_IDENTITY entries own a positional_accessor (cloned),
// the PROJECT_EXPRESSION entries carry a borrowed sql_expression* (the AST, not cloned)
static projection_description* deep_clone_projection_descriptions(const projection_description* src, uint32_t count)
{
	if(src == NULL || count == 0)
		return NULL;

	projection_description* cloned = malloc(sizeof(projection_description) * count);
	for(uint32_t i = 0; i < count; i++)
	{
		cloned[i] = src[i];
		cloned[i].column_name = deep_clone_string(src[i].column_name);
		if(src[i].type == PROJECT_IDENTITY)
		{
			cloned[i].pa.positions_length = src[i].pa.positions_length;
			cloned[i].pa.positions = (src[i].pa.positions_length == 0) ? NULL : malloc(sizeof(uint32_t) * src[i].pa.positions_length);
			for(uint32_t j = 0; j < src[i].pa.positions_length; j++)
				cloned[i].pa.positions[j] = src[i].pa.positions[j];
		}
	}
	return cloned;
}

static void free_projection_descriptions(projection_description* arr, uint32_t count)
{
	if(arr == NULL)
		return;

	for(uint32_t i = 0; i < count; i++)
	{
		if(arr[i].column_name != NULL)
			free(arr[i].column_name);
		if(arr[i].type == PROJECT_IDENTITY)
			free(arr[i].pa.positions);
	}
	free(arr);
}

// deep clones an array of count strings (each entry may be NULL)
static char** deep_clone_string_array(const char* const* src, uint64_t count)
{
	if(src == NULL || count == 0)
		return NULL;

	char** cloned = malloc(sizeof(char*) * count);
	for(uint64_t i = 0; i < count; i++)
		cloned[i] = deep_clone_string(src[i]);
	return cloned;
}

static void free_string_array(char** arr, uint64_t count)
{
	if(arr == NULL)
		return;

	for(uint64_t i = 0; i < count; i++)
		free(arr[i]);
	free(arr);
}

// ---------------------------------------------------------------------------------------------------
// leaves
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_constant_dataset_operator_descriptor(query_plan_desc* qpd, interim_tuple_store* constant_dataset_p, const tuple_def* record_def)
{
	if(record_def == NULL)
	{
		printf("record_def can not be NULL for constant_dataset_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.thread_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_CONSTANT_DATASET);
	d->params.constant_dataset = (constant_dataset_operator_params){constant_dataset_p, record_def};
	d->output_tuple_def = record_def;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_generator_operator_descriptor(query_plan_desc* qpd, void* (*generator)(void* generator_context, const tuple_def* generator_tuple_def), void* generator_context, const tuple_def* generator_tuple_def)
{
	if(generator_tuple_def == NULL)
	{
		printf("generator_tuple_def can not be NULL for generator_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.thread_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_GENERATOR);
	d->params.generator = (generator_operator_params){generator, generator_context, generator_tuple_def};
	d->output_tuple_def = generator_tuple_def;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_stream_input_operator_descriptor(query_plan_desc* qpd, stream* in_strm, const tuple_def* input_tuple_def)
{
	if(input_tuple_def == NULL)
	{
		printf("input_tuple_def can not be NULL for stream_input_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.thread_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_STREAM_INPUT);
	d->params.stream_input = (stream_input_operator_params){in_strm, input_tuple_def};
	d->output_tuple_def = input_tuple_def;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_scan_operator_descriptor(query_plan_desc* qpd, const fetched_table* ftabl, uint32_t max_concurrent_jobs_count, int output_flags, int additional_flags)
{
	if(ftabl == NULL)
	{
		printf("ftabl can not be NULL for scan_operator_descriptor\n");
		return NULL;
	}

	if(qpd->curr_tx->snapshot == NULL)
	{
		printf("must have a snapshot for scan_operator_descriptor\n");
		return NULL;
	}

	if(max_concurrent_jobs_count == 0)
	{
		printf("max_concurrent_jobs_count can not be 0 for scan_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.buffer_counter = 2 * max_concurrent_jobs_count, .job_counter = max_concurrent_jobs_count, .thread_counter = max_concurrent_jobs_count};

	tuple_def* output_tuple_def = NULL;

	if(output_flags != 0)
	{
		output_tuple_def = malloc(sizeof(tuple_def));
		data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(10)); // over allocation for being safe from future changes

		uint32_t output_dti_element_count = 0;
		uint64_t output_tuple_max_size = 8;

		if(MUST_OUTPUT_TABLE_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "table_id", 64);
			output_dti->containees[output_dti_element_count].al.type_info = UINT_NON_NULLABLE[8];

			output_tuple_max_size += 8;
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_PARTITION_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "partition_id", 64);
			output_dti->containees[output_dti_element_count].al.type_info = UINT_NON_NULLABLE[8];

			output_tuple_max_size += 8;
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_TUPLE_POINTER_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "tuple_pointer", 64);
			output_dti->containees[output_dti_element_count].al.type_info = &(qpd->curr_tx->rdb->persistent_acid_rage_engine.pam_p->pas.tuple_pointer_type_info);

			output_tuple_max_size += sizeof(tuple_pointer);
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_HEAP_TUPLE(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, ftabl->table_info.name, 64);
			output_dti->containees[output_dti_element_count].al.type_info = ftabl->final_readers_tuple_def.type_info;

			output_tuple_max_size += 8 + get_maximum_tuple_size(&(ftabl->final_readers_tuple_def));
			output_dti_element_count++;
		}

		initialize_tuple_data_type_info(output_dti, "scanned_tuple_context", 0, output_tuple_max_size, output_dti_element_count);

		initialize_tuple_def(output_tuple_def, output_dti);
	}

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_SCAN);
	d->params.scan = (scan_operator_params){ftabl, max_concurrent_jobs_count, output_flags, additional_flags};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// single input, schema preserving
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_identity_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc)
{
	if(input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_desc))
	{
		printf("input_operator_desc invalid for identity_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for identity_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* input_tuple_def = input_operator_desc->output_tuple_def;
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for identity_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_IDENTITY);
	d->params.identity = (identity_operator_params){input_operator_desc->index};
	d->output_tuple_def = input_tuple_def;
	d->resource_counter = result;

	input_operator_desc->consumers_count++;

	return d;
}

operator_desc* insert_row_rename_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, const char* output_name, const char** column_names, uint64_t columns_count)
{
	if(input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_desc))
	{
		printf("input_operator_desc invalid for row_rename_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for row_rename_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* input_tuple_def = input_operator_desc->output_tuple_def;
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for row_rename_operator_descriptor\n");
		return NULL;
	}

	if(input_tuple_def->type_info->type != TUPLE)
	{
		printf("input must be a tuple for row_rename_operator_descriptor\n");
		return NULL;
	}

	if(columns_count > input_tuple_def->type_info->element_count)
	{
		printf("can not have more renames than input tuple columns for row_rename_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.job_counter = 1};

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(input_tuple_def->type_info->element_count));
	for(uint32_t i = 0; i < input_tuple_def->type_info->element_count; i++)
	{
		if(i < columns_count && column_names[i] != NULL)
			sprintf(output_dti->containees[i].field_name, column_names[i], i);
		else
			sprintf(output_dti->containees[i].field_name, input_tuple_def->type_info->containees[i].field_name, i);

		output_dti->containees[i].al.type_info = input_tuple_def->type_info->containees[i].al.type_info;
	}
	initialize_tuple_data_type_info(output_dti, (output_name == NULL) ? input_tuple_def->type_info->type_name : output_name, 0, get_maximum_tuple_size(input_tuple_def), input_tuple_def->type_info->element_count);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_ROW_RENAME);
	d->params.row_rename = (row_rename_operator_params){input_operator_desc->index, deep_clone_string(output_name), deep_clone_string_array(column_names, columns_count), columns_count};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	input_operator_desc->consumers_count++;

	return d;
}

operator_desc* insert_offset_limit_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, tuples_down_counter offset_counter, tuples_down_counter limit_counter)
{
	if(input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_desc))
	{
		printf("input_operator_desc invalid for offset_limit_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for offset_limit_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* input_tuple_def = input_operator_desc->output_tuple_def;
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for offset_limit_operator_descriptor\n");
		return NULL;
	}

	if(is_inf_tuples_down_counter(&offset_counter))
	{
		printf("offset can not be infinity for offset_limit_operator_descriptor\n");
		return NULL;
	}
	if(is_zero_tuples_down_counter(&limit_counter))
	{
		printf("limit can not be 0 for offset_limit_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_OFFSET_LIMIT);
	d->params.offset_limit = (offset_limit_operator_params){input_operator_desc->index, offset_counter, limit_counter};
	d->output_tuple_def = input_tuple_def;
	d->resource_counter = result;

	input_operator_desc->consumers_count++;

	return d;
}

operator_desc* insert_selection_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, sql_expression* expr)
{
	if(input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_desc))
	{
		printf("input_operator_desc invalid for selection_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for selection_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* input_tuple_def = input_operator_desc->output_tuple_def;
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for selection_operator_descriptor\n");
		return NULL;
	}

	sql_expr_eval_context ec = get_sql_expr_eval_context_for_rhendb((tuple_def**)(&input_tuple_def), 1, qpd->curr_tx, qpd->this_sub_query_expr_eval_context);

	int error_code = 0;
	if(!is_valid_using_infer_sql_expr_for_rhendb(expr, &ec, &error_code))
	{
		printf("type validation errored for selection_operator_descriptor : %d\n", error_code);
		delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p);
		return NULL;
	}
	int has_reference_to_extended_type = has_reference_to_persistent_extended_type_from_expression(ec.context_p);

	operator_resource_counter result = has_sub_query_in_sql_exp(expr)
		? (operator_resource_counter){.buffer_counter = has_reference_to_extended_type * 2, .thread_counter = 1}
		: (operator_resource_counter){.buffer_counter = has_reference_to_extended_type * 2, .job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_SELECTION);
	d->params.selection = (selection_operator_params){input_operator_desc->index, expr};
	d->output_tuple_def = input_tuple_def;
	d->resource_counter = result;
	d->expr_eval_context = malloc(sizeof(sql_expr_eval_context));
	*(d->expr_eval_context) = ec;

	input_operator_desc->consumers_count++;

	return d;
}

operator_desc* insert_sort_operator_descriptor(query_plan_desc* qpd, tuples_down_counter result_counter, operator_desc* input_operator_desc, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint64_t min_run_size, uint32_t N_way_sort, uint32_t max_concurrent_jobs_count)
{
	if(input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_desc))
	{
		printf("input_operator_desc invalid for sort_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for sort_operator_descriptor\n");
		return NULL;
	}

	if(is_zero_tuples_down_counter(&result_counter))
	{
		printf("result_counter can not be 0 for sort_operator_descriptor\n");
		return NULL;
	}

	if(key_element_count == 0)
	{
		printf("key_element_count can not be 0 for sort_operator_descriptor\n");
		return NULL;
	}

	if(min_run_size == 0)
	{
		printf("min_run_size can not be 0 for sort_operator_descriptor\n");
		return NULL;
	}

	if(N_way_sort < 2)
	{
		printf("N_way_sort can not be less than 2 for sort_operator_descriptor\n");
		return NULL;
	}

	if(max_concurrent_jobs_count == 0)
	{
		printf("max_concurrent_jobs_count can not be 0 for sort_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* record_def = input_operator_desc->output_tuple_def;
	if(record_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for sort_operator_descriptor\n");
		return NULL;
	}

	if(key_element_count > 0 && key_element_ids != NULL && !are_all_positions_accessible_for_tuple_def(record_def, key_element_ids, key_element_count))
	{
		printf("key positions not accessible for sort_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.buffer_counter = 2 * has_extended_type_info3(record_def, key_element_count, key_element_ids, PERSISTENT_EXT_SUB_TYPE) * max_concurrent_jobs_count, .job_counter = max_concurrent_jobs_count + 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_SORT);
	d->params.sort = (sort_operator_params){result_counter, input_operator_desc->index, key_element_count, deep_clone_positional_accessors(key_element_ids, key_element_count), deep_clone_compare_directions(key_compare_direction, key_element_count), min_run_size, N_way_sort, max_concurrent_jobs_count};
	d->output_tuple_def = record_def;
	d->resource_counter = result;

	input_operator_desc->consumers_count++;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// projection
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_projection_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, const char* output_name, projection_description* projection_descriptions, uint32_t projection_descriptions_count)
{
	if(input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_desc))
	{
		printf("input_operator_desc invalid for projection_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for projection_operator_descriptor\n");
		return NULL;
	}

	if(projection_descriptions == NULL || projection_descriptions_count == 0)
	{
		printf("projection_descriptions can not be empty for projection_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* input_tuple_def = input_operator_desc->output_tuple_def;
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for projection_operator_descriptor\n");
		return NULL;
	}

	sql_expr_eval_context ec = get_sql_expr_eval_context_for_rhendb((tuple_def**)(&input_tuple_def), 1, qpd->curr_tx, qpd->this_sub_query_expr_eval_context);

	int has_sub_query = 0;

	for(uint32_t i = 0; i < projection_descriptions_count; i++)
	{
		if(projection_descriptions[i].type == PROJECT_EXPRESSION)
		{
			int error_code = 0;
			if(!is_valid_using_infer_sql_expr_for_rhendb(projection_descriptions[i].expr, &ec, &error_code))
			{
				printf("type validation errored for projection_operator_descriptor : %d @ index : %u\n", error_code, i);
				delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p);
				return NULL;
			}

			has_sub_query = has_sub_query || has_sub_query_in_sql_exp(projection_descriptions[i].expr);
		}
	}
	int has_reference_to_extended_type = has_reference_to_persistent_extended_type_from_expression(ec.context_p);

	operator_resource_counter result = has_sub_query
		? (operator_resource_counter){.buffer_counter = has_reference_to_extended_type * 2, .thread_counter = 1}
		: (operator_resource_counter){.buffer_counter = has_reference_to_extended_type * 2, .job_counter = 1};

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(projection_descriptions_count));
	uint64_t max_output_tuple_size = 8;

	for(uint32_t i = 0; i < projection_descriptions_count; i++)
	{
		data_type_info* col_dti = NULL;

		if(projection_descriptions[i].type == PROJECT_EXPRESSION)
		{
			int error_code = 0;
			col_dti = infer_projected_type_sql_expr_for_rhendb(projection_descriptions[i].expr, &ec, &error_code);
		}
		else
			col_dti = shallow_clone_into_nullable_type((data_type_info*) get_type_info_for_element_from_tuple_def(input_tuple_def, projection_descriptions[i].pa));

		if(col_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += col_dti->is_variable_sized ? (8 + col_dti->max_size) : (1 + col_dti->size);

		// if projection has a column name use that, else make a custom unique one
		if(projection_descriptions[i].column_name != NULL)
			sprintf(output_dti->containees[i].field_name, projection_descriptions[i].column_name, i);
		else
			sprintf(output_dti->containees[i].field_name, "proj_%u", i);

		output_dti->containees[i].al.type_info = col_dti;
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for projection_operator_descriptor\n");
		for(uint32_t i = 0; i < projection_descriptions_count; i++)
		{
			if(projection_descriptions[i].type == PROJECT_IDENTITY)
				free((data_type_info*) output_dti->containees[i].al.type_info);
		}
		free(output_dti);
		delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p);
		return NULL;
	}

	initialize_tuple_data_type_info(output_dti, ((output_name == NULL) ? "projection" : output_name), 0, max_output_tuple_size, projection_descriptions_count);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_PROJECTION);
	d->params.projection = (projection_operator_params){input_operator_desc->index, deep_clone_string(output_name), deep_clone_projection_descriptions(projection_descriptions, projection_descriptions_count), projection_descriptions_count};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;
	d->expr_eval_context = malloc(sizeof(sql_expr_eval_context));
	*(d->expr_eval_context) = ec;

	input_operator_desc->consumers_count++;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// aggregations
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_simple_aggregation_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids)
{
	if(input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_desc))
	{
		printf("input_operator_desc invalid for simple_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for simple_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(aggregate_functions_count == 0)
	{
		printf("aggregate_functions_count can not be 0 for simple_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->output_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for simple_aggregation_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.buffer_counter = get_max_buffers_count_for_all_aggregate_functions(aggregate_functions_count, (aggregate_function const * const *) aggregate_functions), .job_counter = 1};

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(aggregate_functions_count));
	uint64_t max_output_tuple_size = 8;

	for(uint32_t i = 0; i < aggregate_functions_count; i++)
	{
		if(aggregate_functions[i]->output_type_info->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += aggregate_functions[i]->output_type_info->is_variable_sized ? (8 + aggregate_functions[i]->output_type_info->max_size) : (1 + aggregate_functions[i]->output_type_info->size);

		sprintf(output_dti->containees[i].field_name, "agg_%u", i);
		output_dti->containees[i].al.type_info = (data_type_info*) aggregate_functions[i]->output_type_info;
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for simple_aggregation_operator_descriptor\n");
		free(output_dti);
		return NULL;
	}

	initialize_tuple_data_type_info(output_dti, "aggregates", 0, max_output_tuple_size, aggregate_functions_count);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_SIMPLE_AGGREGATION);
	d->params.simple_aggregation = (simple_aggregation_operator_params){input_operator_desc->index, aggregate_functions_count, shallow_clone_aggregate_functions(aggregate_functions, aggregate_functions_count), deep_clone_aggregate_input_element_ids(aggregate_input_element_ids, aggregate_functions_count, aggregate_functions)};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	input_operator_desc->consumers_count++;

	return d;
}

operator_desc* insert_sorted_aggregation_operator_descriptor(query_plan_desc* qpd,  operator_desc* input_operator_desc, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids)
{
	if(input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_desc))
	{
		printf("input_operator_desc invalid for sorted_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for sorted_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(key_element_count == 0)
	{
		printf("key_element_count is zero in sorted_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(aggregate_functions_count == 0)
	{
		printf("aggregate_functions_count can not be 0 for sorted_aggregation_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* input_tuple_def = input_operator_desc->output_tuple_def;
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for sorted_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(!are_all_positions_accessible_for_tuple_def(input_tuple_def, key_element_ids, key_element_count))
	{
		printf("key positions not accessible for sorted_aggregation_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.buffer_counter = max(2 * has_extended_type_info3(input_tuple_def, key_element_count, key_element_ids, PERSISTENT_EXT_SUB_TYPE), get_max_buffers_count_for_all_aggregate_functions(aggregate_functions_count, (aggregate_function const * const *) aggregate_functions)), .job_counter = 1};

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(key_element_count + aggregate_functions_count));
	uint64_t max_output_tuple_size = 8;

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		data_type_info* key_dti = (data_type_info*) get_type_info_for_element_from_tuple_def(input_tuple_def, key_element_ids[i]);

		if(key_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += key_dti->is_variable_sized ? (8 + key_dti->max_size) : (1 + key_dti->size);

		sprintf(output_dti->containees[i].field_name, "key_%u", i);
		output_dti->containees[i].al.type_info = shallow_clone_into_nullable_type(key_dti);
	}

	for(uint32_t i = 0, j = key_element_count; i < aggregate_functions_count; i++, j++)
	{
		if(aggregate_functions[i]->output_type_info->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += aggregate_functions[i]->output_type_info->is_variable_sized ? (8 + aggregate_functions[i]->output_type_info->max_size) : (1 + aggregate_functions[i]->output_type_info->size);

		sprintf(output_dti->containees[j].field_name, "agg_%u", i);
		output_dti->containees[j].al.type_info = (data_type_info*) aggregate_functions[i]->output_type_info;
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for sorted_aggregation_operator_descriptor\n");
		for(uint32_t i = 0; i < key_element_count; i++)
			free((data_type_info*) output_dti->containees[i].al.type_info);
		free(output_dti);
		return NULL;
	}

	initialize_tuple_data_type_info(output_dti, "keyed_aggregates", 0, max_output_tuple_size, key_element_count + aggregate_functions_count);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_SORTED_AGGREGATION);
	d->params.sorted_aggregation = (sorted_aggregation_operator_params){input_operator_desc->index, key_element_count, deep_clone_positional_accessors(key_element_ids, key_element_count), aggregate_functions_count, shallow_clone_aggregate_functions(aggregate_functions, aggregate_functions_count), deep_clone_aggregate_input_element_ids(aggregate_input_element_ids, aggregate_functions_count, aggregate_functions)};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	input_operator_desc->consumers_count++;

	return d;
}

operator_desc* insert_hash_aggregation_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_build_tuple_buffer_size)
{
	if(input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_desc))
	{
		printf("input_operator_desc invalid for hash_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for hash_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(key_element_count == 0)
	{
		printf("key_element_count must not be 0 for hash_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(aggregate_functions_count == 0)
	{
		printf("aggregate_functions_count can not be 0 for hash_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(partitions_count == 0)
	{
		printf("partitions_count can not be 0 for hash_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(max_concurrent_jobs_count == 0)
	{
		printf("max_concurrent_jobs_count can not be 0 for hash_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(max_concurrent_jobs_queue_size == 0)
	{
		printf("max_concurrent_jobs_queue_size can not be 0 for hash_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(min_build_tuple_buffer_size == 0)
	{
		printf("min_build_tuple_buffer_size can not be 0 for hash_aggregation_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* input_tuple_def = input_operator_desc->output_tuple_def;
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for hash_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(!are_all_positions_accessible_for_tuple_def(input_tuple_def, key_element_ids, key_element_count))
	{
		printf("key positions not accessible for hash_aggregation_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.buffer_counter = max_concurrent_jobs_count * max(2 * has_extended_type_info3(input_tuple_def, key_element_count, key_element_ids, PERSISTENT_EXT_SUB_TYPE), get_max_buffers_count_for_all_aggregate_functions(aggregate_functions_count, (aggregate_function const * const *) aggregate_functions)), .job_counter = max_concurrent_jobs_count + 1};

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(key_element_count + aggregate_functions_count));
	uint64_t max_output_tuple_size = 8;

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		data_type_info* key_dti = (data_type_info*) get_type_info_for_element_from_tuple_def(input_tuple_def, key_element_ids[i]);

		if(key_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += key_dti->is_variable_sized ? (8 + key_dti->max_size) : (1 + key_dti->size);

		sprintf(output_dti->containees[i].field_name, "key_%u", i);
		output_dti->containees[i].al.type_info = shallow_clone_into_nullable_type(key_dti);
	}

	for(uint32_t i = 0, j = key_element_count; i < aggregate_functions_count; i++, j++)
	{
		if(aggregate_functions[i]->output_type_info->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += aggregate_functions[i]->output_type_info->is_variable_sized ? (8 + aggregate_functions[i]->output_type_info->max_size) : (1 + aggregate_functions[i]->output_type_info->size);

		sprintf(output_dti->containees[j].field_name, "agg_%u", i);
		output_dti->containees[j].al.type_info = (data_type_info*) aggregate_functions[i]->output_type_info;
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for hash_aggregation_operator_descriptor\n");
		for(uint32_t i = 0; i < key_element_count; i++)
			free((data_type_info*) output_dti->containees[i].al.type_info);
		free(output_dti);
		return NULL;
	}

	initialize_tuple_data_type_info(output_dti, "keyed_aggregates", 0, max_output_tuple_size, key_element_count + aggregate_functions_count);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_HASH_AGGREGATION);
	d->params.hash_aggregation = (hash_aggregation_operator_params){input_operator_desc->index, key_element_count, deep_clone_positional_accessors(key_element_ids, key_element_count), aggregate_functions_count, shallow_clone_aggregate_functions(aggregate_functions, aggregate_functions_count), deep_clone_aggregate_input_element_ids(aggregate_input_element_ids, aggregate_functions_count, aggregate_functions), partitions_count, max_concurrent_jobs_count, max_concurrent_jobs_queue_size, min_build_tuple_buffer_size};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	input_operator_desc->consumers_count++;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// multiple inputs
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_union_operator_descriptor(query_plan_desc* qpd, operator_desc** input_operator_descs, uint64_t input_operators_count)
{
	if(input_operators_count < 2 || input_operator_descs == NULL)
	{
		printf("input_operators can not be less than 2 for union_operator_descriptor\n");
		return NULL;
	}

	for(uint64_t i = 0; i < input_operators_count; i++)
	{
		if(input_operator_descs[i] == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_descs[i]))
		{
			printf("input_operator_desc invalid for union_operator_descriptor\n");
			return NULL;
		}
	}

	for(uint64_t i = 0; i < input_operators_count; i++)
	{
		if(input_operator_descs[i]->consumers_count > 0)
		{
			printf("an input_operator_desc already has a consumer for union_operator_descriptor\n");
			return NULL;
		}
	}

	const tuple_def* input_tuple_def = input_operator_descs[0]->output_tuple_def;
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for union_operator_descriptor\n");
		return NULL;
	}
	for(uint64_t i = 1; i < input_operators_count; i++)
	{
		const tuple_def* other_tuple_def = input_operator_descs[i]->output_tuple_def;
		if(other_tuple_def == NULL)
		{
			printf("input operator descriptor has no output_tuple_def for union_operator_descriptor\n");
			return NULL;
		}
		if(!are_accessibly_equivalent_type_info(input_tuple_def->type_info, other_tuple_def->type_info))
		{
			printf("input tuple_defs are not accessibly equivalent for union_operator_descriptor\n");
			return NULL;
		}
	}

	uint64_t* input_operator_indices_cloned = malloc(sizeof(uint64_t) * input_operators_count);
	for(uint64_t i = 0; i < input_operators_count; i++)
		input_operator_indices_cloned[i] = input_operator_descs[i]->index;

	operator_resource_counter result = {.job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_UNION);
	d->params.union_op = (union_operator_params){input_operator_indices_cloned, input_operators_count};
	d->output_tuple_def = input_tuple_def;
	d->resource_counter = result;

	for(uint64_t i = 0; i < input_operators_count; i++)
		input_operator_descs[i]->consumers_count++;

	return d;
}

operator_desc* insert_merge_sorted_inputs_operator_descriptor(query_plan_desc* qpd, operator_desc** input_operator_descs, uint64_t input_operators_count, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction)
{
	if(input_operators_count < 2 || input_operator_descs == NULL)
	{
		printf("input_operators can not be lesser than 2 for merge_sorted_inputs_operator_descriptor\n");
		return NULL;
	}

	for(uint64_t i = 0; i < input_operators_count; i++)
	{
		if(input_operator_descs[i] == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_descs[i]))
		{
			printf("input_operator_desc invalid for merge_sorted_inputs_operator_descriptor\n");
			return NULL;
		}
	}

	for(uint64_t i = 0; i < input_operators_count; i++)
	{
		if(input_operator_descs[i]->consumers_count > 0)
		{
			printf("an input_operator_desc already has a consumer for merge_sorted_inputs_operator_descriptor\n");
			return NULL;
		}
	}

	const tuple_def* record_def = input_operator_descs[0]->output_tuple_def;
	if(record_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for merge_sorted_inputs_operator_descriptor\n");
		return NULL;
	}
	for(uint64_t i = 1; i < input_operators_count; i++)
	{
		const tuple_def* other_tuple_def = input_operator_descs[i]->output_tuple_def;
		if(other_tuple_def == NULL)
		{
			printf("input operator descriptor has no output_tuple_def for merge_sorted_inputs_operator_descriptor\n");
			return NULL;
		}
		if(!are_accessibly_equivalent_type_info(record_def->type_info, other_tuple_def->type_info))
		{
			printf("input tuple_defs are not accessibly equivalent for merge_sorted_inputs_operator_descriptor\n");
			return NULL;
		}
	}

	if(key_element_count == 0)
	{
		printf("key_element_count can not be 0 for merge_sorted_inputs_operator_descriptor\n");
		return NULL;
	}

	uint64_t* input_operator_indices_cloned = malloc(sizeof(uint64_t) * input_operators_count);
	for(uint64_t i = 0; i < input_operators_count; i++)
		input_operator_indices_cloned[i] = input_operator_descs[i]->index;

	operator_resource_counter result = {.buffer_counter = 2 * has_extended_type_info3(record_def, key_element_count, key_element_ids, PERSISTENT_EXT_SUB_TYPE), .job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_MERGE_SORTED_INPUTS);
	d->params.merge_sorted_inputs = (merge_sorted_inputs_operator_params){input_operator_indices_cloned, input_operators_count, key_element_count, deep_clone_positional_accessors(key_element_ids, key_element_count), deep_clone_compare_directions(key_compare_direction, key_element_count)};
	d->output_tuple_def = record_def;
	d->resource_counter = result;

	for(uint64_t i = 0; i < input_operators_count; i++)
		input_operator_descs[i]->consumers_count++;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// joins ([0] = left, [1] = right), output is the two columns "left" and "right"
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_block_nested_loop_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, operator_desc* right_input_operator_desc, sql_expression* join_expr, join_preserve_type ptype, uint32_t min_block_size)
{
	if(left_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, left_input_operator_desc) || right_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, right_input_operator_desc))
	{
		printf("input_operator_desc invalid for block_nested_loop_join_operator_descriptor\n");
		return NULL;
	}

	if(left_input_operator_desc->consumers_count > 0 || right_input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for block_nested_loop_join_operator_descriptor\n");
		return NULL;
	}

	if(min_block_size == 0)
	{
		printf("min_block_size can not be 0 for block_nested_loop_join_operator_descriptor\n");
		return NULL;
	}

	if(DOES_IT_PRESERVE_RIGHT(ptype))
	{
		printf("can not do right side preserving joins for block_nested_loop_join_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* left_input_tuple_def = left_input_operator_desc->output_tuple_def;
	const tuple_def* right_input_tuple_def = right_input_operator_desc->output_tuple_def;
	if(left_input_tuple_def == NULL || right_input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for block_nested_loop_join_operator_descriptor\n");
		return NULL;
	}

	sql_expr_eval_context ec;
	int has_expr_eval_context = 0;
	int has_reference_to_extended_type = 0;
	int has_sub_query = 0;
	if(join_expr != NULL)
	{
		ec = get_sql_expr_eval_context_for_rhendb((tuple_def* []){(tuple_def*)left_input_tuple_def, (tuple_def*)right_input_tuple_def}, 2, qpd->curr_tx, qpd->this_sub_query_expr_eval_context);

		int error_code = 0;
		if(!is_valid_using_infer_sql_expr_for_rhendb(join_expr, &ec, &error_code))
		{
			printf("type validation errored for block_nested_loop_join_operator_descriptor : %d\n", error_code);
			delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p);
			return NULL;
		}
		has_reference_to_extended_type = has_reference_to_persistent_extended_type_from_expression(ec.context_p);
		has_sub_query = has_sub_query_in_sql_exp(join_expr);

		has_expr_eval_context = 1;
	}

	operator_resource_counter result = has_sub_query
		? (operator_resource_counter){.buffer_counter = has_reference_to_extended_type * 2, .thread_counter = 1}
		: (operator_resource_counter){.buffer_counter = has_reference_to_extended_type * 2, .job_counter = 1};

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(2));
	uint64_t max_output_tuple_size = 8;

	{
		data_type_info* left_dti = left_input_tuple_def->type_info;

		if(left_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += left_dti->is_variable_sized ? (8 + left_dti->max_size) : (1 + left_dti->size);

		strcpy(output_dti->containees[0].field_name, "left");
		output_dti->containees[0].al.type_info = shallow_clone_into_nullable_type(left_dti);
	}

	{
		data_type_info* right_dti = right_input_tuple_def->type_info;

		if(right_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += right_dti->is_variable_sized ? (8 + right_dti->max_size) : (1 + right_dti->size);

		strcpy(output_dti->containees[1].field_name, "right");
		output_dti->containees[1].al.type_info = shallow_clone_into_nullable_type(right_dti);
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for block_nested_loop_join_operator_descriptor\n");
		free((data_type_info*) output_dti->containees[0].al.type_info);
		free((data_type_info*) output_dti->containees[1].al.type_info);
		free(output_dti);
		if(has_expr_eval_context)
			delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p);
		return NULL;
	}

	initialize_tuple_data_type_info(output_dti, "join", 0, max_output_tuple_size, 2);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_BLOCK_NESTED_LOOP_JOIN);
	d->params.block_nested_loop_join = (block_nested_loop_join_operator_params){left_input_operator_desc->index, right_input_operator_desc->index, join_expr, ptype, min_block_size};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;
	if(has_expr_eval_context)
	{
		d->expr_eval_context = malloc(sizeof(sql_expr_eval_context));
		*(d->expr_eval_context) = ec;

	}

	left_input_operator_desc->consumers_count++;
	right_input_operator_desc->consumers_count++;

	return d;
}

operator_desc* insert_sort_merge_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, const positional_accessor* left_key_element_ids, operator_desc* right_input_operator_desc, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, join_preserve_type ptype, uint32_t min_block_size)
{
	if(left_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, left_input_operator_desc) || right_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, right_input_operator_desc))
	{
		printf("input_operator_desc invalid for sort_merge_join_operator_descriptor\n");
		return NULL;
	}

	if(left_input_operator_desc->consumers_count > 0 || right_input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for sort_merge_join_operator_descriptor\n");
		return NULL;
	}

	if(key_element_count == 0)
	{
		printf("key_element_count must not be 0 for sort_merge_join_operator_descriptor\n");
		return NULL;
	}

	if(min_block_size == 0)
	{
		printf("min_block_size can not be 0 for sort_merge_join_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* left_input_tuple_def = left_input_operator_desc->output_tuple_def;
	const tuple_def* right_input_tuple_def = right_input_operator_desc->output_tuple_def;
	if(left_input_tuple_def == NULL || right_input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for sort_merge_join_operator_descriptor\n");
		return NULL;
	}

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		if(!can_compare_datum_rhendb(get_type_info_for_element_from_tuple_def(left_input_tuple_def, left_key_element_ids[i]), get_type_info_for_element_from_tuple_def(right_input_tuple_def, right_key_element_ids[i])))
		{
			printf("input_operators must produce comparable key outputs for inputs to sort_merge_join_operator_descriptor\n");
			return NULL;
		}
	}

	operator_resource_counter result = {.buffer_counter = has_extended_type_info3(left_input_tuple_def, key_element_count, left_key_element_ids, PERSISTENT_EXT_SUB_TYPE) + has_extended_type_info3(right_input_tuple_def, key_element_count, right_key_element_ids, PERSISTENT_EXT_SUB_TYPE), .job_counter = 1};

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(2));
	uint64_t max_output_tuple_size = 8;

	{
		data_type_info* left_dti = left_input_tuple_def->type_info;

		if(left_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += left_dti->is_variable_sized ? (8 + left_dti->max_size) : (1 + left_dti->size);

		strcpy(output_dti->containees[0].field_name, "left");
		output_dti->containees[0].al.type_info = shallow_clone_into_nullable_type(left_dti);
	}

	{
		data_type_info* right_dti = right_input_tuple_def->type_info;

		if(right_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += right_dti->is_variable_sized ? (8 + right_dti->max_size) : (1 + right_dti->size);

		strcpy(output_dti->containees[1].field_name, "right");
		output_dti->containees[1].al.type_info = shallow_clone_into_nullable_type(right_dti);
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for sort_merge_join_operator_descriptor\n");
		free((data_type_info*) output_dti->containees[0].al.type_info);
		free((data_type_info*) output_dti->containees[1].al.type_info);
		free(output_dti);
		return NULL;
	}

	initialize_tuple_data_type_info(output_dti, "join", 0, max_output_tuple_size, 2);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_SORT_MERGE_JOIN);
	d->params.sort_merge_join = (sort_merge_join_operator_params){left_input_operator_desc->index, deep_clone_positional_accessors(left_key_element_ids, key_element_count), right_input_operator_desc->index, deep_clone_positional_accessors(right_key_element_ids, key_element_count), deep_clone_compare_directions(key_compare_direction, key_element_count), key_element_count, ptype, min_block_size};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	left_input_operator_desc->consumers_count++;
	right_input_operator_desc->consumers_count++;

	return d;
}

operator_desc* insert_hash_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, const positional_accessor* left_key_element_ids, operator_desc* right_input_operator_desc, const positional_accessor* right_key_element_ids, uint32_t key_element_count, join_preserve_type ptype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size)
{
	if(left_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, left_input_operator_desc) || right_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, right_input_operator_desc))
	{
		printf("input_operator_desc invalid for hash_join_operator_descriptor\n");
		return NULL;
	}

	if(left_input_operator_desc->consumers_count > 0 || right_input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for hash_join_operator_descriptor\n");
		return NULL;
	}

	if(key_element_count == 0)
	{
		printf("key_element_count must not be 0 for hash_join_operator_descriptor\n");
		return NULL;
	}

	if(partitions_count == 0)
	{
		printf("partitions_count can not be 0 for hash_join_operator_descriptor\n");
		return NULL;
	}

	if(max_concurrent_jobs_count == 0)
	{
		printf("max_concurrent_jobs_count can not be 0 for hash_join_operator_descriptor\n");
		return NULL;
	}

	if(max_concurrent_jobs_queue_size == 0)
	{
		printf("max_concurrent_jobs_queue_size can not be 0 for hash_join_operator_descriptor\n");
		return NULL;
	}

	if(min_pending_buffer_size == 0)
	{
		printf("min_pending_buffer_size can not be 0 for hash_join_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* left_input_tuple_def = left_input_operator_desc->output_tuple_def;
	const tuple_def* right_input_tuple_def = right_input_operator_desc->output_tuple_def;
	if(left_input_tuple_def == NULL || right_input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for hash_join_operator_descriptor\n");
		return NULL;
	}

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		if(!are_hashably_equivalent_rhendb(get_type_info_for_element_from_tuple_def(left_input_tuple_def, left_key_element_ids[i]), get_type_info_for_element_from_tuple_def(right_input_tuple_def, right_key_element_ids[i])))
		{
			printf("input_operators must produce comparably hash equal on having equal key for inputs to hash_join_operator_descriptor\n");
			return NULL;
		}
	}

	uint64_t left_side_buffers = has_extended_type_info3(left_input_tuple_def, key_element_count, left_key_element_ids, PERSISTENT_EXT_SUB_TYPE);
	uint64_t right_side_buffers = has_extended_type_info3(right_input_tuple_def, key_element_count, right_key_element_ids, PERSISTENT_EXT_SUB_TYPE);

	operator_resource_counter result = {.buffer_counter = max_concurrent_jobs_count * (max(2 * right_side_buffers, left_side_buffers + right_side_buffers)), .job_counter = max_concurrent_jobs_count + 1};

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(2));
	uint64_t max_output_tuple_size = 8;

	{
		data_type_info* left_dti = left_input_tuple_def->type_info;

		if(left_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += left_dti->is_variable_sized ? (8 + left_dti->max_size) : (1 + left_dti->size);

		strcpy(output_dti->containees[0].field_name, "left");
		output_dti->containees[0].al.type_info = shallow_clone_into_nullable_type(left_dti);
	}

	{
		data_type_info* right_dti = right_input_tuple_def->type_info;

		if(right_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += right_dti->is_variable_sized ? (8 + right_dti->max_size) : (1 + right_dti->size);

		strcpy(output_dti->containees[1].field_name, "right");
		output_dti->containees[1].al.type_info = shallow_clone_into_nullable_type(right_dti);
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for hash_join_operator_descriptor\n");
		free((data_type_info*) output_dti->containees[0].al.type_info);
		free((data_type_info*) output_dti->containees[1].al.type_info);
		free(output_dti);
		return NULL;
	}

	initialize_tuple_data_type_info(output_dti, "join", 0, max_output_tuple_size, 2);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_HASH_JOIN);
	d->params.hash_join = (hash_join_operator_params){left_input_operator_desc->index, deep_clone_positional_accessors(left_key_element_ids, key_element_count), right_input_operator_desc->index, deep_clone_positional_accessors(right_key_element_ids, key_element_count), key_element_count, ptype, partitions_count, max_concurrent_jobs_count, max_concurrent_jobs_queue_size, min_pending_buffer_size};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	left_input_operator_desc->consumers_count++;
	right_input_operator_desc->consumers_count++;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// semi / anti joins, output is the left input's tuple_def
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_block_nested_loop_semi_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, operator_desc* right_input_operator_desc, sql_expression* join_expr, semi_join_type stype, uint32_t min_block_size)
{
	if(left_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, left_input_operator_desc) || right_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, right_input_operator_desc))
	{
		printf("input_operator_desc invalid for block_nested_loop_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(left_input_operator_desc->consumers_count > 0 || right_input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for block_nested_loop_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(min_block_size == 0)
	{
		printf("min_block_size can not be 0 for block_nested_loop_semi_join_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* left_input_tuple_def = left_input_operator_desc->output_tuple_def;
	const tuple_def* right_input_tuple_def = right_input_operator_desc->output_tuple_def;
	if(left_input_tuple_def == NULL || right_input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for block_nested_loop_semi_join_operator_descriptor\n");
		return NULL;
	}

	sql_expr_eval_context ec;
	int has_expr_eval_context = 0;
	int has_reference_to_extended_type = 0;
	int has_sub_query = 0;
	if(join_expr != NULL)
	{
		ec = get_sql_expr_eval_context_for_rhendb((tuple_def* []){(tuple_def*)left_input_tuple_def, (tuple_def*)right_input_tuple_def}, 2, qpd->curr_tx, qpd->this_sub_query_expr_eval_context);

		int error_code = 0;
		if(!is_valid_using_infer_sql_expr_for_rhendb(join_expr, &ec, &error_code))
		{
			printf("type validation errored for block_nested_loop_semi_join_operator_descriptor : %d\n", error_code);
			delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p);
			return NULL;
		}
		has_reference_to_extended_type = has_reference_to_persistent_extended_type_from_expression(ec.context_p);
		has_sub_query = has_sub_query_in_sql_exp(join_expr);

		has_expr_eval_context = 1;
	}

	operator_resource_counter result = has_sub_query
		? (operator_resource_counter){.buffer_counter = has_reference_to_extended_type * 2, .thread_counter = 1}
		: (operator_resource_counter){.buffer_counter = has_reference_to_extended_type * 2, .job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_BLOCK_NESTED_LOOP_SEMI_JOIN);
	d->params.block_nested_loop_semi_join = (block_nested_loop_semi_join_operator_params){left_input_operator_desc->index, right_input_operator_desc->index, join_expr, stype, min_block_size};
	d->output_tuple_def = left_input_tuple_def;
	d->resource_counter = result;
	if(has_expr_eval_context)
	{
		d->expr_eval_context = malloc(sizeof(sql_expr_eval_context));
		*(d->expr_eval_context) = ec;

	}

	left_input_operator_desc->consumers_count++;
	right_input_operator_desc->consumers_count++;

	return d;
}

operator_desc* insert_sort_merge_semi_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, const positional_accessor* left_key_element_ids, operator_desc* right_input_operator_desc, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, semi_join_type stype, uint32_t min_block_size)
{
	if(left_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, left_input_operator_desc) || right_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, right_input_operator_desc))
	{
		printf("input_operator_desc invalid for sort_merge_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(left_input_operator_desc->consumers_count > 0 || right_input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for sort_merge_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(key_element_count == 0)
	{
		printf("key_element_count must not be 0 for sort_merge_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(min_block_size == 0)
	{
		printf("min_block_size can not be 0 for sort_merge_semi_join_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* left_input_tuple_def = left_input_operator_desc->output_tuple_def;
	const tuple_def* right_input_tuple_def = right_input_operator_desc->output_tuple_def;
	if(left_input_tuple_def == NULL || right_input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for sort_merge_semi_join_operator_descriptor\n");
		return NULL;
	}

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		if(!can_compare_datum_rhendb(get_type_info_for_element_from_tuple_def(left_input_tuple_def, left_key_element_ids[i]), get_type_info_for_element_from_tuple_def(right_input_tuple_def, right_key_element_ids[i])))
		{
			printf("input_operators must produce comparable key outputs for inputs to sort_merge_semi_join_operator_descriptor\n");
			return NULL;
		}
	}

	operator_resource_counter result = {.buffer_counter = has_extended_type_info3(left_input_tuple_def, key_element_count, left_key_element_ids, PERSISTENT_EXT_SUB_TYPE) + has_extended_type_info3(right_input_tuple_def, key_element_count, right_key_element_ids, PERSISTENT_EXT_SUB_TYPE), .job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_SORT_MERGE_SEMI_JOIN);
	d->params.sort_merge_semi_join = (sort_merge_semi_join_operator_params){left_input_operator_desc->index, deep_clone_positional_accessors(left_key_element_ids, key_element_count), right_input_operator_desc->index, deep_clone_positional_accessors(right_key_element_ids, key_element_count), deep_clone_compare_directions(key_compare_direction, key_element_count), key_element_count, stype, min_block_size};
	d->output_tuple_def = left_input_tuple_def;
	d->resource_counter = result;

	left_input_operator_desc->consumers_count++;
	right_input_operator_desc->consumers_count++;

	return d;
}

operator_desc* insert_hash_semi_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, const positional_accessor* left_key_element_ids, operator_desc* right_input_operator_desc, const positional_accessor* right_key_element_ids, uint32_t key_element_count, semi_join_type stype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size)
{
	if(left_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, left_input_operator_desc) || right_input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, right_input_operator_desc))
	{
		printf("input_operator_desc invalid for hash_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(left_input_operator_desc->consumers_count > 0 || right_input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for hash_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(key_element_count == 0)
	{
		printf("key_element_count must not be 0 for hash_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(partitions_count == 0)
	{
		printf("partitions_count can not be 0 for hash_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(max_concurrent_jobs_count == 0)
	{
		printf("max_concurrent_jobs_count can not be 0 for hash_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(max_concurrent_jobs_queue_size == 0)
	{
		printf("max_concurrent_jobs_queue_size can not be 0 for hash_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(min_pending_buffer_size == 0)
	{
		printf("min_pending_buffer_size can not be 0 for hash_semi_join_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* left_input_tuple_def = left_input_operator_desc->output_tuple_def;
	const tuple_def* right_input_tuple_def = right_input_operator_desc->output_tuple_def;
	if(left_input_tuple_def == NULL || right_input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for hash_semi_join_operator_descriptor\n");
		return NULL;
	}

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		if(!are_hashably_equivalent_rhendb(get_type_info_for_element_from_tuple_def(left_input_tuple_def, left_key_element_ids[i]), get_type_info_for_element_from_tuple_def(right_input_tuple_def, right_key_element_ids[i])))
		{
			printf("input_operators must produce comparably hash equal on having equal key for inputs to hash_semi_join_operator_descriptor\n");
			return NULL;
		}
	}

	uint64_t left_side_buffers = has_extended_type_info3(left_input_tuple_def, key_element_count, left_key_element_ids, PERSISTENT_EXT_SUB_TYPE);
	uint64_t right_side_buffers = has_extended_type_info3(right_input_tuple_def, key_element_count, right_key_element_ids, PERSISTENT_EXT_SUB_TYPE);

	operator_resource_counter result = {.buffer_counter = max_concurrent_jobs_count * (max(2 * right_side_buffers, left_side_buffers + right_side_buffers)), .job_counter = max_concurrent_jobs_count + 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_HASH_SEMI_JOIN);
	d->params.hash_semi_join = (hash_semi_join_operator_params){left_input_operator_desc->index, deep_clone_positional_accessors(left_key_element_ids, key_element_count), right_input_operator_desc->index, deep_clone_positional_accessors(right_key_element_ids, key_element_count), key_element_count, stype, partitions_count, max_concurrent_jobs_count, max_concurrent_jobs_queue_size, min_pending_buffer_size};
	d->output_tuple_def = left_input_tuple_def;
	d->resource_counter = result;

	left_input_operator_desc->consumers_count++;
	right_input_operator_desc->consumers_count++;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// table write / lookup ops : output schema is the same flag-based context tuple as the scan operator
// ---------------------------------------------------------------------------------------------------

// builds the flag-based (table_id / partition_id / tuple_pointer / heap_tuple) output context tuple_def, exactly
// as the scan / pointer_lookup operators build it; all its columns are borrowed dtis
tuple_def* build_table_operator_output_tuple_def(rhendb* rdb, const fetched_table* ftabl, int output_flags, const char* context_name)
{
	if(output_flags == 0)
		return NULL;

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(10)); // over allocation for being safe from future changes

	uint32_t output_dti_element_count = 0;
	uint64_t output_tuple_max_size = 8;

	if(MUST_OUTPUT_TABLE_ID(output_flags))
	{
		strncpy(output_dti->containees[output_dti_element_count].field_name, "table_id", 64);
		output_dti->containees[output_dti_element_count].al.type_info = UINT_NON_NULLABLE[8];

		output_tuple_max_size += 8;
		output_dti_element_count++;
	}

	if(MUST_OUTPUT_PARTITION_ID(output_flags))
	{
		strncpy(output_dti->containees[output_dti_element_count].field_name, "partition_id", 64);
		output_dti->containees[output_dti_element_count].al.type_info = UINT_NON_NULLABLE[8];

		output_tuple_max_size += 8;
		output_dti_element_count++;
	}

	if(MUST_OUTPUT_TUPLE_POINTER_ID(output_flags))
	{
		strncpy(output_dti->containees[output_dti_element_count].field_name, "tuple_pointer", 64);
		output_dti->containees[output_dti_element_count].al.type_info = &(rdb->persistent_acid_rage_engine.pam_p->pas.tuple_pointer_type_info);

		output_tuple_max_size += sizeof(tuple_pointer);
		output_dti_element_count++;
	}

	if(MUST_OUTPUT_HEAP_TUPLE(output_flags))
	{
		strncpy(output_dti->containees[output_dti_element_count].field_name, ftabl->table_info.name, 64);
		output_dti->containees[output_dti_element_count].al.type_info = ftabl->final_readers_tuple_def.type_info;

		output_tuple_max_size += 8 + get_maximum_tuple_size(&(ftabl->final_readers_tuple_def));
		output_dti_element_count++;
	}

	initialize_tuple_data_type_info(output_dti, context_name, 0, output_tuple_max_size, output_dti_element_count);

	initialize_tuple_def(output_tuple_def, output_dti);

	return output_tuple_def;
}

operator_desc* insert_pointer_lookup_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, const positional_accessor* partition_id_from_source_positional_accessor, const positional_accessor* tuple_pointer_from_source_positional_accessor, const fetched_table* ftabl, uint64_t lookup_batch_size, int output_flags, int additional_flags)
{
	if(input_operator_desc == NULL || !does_query_plan_descriptor_contains_operator_descriptor(qpd, input_operator_desc))
	{
		printf("input_operator_desc invalid for pointer_lookup_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->consumers_count > 0)
	{
		printf("an input_operator_desc already has a consumer for pointer_lookup_operator_descriptor\n");
		return NULL;
	}

	if(qpd->curr_tx->snapshot == NULL)
	{
		printf("must have a snapshot for pointer_lookup_operator_descriptor\n");
		return NULL;
	}

	if(ftabl == NULL)
	{
		printf("ftabl can not be NULL for pointer_lookup_operator_descriptor\n");
		return NULL;
	}

	if(lookup_batch_size == 0)
	{
		printf("must have a non-zero lookup_batch_size for pointer_lookup_operator_descriptor\n");
		return NULL;
	}

	if(input_operator_desc->output_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for pointer_lookup_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.buffer_counter = 1, .job_counter = 1, .thread_counter = 1}; // read lock a page, read tuples and exit, so 1 buffer atmost

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_POINTER_LOOKUP);
	d->params.pointer_lookup = (pointer_lookup_operator_params){input_operator_desc->index, deep_clone_positional_accessors(partition_id_from_source_positional_accessor, 1), deep_clone_positional_accessors(tuple_pointer_from_source_positional_accessor, 1), ftabl, lookup_batch_size, output_flags, additional_flags};
	d->output_tuple_def = build_table_operator_output_tuple_def(qpd->curr_tx->rdb, ftabl, output_flags, "looked_up_tuple_context");
	d->resource_counter = result;

	input_operator_desc->consumers_count++;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// destroy : free everything the descriptor owns, like each operator's free_resources() does
// ---------------------------------------------------------------------------------------------------

void destroy_query_plan_descriptor(query_plan_desc* qpd)
{
	if(qpd == NULL)
		return;

	uint64_t count = get_element_count_arraylist(&(qpd->operator_descriptors));
	for(uint64_t i = 0; i < count; i++)
	{
		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), i);

		tuple_def* output_tuple_def = (tuple_def*) d->output_tuple_def;

		// free the output_tuple_def exactly as the matching operator's free_resources() does
		// (leaves and schema preserving operators borrow their input/param def, so they are not freed here)
		switch(d->type)
		{
			case OP_PROJECTION :
			{
				for(uint32_t c = 0; c < d->params.projection.projection_descriptions_count; c++)
				{
					if(d->params.projection.projection_descriptions[c].type == PROJECT_IDENTITY)
						free((data_type_info*) output_tuple_def->type_info->containees[c].al.type_info);
				}
				free((data_type_info*) output_tuple_def->type_info);
				free(output_tuple_def);
				break;
			}
			case OP_ROW_RENAME :
			{
				free((data_type_info*) output_tuple_def->type_info);
				free(output_tuple_def);
				break;
			}
			case OP_SIMPLE_AGGREGATION :
			{
				free((data_type_info*) output_tuple_def->type_info);
				free(output_tuple_def);
				break;
			}
			case OP_SORTED_AGGREGATION :
			{
				for(uint32_t c = 0; c < d->params.sorted_aggregation.key_element_count; c++)
					free((data_type_info*) output_tuple_def->type_info->containees[c].al.type_info);
				free((data_type_info*) output_tuple_def->type_info);
				free(output_tuple_def);
				break;
			}
			case OP_HASH_AGGREGATION :
			{
				for(uint32_t c = 0; c < d->params.hash_aggregation.key_element_count; c++)
					free((data_type_info*) output_tuple_def->type_info->containees[c].al.type_info);
				free((data_type_info*) output_tuple_def->type_info);
				free(output_tuple_def);
				break;
			}
			case OP_BLOCK_NESTED_LOOP_JOIN :
			case OP_SORT_MERGE_JOIN :
			case OP_HASH_JOIN :
			{
				free((data_type_info*) output_tuple_def->type_info->containees[0].al.type_info);
				free((data_type_info*) output_tuple_def->type_info->containees[1].al.type_info);
				free((data_type_info*) output_tuple_def->type_info);
				free(output_tuple_def);
				break;
			}
			case OP_SCAN :
			case OP_POINTER_LOOKUP :
			{
				// the table op's output context tuple_def borrows every column dti, exactly like the scan op
				if(output_tuple_def != NULL)
				{
					free((data_type_info*) output_tuple_def->type_info);
					free(output_tuple_def);
				}
				break;
			}
			default :
				break;
		}

		// delete the dataset created for the constant dataset operator
		if(d->type == OP_CONSTANT_DATASET)
			delete_interim_tuple_store(d->params.constant_dataset.constant_dataset_p);

		// free the deep-cloned positional_accessors / compare_directions / arrays this operator descriptor owns.
		// NOTE : freeing the aggregate_input_element_ids reads aggregate_functions[i]->input_type_infos_count, so it
		// must run before the aggregate_functions are destroyed below.
		switch(d->type)
		{
			case OP_SORT :
				free_positional_accessors(d->params.sort.key_element_ids, d->params.sort.key_element_count);
				free(d->params.sort.key_compare_direction);
				break;
			case OP_MERGE_SORTED_INPUTS :
				free_positional_accessors(d->params.merge_sorted_inputs.key_element_ids, d->params.merge_sorted_inputs.key_element_count);
				free(d->params.merge_sorted_inputs.key_compare_direction);
				break;
			case OP_SIMPLE_AGGREGATION :
				free_aggregate_input_element_ids(d->params.simple_aggregation.aggregate_input_element_ids, d->params.simple_aggregation.aggregate_functions_count, d->params.simple_aggregation.aggregate_functions);
				break;
			case OP_SORTED_AGGREGATION :
				free_positional_accessors(d->params.sorted_aggregation.key_element_ids, d->params.sorted_aggregation.key_element_count);
				free_aggregate_input_element_ids(d->params.sorted_aggregation.aggregate_input_element_ids, d->params.sorted_aggregation.aggregate_functions_count, d->params.sorted_aggregation.aggregate_functions);
				break;
			case OP_HASH_AGGREGATION :
				free_positional_accessors(d->params.hash_aggregation.key_element_ids, d->params.hash_aggregation.key_element_count);
				free_aggregate_input_element_ids(d->params.hash_aggregation.aggregate_input_element_ids, d->params.hash_aggregation.aggregate_functions_count, d->params.hash_aggregation.aggregate_functions);
				break;
			case OP_PROJECTION :
				free_projection_descriptions(d->params.projection.projection_descriptions, d->params.projection.projection_descriptions_count);
				break;
			case OP_SORT_MERGE_JOIN :
				free_positional_accessors(d->params.sort_merge_join.left_key_element_ids, d->params.sort_merge_join.key_element_count);
				free_positional_accessors(d->params.sort_merge_join.right_key_element_ids, d->params.sort_merge_join.key_element_count);
				free(d->params.sort_merge_join.key_compare_direction);
				break;
			case OP_HASH_JOIN :
				free_positional_accessors(d->params.hash_join.left_key_element_ids, d->params.hash_join.key_element_count);
				free_positional_accessors(d->params.hash_join.right_key_element_ids, d->params.hash_join.key_element_count);
				break;
			case OP_SORT_MERGE_SEMI_JOIN :
				free_positional_accessors(d->params.sort_merge_semi_join.left_key_element_ids, d->params.sort_merge_semi_join.key_element_count);
				free_positional_accessors(d->params.sort_merge_semi_join.right_key_element_ids, d->params.sort_merge_semi_join.key_element_count);
				free(d->params.sort_merge_semi_join.key_compare_direction);
				break;
			case OP_HASH_SEMI_JOIN :
				free_positional_accessors(d->params.hash_semi_join.left_key_element_ids, d->params.hash_semi_join.key_element_count);
				free_positional_accessors(d->params.hash_semi_join.right_key_element_ids, d->params.hash_semi_join.key_element_count);
				break;
			case OP_POINTER_LOOKUP :
				free_positional_accessors(d->params.pointer_lookup.partition_id_from_source_positional_accessor, 1);
				free_positional_accessors(d->params.pointer_lookup.tuple_pointer_from_source_positional_accessor, 1);
				break;
			default :
				break;
		}

		// delete the aggregate_functions, as free_resources() does
		// NOTE : a query_plan built from this descriptor also destroys these on its own teardown; reconciling
		// that double ownership (so a descriptor can be re-create()d after a run) is the "deal with it later" item.
		if(d->type == OP_SIMPLE_AGGREGATION)
		{
			for(uint32_t a = 0; a < d->params.simple_aggregation.aggregate_functions_count; a++)
			{
				aggregate_function* af = d->params.simple_aggregation.aggregate_functions[a];
				if(af != NULL && af->destroy_aggregate_function != NULL)
					af->destroy_aggregate_function(af);
			}
			free((d->params.simple_aggregation.aggregate_functions));
		}
		else if(d->type == OP_SORTED_AGGREGATION)
		{
			for(uint32_t a = 0; a < d->params.sorted_aggregation.aggregate_functions_count; a++)
			{
				aggregate_function* af = d->params.sorted_aggregation.aggregate_functions[a];
				if(af != NULL && af->destroy_aggregate_function != NULL)
					af->destroy_aggregate_function(af);
			}
			free((d->params.sorted_aggregation.aggregate_functions));
		}
		else if(d->type == OP_HASH_AGGREGATION)
		{
			for(uint32_t a = 0; a < d->params.hash_aggregation.aggregate_functions_count; a++)
			{
				aggregate_function* af = d->params.hash_aggregation.aggregate_functions[a];
				if(af != NULL && af->destroy_aggregate_function != NULL)
					af->destroy_aggregate_function(af);
			}
			free((d->params.hash_aggregation.aggregate_functions));
		}

		// free the cloned input index arrays
		if(d->type == OP_UNION)
			free(d->params.union_op.input_operator_indices);
		else if(d->type == OP_MERGE_SORTED_INPUTS)
			free(d->params.merge_sorted_inputs.input_operator_indices);

		if(d->type == OP_PROJECTION)
			free(d->params.projection.output_name);

		if(d->type == OP_ROW_RENAME)
		{
			free(d->params.row_rename.output_name);
			free_string_array(d->params.row_rename.column_names, d->params.row_rename.columns_count);
		}

		// delete the expression evaluation context template, if this operator built one
		if(d->expr_eval_context != NULL)
		{
			delete_context_p_for_sql_expr_eval_context_for_rhendb(d->expr_eval_context->context_p);
			free(d->expr_eval_context);
		}

		free(d);
	}

	deinitialize_arraylist(&(qpd->operator_descriptors));

	free(qpd);
}