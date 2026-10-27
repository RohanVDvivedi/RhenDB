#include<rhendb/query_plan_descriptor.h>

#include<rhendb/rhendb.h>
#include<rhendb/nullable_type_info_maker.h>

#include<rhendb/function_compare.h>
#include<rhendb/function_hash.h>

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
	d->has_expr_eval_context = 0;

	initialize_arraylist(&(d->transformer_descriptors), 4);

	if(is_full_arraylist(&(qpd->operator_descriptors)) && !expand_arraylist(&(qpd->operator_descriptors)))
		exit(-1);
	push_back_to_arraylist(&(qpd->operator_descriptors), d);

	return d;
}

query_plan_desc* get_new_query_plan_descriptor(transaction* tx, const sql_expr_eval_context* this_sub_query_expr_eval_context)
{
	query_plan_desc* qpd = malloc(sizeof(query_plan_desc));
	if(qpd == NULL)
		exit(-1);

	qpd->tx = tx;
	qpd->this_sub_query_expr_eval_context = this_sub_query_expr_eval_context;

	initialize_arraylist(&(qpd->operator_descriptors), 16);

	return qpd;
}

const tuple_def* get_output_tuple_def_for_operator_descriptor(const operator_desc* operator_descriptor)
{
	if(get_element_count_arraylist(&(operator_descriptor->transformer_descriptors)) == 0)
		return operator_descriptor->output_tuple_def;

	return ((const tuple_transformer_desc*) get_from_front_of_arraylist(&(operator_descriptor->transformer_descriptors), get_element_count_arraylist(&(operator_descriptor->transformer_descriptors)) - 1))->output_def;
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
		if(arr[i].type == PROJECT_IDENTITY)
			free(arr[i].pa.positions);
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

// deep clones an array of `count` strings
static char** deep_clone_strings(char const * const * src, uint32_t count)
{
	if(src == NULL || count == 0)
		return NULL;

	char** cloned = malloc(sizeof(char*) * count);
	for(uint32_t i = 0; i < count; i++)
		cloned[i] = deep_clone_string(src[i]);
	return cloned;
}

static void free_strings(char** arr, uint32_t count)
{
	if(arr == NULL)
		return;

	for(uint32_t i = 0; i < count; i++)
		free(arr[i]);
	free(arr);
}

// deep clones a contiguous array of `count` uint32_t-s (column_ids_to_transform style)
static uint32_t* deep_clone_uint32_array(const uint32_t* src, uint32_t count)
{
	if(src == NULL || count == 0)
		return NULL;

	uint32_t* cloned = malloc(sizeof(uint32_t) * count);
	for(uint32_t i = 0; i < count; i++)
		cloned[i] = src[i];
	return cloned;
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

	if(qpd->tx->snapshot == NULL)
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
			output_dti->containees[output_dti_element_count].al.type_info = &(qpd->tx->rdb->persistent_acid_rage_engine.pam_p->pas.tuple_pointer_type_info);

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

operator_desc* insert_identity_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for identity_operator_descriptor\n");
		return NULL;
	}

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	const tuple_def* input_tuple_def = get_output_tuple_def_for_operator_descriptor(input_operator_desc);
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for identity_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_IDENTITY);
	d->params.identity = (identity_operator_params){input_operator_index};
	d->output_tuple_def = input_tuple_def;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_offset_limit_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, tuples_down_counter offset_counter, tuples_down_counter limit_counter)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for offset_limit_operator_descriptor\n");
		return NULL;
	}

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	const tuple_def* input_tuple_def = get_output_tuple_def_for_operator_descriptor(input_operator_desc);
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
	d->params.offset_limit = (offset_limit_operator_params){input_operator_index, offset_counter, limit_counter};
	d->output_tuple_def = input_tuple_def;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_selection_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, sql_expression* expr)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for selection_operator_descriptor\n");
		return NULL;
	}

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	const tuple_def* input_tuple_def = get_output_tuple_def_for_operator_descriptor(input_operator_desc);
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for selection_operator_descriptor\n");
		return NULL;
	}

	sql_expr_eval_context ec = get_sql_expr_eval_context_for_rhendb((tuple_def**)(&input_tuple_def), 1, qpd->tx, qpd->this_sub_query_expr_eval_context);

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
	d->params.selection = (selection_operator_params){input_operator_index, expr};
	d->output_tuple_def = input_tuple_def;
	d->resource_counter = result;
	d->expr_eval_context = ec;
	d->has_expr_eval_context = 1;

	return d;
}

operator_desc* insert_sort_operator_descriptor(query_plan_desc* qpd, tuples_down_counter result_counter, uint64_t input_operator_index, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint64_t min_run_size, uint32_t N_way_sort, uint32_t max_concurrent_jobs_count)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for sort_operator_descriptor\n");
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

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	const tuple_def* record_def = get_output_tuple_def_for_operator_descriptor(input_operator_desc);
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
	d->params.sort = (sort_operator_params){result_counter, input_operator_index, key_element_count, deep_clone_positional_accessors(key_element_ids, key_element_count), deep_clone_compare_directions(key_compare_direction, key_element_count), min_run_size, N_way_sort, max_concurrent_jobs_count};
	d->output_tuple_def = record_def;
	d->resource_counter = result;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// projection
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_projection_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, projection_description* projection_descriptions, uint32_t projection_descriptions_count)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for projection_operator_descriptor\n");
		return NULL;
	}

	if(projection_descriptions == NULL || projection_descriptions_count == 0)
	{
		printf("projection_descriptions can not be empty for projection_operator_descriptor\n");
		return NULL;
	}

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	const tuple_def* input_tuple_def = get_output_tuple_def_for_operator_descriptor(input_operator_desc);
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for projection_operator_descriptor\n");
		return NULL;
	}

	sql_expr_eval_context ec = get_sql_expr_eval_context_for_rhendb((tuple_def**)(&input_tuple_def), 1, qpd->tx, qpd->this_sub_query_expr_eval_context);

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

	initialize_tuple_data_type_info(output_dti, "projection", 0, max_output_tuple_size, projection_descriptions_count);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_PROJECTION);
	d->params.projection = (projection_operator_params){input_operator_index, deep_clone_projection_descriptions(projection_descriptions, projection_descriptions_count), projection_descriptions_count};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;
	d->expr_eval_context = ec;
	d->has_expr_eval_context = 1;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// aggregations
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_simple_aggregation_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for simple_aggregation_operator_descriptor\n");
		return NULL;
	}

	if(aggregate_functions_count == 0)
	{
		printf("aggregate_functions_count can not be 0 for simple_aggregation_operator_descriptor\n");
		return NULL;
	}

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	if(get_output_tuple_def_for_operator_descriptor(input_operator_desc) == NULL)
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
	d->params.simple_aggregation = (simple_aggregation_operator_params){input_operator_index, aggregate_functions_count, shallow_clone_aggregate_functions(aggregate_functions, aggregate_functions_count), deep_clone_aggregate_input_element_ids(aggregate_input_element_ids, aggregate_functions_count, aggregate_functions)};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_sorted_aggregation_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for sorted_aggregation_operator_descriptor\n");
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

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	const tuple_def* input_tuple_def = get_output_tuple_def_for_operator_descriptor(input_operator_desc);
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
	d->params.sorted_aggregation = (sorted_aggregation_operator_params){input_operator_index, key_element_count, deep_clone_positional_accessors(key_element_ids, key_element_count), aggregate_functions_count, shallow_clone_aggregate_functions(aggregate_functions, aggregate_functions_count), deep_clone_aggregate_input_element_ids(aggregate_input_element_ids, aggregate_functions_count, aggregate_functions)};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_hash_aggregation_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_build_tuple_buffer_size)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for hash_aggregation_operator_descriptor\n");
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

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	const tuple_def* input_tuple_def = get_output_tuple_def_for_operator_descriptor(input_operator_desc);
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
	d->params.hash_aggregation = (hash_aggregation_operator_params){input_operator_index, key_element_count, deep_clone_positional_accessors(key_element_ids, key_element_count), aggregate_functions_count, shallow_clone_aggregate_functions(aggregate_functions, aggregate_functions_count), deep_clone_aggregate_input_element_ids(aggregate_input_element_ids, aggregate_functions_count, aggregate_functions), partitions_count, max_concurrent_jobs_count, max_concurrent_jobs_queue_size, min_build_tuple_buffer_size};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// sinks (no output tuple_def)
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_consumer_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, int (*consumer)(void* consumer_context, const void* tuple, const tuple_def* input_tuple_def), void* consumer_context)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for consumer_operator_descriptor\n");
		return NULL;
	}

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	if(get_output_tuple_def_for_operator_descriptor(input_operator_desc) == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for consumer_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_CONSUMER);
	d->params.consumer = (consumer_operator_params){input_operator_index, consumer, consumer_context};
	d->output_tuple_def = NULL;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_stream_output_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, stream* out_strm)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for stream_output_operator_descriptor\n");
		return NULL;
	}

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	if(get_output_tuple_def_for_operator_descriptor(input_operator_desc) == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for stream_output_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_STREAM_OUTPUT);
	d->params.stream_output = (stream_output_operator_params){input_operator_index, out_strm};
	d->output_tuple_def = NULL;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_subquery_output_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, sync_pipe* out_pipe)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for subquery_output_operator_descriptor\n");
		return NULL;
	}

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	if(get_output_tuple_def_for_operator_descriptor(input_operator_desc) == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for subquery_output_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.thread_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_SUBQUERY_OUTPUT);
	d->params.subquery_output = (subquery_output_operator_params){input_operator_index, out_pipe};
	d->output_tuple_def = NULL;
	d->resource_counter = result;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// multiple inputs
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_union_operator_descriptor(query_plan_desc* qpd, const uint64_t* input_operator_indices, uint64_t input_operators_count)
{
	if(input_operators_count == 0 || input_operator_indices == NULL)
	{
		printf("input_operators can not be empty for union_operator_descriptor\n");
		return NULL;
	}

	for(uint64_t i = 0; i < input_operators_count; i++)
	{
		if(input_operator_indices[i] >= get_element_count_arraylist(&(qpd->operator_descriptors)))
		{
			printf("input_operator_index out of range for union_operator_descriptor\n");
			return NULL;
		}
	}

	const tuple_def* input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_indices[0]));
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for union_operator_descriptor\n");
		return NULL;
	}
	for(uint64_t i = 1; i < input_operators_count; i++)
	{
		const tuple_def* other_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_indices[i]));
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
		input_operator_indices_cloned[i] = input_operator_indices[i];

	operator_resource_counter result = {.job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_UNION);
	d->params.union_op = (union_operator_params){input_operator_indices_cloned, input_operators_count};
	d->output_tuple_def = input_tuple_def;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_merge_sorted_inputs_operator_descriptor(query_plan_desc* qpd, const uint64_t* input_operator_indices, uint64_t input_operators_count, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction)
{
	if(input_operators_count == 0 || input_operator_indices == NULL)
	{
		printf("input_operators can not be empty for merge_sorted_inputs_operator_descriptor\n");
		return NULL;
	}

	for(uint64_t i = 0; i < input_operators_count; i++)
	{
		if(input_operator_indices[i] >= get_element_count_arraylist(&(qpd->operator_descriptors)))
		{
			printf("input_operator_index out of range for merge_sorted_inputs_operator_descriptor\n");
			return NULL;
		}
	}

	const tuple_def* record_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_indices[0]));
	if(record_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for merge_sorted_inputs_operator_descriptor\n");
		return NULL;
	}
	for(uint64_t i = 1; i < input_operators_count; i++)
	{
		const tuple_def* other_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_indices[i]));
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
		input_operator_indices_cloned[i] = input_operator_indices[i];

	operator_resource_counter result = {.buffer_counter = 2 * has_extended_type_info3(record_def, key_element_count, key_element_ids, PERSISTENT_EXT_SUB_TYPE), .job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_MERGE_SORTED_INPUTS);
	d->params.merge_sorted_inputs = (merge_sorted_inputs_operator_params){input_operator_indices_cloned, input_operators_count, key_element_count, deep_clone_positional_accessors(key_element_ids, key_element_count), deep_clone_compare_directions(key_compare_direction, key_element_count)};
	d->output_tuple_def = record_def;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_result_match_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index_0, uint64_t input_operator_index_1)
{
	if(input_operator_index_0 >= get_element_count_arraylist(&(qpd->operator_descriptors)) || input_operator_index_1 >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for result_match_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* input_tuple_def_0 = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index_0));
	const tuple_def* input_tuple_def_1 = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index_1));
	if(input_tuple_def_0 == NULL || input_tuple_def_1 == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for result_match_operator_descriptor\n");
		return NULL;
	}
	if(!can_compare_datum_rhendb(input_tuple_def_0->type_info, input_tuple_def_1->type_info))
	{
		printf("input operator descriptor's for result_match_operator_descriptor are not comparable\n");
		return NULL;
	}

	operator_resource_counter result = {.buffer_counter = has_extended_type_info2(input_tuple_def_0, SELF, PERSISTENT_EXT_SUB_TYPE) + has_extended_type_info2(input_tuple_def_1, SELF, PERSISTENT_EXT_SUB_TYPE), .job_counter = 1};

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_RESULT_MATCH);
	d->params.result_match.input_operator_indices[0] = input_operator_index_0;
	d->params.result_match.input_operator_indices[1] = input_operator_index_1;
	d->output_tuple_def = NULL; // does not output anything
	d->resource_counter = result;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// joins ([0] = left, [1] = right), output is the two columns "left" and "right"
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_block_nested_loop_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, uint64_t right_input_operator_index, sql_expression* join_expr, join_preserve_type ptype, uint32_t min_block_size)
{
	if(left_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)) || right_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for block_nested_loop_join_operator_descriptor\n");
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

	const tuple_def* left_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), left_input_operator_index));
	const tuple_def* right_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), right_input_operator_index));
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
		ec = get_sql_expr_eval_context_for_rhendb((tuple_def* []){(tuple_def*)left_input_tuple_def, (tuple_def*)right_input_tuple_def}, 2, qpd->tx, qpd->this_sub_query_expr_eval_context);

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
	d->params.block_nested_loop_join = (block_nested_loop_join_operator_params){left_input_operator_index, right_input_operator_index, join_expr, ptype, min_block_size};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;
	if(has_expr_eval_context)
	{
		d->expr_eval_context = ec;
		d->has_expr_eval_context = 1;
	}

	return d;
}

operator_desc* insert_sort_merge_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, join_preserve_type ptype, uint32_t min_block_size)
{
	if(left_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)) || right_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for sort_merge_join_operator_descriptor\n");
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

	const tuple_def* left_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), left_input_operator_index));
	const tuple_def* right_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), right_input_operator_index));
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
	d->params.sort_merge_join = (sort_merge_join_operator_params){left_input_operator_index, deep_clone_positional_accessors(left_key_element_ids, key_element_count), right_input_operator_index, deep_clone_positional_accessors(right_key_element_ids, key_element_count), deep_clone_compare_directions(key_compare_direction, key_element_count), key_element_count, ptype, min_block_size};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_hash_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, uint32_t key_element_count, join_preserve_type ptype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size)
{
	if(left_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)) || right_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for hash_join_operator_descriptor\n");
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

	const tuple_def* left_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), left_input_operator_index));
	const tuple_def* right_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), right_input_operator_index));
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
	d->params.hash_join = (hash_join_operator_params){left_input_operator_index, deep_clone_positional_accessors(left_key_element_ids, key_element_count), right_input_operator_index, deep_clone_positional_accessors(right_key_element_ids, key_element_count), key_element_count, ptype, partitions_count, max_concurrent_jobs_count, max_concurrent_jobs_queue_size, min_pending_buffer_size};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = result;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// semi / anti joins, output is the left input's tuple_def
// ---------------------------------------------------------------------------------------------------

operator_desc* insert_block_nested_loop_semi_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, uint64_t right_input_operator_index, sql_expression* join_expr, semi_join_type stype, uint32_t min_block_size)
{
	if(left_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)) || right_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for block_nested_loop_semi_join_operator_descriptor\n");
		return NULL;
	}

	if(min_block_size == 0)
	{
		printf("min_block_size can not be 0 for block_nested_loop_semi_join_operator_descriptor\n");
		return NULL;
	}

	const tuple_def* left_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), left_input_operator_index));
	const tuple_def* right_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), right_input_operator_index));
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
		ec = get_sql_expr_eval_context_for_rhendb((tuple_def* []){(tuple_def*)left_input_tuple_def, (tuple_def*)right_input_tuple_def}, 2, qpd->tx, qpd->this_sub_query_expr_eval_context);

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
	d->params.block_nested_loop_semi_join = (block_nested_loop_semi_join_operator_params){left_input_operator_index, right_input_operator_index, join_expr, stype, min_block_size};
	d->output_tuple_def = left_input_tuple_def;
	d->resource_counter = result;
	if(has_expr_eval_context)
	{
		d->expr_eval_context = ec;
		d->has_expr_eval_context = 1;
	}

	return d;
}

operator_desc* insert_sort_merge_semi_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, semi_join_type stype, uint32_t min_block_size)
{
	if(left_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)) || right_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for sort_merge_semi_join_operator_descriptor\n");
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

	const tuple_def* left_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), left_input_operator_index));
	const tuple_def* right_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), right_input_operator_index));
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
	d->params.sort_merge_semi_join = (sort_merge_semi_join_operator_params){left_input_operator_index, deep_clone_positional_accessors(left_key_element_ids, key_element_count), right_input_operator_index, deep_clone_positional_accessors(right_key_element_ids, key_element_count), deep_clone_compare_directions(key_compare_direction, key_element_count), key_element_count, stype, min_block_size};
	d->output_tuple_def = left_input_tuple_def;
	d->resource_counter = result;

	return d;
}

operator_desc* insert_hash_semi_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, uint32_t key_element_count, semi_join_type stype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size)
{
	if(left_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)) || right_input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for hash_semi_join_operator_descriptor\n");
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

	const tuple_def* left_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), left_input_operator_index));
	const tuple_def* right_input_tuple_def = get_output_tuple_def_for_operator_descriptor((operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), right_input_operator_index));
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
	d->params.hash_semi_join = (hash_semi_join_operator_params){left_input_operator_index, deep_clone_positional_accessors(left_key_element_ids, key_element_count), right_input_operator_index, deep_clone_positional_accessors(right_key_element_ids, key_element_count), key_element_count, stype, partitions_count, max_concurrent_jobs_count, max_concurrent_jobs_queue_size, min_pending_buffer_size};
	d->output_tuple_def = left_input_tuple_def;
	d->resource_counter = result;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// table write / lookup ops : output schema is the same flag-based context tuple as the scan operator
// ---------------------------------------------------------------------------------------------------

// builds the flag-based (table_id / partition_id / tuple_pointer / heap_tuple) output context tuple_def, exactly
// as the scan / insertion / deletion / pointer_lookup operators build it; all its columns are borrowed dtis
static tuple_def* build_table_operator_output_tuple_def(rhendb* rdb, const fetched_table* ftabl, int output_flags, const char* context_name)
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

operator_desc* insert_insertion_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, const positional_accessor* insertion_from_source_positional_accessors, const fetched_table* ftabl, int output_flags, heap_table_notifier* global_heap_notifier, int additional_flags)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for insertion_operator_descriptor\n");
		return NULL;
	}

	if(qpd->tx->snapshot == NULL)
	{
		printf("must have writable snapshot for insertion_operator_descriptor\n");
		return NULL;
	}

	if(!qpd->tx->snapshot->has_self_transaction_id)
	{
		printf("must have self_transaction_id for insertion_operator_descriptor\n");
		return NULL;
	}

	if(ftabl == NULL)
	{
		printf("ftabl can not be NULL for insertion_operator_descriptor\n");
		return NULL;
	}

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	const tuple_def* input_tuple_def = get_output_tuple_def_for_operator_descriptor(input_operator_desc);
	if(input_tuple_def == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for insertion_operator_descriptor\n");
		return NULL;
	}

	// validate each source column's type against the destination partition column kind, exactly as setup_insertion_operator does
	const tuple_def* partition_tuple_def = &(ftabl->table_partition_tuple_defs[ftabl->partitions_count - 1]);
	for(uint32_t i = 0; i < partition_tuple_def->type_info->element_count - 1; i++)
	{
		const data_type_info* col_dti = get_type_info_for_element_from_tuple_def(partition_tuple_def, STATIC_POSITION(i + 1));
		const data_type_info* inp_dti = get_type_info_for_element_from_tuple_def(input_tuple_def, insertion_from_source_positional_accessors[i]);
		if(is_primitive_numeral_type_info(col_dti))
		{
			if(!is_primitive_numeral_type_info(inp_dti))
			{
				printf("incompatible type info for primitive_numeral column of table_partition for insertion_operator_descriptor\n");
				return NULL;
			}
		}
		else if(is_text_extended_type_info(col_dti))
		{
			if(!is_text_type_info(inp_dti))
			{
				printf("incompatible type info for text column of table_partition for insertion_operator_descriptor\n");
				return NULL;
			}
		}
		else if(is_blob_extended_type_info(col_dti))
		{
			if(!is_blob_type_info(inp_dti))
			{
				printf("incompatible type info for blob column of table_partition for insertion_operator_descriptor\n");
				return NULL;
			}
		}
		else if(is_numeric_extended_type_info(col_dti))
		{
			if(!is_numeric_type_info(inp_dti))
			{
				printf("incompatible type info for numeric column of table_partition for insertion_operator_descriptor\n");
				return NULL;
			}
		}
		else if(is_jsonb_extended_type_info(col_dti))
		{
			if(!is_jsonb_extended_type_info(inp_dti))
			{
				printf("incompatible type info for jsonb column of table_partition for insertion_operator_descriptor\n");
				return NULL;
			}
		}
		else
		{
			printf("unsupported type info in column of table_partition for insertion_operator_descriptor\n");
			return NULL;
		}
	}

	// one source accessor per insertable column of the latest partition (its 0-th column is not insert-provided)
	uint32_t insertion_accessors_count = partition_tuple_def->type_info->element_count - 1;

	operator_resource_counter result = {.buffer_counter = 8, .job_counter = 1}; // 8 maximum buffers as we do not expect any btree in the system to exceed this height

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_INSERTION);
	d->params.insertion = (insertion_operator_params){input_operator_index, deep_clone_positional_accessors(insertion_from_source_positional_accessors, insertion_accessors_count), ftabl, output_flags, global_heap_notifier, additional_flags};
	d->output_tuple_def = build_table_operator_output_tuple_def(qpd->tx->rdb, ftabl, output_flags, "inserted_tuple_context");
	d->resource_counter = result;

	return d;
}

operator_desc* insert_deletion_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, const positional_accessor* partition_id_from_source_positional_accessor, const positional_accessor* tuple_pointer_from_source_positional_accessor, const fetched_table* ftabl, uint64_t deletion_batch_size, int output_flags, int additional_flags)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for deletion_operator_descriptor\n");
		return NULL;
	}

	if(qpd->tx->snapshot == NULL)
	{
		printf("must have writable snapshot for deletion_operator_descriptor\n");
		exit(-1);
	}

	if(!qpd->tx->snapshot->has_self_transaction_id)
	{
		printf("must have self_transaction_id for deletion_operator_descriptor\n");
		exit(-1);
	}

	if(ftabl == NULL)
	{
		printf("ftabl can not be NULL for deletion_operator_descriptor\n");
		return NULL;
	}

	if(deletion_batch_size == 0)
	{
		printf("must have a non-zero deletion_batch_size for deletion_operator_descriptor\n");
		return NULL;
	}

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	if(get_output_tuple_def_for_operator_descriptor(input_operator_desc) == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for deletion_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.buffer_counter = 1, .job_counter = 1, .thread_counter = 1}; // lock a page delete tuples and exit, so 1 buffer atmost

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_DELETION);
	d->params.deletion = (deletion_operator_params){input_operator_index, deep_clone_positional_accessors(partition_id_from_source_positional_accessor, 1), deep_clone_positional_accessors(tuple_pointer_from_source_positional_accessor, 1), ftabl, deletion_batch_size, output_flags, additional_flags};
	d->output_tuple_def = build_table_operator_output_tuple_def(qpd->tx->rdb, ftabl, output_flags, "deleted_tuple_context");
	d->resource_counter = result;

	return d;
}

operator_desc* insert_pointer_lookup_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, const positional_accessor* partition_id_from_source_positional_accessor, const positional_accessor* tuple_pointer_from_source_positional_accessor, const fetched_table* ftabl, uint64_t lookup_batch_size, int output_flags, int additional_flags)
{
	if(input_operator_index >= get_element_count_arraylist(&(qpd->operator_descriptors)))
	{
		printf("input_operator_index out of range for pointer_lookup_operator_descriptor\n");
		return NULL;
	}

	if(qpd->tx->snapshot == NULL)
	{
		printf("must have a snapshot for pointer_lookup_operator_descriptor\n");
		exit(-1);
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

	operator_desc* input_operator_desc = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), input_operator_index);
	if(get_output_tuple_def_for_operator_descriptor(input_operator_desc) == NULL)
	{
		printf("input operator descriptor has no output_tuple_def for pointer_lookup_operator_descriptor\n");
		return NULL;
	}

	operator_resource_counter result = {.buffer_counter = 1, .job_counter = 1, .thread_counter = 1}; // read lock a page, read tuples and exit, so 1 buffer atmost

	operator_desc* d = get_new_registered_operator_descriptor_for_query_plan_descriptor(qpd, OP_POINTER_LOOKUP);
	d->params.pointer_lookup = (pointer_lookup_operator_params){input_operator_index, deep_clone_positional_accessors(partition_id_from_source_positional_accessor, 1), deep_clone_positional_accessors(tuple_pointer_from_source_positional_accessor, 1), ftabl, lookup_batch_size, output_flags, additional_flags};
	d->output_tuple_def = build_table_operator_output_tuple_def(qpd->tx->rdb, ftabl, output_flags, "looked_up_tuple_context");
	d->resource_counter = result;

	return d;
}

// ---------------------------------------------------------------------------------------------------
// tuple transformer descriptors, each appended to the last inserted operator descriptor
// ---------------------------------------------------------------------------------------------------

// parallels get_new_registered_operator_descriptor_for_query_plan_descriptor(), for the transformer that
// follows the last inserted operator. returns NULL if no operator has been inserted yet, or if that operator
// produces no tuples of its own (no output_tuple_def) for the transformer chain to consume.
static tuple_transformer_desc* get_new_tuple_transformer_descriptor_for_last_operator_descriptor(query_plan_desc* qpd, tuple_transformer_descriptor_type type, const tuple_def** input_def)
{
	if(get_element_count_arraylist(&(qpd->operator_descriptors)) == 0)
	{
		printf("no operator descriptor to attach a tuple transformer descriptor to\n");
		return NULL;
	}

	operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), get_element_count_arraylist(&(qpd->operator_descriptors)) - 1);

	if(d->output_tuple_def == NULL)
	{
		printf("can not append a tuple transformer descriptor to an operator descriptor that has no output_tuple_def\n");
		return NULL;
	}

	tuple_transformer_desc* ttd = malloc(sizeof(tuple_transformer_desc));
	if(ttd == NULL)
		exit(-1);

	ttd->type = type;

	(*input_def) = get_output_tuple_def_for_operator_descriptor(d);

	ttd->output_def = NULL;

	return ttd;
}

tuple_transformer_desc* insert_identity_tuple_transformer_descriptor(query_plan_desc* qpd)
{
	const tuple_def* input_def = NULL;
	tuple_transformer_desc* ttd = get_new_tuple_transformer_descriptor_for_last_operator_descriptor(qpd, TT_IDENTITY, &input_def);
	if(ttd == NULL)
		return NULL;

	ttd->output_def = input_def;

	{
		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), get_element_count_arraylist(&(qpd->operator_descriptors)) - 1);
		if(is_full_arraylist(&(d->transformer_descriptors)) && !expand_arraylist(&(d->transformer_descriptors)))
			exit(-1);
		push_back_to_arraylist(&(d->transformer_descriptors), ttd);
	}

	return ttd;
}

tuple_transformer_desc* insert_clone_tuple_transformer_descriptor(query_plan_desc* qpd)
{
	const tuple_def* input_def = NULL;
	tuple_transformer_desc* ttd =  get_new_tuple_transformer_descriptor_for_last_operator_descriptor(qpd, TT_CLONE, &input_def);
	if(ttd == NULL)
		return NULL;

	ttd->output_def = input_def;

	{
		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), get_element_count_arraylist(&(qpd->operator_descriptors)) - 1);
		if(is_full_arraylist(&(d->transformer_descriptors)) && !expand_arraylist(&(d->transformer_descriptors)))
			exit(-1);
		push_back_to_arraylist(&(d->transformer_descriptors), ttd);
	}

	return ttd;
}

tuple_transformer_desc* insert_is_readable_for_mvcc_snapshot_tuple_transformer_descriptor(query_plan_desc* qpd, int* were_hints_updated)
{
	const tuple_def* input_def = NULL;
	tuple_transformer_desc* ttd = get_new_tuple_transformer_descriptor_for_last_operator_descriptor(qpd, TT_IS_READABLE_FOR_MVCC_SNAPSHOT, &input_def);
	if(ttd == NULL)
		return NULL;

	ttd->params.is_readable_for_mvcc_snapshot = (is_readable_for_mvcc_snapshot_transformer_params){were_hints_updated};
	ttd->output_def = input_def;

	{
		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), get_element_count_arraylist(&(qpd->operator_descriptors)) - 1);
		if(is_full_arraylist(&(d->transformer_descriptors)) && !expand_arraylist(&(d->transformer_descriptors)))
			exit(-1);
		push_back_to_arraylist(&(d->transformer_descriptors), ttd);
	}

	return ttd;
}

tuple_transformer_desc* insert_simple_projection_tuple_transformer_descriptor(query_plan_desc* qpd, const char* output_table_name, uint32_t projections_count, const positional_accessor* projections, char const * const *  field_names)
{
	const tuple_def* input_def = NULL;
	tuple_transformer_desc* ttd = get_new_tuple_transformer_descriptor_for_last_operator_descriptor(qpd, TT_SIMPLE_PROJECTION, &input_def);
	if(ttd == NULL)
		return NULL;

	tuple_def* output_def = malloc(sizeof(tuple_def));
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(projections_count));

	uint32_t max_output_tuple_size = 8;

	for(uint32_t i = 0; i < projections_count; i++)
	{
		data_type_info* input_dti = shallow_clone_into_nullable_type((data_type_info*) get_type_info_for_element_from_tuple_def(input_def, projections[i]));

		strcpy(output_dti->containees[i].field_name, field_names[i]);
		output_dti->containees[i].al.type_info = input_dti;

		if(input_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += input_dti->is_variable_sized ? (8 + input_dti->max_size) : (1 + input_dti->size);
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for simple_projection_transformer\n");
		for(uint32_t i = 0; i < projections_count; i++)
			free(output_dti->containees[i].al.type_info);
		free(output_dti);
		free(output_def);
		free(ttd);
		return NULL;
	}

	initialize_tuple_data_type_info(output_dti, output_table_name, 1, max_output_tuple_size, projections_count);
	initialize_tuple_def(output_def, output_dti);

	ttd->params.simple_projection = (simple_projection_transformer_params){deep_clone_string(output_table_name), projections_count, deep_clone_positional_accessors(projections, projections_count), deep_clone_strings(field_names, projections_count)};
	ttd->output_def = output_def;

	{
		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), get_element_count_arraylist(&(qpd->operator_descriptors)) - 1);
		if(is_full_arraylist(&(d->transformer_descriptors)) && !expand_arraylist(&(d->transformer_descriptors)))
			exit(-1);
		push_back_to_arraylist(&(d->transformer_descriptors), ttd);
	}

	return ttd;
}

tuple_transformer_desc* insert_scalar_projection_tuple_transformer_descriptor(query_plan_desc* qpd, const positional_accessor* projection)
{
	const tuple_def* input_def = NULL;
	tuple_transformer_desc* ttd = get_new_tuple_transformer_descriptor_for_last_operator_descriptor(qpd, TT_SCALAR_PROJECTION, &input_def);
	if(ttd == NULL)
		return NULL;

	tuple_def* output_def = malloc(sizeof(tuple_def));
	data_type_info* output_dti = NULL;
	data_type_info* input_dti = (data_type_info*) get_type_info_for_element_from_tuple_def(input_def, *projection);
	if(input_dti->type == BIT_FIELD)
		output_dti = UINT_NULLABLE[8]; // a integer big enough to hold it, a bit_field always lives in some container
	else
		output_dti = shallow_clone_into_nullable_type(input_dti);

	initialize_tuple_def(output_def, output_dti);

	ttd->params.scalar_projection = (scalar_projection_transformer_params){deep_clone_positional_accessors(projection, 1)};
	ttd->output_def = output_def;

	{
		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), get_element_count_arraylist(&(qpd->operator_descriptors)) - 1);
		if(is_full_arraylist(&(d->transformer_descriptors)) && !expand_arraylist(&(d->transformer_descriptors)))
			exit(-1);
		push_back_to_arraylist(&(d->transformer_descriptors), ttd);
	}

	return ttd;
}

tuple_transformer_desc* insert_row_number_prepender_tuple_transformer_descriptor(query_plan_desc* qpd, uint64_t next_row_number)
{
	const tuple_def* input_def = NULL;
	tuple_transformer_desc* ttd = get_new_tuple_transformer_descriptor_for_last_operator_descriptor(qpd, TT_ROW_NUMBER_PREPENDER, &input_def);
	if(ttd == NULL)
		return NULL;

	tuple_def* output_def = malloc(sizeof(tuple_def));
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(2));

	uint32_t output_max_size = get_maximum_tuple_size(input_def) + 24;

	strcpy(output_dti->containees[0].field_name, "row_number");
	output_dti->containees[0].al.type_info = UINT_NON_NULLABLE[8];

	strcpy(output_dti->containees[1].field_name, "row");
	output_dti->containees[1].al.type_info = input_def->type_info;

	if(output_max_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for row_number_prepender_transformer\n");
		free(output_def);
		free(output_dti);
		free(ttd);
		return NULL;
	}

	initialize_tuple_data_type_info(output_dti, "row_numbered", 1, output_max_size, 2);
	initialize_tuple_def(output_def, output_dti);

	ttd->params.row_number_prepender = (row_number_prepender_transformer_params){next_row_number};
	ttd->output_def = output_def;

	{
		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), get_element_count_arraylist(&(qpd->operator_descriptors)) - 1);
		if(is_full_arraylist(&(d->transformer_descriptors)) && !expand_arraylist(&(d->transformer_descriptors)))
			exit(-1);
		push_back_to_arraylist(&(d->transformer_descriptors), ttd);
	}

	return ttd;
}

tuple_transformer_desc* insert_row_identifier_prepender_tuple_transformer_descriptor(query_plan_desc* qpd, uint64_t row_identifier)
{
	const tuple_def* input_def = NULL;
	tuple_transformer_desc* ttd = get_new_tuple_transformer_descriptor_for_last_operator_descriptor(qpd, TT_ROW_IDENTIFIER_PREPENDER, &input_def);
	if(ttd == NULL)
		return NULL;

	tuple_def* output_def = malloc(sizeof(tuple_def));
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(2));

	uint32_t output_max_size = get_maximum_tuple_size(input_def) + 24;

	strcpy(output_dti->containees[0].field_name, "row_identifier");
	output_dti->containees[0].al.type_info = UINT_NON_NULLABLE[8];

	strcpy(output_dti->containees[1].field_name, "row");
	output_dti->containees[1].al.type_info = input_def->type_info;

	if(output_max_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for row_identifier_prepender_transformer\n");
		free(output_def);
		free(output_dti);
		free(ttd);
		return NULL;
	}

	initialize_tuple_data_type_info(output_dti, "row_identified", 1, output_max_size, 2);
	initialize_tuple_def(output_def, output_dti);

	ttd->params.row_identifier_prepender = (row_identifier_prepender_transformer_params){row_identifier};
	ttd->output_def = output_def;

	{
		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), get_element_count_arraylist(&(qpd->operator_descriptors)) - 1);
		if(is_full_arraylist(&(d->transformer_descriptors)) && !expand_arraylist(&(d->transformer_descriptors)))
			exit(-1);
		push_back_to_arraylist(&(d->transformer_descriptors), ttd);
	}

	return ttd;
}

tuple_transformer_desc* insert_expressioned_selection_tuple_transformer_descriptor(query_plan_desc* qpd, sql_expression* expr)
{
	const tuple_def* input_def = NULL;
	tuple_transformer_desc* ttd = get_new_tuple_transformer_descriptor_for_last_operator_descriptor(qpd, TT_EXPRESSIONED_SELECTION, &input_def);
	if(ttd == NULL)
		return NULL;

	ttd->params.expressioned_selection = (expressioned_selection_transformer_params){expr};
	ttd->output_def = input_def;

	{
		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), get_element_count_arraylist(&(qpd->operator_descriptors)) - 1);
		if(is_full_arraylist(&(d->transformer_descriptors)) && !expand_arraylist(&(d->transformer_descriptors)))
			exit(-1);
		push_back_to_arraylist(&(d->transformer_descriptors), ttd);
	}

	return ttd;
}

tuple_transformer_desc* insert_unified_typing_tuple_transformer_descriptor(query_plan_desc* qpd, uint32_t* column_ids_to_transform, uint32_t column_to_transform_count)
{
	const tuple_def* input_def = NULL;
	tuple_transformer_desc* ttd = get_new_tuple_transformer_descriptor_for_last_operator_descriptor(qpd, TT_UNIFIED_TYPING, &input_def);
	if(ttd == NULL)
		return NULL;

	tuple_def* output_def = malloc(sizeof(tuple_def));
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(input_def->type_info->element_count));

	uint32_t max_output_tuple_size = 8;

	for(uint32_t i = 0, j = 0; i < input_def->type_info->element_count; i++)
	{
		strncpy(output_dti->containees[i].field_name, input_def->type_info->containees[i].field_name, 64);
		output_dti->containees[i].al.type_info = (data_type_info*) get_type_info_for_element_from_tuple_def(input_def, STATIC_POSITION(i));

		if(j < column_to_transform_count && column_ids_to_transform[j] == i)
		{
			if(is_extended_type_info(output_dti->containees[i].al.type_info) || is_unified_type_info(output_dti->containees[i].al.type_info))
			{
				if(is_text_type_info(output_dti->containees[i].al.type_info))
					output_dti->containees[i].al.type_info = qpd->tx->rdb->union_text_type_info;
				else if(is_blob_type_info(output_dti->containees[i].al.type_info))
					output_dti->containees[i].al.type_info = qpd->tx->rdb->union_blob_type_info;
				else if(is_numeric_type_info(output_dti->containees[i].al.type_info))
					output_dti->containees[i].al.type_info = qpd->tx->rdb->union_numeric_type_info;
				else if(is_jsonb_type_info(output_dti->containees[i].al.type_info))
					output_dti->containees[i].al.type_info = qpd->tx->rdb->union_jsonb_type_info;
			}
			j++;
		}

		if(output_dti->containees[i].al.type_info->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += output_dti->containees[i].al.type_info->is_variable_sized ? (8 + output_dti->containees[i].al.type_info->max_size) : (1 + output_dti->containees[i].al.type_info->size);
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for simple_projection_transformer\n");
		free(output_dti);
		free(output_def);
		free(ttd);
		return NULL;
	}

	initialize_tuple_data_type_info(output_dti, input_def->type_info->type_name, 1, max_output_tuple_size, input_def->type_info->element_count);
	initialize_tuple_def(output_def, output_dti);

	ttd->params.unified_typing = (unified_typing_transformer_params){deep_clone_uint32_array(column_ids_to_transform, column_to_transform_count), column_to_transform_count};
	ttd->output_def = output_def;

	{
		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), get_element_count_arraylist(&(qpd->operator_descriptors)) - 1);
		if(is_full_arraylist(&(d->transformer_descriptors)) && !expand_arraylist(&(d->transformer_descriptors)))
			exit(-1);
		push_back_to_arraylist(&(d->transformer_descriptors), ttd);
	}

	return ttd;
}

// ---------------------------------------------------------------------------------------------------
// main_query has this_sub_query_expr_eval_context == NULL
// ---------------------------------------------------------------------------------------------------

int is_main_query_plan_descriptor(const query_plan_desc* qpd)
{
	return qpd->this_sub_query_expr_eval_context == NULL;
}

// ---------------------------------------------------------------------------------------------------
// create : build a real, executable query_plan from the descriptor
// ---------------------------------------------------------------------------------------------------

query_plan* create_query_plan(query_plan_desc* qpd, const sql_expr_eval_context* this_sub_query_expr_eval_context)
{
	uint64_t count = get_element_count_arraylist(&(qpd->operator_descriptors));

	query_plan* qp = get_new_query_plan(qpd->tx, this_sub_query_expr_eval_context, count);
	if(qp == NULL)
		return NULL;

	// an operator built at descriptor index i is registered at position i in qp->operators (see
	// get_new_registered_operator_for_query_plan), so an already-built input operator at descriptor index j < i
	// is fetched as get_from_front_of_arraylist(&(qp->operators), j)
	for(uint64_t i = 0; i < count; i++)
	{
		operator_desc* d = (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), i);
		operator* o = get_new_registered_operator_for_query_plan(qp);

		switch(d->type)
		{
			case OP_CONSTANT_DATASET :
				setup_constant_dataset_operator(o, d->params.constant_dataset.constant_dataset_p, d->params.constant_dataset.record_def);
				break;
			case OP_GENERATOR :
				setup_generator_operator(o, d->params.generator.generator, d->params.generator.generator_context, d->params.generator.generator_tuple_def);
				break;
			case OP_STREAM_INPUT :
				setup_stream_input_operator(o, d->params.stream_input.in_strm, d->params.stream_input.input_tuple_def);
				d->params.stream_input.in_strm = NULL; // consumed : must be set again by the user before the next create_query_plan()
				break;
			case OP_SCAN :
				setup_scan_operator(o, qp, d->params.scan.ftabl, d->params.scan.max_concurrent_jobs_count, d->params.scan.output_flags, d->params.scan.additional_flags);
				break;
			case OP_IDENTITY :
				setup_identity_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.identity.input_operator_index));
				break;
			case OP_OFFSET_LIMIT :
				setup_offset_limit_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.offset_limit.input_operator_index), d->params.offset_limit.offset_counter, d->params.offset_limit.limit_counter);
				break;
			case OP_SELECTION :
				setup_selection_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.selection.input_operator_index), d->params.selection.expr);
				break;
			case OP_SORT :
				setup_sort_operator(o, d->params.sort.result_counter, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.sort.input_operator_index), d->params.sort.key_element_count, d->params.sort.key_element_ids, d->params.sort.key_compare_direction, d->params.sort.min_run_size, d->params.sort.N_way_sort, d->params.sort.max_concurrent_jobs_count);
				break;
			case OP_PROJECTION :
				setup_projection_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.projection.input_operator_index), d->params.projection.projection_descriptions, d->params.projection.projection_descriptions_count);
				break;
			case OP_SIMPLE_AGGREGATION :
				setup_simple_aggregation_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.simple_aggregation.input_operator_index), d->params.simple_aggregation.aggregate_functions_count, d->params.simple_aggregation.aggregate_functions, (positional_accessor const * const *) d->params.simple_aggregation.aggregate_input_element_ids);
				break;
			case OP_SORTED_AGGREGATION :
				setup_sorted_aggregation_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.sorted_aggregation.input_operator_index), d->params.sorted_aggregation.key_element_count, d->params.sorted_aggregation.key_element_ids, d->params.sorted_aggregation.aggregate_functions_count, d->params.sorted_aggregation.aggregate_functions, (positional_accessor const * const *) d->params.sorted_aggregation.aggregate_input_element_ids);
				break;
			case OP_HASH_AGGREGATION :
				setup_hash_aggregation_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.hash_aggregation.input_operator_index), d->params.hash_aggregation.key_element_count, d->params.hash_aggregation.key_element_ids, d->params.hash_aggregation.aggregate_functions_count, d->params.hash_aggregation.aggregate_functions, (positional_accessor const * const *) d->params.hash_aggregation.aggregate_input_element_ids, d->params.hash_aggregation.partitions_count, d->params.hash_aggregation.max_concurrent_jobs_count, d->params.hash_aggregation.max_concurrent_jobs_queue_size, d->params.hash_aggregation.min_build_tuple_buffer_size);
				break;
			case OP_CONSUMER :
				setup_consumer_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.consumer.input_operator_index), d->params.consumer.consumer, d->params.consumer.consumer_context);
				break;
			case OP_STREAM_OUTPUT :
				setup_stream_output_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.stream_output.input_operator_index), d->params.stream_output.out_strm);
				d->params.stream_output.out_strm = NULL; // consumed : must be set again by the user before the next create_query_plan()
				break;
			case OP_SUBQUERY_OUTPUT :
				setup_subquery_output_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.subquery_output.input_operator_index), d->params.subquery_output.out_pipe);
				d->params.subquery_output.out_pipe = NULL; // consumed : must be set again by the user before the next create_query_plan()
				break;
			case OP_UNION :
			{
				operator** input_operators = malloc(sizeof(operator*) * d->params.union_op.input_operators_count);
				for(uint64_t k = 0; k < d->params.union_op.input_operators_count; k++)
					input_operators[k] = (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.union_op.input_operator_indices[k]);
				setup_union_operator(o, input_operators, (uint32_t) d->params.union_op.input_operators_count);
				free(input_operators);
				break;
			}
			case OP_MERGE_SORTED_INPUTS :
			{
				operator** input_operators = malloc(sizeof(operator*) * d->params.merge_sorted_inputs.input_operators_count);
				for(uint64_t k = 0; k < d->params.merge_sorted_inputs.input_operators_count; k++)
					input_operators[k] = (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.merge_sorted_inputs.input_operator_indices[k]);
				setup_merge_sorted_inputs_operator(o, input_operators, (uint32_t) d->params.merge_sorted_inputs.input_operators_count, d->params.merge_sorted_inputs.key_element_count, d->params.merge_sorted_inputs.key_element_ids, d->params.merge_sorted_inputs.key_compare_direction);
				free(input_operators);
				break;
			}
			case OP_RESULT_MATCH :
			{
				operator* input_operators[2] = {(operator*) get_from_front_of_arraylist(&(qp->operators), d->params.result_match.input_operator_indices[0]), (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.result_match.input_operator_indices[1])};
				setup_result_match_operator(o, input_operators);
				break;
			}
			case OP_BLOCK_NESTED_LOOP_JOIN :
				setup_block_nested_loop_join_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.block_nested_loop_join.left_input_operator_index), (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.block_nested_loop_join.right_input_operator_index), d->params.block_nested_loop_join.join_expr, d->params.block_nested_loop_join.ptype, d->params.block_nested_loop_join.min_block_size);
				break;
			case OP_SORT_MERGE_JOIN :
				setup_sort_merge_join_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.sort_merge_join.left_input_operator_index), d->params.sort_merge_join.left_key_element_ids, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.sort_merge_join.right_input_operator_index), d->params.sort_merge_join.right_key_element_ids, d->params.sort_merge_join.key_compare_direction, d->params.sort_merge_join.key_element_count, d->params.sort_merge_join.ptype, d->params.sort_merge_join.min_block_size);
				break;
			case OP_HASH_JOIN :
				setup_hash_join_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.hash_join.left_input_operator_index), d->params.hash_join.left_key_element_ids, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.hash_join.right_input_operator_index), d->params.hash_join.right_key_element_ids, d->params.hash_join.key_element_count, d->params.hash_join.ptype, d->params.hash_join.partitions_count, d->params.hash_join.max_concurrent_jobs_count, d->params.hash_join.max_concurrent_jobs_queue_size, d->params.hash_join.min_pending_buffer_size);
				break;
			case OP_BLOCK_NESTED_LOOP_SEMI_JOIN :
				setup_block_nested_loop_semi_join_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.block_nested_loop_semi_join.left_input_operator_index), (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.block_nested_loop_semi_join.right_input_operator_index), d->params.block_nested_loop_semi_join.join_expr, d->params.block_nested_loop_semi_join.stype, d->params.block_nested_loop_semi_join.min_block_size);
				break;
			case OP_SORT_MERGE_SEMI_JOIN :
				setup_sort_merge_semi_join_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.sort_merge_semi_join.left_input_operator_index), d->params.sort_merge_semi_join.left_key_element_ids, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.sort_merge_semi_join.right_input_operator_index), d->params.sort_merge_semi_join.right_key_element_ids, d->params.sort_merge_semi_join.key_compare_direction, d->params.sort_merge_semi_join.key_element_count, d->params.sort_merge_semi_join.stype, d->params.sort_merge_semi_join.min_block_size);
				break;
			case OP_HASH_SEMI_JOIN :
				setup_hash_semi_join_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.hash_semi_join.left_input_operator_index), d->params.hash_semi_join.left_key_element_ids, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.hash_semi_join.right_input_operator_index), d->params.hash_semi_join.right_key_element_ids, d->params.hash_semi_join.key_element_count, d->params.hash_semi_join.stype, d->params.hash_semi_join.partitions_count, d->params.hash_semi_join.max_concurrent_jobs_count, d->params.hash_semi_join.max_concurrent_jobs_queue_size, d->params.hash_semi_join.min_pending_buffer_size);
				break;
			case OP_INSERTION :
				setup_insertion_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.insertion.input_operator_index), d->params.insertion.insertion_from_source_positional_accessors, d->params.insertion.ftabl, d->params.insertion.output_flags, d->params.insertion.global_heap_notifier, d->params.insertion.additional_flags);
				break;
			case OP_DELETION :
				setup_deletion_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.deletion.input_operator_index), d->params.deletion.partition_id_from_source_positional_accessor, d->params.deletion.tuple_pointer_from_source_positional_accessor, d->params.deletion.ftabl, d->params.deletion.deletion_batch_size, d->params.deletion.output_flags, d->params.deletion.additional_flags);
				break;
			case OP_POINTER_LOOKUP :
				setup_pointer_lookup_operator(o, (operator*) get_from_front_of_arraylist(&(qp->operators), d->params.pointer_lookup.input_operator_index), d->params.pointer_lookup.partition_id_from_source_positional_accessor, d->params.pointer_lookup.tuple_pointer_from_source_positional_accessor, d->params.pointer_lookup.ftabl, d->params.pointer_lookup.lookup_batch_size, d->params.pointer_lookup.output_flags, d->params.pointer_lookup.additional_flags);
				break;
		}

		// append the transformer chain that follows this operator
		uint64_t transformer_count = get_element_count_arraylist(&(d->transformer_descriptors));
		for(uint64_t t = 0; t < transformer_count; t++)
		{
			tuple_transformer_desc* ttd = (tuple_transformer_desc*) get_from_front_of_arraylist(&(d->transformer_descriptors), t);
			const tuple_def* input_def = get_output_def_for_tuple_transformers(&(o->output_tuple_transformers));

			tuple_transformer* tt = NULL;
			switch(ttd->type)
			{
				case TT_IDENTITY :
					tt = get_new_identity_tuple_transformer(input_def);
					break;
				case TT_CLONE :
					tt = get_new_clone_tuple_transformer(input_def);
					break;
				case TT_IS_READABLE_FOR_MVCC_SNAPSHOT :
					tt = get_new_is_readable_for_mvcc_snapshot_transformer(qp->curr_tx->snapshot, &(qp->curr_tx->rdb->tsg), ttd->params.is_readable_for_mvcc_snapshot.were_hints_updated, input_def);
					break;
				case TT_SIMPLE_PROJECTION :
					tt = get_new_simple_projection_transformer(ttd->params.simple_projection.output_table_name, input_def, ttd->params.simple_projection.projections_count, ttd->params.simple_projection.projections, (char const * const * ) ttd->params.simple_projection.field_names);
					break;
				case TT_SCALAR_PROJECTION :
					tt = get_new_scalar_projection_transformer(input_def, ttd->params.scalar_projection.projection);
					break;
				case TT_ROW_NUMBER_PREPENDER :
					tt = get_new_row_number_prepender_transformer(input_def, ttd->params.row_number_prepender.next_row_number);
					break;
				case TT_ROW_IDENTIFIER_PREPENDER :
					tt = get_new_row_identifier_prepender_transformer(input_def, ttd->params.row_identifier_prepender.row_identifier);
					break;
				case TT_EXPRESSIONED_SELECTION :
					tt = get_new_expressioned_selection_transformer(input_def, qp->curr_tx, qp, ttd->params.expressioned_selection.expr);
					break;
				case TT_UNIFIED_TYPING :
					tt = get_new_unified_typing_transformer(input_def, qp->curr_tx, ttd->params.unified_typing.column_ids_to_transform, ttd->params.unified_typing.column_to_transform_count);
					break;
			}

			append_tuple_transformer(&(o->output_tuple_transformers), tt);
		}
	}

	return qp;
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
			case OP_INSERTION :
			case OP_DELETION :
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
			case OP_INSERTION :
				free_positional_accessors(d->params.insertion.insertion_from_source_positional_accessors, d->params.insertion.ftabl->table_partition_tuple_defs[d->params.insertion.ftabl->partitions_count - 1].type_info->element_count - 1);
				break;
			case OP_DELETION :
				free_positional_accessors(d->params.deletion.partition_id_from_source_positional_accessor, 1);
				free_positional_accessors(d->params.deletion.tuple_pointer_from_source_positional_accessor, 1);
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

		// delete the expression evaluation context template, if this operator built one
		if(d->has_expr_eval_context)
			delete_context_p_for_sql_expr_eval_context_for_rhendb(d->expr_eval_context.context_p);

		// free the deep-cloned arrays / strings each tuple transformer descriptor owns, then the descriptors and the list
		uint64_t transformer_count = get_element_count_arraylist(&(d->transformer_descriptors));
		for(uint64_t t = 0; t < transformer_count; t++)
		{
			tuple_transformer_desc* ttd = (tuple_transformer_desc*) get_from_front_of_arraylist(&(d->transformer_descriptors), t);

			switch(ttd->type)
			{
				case TT_ROW_IDENTIFIER_PREPENDER :
				case TT_ROW_NUMBER_PREPENDER :
					free(ttd->output_def->type_info);
					free((void*)ttd->output_def);
					break;
				case TT_SIMPLE_PROJECTION :
					uint32_t element_count = ttd->output_def->type_info->element_count;
					for(uint32_t i = 0; i < element_count; i++)
						free((data_type_info*)(ttd->output_def->type_info->containees[i].al.type_info));
					free(ttd->output_def->type_info);
					free((void*)(ttd->output_def));
					break;
				case TT_SCALAR_PROJECTION :
					if(ttd->output_def->type_info != UINT_NULLABLE[8])
						free(ttd->output_def->type_info);
					free((void*)(ttd->output_def));
					break;
				case TT_UNIFIED_TYPING :
					free(ttd->output_def->type_info);
					free((void*)(ttd->output_def));
					break;
				default :
					break;
			}

			switch(ttd->type)
			{
				case TT_SIMPLE_PROJECTION :
					free(ttd->params.simple_projection.output_table_name);
					free_positional_accessors(ttd->params.simple_projection.projections, ttd->params.simple_projection.projections_count);
					free_strings(ttd->params.simple_projection.field_names, ttd->params.simple_projection.projections_count);
					break;
				case TT_SCALAR_PROJECTION :
					free_positional_accessors(ttd->params.scalar_projection.projection, 1);
					break;
				case TT_UNIFIED_TYPING :
					free(ttd->params.unified_typing.column_ids_to_transform);
					break;
				default :
					break;
			}

			free(ttd);
		}
		deinitialize_arraylist(&(d->transformer_descriptors));

		free(d);
	}

	deinitialize_arraylist(&(qpd->operator_descriptors));

	free(qpd);
}