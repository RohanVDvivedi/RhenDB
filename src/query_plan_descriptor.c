#include<rhendb/query_plan_descriptor.h>

#include<rhendb/rhendb.h>
#include<rhendb/nullable_type_info_maker.h>

#include<tuplestore/data_type_info.h>
#include<tuplestore/data_type_info_defaults.h>
#include<tuplestore/tuple.h>

#include<tuplelargetypes/common_extended.h> // has_extended_type_info3, PERSISTENT_EXT_SUB_TYPE

#include<stdlib.h>
#include<stdio.h>
#include<string.h>

/*
	Implementation of the query_plan_descriptor factory (see query_plan_descriptor.h).
	Every insert_* mirrors the matching setup_* : the SAME validation and the SAME output_tuple_def
	construction (borrowing the same dtis), but it never exit()-s (returns NULL) and never builds a live
	operator. create_query_plan() is the single switch that turns the descriptor into a real query_plan.
*/

// ===================================================================================================
// list plumbing
// ===================================================================================================

static operator_desc* get_operator_descriptor_at(query_plan_desc* qpd, uint64_t idx)
{
	return (operator_desc*) get_from_front_of_arraylist(&(qpd->operator_descriptors), idx);
}

static int is_valid_operator_index(query_plan_desc* qpd, uint64_t idx)
{
	return idx < get_element_count_arraylist(&(qpd->operator_descriptors));
}

static const tuple_def* out_def_at(query_plan_desc* qpd, uint64_t idx)
{
	return get_operator_descriptor_at(qpd, idx)->output_tuple_def;
}

// allocate a new operator_desc, assign its index, init its transformer list, append it to qpd. NULL on failure.
static operator_desc* push_operator_desc(query_plan_desc* qpd, operator_descriptor_type type)
{
	operator_desc* od = malloc(sizeof(operator_desc));
	if(d == NULL)
		return NULL;

	// primitive initialization
	od->type = type;
	od->index = get_element_count_arraylist(&(qpd->operator_descriptors));
	od->output_tuple_def = NULL;
	od->resource_counter = ZERO_OPERATOR_RESOURCE_COUNTER;
	if(!initialize_arraylist(&(d->transformer_descriptors), 4))
	{
		free(d);
		return NULL;
	}

	// insert it
	if(is_full_arraylist(&(qpd->operator_descriptors)) && !expand_arraylist(&(qpd->operator_descriptors)))
	{
		deinitialize_arraylist(&(d->transformer_descriptors));
		free(d);
		return NULL;
	}
	push_back_to_arraylist(&(qpd->operator_descriptors), d);
	return d;
}

// ===================================================================================================
// output-def builders (identical to the operators, borrowing the same dtis)
// ===================================================================================================

static uint64_t add_column_size(uint64_t acc, const data_type_info* col)
{
	if(col->type == BIT_FIELD)
		return acc + 9;
	return acc + (col->is_variable_sized ? (8 + col->max_size) : (1 + col->size));
}

// aggregates output def: [key_i : shallow_clone_into_nullable_type] ++ [agg_i : borrowed output_type_info]
static tuple_def* build_aggregation_output_def(const char* name, const tuple_def* input_def, uint32_t key_count, const positional_accessor* key_ids, uint32_t agg_count, aggregate_function* const * aggs)
{
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(key_count + agg_count));
	if(output_dti == NULL) return NULL;
	uint64_t max_output_tuple_size = 8;
	for(uint32_t i = 0; i < key_count; i++)
	{
		data_type_info* key_dti = (data_type_info*) get_type_info_for_element_from_tuple_def(input_def, key_ids[i]);
		max_output_tuple_size = add_column_size(max_output_tuple_size, key_dti);
		sprintf(output_dti->containees[i].field_name, "key_%u", i);
		output_dti->containees[i].al.type_info = shallow_clone_into_nullable_type(key_dti);
	}
	for(uint32_t i = 0, j = key_count; i < agg_count; i++, j++)
	{
		max_output_tuple_size = add_column_size(max_output_tuple_size, aggs[i]->output_type_info);
		sprintf(output_dti->containees[j].field_name, "agg_%u", i);
		output_dti->containees[j].al.type_info = (data_type_info*) aggs[i]->output_type_info;
	}
	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		for(uint32_t i = 0; i < key_count; i++)
			free((data_type_info*) output_dti->containees[i].al.type_info);
		free(output_dti);
		return NULL;
	}
	initialize_tuple_data_type_info(output_dti, name, 0, max_output_tuple_size, key_count + agg_count);
	tuple_def* td = malloc(sizeof(tuple_def));
	if(td == NULL)
	{
		for(uint32_t i = 0; i < key_count; i++)
			free((data_type_info*) output_dti->containees[i].al.type_info);
		free(output_dti);
		return NULL;
	}
	initialize_tuple_def(td, output_dti);
	return td;
}

// join output def: "left" and "right", both shallow_clone_into_nullable_type of the input type_info
static tuple_def* build_join_output_def(const tuple_def* left_def, const tuple_def* right_def)
{
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(2));
	if(output_dti == NULL) return NULL;
	uint64_t max_output_tuple_size = 8;
	max_output_tuple_size = add_column_size(max_output_tuple_size, left_def->type_info);
	strcpy(output_dti->containees[0].field_name, "left");
	output_dti->containees[0].al.type_info = shallow_clone_into_nullable_type(left_def->type_info);
	max_output_tuple_size = add_column_size(max_output_tuple_size, right_def->type_info);
	strcpy(output_dti->containees[1].field_name, "right");
	output_dti->containees[1].al.type_info = shallow_clone_into_nullable_type(right_def->type_info);
	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		free((data_type_info*) output_dti->containees[0].al.type_info);
		free((data_type_info*) output_dti->containees[1].al.type_info);
		free(output_dti);
		return NULL;
	}
	initialize_tuple_data_type_info(output_dti, "join", 0, max_output_tuple_size, 2);
	tuple_def* td = malloc(sizeof(tuple_def));
	if(td == NULL)
	{
		free((data_type_info*) output_dti->containees[0].al.type_info);
		free((data_type_info*) output_dti->containees[1].al.type_info);
		free(output_dti);
		return NULL;
	}
	initialize_tuple_def(td, output_dti);
	return td;
}

// scan output def from output_flags (all columns borrowed, as the scan operator builds them)
static tuple_def* build_scan_output_def(transaction* tx, const fetched_table* ftabl, int output_flags)
{
	if(output_flags == 0)
		return NULL;
	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	if(output_tuple_def == NULL) return NULL;
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(10));
	if(output_dti == NULL) { free(output_tuple_def); return NULL; }
	uint32_t n = 0;
	uint64_t max_sz = 8;
	if(MUST_OUTPUT_TABLE_ID(output_flags))
	{
		strncpy(output_dti->containees[n].field_name, "table_id", 64);
		output_dti->containees[n].al.type_info = UINT_NON_NULLABLE[8];
		max_sz += 8; n++;
	}
	if(MUST_OUTPUT_PARTITION_ID(output_flags))
	{
		strncpy(output_dti->containees[n].field_name, "partition_id", 64);
		output_dti->containees[n].al.type_info = UINT_NON_NULLABLE[8];
		max_sz += 8; n++;
	}
	if(MUST_OUTPUT_TUPLE_POINTER_ID(output_flags))
	{
		strncpy(output_dti->containees[n].field_name, "tuple_pointer", 64);
		output_dti->containees[n].al.type_info = &(tx->rdb->persistent_acid_rage_engine.pam_p->pas.tuple_pointer_type_info);
		max_sz += sizeof(tuple_pointer); n++;
	}
	if(MUST_OUTPUT_HEAP_TUPLE(output_flags))
	{
		strncpy(output_dti->containees[n].field_name, ftabl->table_info.name, 64);
		output_dti->containees[n].al.type_info = ftabl->final_readers_tuple_def.type_info;
		max_sz += 8 + get_maximum_tuple_size(&(ftabl->final_readers_tuple_def)); n++;
	}
	initialize_tuple_data_type_info(output_dti, "scanned_tuple_context", 0, max_sz, n);
	initialize_tuple_def(output_tuple_def, output_dti);
	return output_tuple_def;
}

// build a throwaway eval context over one input def, using the descriptor's tx + parent context.
// get_sql_expr_eval_context_for_rhendb only reads qp->parent_expr_eval_context, so a minimal stub qp suffices.
static sql_expr_eval_context make_eval_context(query_plan_desc* qpd, const tuple_def* input_def)
{
	query_plan stub_qp;
	memset(&stub_qp, 0, sizeof(stub_qp));
	stub_qp.curr_tx = qpd->tx;
	stub_qp.parent_expr_eval_context = qpd->parent_expr_eval_context;
	return get_sql_expr_eval_context_for_rhendb((tuple_def**)(&input_def), 1, qpd->tx, &stub_qp);
}

// ===================================================================================================
// get_new / leaves
// ===================================================================================================

query_plan_desc* get_new_query_plan_descriptor(transaction* tx, const sql_expr_eval_context* parent_expr_eval_context)
{
	query_plan_desc* qpd = malloc(sizeof(query_plan_desc));
	if(qpd == NULL)
		return NULL;
	qpd->tx = tx;
	qpd->parent_expr_eval_context = parent_expr_eval_context;
	if(!initialize_arraylist(&(qpd->operator_descriptors), 16))
	{ free(qpd); return NULL; }
	return qpd;
}

operator_desc* insert_constant_dataset_operator_descriptor(query_plan_desc* qpd, interim_tuple_store* constant_dataset_p, const tuple_def* record_def)
{
	if(record_def == NULL)
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_CONSTANT_DATASET);
	if(d == NULL) return NULL;
	d->params.constant_dataset = (constant_dataset_operator_params){constant_dataset_p, record_def};
	d->output_tuple_def = record_def;
	d->resource_counter = (operator_resource_counter){.thread_counter = 1};
	return d;
}

operator_desc* insert_generator_operator_descriptor(query_plan_desc* qpd, void* (*generator)(void* generator_context, const tuple_def* generator_tuple_def), void* generator_context, const tuple_def* generator_tuple_def)
{
	if(generator_tuple_def == NULL)
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_GENERATOR);
	if(d == NULL) return NULL;
	d->params.generator = (generator_operator_params){generator, generator_context, generator_tuple_def};
	d->output_tuple_def = generator_tuple_def;
	d->resource_counter = (operator_resource_counter){.thread_counter = 1};
	return d;
}

operator_desc* insert_stream_input_operator_descriptor(query_plan_desc* qpd, stream* in_strm, const tuple_def* input_tuple_def)
{
	if(input_tuple_def == NULL)
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_STREAM_INPUT);
	if(d == NULL) return NULL;
	d->params.stream_input = (stream_input_operator_params){in_strm, input_tuple_def};
	d->output_tuple_def = input_tuple_def;
	d->resource_counter = (operator_resource_counter){.thread_counter = 1};
	return d;
}

operator_desc* insert_scan_operator_descriptor(query_plan_desc* qpd, const fetched_table* ftabl, uint32_t max_concurrent_jobs_count, int output_flags, int additional_flags)
{
	if(ftabl == NULL)
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_SCAN);
	if(d == NULL) return NULL;
	d->params.scan = (scan_operator_params){ftabl, max_concurrent_jobs_count, output_flags, additional_flags};
	d->output_tuple_def = build_scan_output_def(qpd->tx, ftabl, output_flags); // NULL when output_flags==0, as in the operator
	d->resource_counter = (operator_resource_counter){.job_counter = (max_concurrent_jobs_count ? max_concurrent_jobs_count : 1)};
	return d;
}

// ===================================================================================================
// single-input pass-through
// ===================================================================================================

operator_desc* insert_identity_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index)
{
	if(!is_valid_operator_index(qpd, input_operator_index))
		return NULL;
	const tuple_def* in = out_def_at(qpd, input_operator_index);
	operator_desc* d = push_operator_desc(qpd, OP_IDENTITY);
	if(d == NULL) return NULL;
	d->params.identity.input_operator_index = input_operator_index;
	d->output_tuple_def = in;
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

operator_desc* insert_offset_limit_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, tuples_down_counter offset_counter, tuples_down_counter limit_counter)
{
	if(!is_valid_operator_index(qpd, input_operator_index))
		return NULL;
	const tuple_def* in = out_def_at(qpd, input_operator_index);
	operator_desc* d = push_operator_desc(qpd, OP_OFFSET_LIMIT);
	if(d == NULL) return NULL;
	d->params.offset_limit = (offset_limit_operator_params){input_operator_index, offset_counter, limit_counter};
	d->output_tuple_def = in;
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

operator_desc* insert_selection_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, sql_expression* expr)
{
	if(!is_valid_operator_index(qpd, input_operator_index))
		return NULL;
	const tuple_def* in = out_def_at(qpd, input_operator_index);

	// replicate setup_selection validation + counter via a throwaway eval context (no exit on error)
	sql_expr_eval_context ec = make_eval_context(qpd, in);
	int error_code = 0;
	if(!is_valid_using_infer_sql_expr_for_rhendb(expr, &ec, &error_code))
	{ delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p); return NULL; }
	int has_ref = has_reference_to_persistent_extended_type_from_expression(ec.context_p);
	operator_resource_counter rc = has_sub_query_in_sql_exp(expr)
		? (operator_resource_counter){.buffer_counter = has_ref * 2, .thread_counter = 1}
		: (operator_resource_counter){.buffer_counter = has_ref * 2, .job_counter = 1};
	delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p);

	operator_desc* d = push_operator_desc(qpd, OP_SELECTION);
	if(d == NULL) return NULL;
	d->params.selection = (selection_operator_params){input_operator_index, expr};
	d->output_tuple_def = in;   // selection is identity on schema
	d->resource_counter = rc;
	return d;
}

operator_desc* insert_sort_operator_descriptor(query_plan_desc* qpd, tuples_down_counter result_counter, uint64_t input_operator_index, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint64_t min_run_size, uint32_t N_way_sort, uint32_t max_concurrent_jobs_count)
{
	if(!is_valid_operator_index(qpd, input_operator_index))
		return NULL;
	const tuple_def* in = out_def_at(qpd, input_operator_index);
	if(in != NULL && key_element_count > 0 && key_element_ids != NULL && !are_all_positions_accessible_for_tuple_def(in, key_element_ids, key_element_count))
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_SORT);
	if(d == NULL) return NULL;
	d->params.sort = (sort_operator_params){result_counter, input_operator_index, key_element_count, key_element_ids, key_compare_direction, min_run_size, N_way_sort, max_concurrent_jobs_count};
	d->output_tuple_def = in;
	uint64_t ext = (in != NULL) ? has_extended_type_info3(in, key_element_count, key_element_ids, PERSISTENT_EXT_SUB_TYPE) : 0;
	d->resource_counter = (operator_resource_counter){.buffer_counter = 2 * ext * max_concurrent_jobs_count, .job_counter = max_concurrent_jobs_count + 1};
	return d;
}

// ===================================================================================================
// projection
// ===================================================================================================

operator_desc* insert_projection_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, projection_description* projection_descriptions, uint32_t projection_descriptions_count)
{
	if(!is_valid_operator_index(qpd, input_operator_index) || projection_descriptions == NULL || projection_descriptions_count == 0)
		return NULL;
	const tuple_def* in = out_def_at(qpd, input_operator_index);
	if(in == NULL)
		return NULL;

	sql_expr_eval_context ec = make_eval_context(qpd, in);

	uint32_t expression_projections_count = 0;
	int has_sub_query = 0;
	for(uint32_t i = 0; i < projection_descriptions_count; i++)
	{
		if(projection_descriptions[i].type == PROJECT_EXPRESSION)
		{
			expression_projections_count++;
			int error_code = 0;
			if(!is_valid_using_infer_sql_expr_for_rhendb(projection_descriptions[i].expr, &ec, &error_code))
			{ delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p); return NULL; }
			has_sub_query = has_sub_query || has_sub_query_in_sql_exp(projection_descriptions[i].expr);
		}
	}
	int has_ref = has_reference_to_persistent_extended_type_from_expression(ec.context_p);

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(projection_descriptions_count));
	if(output_dti == NULL) { delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p); return NULL; }
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
			col_dti = shallow_clone_into_nullable_type((data_type_info*) get_type_info_for_element_from_tuple_def(in, projection_descriptions[i].pa));
		max_output_tuple_size = add_column_size(max_output_tuple_size, col_dti);
		sprintf(output_dti->containees[i].field_name, "proj_%u", i);
		output_dti->containees[i].al.type_info = col_dti;
	}
	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		// mirror free_resources: only the PROJECT_IDENTITY columns are owned clones here
		for(uint32_t i = 0; i < projection_descriptions_count; i++)
			if(projection_descriptions[i].type == PROJECT_IDENTITY)
				free((data_type_info*) output_dti->containees[i].al.type_info);
		free(output_dti);
		delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p);
		return NULL;
	}
	initialize_tuple_data_type_info(output_dti, "projection", 0, max_output_tuple_size, projection_descriptions_count);
	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	if(output_tuple_def == NULL)
	{
		for(uint32_t i = 0; i < projection_descriptions_count; i++)
			if(projection_descriptions[i].type == PROJECT_IDENTITY)
				free((data_type_info*) output_dti->containees[i].al.type_info);
		free(output_dti);
		delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p);
		return NULL;
	}
	initialize_tuple_def(output_tuple_def, output_dti);

	operator_resource_counter rc = has_sub_query
		? (operator_resource_counter){.buffer_counter = has_ref * 2, .thread_counter = 1}
		: (operator_resource_counter){.buffer_counter = has_ref * 2, .job_counter = 1};

	delete_context_p_for_sql_expr_eval_context_for_rhendb(ec.context_p);

	operator_desc* d = push_operator_desc(qpd, OP_PROJECTION);
	if(d == NULL)
	{
		for(uint32_t i = 0; i < projection_descriptions_count; i++)
			if(projection_descriptions[i].type == PROJECT_IDENTITY)
				free((data_type_info*) output_tuple_def->type_info->containees[i].al.type_info);
		free((data_type_info*) output_tuple_def->type_info);
		free(output_tuple_def);
		return NULL;
	}
	d->params.projection = (projection_operator_params){input_operator_index, projection_descriptions, projection_descriptions_count};
	d->output_tuple_def = output_tuple_def;
	d->resource_counter = rc;
	return d;
}

// ===================================================================================================
// aggregations
// ===================================================================================================

operator_desc* insert_simple_aggregation_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids)
{
	if(!is_valid_operator_index(qpd, input_operator_index) || aggregate_functions_count == 0 || aggregate_functions == NULL)
		return NULL;
	const tuple_def* in = out_def_at(qpd, input_operator_index);
	tuple_def* td = build_aggregation_output_def("aggregates", in, 0, NULL, aggregate_functions_count, aggregate_functions);
	if(td == NULL) return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_SIMPLE_AGGREGATION);
	if(d == NULL) { free((data_type_info*) td->type_info); free(td); return NULL; }
	d->params.simple_aggregation = (simple_aggregation_operator_params){input_operator_index, aggregate_functions_count, aggregate_functions, aggregate_input_element_ids};
	d->output_tuple_def = td;
	d->resource_counter = (operator_resource_counter){.buffer_counter = get_max_buffers_count_for_all_aggregate_functions(aggregate_functions_count, (aggregate_function const * const *) aggregate_functions), .job_counter = 1};
	return d;
}

operator_desc* insert_sorted_aggregation_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids)
{
	if(!is_valid_operator_index(qpd, input_operator_index) || key_element_count == 0 || key_element_ids == NULL || aggregate_functions_count == 0 || aggregate_functions == NULL)
		return NULL;
	const tuple_def* in = out_def_at(qpd, input_operator_index);
	if(in != NULL && !are_all_positions_accessible_for_tuple_def(in, key_element_ids, key_element_count))
		return NULL;
	tuple_def* td = build_aggregation_output_def("keyed_aggregates", in, key_element_count, key_element_ids, aggregate_functions_count, aggregate_functions);
	if(td == NULL) return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_SORTED_AGGREGATION);
	if(d == NULL)
	{
		for(uint32_t i = 0; i < key_element_count; i++) free((data_type_info*) td->type_info->containees[i].al.type_info);
		free((data_type_info*) td->type_info); free(td); return NULL;
	}
	d->params.sorted_aggregation = (sorted_aggregation_operator_params){input_operator_index, key_element_count, key_element_ids, aggregate_functions_count, aggregate_functions, aggregate_input_element_ids};
	d->output_tuple_def = td;
	uint64_t ext = (in != NULL) ? has_extended_type_info3(in, key_element_count, key_element_ids, PERSISTENT_EXT_SUB_TYPE) : 0;
	uint64_t aggbuf = get_max_buffers_count_for_all_aggregate_functions(aggregate_functions_count, (aggregate_function const * const *) aggregate_functions);
	d->resource_counter = (operator_resource_counter){.buffer_counter = (2*ext > aggbuf ? 2*ext : aggbuf), .job_counter = 1};
	return d;
}

operator_desc* insert_hash_aggregation_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_build_tuple_buffer_size)
{
	if(!is_valid_operator_index(qpd, input_operator_index) || key_element_count == 0 || key_element_ids == NULL || aggregate_functions_count == 0 || aggregate_functions == NULL)
		return NULL;
	const tuple_def* in = out_def_at(qpd, input_operator_index);
	if(in != NULL && !are_all_positions_accessible_for_tuple_def(in, key_element_ids, key_element_count))
		return NULL;
	tuple_def* td = build_aggregation_output_def("keyed_aggregates", in, key_element_count, key_element_ids, aggregate_functions_count, aggregate_functions);
	if(td == NULL) return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_HASH_AGGREGATION);
	if(d == NULL)
	{
		for(uint32_t i = 0; i < key_element_count; i++) free((data_type_info*) td->type_info->containees[i].al.type_info);
		free((data_type_info*) td->type_info); free(td); return NULL;
	}
	d->params.hash_aggregation = (hash_aggregation_operator_params){input_operator_index, key_element_count, key_element_ids, aggregate_functions_count, aggregate_functions, aggregate_input_element_ids, partitions_count, max_concurrent_jobs_count, max_concurrent_jobs_queue_size, min_build_tuple_buffer_size};
	d->output_tuple_def = td;
	uint64_t ext = (in != NULL) ? has_extended_type_info3(in, key_element_count, key_element_ids, PERSISTENT_EXT_SUB_TYPE) : 0;
	uint64_t aggbuf = get_max_buffers_count_for_all_aggregate_functions(aggregate_functions_count, (aggregate_function const * const *) aggregate_functions);
	uint64_t per_job = (2*ext > aggbuf ? 2*ext : aggbuf);
	d->resource_counter = (operator_resource_counter){.buffer_counter = max_concurrent_jobs_count * per_job, .job_counter = max_concurrent_jobs_count + 1};
	return d;
}

// ===================================================================================================
// sinks
// ===================================================================================================

operator_desc* insert_consumer_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, int (*consumer)(void* consumer_context, const void* tuple, const tuple_def* input_tuple_def), void* consumer_context)
{
	if(!is_valid_operator_index(qpd, input_operator_index))
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_CONSUMER);
	if(d == NULL) return NULL;
	d->params.consumer = (consumer_operator_params){input_operator_index, consumer, consumer_context};
	d->output_tuple_def = NULL;
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

operator_desc* insert_stream_output_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, stream* out_strm)
{
	if(!is_valid_operator_index(qpd, input_operator_index))
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_STREAM_OUTPUT);
	if(d == NULL) return NULL;
	d->params.stream_output = (stream_output_operator_params){input_operator_index, out_strm};
	d->output_tuple_def = NULL;
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

operator_desc* insert_subquery_output_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, sync_pipe* out_pipe)
{
	if(!is_valid_operator_index(qpd, input_operator_index))
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_SUBQUERY_OUTPUT);
	if(d == NULL) return NULL;
	d->params.subquery_output = (subquery_output_operator_params){input_operator_index, out_pipe};
	d->output_tuple_def = NULL;
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

// ===================================================================================================
// multi-input
// ===================================================================================================

static uint64_t* clone_indices(const uint64_t* src, uint64_t count)
{
	uint64_t* dst = malloc(sizeof(uint64_t) * (count ? count : 1));
	if(dst == NULL) return NULL;
	for(uint64_t i = 0; i < count; i++) dst[i] = src[i];
	return dst;
}

operator_desc* insert_union_operator_descriptor(query_plan_desc* qpd, const uint64_t* input_operator_indices, uint64_t input_operators_count)
{
	if(input_operators_count == 0 || input_operator_indices == NULL)
		return NULL;
	for(uint64_t i = 0; i < input_operators_count; i++)
		if(!is_valid_operator_index(qpd, input_operator_indices[i])) return NULL;
	const tuple_def* first = out_def_at(qpd, input_operator_indices[0]);
	for(uint64_t i = 1; i < input_operators_count; i++)
	{
		const tuple_def* oth = out_def_at(qpd, input_operator_indices[i]);
		if(first != NULL && oth != NULL && !are_accessibly_equivalent_type_info(first->type_info, oth->type_info))
			return NULL;
	}
	uint64_t* idx = clone_indices(input_operator_indices, input_operators_count);
	if(idx == NULL) return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_UNION);
	if(d == NULL) { free(idx); return NULL; }
	d->params.union_op = (union_operator_params){idx, input_operators_count};
	d->output_tuple_def = first;
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

operator_desc* insert_merge_sorted_inputs_operator_descriptor(query_plan_desc* qpd, const uint64_t* input_operator_indices, uint64_t input_operators_count, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction)
{
	if(input_operators_count == 0 || input_operator_indices == NULL)
		return NULL;
	for(uint64_t i = 0; i < input_operators_count; i++)
		if(!is_valid_operator_index(qpd, input_operator_indices[i])) return NULL;
	const tuple_def* first = out_def_at(qpd, input_operator_indices[0]);
	for(uint64_t i = 1; i < input_operators_count; i++)
	{
		const tuple_def* oth = out_def_at(qpd, input_operator_indices[i]);
		if(first != NULL && oth != NULL && !are_accessibly_equivalent_type_info(first->type_info, oth->type_info))
			return NULL;
	}
	if(first != NULL && key_element_count > 0 && key_element_ids != NULL && !are_all_positions_accessible_for_tuple_def(first, key_element_ids, key_element_count))
		return NULL;
	uint64_t* idx = clone_indices(input_operator_indices, input_operators_count);
	if(idx == NULL) return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_MERGE_SORTED_INPUTS);
	if(d == NULL) { free(idx); return NULL; }
	d->params.merge_sorted_inputs = (merge_sorted_inputs_operator_params){idx, input_operators_count, key_element_count, key_element_ids, key_compare_direction};
	d->output_tuple_def = first;
	uint64_t ext = (first != NULL) ? has_extended_type_info3(first, key_element_count, key_element_ids, PERSISTENT_EXT_SUB_TYPE) : 0;
	d->resource_counter = (operator_resource_counter){.buffer_counter = 2 * ext, .job_counter = 1};
	return d;
}

operator_desc* insert_result_match_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index_0, uint64_t input_operator_index_1)
{
	if(!is_valid_operator_index(qpd, input_operator_index_0) || !is_valid_operator_index(qpd, input_operator_index_1))
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_RESULT_MATCH);
	if(d == NULL) return NULL;
	d->params.result_match.input_operator_indices[0] = input_operator_index_0;
	d->params.result_match.input_operator_indices[1] = input_operator_index_1;
	d->output_tuple_def = NULL;   // TODO: port result_match's exact output schema
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

// ===================================================================================================
// joins / semi-joins
// ===================================================================================================

static tuple_def* join_def_from_indices(query_plan_desc* qpd, uint64_t left_index, uint64_t right_index)
{
	const tuple_def* l = out_def_at(qpd, left_index);
	const tuple_def* r = out_def_at(qpd, right_index);
	if(l == NULL || r == NULL) return NULL;
	return build_join_output_def(l, r);
}

operator_desc* insert_block_nested_loop_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, uint64_t right_input_operator_index, sql_expression* join_expr, join_preserve_type ptype, uint32_t min_block_size)
{
	if(!is_valid_operator_index(qpd, left_input_operator_index) || !is_valid_operator_index(qpd, right_input_operator_index) || DOES_IT_PRESERVE_RIGHT(ptype))
		return NULL;
	tuple_def* td = join_def_from_indices(qpd, left_input_operator_index, right_input_operator_index);
	operator_desc* d = push_operator_desc(qpd, OP_BLOCK_NESTED_LOOP_JOIN);
	if(d == NULL) { if(td){ free((data_type_info*)td->type_info->containees[0].al.type_info); free((data_type_info*)td->type_info->containees[1].al.type_info); free((data_type_info*)td->type_info); free(td);} return NULL; }
	d->params.block_nested_loop_join = (block_nested_loop_join_operator_params){left_input_operator_index, right_input_operator_index, join_expr, ptype, min_block_size};
	d->output_tuple_def = td;
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

operator_desc* insert_sort_merge_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, join_preserve_type ptype, uint32_t min_block_size)
{
	if(!is_valid_operator_index(qpd, left_input_operator_index) || !is_valid_operator_index(qpd, right_input_operator_index))
		return NULL;
	tuple_def* td = join_def_from_indices(qpd, left_input_operator_index, right_input_operator_index);
	operator_desc* d = push_operator_desc(qpd, OP_SORT_MERGE_JOIN);
	if(d == NULL) { if(td){ free((data_type_info*)td->type_info->containees[0].al.type_info); free((data_type_info*)td->type_info->containees[1].al.type_info); free((data_type_info*)td->type_info); free(td);} return NULL; }
	d->params.sort_merge_join = (sort_merge_join_operator_params){left_input_operator_index, left_key_element_ids, right_input_operator_index, right_key_element_ids, key_compare_direction, key_element_count, ptype, min_block_size};
	d->output_tuple_def = td;
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

operator_desc* insert_hash_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, uint32_t key_element_count, join_preserve_type ptype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size)
{
	if(!is_valid_operator_index(qpd, left_input_operator_index) || !is_valid_operator_index(qpd, right_input_operator_index))
		return NULL;
	tuple_def* td = join_def_from_indices(qpd, left_input_operator_index, right_input_operator_index);
	operator_desc* d = push_operator_desc(qpd, OP_HASH_JOIN);
	if(d == NULL) { if(td){ free((data_type_info*)td->type_info->containees[0].al.type_info); free((data_type_info*)td->type_info->containees[1].al.type_info); free((data_type_info*)td->type_info); free(td);} return NULL; }
	d->params.hash_join = (hash_join_operator_params){left_input_operator_index, left_key_element_ids, right_input_operator_index, right_key_element_ids, key_element_count, ptype, partitions_count, max_concurrent_jobs_count, max_concurrent_jobs_queue_size, min_pending_buffer_size};
	d->output_tuple_def = td;
	d->resource_counter = (operator_resource_counter){.job_counter = max_concurrent_jobs_count + 1};
	return d;
}

static operator_desc* push_semi_join(query_plan_desc* qpd, operator_descriptor_type type, uint64_t left_index, uint64_t right_index)
{
	if(!is_valid_operator_index(qpd, left_index) || !is_valid_operator_index(qpd, right_index))
		return NULL;
	const tuple_def* l = out_def_at(qpd, left_index);
	operator_desc* d = push_operator_desc(qpd, type);
	if(d == NULL) return NULL;
	d->output_tuple_def = l;   // semi/anti join emits left tuples
	return d;
}

operator_desc* insert_block_nested_loop_semi_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, uint64_t right_input_operator_index, sql_expression* join_expr, semi_join_type stype, uint32_t min_block_size)
{
	operator_desc* d = push_semi_join(qpd, OP_BLOCK_NESTED_LOOP_SEMI_JOIN, left_input_operator_index, right_input_operator_index);
	if(d == NULL) return NULL;
	d->params.block_nested_loop_semi_join = (block_nested_loop_semi_join_operator_params){left_input_operator_index, right_input_operator_index, join_expr, stype, min_block_size};
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

operator_desc* insert_sort_merge_semi_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, semi_join_type stype, uint32_t min_block_size)
{
	operator_desc* d = push_semi_join(qpd, OP_SORT_MERGE_SEMI_JOIN, left_input_operator_index, right_input_operator_index);
	if(d == NULL) return NULL;
	d->params.sort_merge_semi_join = (sort_merge_semi_join_operator_params){left_input_operator_index, left_key_element_ids, right_input_operator_index, right_key_element_ids, key_compare_direction, key_element_count, stype, min_block_size};
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

operator_desc* insert_hash_semi_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, uint32_t key_element_count, semi_join_type stype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size)
{
	operator_desc* d = push_semi_join(qpd, OP_HASH_SEMI_JOIN, left_input_operator_index, right_input_operator_index);
	if(d == NULL) return NULL;
	d->params.hash_semi_join = (hash_semi_join_operator_params){left_input_operator_index, left_key_element_ids, right_input_operator_index, right_key_element_ids, key_element_count, stype, partitions_count, max_concurrent_jobs_count, max_concurrent_jobs_queue_size, min_pending_buffer_size};
	d->resource_counter = (operator_resource_counter){.job_counter = max_concurrent_jobs_count + 1};
	return d;
}

// ===================================================================================================
// table write / lookup ops  (output schema depends on fetched_table internals -> ported later)
// ===================================================================================================

operator_desc* insert_insertion_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, positional_accessor* insertion_from_source_positional_accessors, const fetched_table* ftabl, int output_flags, heap_table_notifier* global_heap_notifier, int additional_flags)
{
	if(!is_valid_operator_index(qpd, input_operator_index) || ftabl == NULL)
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_INSERTION);
	if(d == NULL) return NULL;
	d->params.insertion = (insertion_operator_params){input_operator_index, insertion_from_source_positional_accessors, ftabl, output_flags, global_heap_notifier, additional_flags};
	d->output_tuple_def = NULL;   // TODO: port flag-based output schema
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

operator_desc* insert_deletion_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, positional_accessor* partition_id_from_source_positional_accessor, positional_accessor* tuple_pointer_from_source_positional_accessor, const fetched_table* ftabl, uint64_t deletion_batch_size, int output_flags, int additional_flags)
{
	if(!is_valid_operator_index(qpd, input_operator_index) || ftabl == NULL)
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_DELETION);
	if(d == NULL) return NULL;
	d->params.deletion = (deletion_operator_params){input_operator_index, partition_id_from_source_positional_accessor, tuple_pointer_from_source_positional_accessor, ftabl, deletion_batch_size, output_flags, additional_flags};
	d->output_tuple_def = NULL;   // TODO
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

operator_desc* insert_pointer_lookup_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, positional_accessor* partition_id_from_source_positional_accessor, positional_accessor* tuple_pointer_from_source_positional_accessor, const fetched_table* ftabl, uint64_t lookup_batch_size, int output_flags, int additional_flags)
{
	if(!is_valid_operator_index(qpd, input_operator_index) || ftabl == NULL)
		return NULL;
	operator_desc* d = push_operator_desc(qpd, OP_POINTER_LOOKUP);
	if(d == NULL) return NULL;
	d->params.pointer_lookup = (pointer_lookup_operator_params){input_operator_index, partition_id_from_source_positional_accessor, tuple_pointer_from_source_positional_accessor, ftabl, lookup_batch_size, output_flags, additional_flags};
	d->output_tuple_def = NULL;   // TODO
	d->resource_counter = (operator_resource_counter){.job_counter = 1};
	return d;
}

// ===================================================================================================
// tuple transformer inserts  (append to the LAST inserted operator)
// ===================================================================================================

static tuple_transformer_desc* push_transformer_desc(query_plan_desc* qpd, tuple_transformer_descriptor_type type)
{
	uint64_t n = get_element_count_arraylist(&(qpd->operator_descriptors));
	if(n == 0)
		return NULL;
	operator_desc* d = get_operator_descriptor_at(qpd, n - 1);

	tuple_transformer_desc* ttd = malloc(sizeof(tuple_transformer_desc));
	if(ttd == NULL) return NULL;
	memset(ttd, 0, sizeof(tuple_transformer_desc));
	ttd->type = type;

	// chain input def = last transformer's output_def, else the operator's own output_tuple_def
	uint64_t tcount = get_element_count_arraylist(&(d->transformer_descriptors));
	const tuple_def* chain_in = (tcount == 0) ? d->output_tuple_def : ((const tuple_transformer_desc*) get_from_front_of_arraylist(&(d->transformer_descriptors), tcount - 1))->output_def;
	// identity/clone preserve schema; schema-changing transformers' output_def is resolved at create() for now
	ttd->output_def = (type == TTD_IDENTITY || type == TTD_CLONE) ? chain_in : NULL;

	if(is_full_arraylist(&(d->transformer_descriptors)) && !expand_arraylist(&(d->transformer_descriptors)))
	{ free(ttd); return NULL; }
	push_back_to_arraylist(&(d->transformer_descriptors), ttd);
	return ttd;
}

tuple_transformer_desc* insert_identity_tuple_transformer_descriptor(query_plan_desc* qpd)
{ return push_transformer_desc(qpd, TTD_IDENTITY); }

tuple_transformer_desc* insert_clone_tuple_transformer_descriptor(query_plan_desc* qpd)
{ return push_transformer_desc(qpd, TTD_CLONE); }

tuple_transformer_desc* insert_is_readable_for_mvcc_snapshot_tuple_transformer_descriptor(query_plan_desc* qpd, const mvcc_snapshot* mvccsnp_p, transaction_status_getter* tsg_p, int* were_hints_updated)
{
	tuple_transformer_desc* ttd = push_transformer_desc(qpd, TTD_IS_READABLE_FOR_MVCC_SNAPSHOT);
	if(ttd == NULL) return NULL;
	ttd->params.is_readable_for_mvcc_snapshot = (is_readable_for_mvcc_snapshot_transformer_params){mvccsnp_p, tsg_p, were_hints_updated};
	return ttd;
}

tuple_transformer_desc* insert_simple_projection_tuple_transformer_descriptor(query_plan_desc* qpd, const char* output_table_name, uint32_t projections_count, positional_accessor** projections, const char** field_names)
{
	tuple_transformer_desc* ttd = push_transformer_desc(qpd, TTD_SIMPLE_PROJECTION);
	if(ttd == NULL) return NULL;
	ttd->params.simple_projection = (simple_projection_transformer_params){output_table_name, projections_count, projections, field_names};
	return ttd;
}

tuple_transformer_desc* insert_scalar_projection_tuple_transformer_descriptor(query_plan_desc* qpd, positional_accessor* projection)
{
	tuple_transformer_desc* ttd = push_transformer_desc(qpd, TTD_SCALAR_PROJECTION);
	if(ttd == NULL) return NULL;
	ttd->params.scalar_projection = (scalar_projection_transformer_params){projection};
	return ttd;
}

tuple_transformer_desc* insert_row_number_prepender_tuple_transformer_descriptor(query_plan_desc* qpd, uint64_t next_row_number)
{
	tuple_transformer_desc* ttd = push_transformer_desc(qpd, TTD_ROW_NUMBER_PREPENDER);
	if(ttd == NULL) return NULL;
	ttd->params.row_number_prepender = (row_number_prepender_transformer_params){next_row_number};
	return ttd;
}

tuple_transformer_desc* insert_row_identifier_prepender_tuple_transformer_descriptor(query_plan_desc* qpd, uint64_t row_identifier)
{
	tuple_transformer_desc* ttd = push_transformer_desc(qpd, TTD_ROW_IDENTIFIER_PREPENDER);
	if(ttd == NULL) return NULL;
	ttd->params.row_identifier_prepender = (row_identifier_prepender_transformer_params){row_identifier};
	return ttd;
}

tuple_transformer_desc* insert_expressioned_selection_tuple_transformer_descriptor(query_plan_desc* qpd, sql_expression* expr)
{
	tuple_transformer_desc* ttd = push_transformer_desc(qpd, TTD_EXPRESSIONED_SELECTION);
	if(ttd == NULL) return NULL;
	ttd->params.expressioned_selection = (expressioned_selection_transformer_params){expr};
	return ttd;
}

tuple_transformer_desc* insert_unified_typing_tuple_transformer_descriptor(query_plan_desc* qpd, uint32_t* column_ids_to_transform, uint32_t column_to_transform_count)
{
	tuple_transformer_desc* ttd = push_transformer_desc(qpd, TTD_UNIFIED_TYPING);
	if(ttd == NULL) return NULL;
	ttd->params.unified_typing = (unified_typing_transformer_params){column_ids_to_transform, column_to_transform_count};
	return ttd;
}

// ===================================================================================================
// create_query_plan : descriptor -> real, executable query_plan
// ===================================================================================================

static tuple_transformer* build_transformer(query_plan_desc* qpd, const tuple_transformer_desc* ttd, const tuple_def* input_def)
{
	switch(ttd->type)
	{
		case TTD_IDENTITY : return get_new_identity_tuple_transformer(input_def);
		case TTD_CLONE :    return get_new_clone_tuple_transformer(input_def);
		case TTD_IS_READABLE_FOR_MVCC_SNAPSHOT :
			return get_new_is_readable_for_mvcc_snapshot_transformer(ttd->params.is_readable_for_mvcc_snapshot.mvccsnp_p, ttd->params.is_readable_for_mvcc_snapshot.tsg_p, ttd->params.is_readable_for_mvcc_snapshot.were_hints_updated, input_def);
		case TTD_SIMPLE_PROJECTION :
			return get_new_simple_projection_transformer(ttd->params.simple_projection.output_table_name, input_def, ttd->params.simple_projection.projections_count, ttd->params.simple_projection.projections, ttd->params.simple_projection.field_names);
		case TTD_SCALAR_PROJECTION :
			return get_new_scalar_projection_transformer(input_def, ttd->params.scalar_projection.projection);
		case TTD_ROW_NUMBER_PREPENDER :
			return get_new_row_number_prepender_transformer(input_def, ttd->params.row_number_prepender.next_row_number);
		case TTD_ROW_IDENTIFIER_PREPENDER :
			return get_new_row_identifier_prepender_transformer(input_def, ttd->params.row_identifier_prepender.row_identifier);
		case TTD_EXPRESSIONED_SELECTION :
			return get_new_expressioned_selection_transformer(input_def, qpd->tx, ttd->params.expressioned_selection.expr);
		case TTD_UNIFIED_TYPING :
			return get_new_unified_typing_transformer(input_def, qpd->tx, ttd->params.unified_typing.column_ids_to_transform, ttd->params.unified_typing.column_to_transform_count);
		default : return NULL;
	}
}

query_plan* create_query_plan(query_plan_desc* qpd)
{
	uint64_t count = get_element_count_arraylist(&(qpd->operator_descriptors));

	query_plan* qp = get_new_query_plan(qpd->tx, (uint32_t) count);
	if(qp == NULL)
		return NULL;

	operator** built = malloc(sizeof(operator*) * (count ? count : 1));
	if(built == NULL)
		return NULL;

	for(uint64_t i = 0; i < count; i++)
	{
		operator_desc* d = get_operator_descriptor_at(qpd, i);
		operator* o = get_new_registered_operator_for_query_plan(qp);
		if(o == NULL) { free(built); return NULL; }

		operator* in0 = NULL; operator* in1 = NULL;
		switch(d->type)
		{
			case OP_CONSTANT_DATASET :
				d->resource_counter = setup_constant_dataset_operator(o, d->params.constant_dataset.constant_dataset_p, d->params.constant_dataset.record_def); break;
			case OP_GENERATOR :
				d->resource_counter = setup_generator_operator(o, d->params.generator.generator, d->params.generator.generator_context, d->params.generator.generator_tuple_def); break;
			case OP_STREAM_INPUT :
				d->resource_counter = setup_stream_input_operator(o, d->params.stream_input.in_strm, d->params.stream_input.input_tuple_def); break;
			case OP_SCAN :
				d->resource_counter = setup_scan_operator(o, qp, d->params.scan.ftabl, d->params.scan.max_concurrent_jobs_count, d->params.scan.output_flags, d->params.scan.additional_flags); break;
			case OP_IDENTITY :
				d->resource_counter = setup_identity_operator(o, built[d->params.identity.input_operator_index]); break;
			case OP_OFFSET_LIMIT :
				d->resource_counter = setup_offset_limit_operator(o, built[d->params.offset_limit.input_operator_index], d->params.offset_limit.offset_counter, d->params.offset_limit.limit_counter); break;
			case OP_SELECTION :
				d->resource_counter = setup_selection_operator(o, built[d->params.selection.input_operator_index], d->params.selection.expr); break;
			case OP_SORT :
				d->resource_counter = setup_sort_operator(o, d->params.sort.result_counter, built[d->params.sort.input_operator_index], d->params.sort.key_element_count, d->params.sort.key_element_ids, d->params.sort.key_compare_direction, d->params.sort.min_run_size, d->params.sort.N_way_sort, d->params.sort.max_concurrent_jobs_count); break;
			case OP_PROJECTION :
				d->resource_counter = setup_projection_operator(o, built[d->params.projection.input_operator_index], d->params.projection.projection_descriptions, d->params.projection.projection_descriptions_count); break;
			case OP_SIMPLE_AGGREGATION :
				d->resource_counter = setup_simple_aggregation_operator(o, built[d->params.simple_aggregation.input_operator_index], d->params.simple_aggregation.aggregate_functions_count, d->params.simple_aggregation.aggregate_functions, d->params.simple_aggregation.aggregate_input_element_ids); break;
			case OP_SORTED_AGGREGATION :
				d->resource_counter = setup_sorted_aggregation_operator(o, built[d->params.sorted_aggregation.input_operator_index], d->params.sorted_aggregation.key_element_count, d->params.sorted_aggregation.key_element_ids, d->params.sorted_aggregation.aggregate_functions_count, d->params.sorted_aggregation.aggregate_functions, d->params.sorted_aggregation.aggregate_input_element_ids); break;
			case OP_HASH_AGGREGATION :
				d->resource_counter = setup_hash_aggregation_operator(o, built[d->params.hash_aggregation.input_operator_index], d->params.hash_aggregation.key_element_count, d->params.hash_aggregation.key_element_ids, d->params.hash_aggregation.aggregate_functions_count, d->params.hash_aggregation.aggregate_functions, d->params.hash_aggregation.aggregate_input_element_ids, d->params.hash_aggregation.partitions_count, d->params.hash_aggregation.max_concurrent_jobs_count, d->params.hash_aggregation.max_concurrent_jobs_queue_size, d->params.hash_aggregation.min_build_tuple_buffer_size); break;
			case OP_CONSUMER :
				d->resource_counter = setup_consumer_operator(o, built[d->params.consumer.input_operator_index], d->params.consumer.consumer, d->params.consumer.consumer_context); break;
			case OP_STREAM_OUTPUT :
				d->resource_counter = setup_stream_output_operator(o, built[d->params.stream_output.input_operator_index], d->params.stream_output.out_strm); break;
			case OP_SUBQUERY_OUTPUT :
				d->resource_counter = setup_subquery_output_operator(o, built[d->params.subquery_output.input_operator_index], d->params.subquery_output.out_pipe); break;
			case OP_UNION :
			{
				operator** insp = malloc(sizeof(operator*) * d->params.union_op.input_operators_count);
				for(uint64_t k = 0; k < d->params.union_op.input_operators_count; k++) insp[k] = built[d->params.union_op.input_operator_indices[k]];
				d->resource_counter = setup_union_operator(o, insp, (uint32_t) d->params.union_op.input_operators_count);
				if(insp != ins) free(insp);
				break;
			}
			case OP_MERGE_SORTED_INPUTS :
			{
				operator** insp = malloc(sizeof(operator*) * d->params.merge_sorted_inputs.input_operators_count);
				for(uint64_t k = 0; k < d->params.merge_sorted_inputs.input_operators_count; k++) insp[k] = built[d->params.merge_sorted_inputs.input_operator_indices[k]];
				d->resource_counter = setup_merge_sorted_inputs_operator(o, insp, (uint32_t) d->params.merge_sorted_inputs.input_operators_count, d->params.merge_sorted_inputs.key_element_count, d->params.merge_sorted_inputs.key_element_ids, d->params.merge_sorted_inputs.key_compare_direction);
				if(insp != ins) free(insp);
				break;
			}
			case OP_RESULT_MATCH :
			{
				operator* ins[2] = { built[d->params.result_match.input_operator_indices[0]], built[d->params.result_match.input_operator_indices[1]] };
				d->resource_counter = setup_result_match_operator(o, ins); break;
			}
			case OP_BLOCK_NESTED_LOOP_JOIN :
				in0 = built[d->params.block_nested_loop_join.left_input_operator_index]; in1 = built[d->params.block_nested_loop_join.right_input_operator_index];
				d->resource_counter = setup_block_nested_loop_join_operator(o, in0, in1, d->params.block_nested_loop_join.join_expr, d->params.block_nested_loop_join.ptype, d->params.block_nested_loop_join.min_block_size); break;
			case OP_SORT_MERGE_JOIN :
				in0 = built[d->params.sort_merge_join.left_input_operator_index]; in1 = built[d->params.sort_merge_join.right_input_operator_index];
				d->resource_counter = setup_sort_merge_join_operator(o, in0, d->params.sort_merge_join.left_key_element_ids, in1, d->params.sort_merge_join.right_key_element_ids, d->params.sort_merge_join.key_compare_direction, d->params.sort_merge_join.key_element_count, d->params.sort_merge_join.ptype, d->params.sort_merge_join.min_block_size); break;
			case OP_HASH_JOIN :
				in0 = built[d->params.hash_join.left_input_operator_index]; in1 = built[d->params.hash_join.right_input_operator_index];
				d->resource_counter = setup_hash_join_operator(o, in0, d->params.hash_join.left_key_element_ids, in1, d->params.hash_join.right_key_element_ids, d->params.hash_join.key_element_count, d->params.hash_join.ptype, d->params.hash_join.partitions_count, d->params.hash_join.max_concurrent_jobs_count, d->params.hash_join.max_concurrent_jobs_queue_size, d->params.hash_join.min_pending_buffer_size); break;
			case OP_BLOCK_NESTED_LOOP_SEMI_JOIN :
				in0 = built[d->params.block_nested_loop_semi_join.left_input_operator_index]; in1 = built[d->params.block_nested_loop_semi_join.right_input_operator_index];
				d->resource_counter = setup_block_nested_loop_semi_join_operator(o, in0, in1, d->params.block_nested_loop_semi_join.join_expr, d->params.block_nested_loop_semi_join.stype, d->params.block_nested_loop_semi_join.min_block_size); break;
			case OP_SORT_MERGE_SEMI_JOIN :
				in0 = built[d->params.sort_merge_semi_join.left_input_operator_index]; in1 = built[d->params.sort_merge_semi_join.right_input_operator_index];
				d->resource_counter = setup_sort_merge_semi_join_operator(o, in0, d->params.sort_merge_semi_join.left_key_element_ids, in1, d->params.sort_merge_semi_join.right_key_element_ids, d->params.sort_merge_semi_join.key_compare_direction, d->params.sort_merge_semi_join.key_element_count, d->params.sort_merge_semi_join.stype, d->params.sort_merge_semi_join.min_block_size); break;
			case OP_HASH_SEMI_JOIN :
				in0 = built[d->params.hash_semi_join.left_input_operator_index]; in1 = built[d->params.hash_semi_join.right_input_operator_index];
				d->resource_counter = setup_hash_semi_join_operator(o, in0, d->params.hash_semi_join.left_key_element_ids, in1, d->params.hash_semi_join.right_key_element_ids, d->params.hash_semi_join.key_element_count, d->params.hash_semi_join.stype, d->params.hash_semi_join.partitions_count, d->params.hash_semi_join.max_concurrent_jobs_count, d->params.hash_semi_join.max_concurrent_jobs_queue_size, d->params.hash_semi_join.min_pending_buffer_size); break;
			case OP_INSERTION :
				d->resource_counter = setup_insertion_operator(o, built[d->params.insertion.input_operator_index], d->params.insertion.insertion_from_source_positional_accessors, d->params.insertion.ftabl, d->params.insertion.output_flags, d->params.insertion.global_heap_notifier, d->params.insertion.additional_flags); break;
			case OP_DELETION :
				d->resource_counter = setup_deletion_operator(o, built[d->params.deletion.input_operator_index], d->params.deletion.partition_id_from_source_positional_accessor, d->params.deletion.tuple_pointer_from_source_positional_accessor, d->params.deletion.ftabl, d->params.deletion.deletion_batch_size, d->params.deletion.output_flags, d->params.deletion.additional_flags); break;
			case OP_POINTER_LOOKUP :
				d->resource_counter = setup_pointer_lookup_operator(o, built[d->params.pointer_lookup.input_operator_index], d->params.pointer_lookup.partition_id_from_source_positional_accessor, d->params.pointer_lookup.tuple_pointer_from_source_positional_accessor, d->params.pointer_lookup.ftabl, d->params.pointer_lookup.lookup_batch_size, d->params.pointer_lookup.output_flags, d->params.pointer_lookup.additional_flags); break;
			default : free(built); return NULL;
		}

		// append the transformer chain that follows this operator
		uint64_t tcount = get_element_count_arraylist(&(d->transformer_descriptors));
		for(uint64_t t = 0; t < tcount; t++)
		{
			const tuple_transformer_desc* ttd = (const tuple_transformer_desc*) get_from_front_of_arraylist(&(d->transformer_descriptors), t);
			const tuple_def* cur_in = get_output_def_for_tuple_transformers(&(o->output_tuple_transformers));
			tuple_transformer* tt = build_transformer(qpd, ttd, cur_in);
			if(tt == NULL) { free(built); return NULL; }
			if(!append_tuple_transformer(&(o->output_tuple_transformers), tt)) { delete_tuple_transformer(tt); free(built); return NULL; }
		}

		built[i] = o;
	}

	free(built);
	return qp;
}

// ===================================================================================================
// destroy : free everything the descriptor owns (mirrors each operator's free_resources())
// ===================================================================================================

void destroy_query_plan_descriptor(query_plan_desc* qpd)
{
	if(qpd == NULL)
		return;

	uint64_t count = get_element_count_arraylist(&(qpd->operator_descriptors));
	for(uint64_t i = 0; i < count; i++)
	{
		operator_desc* d = get_operator_descriptor_at(qpd, i);

		// 1) free the output_tuple_def we allocated, exactly as the operator's free_resources() does.
		//    (pass-through / leaf operators borrow their input/param def -> never freed here.)
		tuple_def* td = (tuple_def*) d->output_tuple_def;
		switch(d->type)
		{
			case OP_PROJECTION :
				if(td != NULL)
				{
					for(uint32_t c = 0; c < d->params.projection.projection_descriptions_count; c++)
						if(d->params.projection.projection_descriptions[c].type == PROJECT_IDENTITY)
							free((data_type_info*) td->type_info->containees[c].al.type_info);
					free((data_type_info*) td->type_info); free(td);
				}
				break;
			case OP_SIMPLE_AGGREGATION :
				if(td != NULL) { free((data_type_info*) td->type_info); free(td); }
				break;
			case OP_SORTED_AGGREGATION :
				if(td != NULL)
				{
					for(uint32_t c = 0; c < d->params.sorted_aggregation.key_element_count; c++)
						free((data_type_info*) td->type_info->containees[c].al.type_info);
					free((data_type_info*) td->type_info); free(td);
				}
				break;
			case OP_HASH_AGGREGATION :
				if(td != NULL)
				{
					for(uint32_t c = 0; c < d->params.hash_aggregation.key_element_count; c++)
						free((data_type_info*) td->type_info->containees[c].al.type_info);
					free((data_type_info*) td->type_info); free(td);
				}
				break;
			case OP_BLOCK_NESTED_LOOP_JOIN :
			case OP_SORT_MERGE_JOIN :
			case OP_HASH_JOIN :
				if(td != NULL)
				{
					free((data_type_info*) td->type_info->containees[0].al.type_info);
					free((data_type_info*) td->type_info->containees[1].al.type_info);
					free((data_type_info*) td->type_info); free(td);
				}
				break;
			case OP_SCAN :
				if(td != NULL) { free((data_type_info*) td->type_info); free(td); } // all scan columns borrowed
				break;
			default : break; // IDENTITY/SELECTION/SORT/OFFSET_LIMIT/UNION/MERGE/semi-joins/sinks/leaves borrow -> nothing to free
		}

		// 2) delete the aggregate_functions this operator used (as free_resources() does).
		//    NOTE: a query_plan built from this descriptor also destroys these on its own teardown; reconciling
		//    that double-ownership (so the descriptor can be re-create()d after a run) is the "deal with later" item.
		if(d->type == OP_SIMPLE_AGGREGATION)
			for(uint32_t a = 0; a < d->params.simple_aggregation.aggregate_functions_count; a++)
			{ aggregate_function* af = d->params.simple_aggregation.aggregate_functions[a]; if(af && af->destroy_aggregate_function) af->destroy_aggregate_function(af); }
		else if(d->type == OP_SORTED_AGGREGATION)
			for(uint32_t a = 0; a < d->params.sorted_aggregation.aggregate_functions_count; a++)
			{ aggregate_function* af = d->params.sorted_aggregation.aggregate_functions[a]; if(af && af->destroy_aggregate_function) af->destroy_aggregate_function(af); }
		else if(d->type == OP_HASH_AGGREGATION)
			for(uint32_t a = 0; a < d->params.hash_aggregation.aggregate_functions_count; a++)
			{ aggregate_function* af = d->params.hash_aggregation.aggregate_functions[a]; if(af && af->destroy_aggregate_function) af->destroy_aggregate_function(af); }

		// 3) free owned input-index arrays
		if(d->type == OP_UNION) free(d->params.union_op.input_operator_indices);
		else if(d->type == OP_MERGE_SORTED_INPUTS) free(d->params.merge_sorted_inputs.input_operator_indices);

		// 4) free the transformer descriptors + their list
		uint64_t tcount = get_element_count_arraylist(&(d->transformer_descriptors));
		for(uint64_t t = 0; t < tcount; t++)
			free((void*) get_from_front_of_arraylist(&(d->transformer_descriptors), t));
		deinitialize_arraylist(&(d->transformer_descriptors));

		free(d);
	}

	deinitialize_arraylist(&(qpd->operator_descriptors));
	free(qpd);
}