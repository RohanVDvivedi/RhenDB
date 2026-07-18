#ifndef OPERATORS_H
#define OPERATORS_H

#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

// if o = NULL, in the following functions, then operator is not initialized, but a valid operator_resource_counter is still returned
// generator() and consumer() are considered to be not part of the query_plan and you must take care of their resources externally

// generates fixed set of tuples from constant_dataset_p havuing tuple_def of record_def
operator_resource_counter setup_constant_dataset_operator(operator* o, interim_tuple_store* constant_dataset_p, const tuple_def* record_def);

operator_resource_counter setup_generator_operator(operator* o, void* (*generator)(void* generator_context, const tuple_def* generator_tuple_def), void* generator_context, const tuple_def* generator_tuple_def);

operator_resource_counter setup_identity_operator(operator* o, operator* input_operator);

int print_consumer(void* consumer_context, const void* tuple, const tuple_def* input_tuple_def);
operator_resource_counter setup_consumer_operator(operator* o, operator* input_operator, int (*consumer)(void* consumer_context, const void* tuple, const tuple_def* input_tuple_def), void* consumer_context);

operator_resource_counter setup_result_match_operator(operator* o, operator* input_operators[2]);

operator_resource_counter setup_union_operator(operator* o, operator** input_operators, uint32_t input_operators_count);

#include<rhendb/aggregate_functions.h>

// the data_type_infos pointed to by the aggregate_input_element_ids, must be the same as the output from the input_operator and the input of the corresponding aggregate_functions
operator_resource_counter setup_simple_aggregation_operator(operator* o, operator* input_operator, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids);

#include<rhendb/tuples_down_counter.h>

operator_resource_counter setup_external_sort_operator(operator* o, tuples_down_counter result_counter, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint64_t min_run_size, uint32_t N_way_sort, uint32_t max_concurrent_jobs_count);

operator_resource_counter setup_offset_limit_operator(operator* o, operator* input_operator, tuples_down_counter offset_counter, tuples_down_counter limit_counter);

operator_resource_counter setup_merge_sorted_inputs_operator(operator* o, operator** input_operators, uint32_t input_operators_count, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction);

#include<cutlery/stream.h>

operator_resource_counter setup_stream_input_operator(operator* o, stream* in_strm, const tuple_def* input_tuple_def);

operator_resource_counter setup_stream_output_operator(operator* o, operator* input_operator, stream* out_strm);

// keyed aggregation operators

operator_resource_counter setup_sorted_aggregation_operator(operator* o, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids);

operator_resource_counter setup_hash_aggregation_operator(operator* o, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_build_tuple_buffer_size);

// join operators

#include<sqltoast/sql_expression.h>

#include<rhendb/join_type.h>

// DOES_IT_PRESERVE_RIGHT(ptype) must be 0
// we can not do right size preserving joins
// if join_expr == NULL, this becomes a cross join operator
operator_resource_counter setup_block_nested_loop_join_operator(operator* o, operator* left_input_operator, operator* right_input_operator, sql_expression* join_expr, join_preserve_type ptype, uint32_t min_block_size);

// equi join for sort merge join
// REMEMBER OUTPUT OF SORT-MERGE JOIN MAY NOT BE SORTED UNLESS IT IS INNER JOIN
operator_resource_counter setup_sort_merge_join_operator(operator* o, operator* left_input_operator, const positional_accessor* left_key_element_ids, operator* right_input_operator, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, join_preserve_type ptype, uint32_t min_block_size);

operator_resource_counter setup_hash_join_operator(operator* o, operator* left_input_operator, const positional_accessor* left_key_element_ids, operator* right_input_operator, const positional_accessor* right_key_element_ids, uint32_t key_element_count, join_preserve_type ptype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size);

// semi and anti joins

operator_resource_counter setup_block_nested_loop_semi_join_operator(operator* o, operator* left_input_operator, operator* right_input_operator, sql_expression* join_expr, semi_join_type stype, uint32_t min_block_size);

operator_resource_counter setup_sort_merge_semi_join_operator(operator* o, operator* left_input_operator, const positional_accessor* left_key_element_ids, operator* right_input_operator, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, semi_join_type stype, uint32_t min_block_size);

operator_resource_counter setup_hash_semi_join_operator(operator* o, operator* left_input_operator, const positional_accessor* left_key_element_ids, operator* right_input_operator, const positional_accessor* right_key_element_ids, uint32_t key_element_count, semi_join_type stype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size);

// selection operator
operator_resource_counter setup_selection_operator(operator* o, operator* input_operator, sql_expression* expr);

#include<rhendb/projection_type.h>

// projection operator
operator_resource_counter setup_projection_operator(operator* o, operator* input_operator, projection_description* projection_descriptions, uint32_t projection_descriptions_count);

#endif