#ifndef OPERATORS_H
#define OPERATORS_H

#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

// if o = NULL, in the following functions, then operator is not initialized, but a valid operator_resource_counter is still returned
// generator() and consumer() are considered to be not part of the query_plan and you must take care of their resources externally

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

operator_resource_counter setup_external_sort_operator(operator* o, tuples_down_counter result_counter, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint64_t minimum_run_size, uint32_t N_way_sort, uint32_t max_concurrent_jobs_count);

operator_resource_counter setup_offset_limit_operator(operator* o, operator* input_operator, tuples_down_counter offset_counter, tuples_down_counter limit_counter);

operator_resource_counter setup_merge_sorted_inputs_operator(operator* o, operator** input_operators, uint32_t input_operators_count, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction);

#include<cutlery/stream.h>

operator_resource_counter setup_stream_input_operator(operator* o, stream* in_strm, const tuple_def* input_tuple_def);

operator_resource_counter setup_stream_output_operator(operator* o, operator* input_operator, stream* out_strm);

// keyed aggregation operators

operator_resource_counter setup_sorted_aggregation_operator(operator* o, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids);

void setup_hash_aggregation_operator(operator* o, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids, uint32_t partitions_count, uint32_t bucket_count_per_parttion, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_build_tuple_buffer_size);

#endif