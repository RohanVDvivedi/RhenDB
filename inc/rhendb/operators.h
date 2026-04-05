#ifndef OPERATORS_H
#define OPERATORS_H

#include<rhendb/query_plan_interface.h>

void setup_generator_operator(operator* o, void* (*generator)(void* generator_context, tuple_def* generator_tuple_def), void* generator_context, tuple_def* generator_tuple_def);

void setup_identity_operator(operator* o, operator* input_operator);

void setup_printf_operator(operator* o, operator* input_operator, int do_print);

void setup_external_sort_operator(operator* o, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint64_t minimum_run_size, uint32_t N_way_sort, uint32_t max_concurrent_jobs_count);

void setup_result_match_operator(operator* o, operator* input_operators[2]);

#endif