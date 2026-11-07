#ifndef OPERATORS_H
#define OPERATORS_H

#include<rhendb/query_plan.h>

/*
	These functions are only meant to create the state and populate next_tuple() and destroy_state() callbacks in their operator struct
*/

void open_constant_dataset_operator(operator* o);

void open_generator_operator(operator* o);

void open_identity_operator(operator* o);

void open_row_rename_operator(operator* o);

int print_consumer(void* consumer_context, const void* tuple, const tuple_def* input_tuple_def);
void open_consumer_operator(operator* o);

void open_result_match_operator(operator* o);

void open_union_operator(operator* o);

void open_simple_aggregation_operator(operator* o);

void open_sort_operator(operator* o);

void open_offset_limit_operator(operator* o);

void open_merge_sorted_inputs_operator(operator* o);

void open_stream_input_operator(operator* o);

void open_stream_output_operator(operator* o);

void open_sorted_aggregation_operator(operator* o);

void open_hash_aggregation_operator(operator* o);

void open_block_nested_loop_join_operator(operator* o);

void open_sort_merge_join_operator(operator* o);

void open_hash_join_operator(operator* o);

void open_block_nested_loop_semi_join_operator(operator* o);

void open_sort_merge_semi_join_operator(operator* o);

void open_hash_semi_join_operator(operator* o);

void open_selection_operator(operator* o);

void open_projection_operator(operator* o);

#include<boompar/sync_pipe.h>

void open_subquery_output_operator(operator* o);
void* get_tuple_from_subquery_pipe(sync_pipe* spipe, const tuple_def* tpl_d); // a return NULL implies query completed

void open_insertion_operator(operator* o);

void open_deletion_operator(operator* o);

void open_scan_operator(operator* o);

void open_pointer_lookup_operator(operator* o);

#endif