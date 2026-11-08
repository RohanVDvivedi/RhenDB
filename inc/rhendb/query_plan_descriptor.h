#ifndef QUERY_PLAN_DESCRIPTOR_H
#define QUERY_PLAN_DESCRIPTOR_H

#include<rhendb/aggregate_functions.h>
#include<rhendb/tuples_down_counter.h>
#include<rhendb/join_type.h>
#include<rhendb/projection_type.h>
#include<rhendb/table_operator_output_type.h>
#include<rhendb/fetched_table.h>
#include<rhendb/interim_tuple_store.h>

#include<boompar/sync_pipe.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/expression_evaluator.h>

#include<rhendb/mvcc_snapshot.h>

#include<sqltoast/sql_expression.h>

#include<tuplestore/tuple_def.h>

#include<cutlery/arraylist.h>
#include<cutlery/stream.h>

/*
	QUERY PLAN DESCRIPTOR - a declarative, reconstructible factory for a query_plan.

	A query_plan_desc holds an arraylist of operator_desc (pointers), each of which holds an arraylist of
	tuple_transformer_desc (pointers). It is built ONCE and used to create() a real, executable query_plan
	many times.

	The query_plan descriptor clones almost all it's input values or takes ownership of what ios passed and passes them shallow copy-ly to the actual operators
	reducing the memory usage
	This means a query_plan descriptor's life time must be a superset of the lifetime of all the query_plans created from it
*/

typedef struct transaction transaction;
typedef struct rhendb rhendb;

// operator params

typedef struct constant_dataset_operator_params constant_dataset_operator_params;
struct constant_dataset_operator_params
{
	interim_tuple_store* constant_dataset_p;
	const tuple_def* record_def;
};

typedef struct generator_operator_params generator_operator_params;
struct generator_operator_params
{
	void* (*generator)(void* generator_context, const tuple_def* generator_tuple_def);
	void* generator_context;
	const tuple_def* generator_tuple_def;
};

typedef struct stream_input_operator_params stream_input_operator_params;
struct stream_input_operator_params
{
	stream* in_strm;
	const tuple_def* input_tuple_def;
};

typedef struct identity_operator_params identity_operator_params;
struct identity_operator_params
{
	uint64_t input_operator_index;
};

typedef struct row_rename_operator_params row_rename_operator_params;
struct row_rename_operator_params
{
	uint64_t input_operator_index;
	char* output_name;
	char** column_names;
	uint64_t columns_count;
};

typedef struct offset_limit_operator_params offset_limit_operator_params;
struct offset_limit_operator_params
{
	uint64_t input_operator_index;
	tuples_down_counter offset_counter;
	tuples_down_counter limit_counter;
};

typedef struct selection_operator_params selection_operator_params;
struct selection_operator_params
{
	uint64_t input_operator_index;
	sql_expression* expr;
};

typedef struct projection_operator_params projection_operator_params;
struct projection_operator_params
{
	uint64_t input_operator_index;
	char* output_name;
	projection_description* projection_descriptions; // borrowed
	uint32_t projection_descriptions_count;
};

typedef struct union_operator_params union_operator_params;
struct union_operator_params
{
	uint64_t* input_operator_indices;
	uint64_t input_operators_count;
};

typedef struct merge_sorted_inputs_operator_params merge_sorted_inputs_operator_params;
struct merge_sorted_inputs_operator_params
{
	uint64_t* input_operator_indices;
	uint64_t input_operators_count;
	uint32_t key_element_count;
	positional_accessor* key_element_ids;
	compare_direction* key_compare_direction;
};

typedef struct simple_aggregation_operator_params simple_aggregation_operator_params;
struct simple_aggregation_operator_params
{
	uint64_t input_operator_index;
	uint32_t aggregate_functions_count;
	aggregate_function** aggregate_functions;
	positional_accessor** aggregate_input_element_ids;
};

typedef struct sort_operator_params sort_operator_params;
struct sort_operator_params
{
	tuples_down_counter result_counter;
	uint64_t input_operator_index;
	uint32_t key_element_count;
	positional_accessor* key_element_ids;
	compare_direction* key_compare_direction;

	uint32_t N_way_sort;
	uint64_t memory_allowance;
};

typedef struct sorted_aggregation_operator_params sorted_aggregation_operator_params;
struct sorted_aggregation_operator_params
{
	uint64_t input_operator_index;
	uint32_t key_element_count;
	positional_accessor* key_element_ids;
	uint32_t aggregate_functions_count;
	aggregate_function** aggregate_functions;
	positional_accessor** aggregate_input_element_ids;
};

typedef struct hash_aggregation_operator_params hash_aggregation_operator_params;
struct hash_aggregation_operator_params
{
	uint64_t input_operator_index;
	uint32_t key_element_count;
	positional_accessor* key_element_ids;
	uint32_t aggregate_functions_count;
	aggregate_function** aggregate_functions;
	positional_accessor** aggregate_input_element_ids;
	uint32_t partitions_count;
	uint32_t max_concurrent_jobs_count;
	uint32_t max_concurrent_jobs_queue_size;
	uint32_t min_build_tuple_buffer_size;
};

typedef struct block_nested_loop_join_operator_params block_nested_loop_join_operator_params;
struct block_nested_loop_join_operator_params
{
	uint64_t left_input_operator_index;
	uint64_t right_input_operator_index;
	sql_expression* join_expr;
	join_preserve_type ptype;
	uint32_t min_block_size;
};

typedef struct sort_merge_join_operator_params sort_merge_join_operator_params;
struct sort_merge_join_operator_params
{
	uint64_t left_input_operator_index;
	positional_accessor* left_key_element_ids;
	uint64_t right_input_operator_index;
	positional_accessor* right_key_element_ids;
	compare_direction* key_compare_direction;
	uint32_t key_element_count;
	join_preserve_type ptype;
	uint32_t min_block_size;
};

typedef struct hash_join_operator_params hash_join_operator_params;
struct hash_join_operator_params
{
	uint64_t left_input_operator_index;
	positional_accessor* left_key_element_ids;
	uint64_t right_input_operator_index;
	positional_accessor* right_key_element_ids;
	uint32_t key_element_count;
	join_preserve_type ptype;
	uint32_t partitions_count;
	uint32_t max_concurrent_jobs_count;
	uint32_t max_concurrent_jobs_queue_size;
	uint32_t min_pending_buffer_size;
};

typedef struct block_nested_loop_semi_join_operator_params block_nested_loop_semi_join_operator_params;
struct block_nested_loop_semi_join_operator_params
{
	uint64_t left_input_operator_index;
	uint64_t right_input_operator_index;
	sql_expression* join_expr;
	semi_join_type stype;
	uint32_t min_block_size;
};

typedef struct sort_merge_semi_join_operator_params sort_merge_semi_join_operator_params;
struct sort_merge_semi_join_operator_params
{
	uint64_t left_input_operator_index;
	positional_accessor* left_key_element_ids;
	uint64_t right_input_operator_index;
	positional_accessor* right_key_element_ids;
	compare_direction* key_compare_direction;
	uint32_t key_element_count;
	semi_join_type stype;
	uint32_t min_block_size;
};

typedef struct hash_semi_join_operator_params hash_semi_join_operator_params;
struct hash_semi_join_operator_params
{
	uint64_t left_input_operator_index;
	positional_accessor* left_key_element_ids;
	uint64_t right_input_operator_index;
	positional_accessor* right_key_element_ids;
	uint32_t key_element_count;
	semi_join_type stype;
	uint32_t partitions_count;
	uint32_t max_concurrent_jobs_count;
	uint32_t max_concurrent_jobs_queue_size;
	uint32_t min_pending_buffer_size;
};

typedef struct scan_operator_params scan_operator_params;
struct scan_operator_params
{
	const fetched_table* ftabl;
	uint32_t max_concurrent_jobs_count;
	int output_flags;
	int additional_flags;
};

typedef struct pointer_lookup_operator_params pointer_lookup_operator_params;
struct pointer_lookup_operator_params
{
	uint64_t input_operator_index;
	positional_accessor* partition_id_from_source_positional_accessor;
	positional_accessor* tuple_pointer_from_source_positional_accessor;
	const fetched_table* ftabl;
	uint64_t lookup_batch_size;
	int output_flags;
	int additional_flags;
};

// ===================================================================================================
// descriptor types
// ===================================================================================================

typedef enum operator_descriptor_type operator_descriptor_type;
enum operator_descriptor_type
{
	OP_CONSTANT_DATASET,
	OP_GENERATOR,
	OP_STREAM_INPUT,

	OP_IDENTITY,
	OP_ROW_RENAME,
	OP_OFFSET_LIMIT,

	OP_SELECTION,
	OP_PROJECTION,

	OP_UNION,
	OP_MERGE_SORTED_INPUTS,

	OP_SIMPLE_AGGREGATION,

	OP_SORT,

	OP_SORTED_AGGREGATION,
	OP_HASH_AGGREGATION,

	OP_BLOCK_NESTED_LOOP_JOIN,

	OP_SORT_MERGE_JOIN,
	OP_HASH_JOIN,

	OP_BLOCK_NESTED_LOOP_SEMI_JOIN,

	OP_SORT_MERGE_SEMI_JOIN,
	OP_HASH_SEMI_JOIN,

	OP_SCAN,
	OP_POINTER_LOOKUP,
};

typedef struct operator_desc operator_desc;
struct operator_desc
{
	operator_descriptor_type type;

	uint64_t index;

	union
	{
		constant_dataset_operator_params              constant_dataset;
		generator_operator_params                     generator;
		stream_input_operator_params                  stream_input;

		identity_operator_params                      identity;
		row_rename_operator_params                    row_rename;
		offset_limit_operator_params                  offset_limit;

		selection_operator_params                     selection;
		projection_operator_params                    projection;

		union_operator_params                         union_op;
		merge_sorted_inputs_operator_params           merge_sorted_inputs;

		simple_aggregation_operator_params            simple_aggregation;

		sort_operator_params                          sort;

		sorted_aggregation_operator_params            sorted_aggregation;
		hash_aggregation_operator_params              hash_aggregation;

		block_nested_loop_join_operator_params        block_nested_loop_join;

		sort_merge_join_operator_params               sort_merge_join;
		hash_join_operator_params                     hash_join;

		block_nested_loop_semi_join_operator_params   block_nested_loop_semi_join;

		sort_merge_semi_join_operator_params          sort_merge_semi_join;
		hash_semi_join_operator_params                hash_semi_join;

		scan_operator_params                          scan;
		pointer_lookup_operator_params                pointer_lookup;
	} params;

	const tuple_def* output_tuple_def;

	operator_resource_counter resource_counter;

	sql_expr_eval_context* expr_eval_context; // non-NULL only if the operator actually needs it to resolve expressions

	uint64_t consumers_count; // starts with 0, this is the number of operators in this query that will consume from this operator
	// for the volcano model it is always 1 or 0 for the output operator
};

typedef struct query_plan_desc query_plan_desc;
struct query_plan_desc
{
	transaction* curr_tx;

	const sql_expr_eval_context* this_sub_query_expr_eval_context;

	arraylist operator_descriptors;

	// points to the output_tuple_def of the last operator_descriptor inserted
	const tuple_def* output_tuple_def;
};

// ===================================================================================================
// factory API
// ===================================================================================================

query_plan_desc* get_new_query_plan_descriptor(transaction* curr_tx, const sql_expr_eval_context* this_sub_query_expr_eval_context);

int does_query_plan_descriptor_contains_operator_descriptor(const query_plan_desc* qpd, const operator_desc* d);

operator_desc* insert_constant_dataset_operator_descriptor(query_plan_desc* qpd, interim_tuple_store* constant_dataset_p, const tuple_def* record_def);
operator_desc* insert_generator_operator_descriptor(query_plan_desc* qpd, void* (*generator)(void* generator_context, const tuple_def* generator_tuple_def), void* generator_context, const tuple_def* generator_tuple_def);
operator_desc* insert_stream_input_operator_descriptor(query_plan_desc* qpd, stream* in_strm, const tuple_def* input_tuple_def);

operator_desc* insert_identity_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc);
operator_desc* insert_row_rename_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, const char* output_name, const char** column_names, uint64_t columns_count);
operator_desc* insert_offset_limit_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, tuples_down_counter offset_counter, tuples_down_counter limit_counter);

operator_desc* insert_selection_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, sql_expression* expr);
operator_desc* insert_projection_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, const char* output_name, projection_description* projection_descriptions, uint32_t projection_descriptions_count);

operator_desc* insert_union_operator_descriptor(query_plan_desc* qpd, operator_desc** input_operator_descs, uint64_t input_operators_count);
operator_desc* insert_merge_sorted_inputs_operator_descriptor(query_plan_desc* qpd, operator_desc** input_operator_descs, uint64_t input_operators_count, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction);

operator_desc* insert_simple_aggregation_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids);

operator_desc* insert_sort_operator_descriptor(query_plan_desc* qpd, tuples_down_counter result_counter, operator_desc* input_operator_desc, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint32_t N_way_sort, uint64_t memory_allowance);

operator_desc* insert_sorted_aggregation_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids);
operator_desc* insert_hash_aggregation_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_build_tuple_buffer_size);

operator_desc* insert_block_nested_loop_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, operator_desc* right_input_operator_desc, sql_expression* join_expr, join_preserve_type ptype, uint32_t min_block_size);

operator_desc* insert_sort_merge_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, const positional_accessor* left_key_element_ids, operator_desc* right_input_operator_desc, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, join_preserve_type ptype, uint32_t min_block_size);
operator_desc* insert_hash_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, const positional_accessor* left_key_element_ids, operator_desc* right_input_operator_desc, const positional_accessor* right_key_element_ids, uint32_t key_element_count, join_preserve_type ptype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size);

operator_desc* insert_block_nested_loop_semi_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, operator_desc* right_input_operator_desc, sql_expression* join_expr, semi_join_type stype, uint32_t min_block_size);

operator_desc* insert_sort_merge_semi_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, const positional_accessor* left_key_element_ids, operator_desc* right_input_operator_desc, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, semi_join_type stype, uint32_t min_block_size);
operator_desc* insert_hash_semi_join_operator_descriptor(query_plan_desc* qpd, operator_desc* left_input_operator_desc, const positional_accessor* left_key_element_ids, operator_desc* right_input_operator_desc, const positional_accessor* right_key_element_ids, uint32_t key_element_count, semi_join_type stype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size);

operator_desc* insert_scan_operator_descriptor(query_plan_desc* qpd, const fetched_table* ftabl, uint32_t max_concurrent_jobs_count, int output_flags, int additional_flags);
operator_desc* insert_pointer_lookup_operator_descriptor(query_plan_desc* qpd, operator_desc* input_operator_desc, const positional_accessor* partition_id_from_source_positional_accessor, const positional_accessor* tuple_pointer_from_source_positional_accessor, const fetched_table* ftabl, uint64_t lookup_batch_size, int output_flags, int additional_flags);

void destroy_query_plan_descriptor(query_plan_desc* qpd);

#endif