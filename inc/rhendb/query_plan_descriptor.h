#ifndef QUERY_PLAN_DESCRIPTOR_H
#define QUERY_PLAN_DESCRIPTOR_H

#include<rhendb/operators.h>
#include<rhendb/operator_resource_counter.h>
#include<rhendb/tuple_transformers.h>

#include<rhendb/expression_evaluator.h>

#include<rhendb/mvcc_snapshot.h>
#include<tuplestore/tuple_def.h>

#include<cutlery/arraylist.h>

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

typedef struct identity_operator_params identity_operator_params;
struct identity_operator_params
{
	uint64_t input_operator_index;
};

typedef struct consumer_operator_params consumer_operator_params;
struct consumer_operator_params
{
	uint64_t input_operator_index;
	int (*consumer)(void* consumer_context, const void* tuple, const tuple_def* input_tuple_def);
	void* consumer_context;
};

typedef struct result_match_operator_params result_match_operator_params;
struct result_match_operator_params
{
	uint64_t input_operator_indices[2];
};

typedef struct union_operator_params union_operator_params;
struct union_operator_params
{
	uint64_t* input_operator_indices;
	uint64_t input_operators_count;
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
	uint64_t min_run_size;
	uint32_t N_way_sort;
	uint32_t max_concurrent_jobs_count;
};

typedef struct offset_limit_operator_params offset_limit_operator_params;
struct offset_limit_operator_params
{
	uint64_t input_operator_index;
	tuples_down_counter offset_counter;
	tuples_down_counter limit_counter;
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

typedef struct stream_input_operator_params stream_input_operator_params;
struct stream_input_operator_params
{
	stream* in_strm;
	const tuple_def* input_tuple_def;
};

typedef struct stream_output_operator_params stream_output_operator_params;
struct stream_output_operator_params
{
	uint64_t input_operator_index;
	stream* out_strm;
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
	projection_description* projection_descriptions; // borrowed
	uint32_t projection_descriptions_count;
};

typedef struct subquery_output_operator_params subquery_output_operator_params;
struct subquery_output_operator_params
{
	uint64_t input_operator_index;
	sync_pipe* out_pipe;
};

typedef struct insertion_operator_params insertion_operator_params;
struct insertion_operator_params
{
	uint64_t input_operator_index;
	positional_accessor* insertion_from_source_positional_accessors;
	const fetched_table* ftabl;
	int output_flags;
	heap_table_notifier* global_heap_notifier;
	int additional_flags;
};

typedef struct deletion_operator_params deletion_operator_params;
struct deletion_operator_params
{
	uint64_t input_operator_index;
	positional_accessor* partition_id_from_source_positional_accessor;
	positional_accessor* tuple_pointer_from_source_positional_accessor;
	const fetched_table* ftabl;
	uint64_t deletion_batch_size;
	int output_flags;
	int additional_flags;
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

// tuple transformer params

typedef struct identity_tuple_transformer_params identity_tuple_transformer_params;
struct identity_tuple_transformer_params { int unused; };

typedef struct clone_tuple_transformer_params clone_tuple_transformer_params;
struct clone_tuple_transformer_params { int unused; };

typedef struct is_readable_for_mvcc_snapshot_transformer_params is_readable_for_mvcc_snapshot_transformer_params;
struct is_readable_for_mvcc_snapshot_transformer_params
{
	const mvcc_snapshot* mvccsnp_p;
	transaction_status_getter* tsg_p;
	int* were_hints_updated;
};

typedef struct simple_projection_transformer_params simple_projection_transformer_params;
struct simple_projection_transformer_params
{
	char* output_table_name;
	uint32_t projections_count;
	positional_accessor* projections;
	char** field_names;
};

typedef struct scalar_projection_transformer_params scalar_projection_transformer_params;
struct scalar_projection_transformer_params
{
	positional_accessor* projection;
};

typedef struct row_number_prepender_transformer_params row_number_prepender_transformer_params;
struct row_number_prepender_transformer_params
{
	uint64_t next_row_number;
};

typedef struct row_identifier_prepender_transformer_params row_identifier_prepender_transformer_params;
struct row_identifier_prepender_transformer_params
{
	uint64_t row_identifier;
};

typedef struct expressioned_selection_transformer_params expressioned_selection_transformer_params;
struct expressioned_selection_transformer_params
{
	sql_expression* expr;
};

typedef struct unified_typing_transformer_params unified_typing_transformer_params;
struct unified_typing_transformer_params
{
	uint32_t* column_ids_to_transform;
	uint32_t column_to_transform_count;
};

// ===================================================================================================
// descriptor types
// ===================================================================================================

typedef enum operator_descriptor_type operator_descriptor_type;
enum operator_descriptor_type
{
	OP_CONSTANT_DATASET, OP_GENERATOR, OP_IDENTITY, OP_CONSUMER, OP_RESULT_MATCH, OP_UNION,
	OP_SIMPLE_AGGREGATION, OP_SORT, OP_OFFSET_LIMIT, OP_MERGE_SORTED_INPUTS,
	OP_STREAM_INPUT, OP_STREAM_OUTPUT, OP_SORTED_AGGREGATION, OP_HASH_AGGREGATION,
	OP_BLOCK_NESTED_LOOP_JOIN, OP_SORT_MERGE_JOIN, OP_HASH_JOIN,
	OP_BLOCK_NESTED_LOOP_SEMI_JOIN, OP_SORT_MERGE_SEMI_JOIN, OP_HASH_SEMI_JOIN,
	OP_SELECTION, OP_PROJECTION, OP_SUBQUERY_OUTPUT,
	OP_INSERTION, OP_DELETION, OP_SCAN, OP_POINTER_LOOKUP,
};

typedef enum tuple_transformer_descriptor_type tuple_transformer_descriptor_type;
enum tuple_transformer_descriptor_type
{
	TT_IDENTITY, TT_CLONE, TT_IS_READABLE_FOR_MVCC_SNAPSHOT,
	TT_SIMPLE_PROJECTION, TT_SCALAR_PROJECTION,
	TT_ROW_NUMBER_PREPENDER, TT_ROW_IDENTIFIER_PREPENDER,
	TT_EXPRESSIONED_SELECTION, TT_UNIFIED_TYPING,
};

typedef struct tuple_transformer_desc tuple_transformer_desc;
struct tuple_transformer_desc
{
	tuple_transformer_descriptor_type type;

	union
	{
		identity_tuple_transformer_params                  identity;
		clone_tuple_transformer_params                     clone;
		is_readable_for_mvcc_snapshot_transformer_params   is_readable_for_mvcc_snapshot;
		simple_projection_transformer_params               simple_projection;
		scalar_projection_transformer_params               scalar_projection;
		row_number_prepender_transformer_params            row_number_prepender;
		row_identifier_prepender_transformer_params        row_identifier_prepender;
		expressioned_selection_transformer_params          expressioned_selection;
		unified_typing_transformer_params                  unified_typing;
	} params;

	// the def this transformer produces for the one that follows it in the chain.
	// built identically to the transformer (borrowing), where derivable at insert; NULL means "resolved at
	// create() from the live chain" (schema-changing transformers whose derivation is not ported yet).
	const tuple_def* output_def;
};

typedef struct operator_desc operator_desc;
struct operator_desc
{
	operator_descriptor_type type;

	uint64_t index;   // position in query_plan_desc.operator_descriptors == built o->index

	union
	{
		constant_dataset_operator_params              constant_dataset;
		generator_operator_params                     generator;
		identity_operator_params                      identity;
		consumer_operator_params                      consumer;
		result_match_operator_params                  result_match;
		union_operator_params                         union_op;
		simple_aggregation_operator_params            simple_aggregation;
		sort_operator_params                          sort;
		offset_limit_operator_params                  offset_limit;
		merge_sorted_inputs_operator_params           merge_sorted_inputs;
		stream_input_operator_params                  stream_input;
		stream_output_operator_params                 stream_output;
		sorted_aggregation_operator_params            sorted_aggregation;
		hash_aggregation_operator_params              hash_aggregation;
		block_nested_loop_join_operator_params        block_nested_loop_join;
		sort_merge_join_operator_params               sort_merge_join;
		hash_join_operator_params                     hash_join;
		block_nested_loop_semi_join_operator_params   block_nested_loop_semi_join;
		sort_merge_semi_join_operator_params          sort_merge_semi_join;
		hash_semi_join_operator_params                hash_semi_join;
		selection_operator_params                     selection;
		projection_operator_params                    projection;
		subquery_output_operator_params               subquery_output;
		insertion_operator_params                     insertion;
		deletion_operator_params                      deletion;
		scan_operator_params                          scan;
		pointer_lookup_operator_params                pointer_lookup;
	} params;

	const tuple_def* output_tuple_def;

	operator_resource_counter resource_counter;

	sql_expr_eval_context expr_eval_context;
	int has_expr_eval_context;

	arraylist transformer_descriptors;
};

typedef struct query_plan_desc query_plan_desc;
struct query_plan_desc
{
	rhendb* rdb;

	const sql_expr_eval_context* this_sub_query_expr_eval_context;

	arraylist operator_descriptors;
};

// ===================================================================================================
// factory API
// ===================================================================================================

query_plan_desc* get_new_query_plan_descriptor(rhendb* rdb, const sql_expr_eval_context* this_sub_query_expr_eval_context);

// returns the tuple_def that the consumer of this operator descriptor must read : the output_tuple_def of its
// last tuple transformer descriptor if it has any, else the operator descriptor's own output_tuple_def
const tuple_def* get_output_tuple_def_for_operator_descriptor(const operator_desc* operator_descriptor);

operator_desc* insert_constant_dataset_operator_descriptor(query_plan_desc* qpd, interim_tuple_store* constant_dataset_p, const tuple_def* record_def);
operator_desc* insert_generator_operator_descriptor(query_plan_desc* qpd, void* (*generator)(void* generator_context, const tuple_def* generator_tuple_def), void* generator_context, const tuple_def* generator_tuple_def);
operator_desc* insert_identity_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index);
operator_desc* insert_consumer_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, int (*consumer)(void* consumer_context, const void* tuple, const tuple_def* input_tuple_def), void* consumer_context);
operator_desc* insert_result_match_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index_0, uint64_t input_operator_index_1);
operator_desc* insert_union_operator_descriptor(query_plan_desc* qpd, const uint64_t* input_operator_indices, uint64_t input_operators_count);
operator_desc* insert_simple_aggregation_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids);
operator_desc* insert_sort_operator_descriptor(query_plan_desc* qpd, tuples_down_counter result_counter, uint64_t input_operator_index, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint64_t min_run_size, uint32_t N_way_sort, uint32_t max_concurrent_jobs_count);
operator_desc* insert_offset_limit_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, tuples_down_counter offset_counter, tuples_down_counter limit_counter);
operator_desc* insert_merge_sorted_inputs_operator_descriptor(query_plan_desc* qpd, const uint64_t* input_operator_indices, uint64_t input_operators_count, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction);
operator_desc* insert_stream_input_operator_descriptor(query_plan_desc* qpd, stream* in_strm, const tuple_def* input_tuple_def);
operator_desc* insert_stream_output_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, stream* out_strm);
operator_desc* insert_sorted_aggregation_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids);
operator_desc* insert_hash_aggregation_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_build_tuple_buffer_size);
operator_desc* insert_block_nested_loop_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, uint64_t right_input_operator_index, sql_expression* join_expr, join_preserve_type ptype, uint32_t min_block_size);
operator_desc* insert_sort_merge_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, join_preserve_type ptype, uint32_t min_block_size);
operator_desc* insert_hash_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, uint32_t key_element_count, join_preserve_type ptype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size);
operator_desc* insert_block_nested_loop_semi_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, uint64_t right_input_operator_index, sql_expression* join_expr, semi_join_type stype, uint32_t min_block_size);
operator_desc* insert_sort_merge_semi_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, semi_join_type stype, uint32_t min_block_size);
operator_desc* insert_hash_semi_join_operator_descriptor(query_plan_desc* qpd, uint64_t left_input_operator_index, const positional_accessor* left_key_element_ids, uint64_t right_input_operator_index, const positional_accessor* right_key_element_ids, uint32_t key_element_count, semi_join_type stype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size);
operator_desc* insert_selection_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, sql_expression* expr);
operator_desc* insert_projection_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, projection_description* projection_descriptions, uint32_t projection_descriptions_count);
operator_desc* insert_subquery_output_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, sync_pipe* out_pipe);
operator_desc* insert_insertion_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, const positional_accessor* insertion_from_source_positional_accessors, const fetched_table* ftabl, int output_flags, heap_table_notifier* global_heap_notifier, int additional_flags);
operator_desc* insert_deletion_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, const positional_accessor* partition_id_from_source_positional_accessor, const positional_accessor* tuple_pointer_from_source_positional_accessor, const fetched_table* ftabl, uint64_t deletion_batch_size, int output_flags, int additional_flags);
operator_desc* insert_scan_operator_descriptor(query_plan_desc* qpd, const fetched_table* ftabl, uint32_t max_concurrent_jobs_count, int output_flags, int additional_flags);
operator_desc* insert_pointer_lookup_operator_descriptor(query_plan_desc* qpd, uint64_t input_operator_index, const positional_accessor* partition_id_from_source_positional_accessor, const positional_accessor* tuple_pointer_from_source_positional_accessor, const fetched_table* ftabl, uint64_t lookup_batch_size, int output_flags, int additional_flags);

tuple_transformer_desc* insert_identity_tuple_transformer_descriptor(query_plan_desc* qpd);
tuple_transformer_desc* insert_clone_tuple_transformer_descriptor(query_plan_desc* qpd);
tuple_transformer_desc* insert_is_readable_for_mvcc_snapshot_tuple_transformer_descriptor(query_plan_desc* qpd, const mvcc_snapshot* mvccsnp_p, transaction_status_getter* tsg_p, int* were_hints_updated);
tuple_transformer_desc* insert_simple_projection_tuple_transformer_descriptor(query_plan_desc* qpd, const char* output_table_name, uint32_t projections_count, const positional_accessor* projections, char const * const * field_names);
tuple_transformer_desc* insert_scalar_projection_tuple_transformer_descriptor(query_plan_desc* qpd, const positional_accessor* projection);
tuple_transformer_desc* insert_row_number_prepender_tuple_transformer_descriptor(query_plan_desc* qpd, uint64_t next_row_number);
tuple_transformer_desc* insert_row_identifier_prepender_tuple_transformer_descriptor(query_plan_desc* qpd, uint64_t row_identifier);
tuple_transformer_desc* insert_expressioned_selection_tuple_transformer_descriptor(query_plan_desc* qpd, sql_expression* expr);
tuple_transformer_desc* insert_unified_typing_tuple_transformer_descriptor(query_plan_desc* qpd, uint32_t* column_ids_to_transform, uint32_t column_to_transform_count);

query_plan* create_query_plan(query_plan_desc* qpd, transaction* tx, const sql_expr_eval_context* this_sub_query_expr_eval_context);

void destroy_query_plan_descriptor(query_plan_desc* qpd);

#endif