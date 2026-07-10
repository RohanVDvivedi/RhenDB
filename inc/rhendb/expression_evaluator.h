#ifndef EXPRESSION_EVALUATOR_H
#define EXPRESSION_EVALUATOR_H

#include<sqltoast/sqltoast.h>
#include<sqltoast/sql_expression_eval.h>

#include<tuplestore/datum.h>
#include<tuplestore/data_type_info.h>

#include<tuplelargetypes/text_extended.h>
#include<tuplelargetypes/blob_extended.h>
#include<tuplelargetypes/numeric_extended.h>
#include<tuplelargetypes/materialized_numeric.h>

#include<mpdecimal.h>

#include<cutlery/hashmap.h>

#include<rhendb/rhendb.h>

typedef enum expr_type expr_type;
enum expr_type
{
	RHENDB_BIT_FIELD  = 0,
	RHENDB_UINT	      = 1,
	RHENDB_INT 	      = 2,
	RHENDB_FLOAT      = 3,

	RHENDB_LARGE_UINT = 4,
	RHENDB_LARGE_INT  = 5,

	RHENDB_STRING     = 6,
	RHENDB_BINARY     = 7,

	// below two attributes are the only ones that need data_type_info* dti_p;
	RHENDB_TUPLE      = 8,
	RHENDB_ARRAY      = 9,

	// not yet supported by the system
	// points a valid materialized numeric
	RHENDB_NUMERIC    = 10,
};

typedef struct expr_type_info expr_type_info;
struct expr_type_info
{
	expr_type type;

	// only used for TUPLE and ARRAY
	data_type_info* dti_p;
	int should_free_dti_p;
};

typedef struct expr_value expr_value;
struct expr_value
{
	expr_type_info type_info;

	union
	{
		datum value;
		mpd_t numeric_value; // used only for RHENDB_NUMERIC
	};

	// on destroy buffer must be destroyed, if not NULL
	void* buffer;
	uint64_t capacity;
};

// this is what sits in sql_expr_eval_context.context_p, for now this is unused
typedef struct rhendb_expr_eval_context rhendb_expr_eval_context;
struct rhendb_expr_eval_context
{
	tuple_def** input_tuple_defs;

	// populate this by index, to set tuples for current expression evaluation
	// these input_tuples_count number of pointers will be 0 initialized
	void** input_tuples;

	uint32_t input_tuples_count;

	// variable/identifier cache that stores variable identifier to tuple_index and positional_accessor in it
	// owned by the context; caches "a.b.c" -> (tuple index + positional accessor + type)
	hashmap var_cache;

	// for materializing the on-disk -> text, blob and numeric columns
	rhendb* rdb;

	// now owned by the context
	void* catalog_manager;
};

// error codes written into *error_code by the rhendb expression-evaluation callbacks.
// (the sqltoast evaluator only distinguishes zero from non-zero; these names are for rhendb-side diagnostics.)
typedef enum rhendb_expr_eval_error rhendb_expr_eval_error;
enum rhendb_expr_eval_error
{
	RHENDB_EE_OK                  = 0,  // no error
	RHENDB_EE_UNSUPPORTED_TYPE    = 1,  // operand/result type not supported (TUPLE, ARRAY, unknown sql type)
	RHENDB_EE_NON_NUMERIC_OPERAND = 2,  // arithmetic on a non-numeric operand
	RHENDB_EE_NON_INTEGER_OPERAND = 3,  // bitwise/shift on a non-integer operand
	RHENDB_EE_DIVIDE_BY_ZERO      = 4,  // division or modulo by exact zero
	RHENDB_EE_INCOMPARABLE_TYPES  = 5,  // compare()/unify_types() on types that cannot be reconciled
	RHENDB_EE_UNSUPPORTED_CAST    = 6,  // cast() to an unsupported target type
	RHENDB_EE_NON_STRING_OPERAND  = 7,  // concat()/like() on a non string/binary operand
	RHENDB_EE_MATERIALIZE_FAILED  = 8,  // failed while reading/materializing an extended text/blob/numeric value
	RHENDB_EE_MISSING_ENGINE      = 9,  // no rage_engine reachable to materialize an extended value
	RHENDB_EE_OUT_OF_MEMORY       = 10, // allocation failure
	RHENDB_EE_NULL_OPERAND        = 11, // a required operand pointer was NULL
	RHENDB_EE_STRING_TOO_LONG     = 12, // concat result size would not fit in the uint32_t size field
};

// intitialize this per instance for evaluation context of one stream of tuples of 1 type
sql_expr_eval_context get_sql_expr_eval_context_for_rhendb(tuple_def** input_tuple_defs, uint32_t input_tuples_count, rhendb* rdb, void* catalog_manager);

// frees shallow copied input_tuple_defs and input_tuples, and the pointer itself
void delete_context_p_for_sql_expr_eval_context_for_rhendb(rhendb_expr_eval_context* context_p);

#endif