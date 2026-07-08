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

	void** input_tuples;

	uint32_t input_tuples_count;

	// now owned by the context
	void* catalog_manager;
};

/*
	***
	No support in the first iteration for
	tuplelargetypes, these will be materialized into text -> RHENDB_STRING, blob -> RHENDB_BINARY, numeric -> RHENDB_NUMERIC, i.e. tuple -> RHENDB_* when it is required, and as much of it is required
	user defined functions these call backs will be added by catalog manager
	nested query expressions these call backs will be added by catalog manager
	variables will be deciphered from the input_tuple, in future
*/
sql_expr_eval_context get_sql_expr_eval_context_for_rhendb(tuple_def** input_tuple_defs, void** input_tuples, uint32_t input_tuples_count, void* catalog_manager);

// frees shallow copied input_tuple_defs and input_tuples, and the pointer itself
void delete_context_p_for_sql_expr_eval_context_for_rhendb(rhendb_expr_eval_context* context_p);

#endif