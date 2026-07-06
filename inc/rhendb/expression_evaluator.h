#ifndef EXPRESSION_EVALUATOR_H
#define EXPRESSION_EVALUATOR_H

#include<sqltoast/sqltoast.h>
#include<sqltoast/sql_expression_eval.h>

#include<tuplestore/datum.h>
#include<tuplestore/data_type_info.h>

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
};

typedef struct expr_value expr_value;
struct expr_value
{
	expr_type_info type_info;

	union
	{
		datum value;
		materialized_numeric number; // only used for materialized numeric, and must be destroyed as it is always an in-memory construct
	};

	// on destroy buffer must be destroyed, if not NULL
	void* buffer;
	uint64_t capacity;
};

/*
	***
	No support in the first iteration for
	tuplelargetypes, these will be materialized into text -> RHENDB_STRING, blob -> RHENDB_BINARY, numeric -> RHENDB_NUMERIC, i.e. tuple -> RHENDB_* when it is required, and as much of it is required
	user defined functions these call backs will be added by catalog manager
	nested query expressions these call backs will be added by catalog manager
	variables will be deciphered from the input_tuple, in future
*/
sql_expr_eval_context get_sql_expr_eval_context_for_rhendb(tuple_def** input_tuple_defs, void** input_tuples, uint32_t input_tuples_count);

#endif