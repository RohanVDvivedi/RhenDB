#ifndef PROJECTION_TYPE_H
#define PROJECTION_TYPE_H

#include<sqltoast/sql_expression.h>

#include<tuplestore/tuple_def.h>

typedef enum projection_type projection_type;
enum projection_type
{
	PROJECT_IDENTITY,
	PROJECT_EXPRESSION,
};

typedef struct projection_description projection_description;
struct projection_description
{
	projection_type type;

	char* column_name; // if NULL, PROJECT_IDENTITY -> take from incomming column, PROJECT_EXPRESSION -> "proj-<no>"

	union
	{
		positional_accessor pa; // for PROJECT_IDENTITY
		sql_expression* expr; // for PROJECT_EXPRESSION
	};
};

#define project_from(col_name, pa_v)        ((projection_description){.type = PROJECT_IDENTITY, .column_name = col_name, .pa = pa_v})
#define project_using(col_name, expr_v_p)   ((projection_description){.type = PROJECT_EXPRESSION, .column_name = col_name, .expr = expr_v_p})

#endif