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
	union
	{
		positional_accessor pa; // for PROJECT_IDENTITY
		sql_expression* expr; // for PROJECT_EXPRESSION
	};
};

#define project_from(pa_v)        ((projection_description){.type = PROJECT_IDENTITY, .pa = pa_v})
#define project_using(expr_v_p)   ((projection_description){.type = PROJECT_EXPRESSION, .expr = expr_v_p})

#endif