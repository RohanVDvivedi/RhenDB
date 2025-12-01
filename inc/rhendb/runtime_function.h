#ifndef RUNTIME_FUNCTION_H
#define RUNTIME_FUNCTION_H

#include<rhendb/rage_engine.h>

#include<tuplestore/data_type_info.h>
#include<tuplestore/user_value.h>
#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<cutlery/dstring.h>

typedef struct runtime_data runtime_data;
struct runtime_data
{
	// type of the value
	const data_type_info* type;

	// value itself, it may not have been materialized
	user_value value;

	// if it is an extended data type, we will need rage_engine to read it
	// please do not attempt to store the output here, even if it is an extended data_type, instead malloc a larger chunk for value and change the type to inline
	rage_engine* db;

	// free the memory associated with data only if this flag is set
	int value_needs_to_be_freed;
};

void destroy_runtime_data(runtime_data* rd);

typedef enum runtime_function_error_type runtime_function_error_type;
enum runtime_function_error_type
{
	// runtime errors
	RUNT_SUCCESS,
	RUNT_OOM_ERROR, // out of memory
	RUNT_ABORT_ERROR, // could happen on reading the extended datatypes

	// validation time errors
	VALD_TOO_FEW_INPUTS,
	VALD_TOO_FEW_OUTPUTS,
	VALD_TOO_MANY_INPUTS,
	VALD_TOO_MANY_OUTPUTS,

	// validation time errors
	VALD_INVALID_INPUT_TYPES,
	VALD_INVALID_OUTPUT_TYPES,
};

typedef struct runtime_function_error runtime_function_error;
struct runtime_function_error
{
	runtime_function_error_type type;
	dstring message;
};

#define RUNTIME_FUNCTION_SUCCESS ((runtime_function_error){,type = RUNT_SUCCESS, .message = get_dstring_pointing_to_literal_cstring("")})

typedef struct runtime_function runtime_function;
struct runtime_function
{
	// validate inputs before calling the actual, only the input 
	void (*validate)(runtime_function_error* error, char* message, uint32_t output_count, runtime_data * * const output, uint32_t input_count, runtime_data const * const * const input);

	int64_t (*function)(runtime_function_error* error, char* message, uint32_t output_count, runtime_data * * const output, uint32_t input_count, runtime_data const * const * const input);
};

/*
	runtime function evaluates a set of inputs to produce output, like addition of 2 numbers produces another 1 number

*/

#endif