#ifndef SUB_OPERATOR_H
#define SUB_OPERATOR_H

#include<tuplestore/data_type_info.h>
#include<tuplestore/user_value.h>
#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<cutlery/singlylist.h>

typedef struct typed_user_value typed_user_value;
struct typed_user_value
{
	// type of the value
	const data_type_info* type;

	// value itself
	user_value value;

	// set the below flag if you allocated memory for storing the value above
	// this need to b set by your transformer
	int value_needs_to_be_freed;
};

void destroy_typed_user_value(typed_user_value t);

typedef struct transformer transformer;
struct transformer
{
	const void* context; // context may hold a set of instructions and a virtual machine to execute them, or a list of transformers to be applied in sequence

	typed_user_value (*transform)(const void* context, uint32_t input_count, const typed_user_value** input);

	const data_type_info* (*transformed_type)(const void* context, uint32_t input_count, const data_type_info** input_types);
};

const data_type_info* get_transformed_type(const transformer* t, uint32_t input_count, const data_type_info** input_types);

typed_user_value transform(const transformer* t, uint32_t input_count, const typed_user_value** input);

#endif