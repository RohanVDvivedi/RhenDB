#ifndef TRANSFORMER_H
#define TRANSFORMER_H

#include<tuplestore/data_type_info.h>
#include<tuplestore/user_value.h>

typedef struct typed_user_value typed_user_value;
struct typed_user_value
{
	// type of the value
	data_type_info* type;

	// value itself
	user_value value;
};

typedef struct transformer transformer;
struct transformer
{
	const void* context;

	typed_user_value (*transform)(const void* context, uint32_t input_count, const typed_user_value** input);
};

typed_user_value transform(transformer* t, uint32_t input_count, const typed_user_value** input);

#endif