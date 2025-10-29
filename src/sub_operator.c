#include<rhendb/sub_operator.h>

#include<stdlib.h>

void destroy_typed_user_value(typed_user_value t)
{
	// if no freeing needed we quit early
	if(!(t.value_needs_to_be_freed))
		return;

	switch(t.type->type)
	{
		case STRING :
		{
			free((void*)(t.value.string_value));
			break;
		}
		case BLOB :
		{
			free((void*)(t.value.blob_value));
			break;
		}
		case TUPLE :
		{
			free((void*)(t.value.tuple_value));
			break;
		}
		case ARRAY :
		{
			free((void*)(t.value.array_value));
			break;
		}
		default :
		{
			break;
		}
	}
}

const data_type_info* get_transformed_type(const transformer* t, uint32_t input_count, const data_type_info** input_types)
{
	return t->transformed_type(t->context, input_count, input_types);
}

typed_user_value transform(const transformer* t, uint32_t input_count, const typed_user_value** input)
{
	return t->transform(t->context, input_count, input);
}
