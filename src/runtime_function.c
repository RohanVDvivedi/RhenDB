#include<rhendb/runtime_function.h>

#include<stdlib.h>

void destroy_runtime_data(runtime_data* rd)
{
	// if no freeing needed we quit early
	if(!(rd->value_needs_to_be_freed))
		return;

	switch(rd->type->type)
	{
		case STRING :
		{
			free((void*)(rd->value.string_value));
			break;
		}
		case BINARY :
		{
			free((void*)(rd->value.binary_value));
			break;
		}
		case TUPLE :
		{
			free((void*)(rd->value.tuple_value));
			break;
		}
		case ARRAY :
		{
			free((void*)(rd->value.array_value));
			break;
		}
		default :
		{
			break;
		}
	}

	rd->type = NULL;
	rd->value = *NULL_DATUM;
	rd->db = NULL;
	rd->value_needs_to_be_freed = 0;
}
