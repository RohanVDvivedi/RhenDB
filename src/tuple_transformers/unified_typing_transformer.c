#include<rhendb/tuple_transformer_interface.h>

/*
	This tuple transformer is meants to be placed for the inputs of the union_operator and merge_sorted_inputs_operator
	This is only required if the text, blob, numeric and jsonb extended types are not identical extended types, i.e. when some come from persistent engine and some from volatile engine
	This is done outside the operator to retain the wide range of functionality and tuple types supported by these operators
*/

static void* process(tuple_transformer* tt_p, void* tuple)
{
	return tuple;
}

static void destroy(tuple_transformer* tt_p)
{
}

tuple_transformer* get_new_unified_typing_transformer(const tuple_def* input_def, uint32_t* column_ids_to_transform, uint32_t column_to_transform_count)
{
	return get_new_tuple_transformer(NULL, input_def, input_def, process, destroy);
}