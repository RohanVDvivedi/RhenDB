#include<rhendb/tuple_transformer_interface.h>

static void* process(tuple_transformer* tt_p, void* tuple)
{
	return tuple;
}

static void destroy(tuple_transformer* tt_p)
{
}

tuple_transformer* get_new_identity_tuple_transformer(const tuple_def* input_def)
{
	return get_new_tuple_transformer(NULL, input_def, input_def, process, destroy);
}