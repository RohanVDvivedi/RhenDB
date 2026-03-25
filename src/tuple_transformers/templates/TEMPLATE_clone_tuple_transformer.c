#include<rhendb/tuple_transformer_interface.h>

#include<stdlib.h>

// any allocation made here will be freed by the caller
static void* process(tuple_transformer* tt_p, void* tuple)
{
	uint32_t tuple_size = get_tuple_size(tt_p->input_def, tuple);
	void* output_tuple = malloc(tuple_size);
	memory_move(output_tuple, tuple, tuple_size);
	return output_tuple;
}

static void destroy(tuple_transformer* tt_p)
{
}

tuple_transformer* get_new_clone_tuple_transformer(const void* input_def)
{
	return get_new_tuple_transformer(NULL, input_def, input_def, process, destroy);
}