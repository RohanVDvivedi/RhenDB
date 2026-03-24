#ifndef TUPLE_TRANSFORMER_H
#define TUPLE_TRANSFORMER_H

#include<rhendb/query_plan_interface.h>

/*
	tuple_transformer can only be either a selection or projection operation on the tuple
*/

typedef struct tuple_transformer tuple_transformer;
struct tuple_transformer
{
	void* context;

	tuple_def* input_def;
	tuple_def* output_def;

	void* (*process)(tuple_transformer* tt_p, void* tuple);
	/*
		return NULL, if the selection fails on the predicate
		return tuple, if the selection passed on the predicate OR the projection was made to perform update in-place
		return some other malloc-ed tuple, on projection (this returned pointer tuple will be freed by the caller function after use of the tuple)

		the input tuple will never be freed/modified by this function

		this means for a projection you are allowed to modify the tuple in place, for lets say incrementing the fixed sized integer by 100
		OR trancating the STRING attribute in the tuple (something that needs less space)

		this implies you only need to call malloc() inside the process() and never need to call free()
	*/

	void (*destroy)(tuple_transformer* tt_p);

	// next tuple_transforn to perform
	// this will be NULL, if there are no more transformations to be performed
	tuple_transformer* next_tt_p;
};

void* process_all_transformers(tuple_transformer* tt_p, void* tuple, int* need_to_free_output);
/*
	returns output, that may need freeing, if so need_to_free_output will be set to 1, else it will be 0

	the input tuple will never be freed/modified by this function
*/

void destroy_all_transformers(tuple_transformer* tt_p);

#endif