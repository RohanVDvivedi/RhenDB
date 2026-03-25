#ifndef TUPLE_TRANSFORMER_H
#define TUPLE_TRANSFORMER_H

#include<rhendb/query_plan_interface.h>

#include<cutlery/linkedlist.h>

/*
	tuple_transformer can only be either a selection or projection operation on the tuple
*/

typedef struct tuple_transformer tuple_transformer;
struct tuple_transformer
{
	void* context;

	// input definition for this transformer
	// just the pointer is stored, as is given by the user
	const tuple_def* input_def;

	// output_def is actually owned by this tuple_transformer, and must be destroyed by the destory() function call
	const tuple_def* output_def;

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

	// embed_node to link tuple_transfromer in a linkedlist
	// the tuple_transformer is the first one to be applied to the tuple
	llnode embed_node;
};

tuple_transformer* get_new_tuple_transformer(void* context, const tuple_def* input_def, const tuple_def* output_def, void* (*process)(tuple_transformer* tt_p, void* tuple), void (*destroy)(tuple_transformer* tt_p));

// calls destroy on the tuple_transformer first, then frees it
void delete_tuple_transformer(tuple_transformer* tt_p);

typedef struct tuple_transformers tuple_transformers;
struct tuple_transformers
{
	// input definition for the first transformer
	// just the pointer is stored, as is given by the user
	const tuple_def* input_def;

	linkedlist tuple_transformers_list;
};

void init_transformers(tuple_transformers* tts_p, const tuple_def* input_def);

void* process_all_transformers(const tuple_transformers* tts_p, void* tuple, int* need_to_free_output);
/*
	returns output, that may need freeing, if so need_to_free_output will be set to 1, else it will be 0

	the input tuple will never be freed/modified by this function
*/

// same as tts_p->input_def
const tuple_def* get_input_def_all_transformers(const tuple_transformers* tts_p);

// if no transformers it is tts_p->input_def
// else it is output_def of the tail (the last of the) tuple_transformers
const tuple_def* get_output_def_all_transformers(const tuple_transformers* tts_p);

void destroy_all_transformers(tuple_transformers* tts_p);

#endif