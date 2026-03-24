#include<rhendb/tuple_transformer.h>

#include<stdlib.h>

tuple_transformer* get_new_tuple_transformer(void* context, const tuple_def* input_def, const tuple_def* output_def, void* (*process)(tuple_transformer* tt_p, void* tuple), void (*destroy)(tuple_transformer* tt_p))
{
	tuple_transformer* tt_p = malloc(sizeof(tuple_transformer));
	tt_p->context = context;
	tt_p->input_def = input_def;
	tt_p->output_def = output_def;
	tt_p->process = process;
	tt_p->destroy = destroy;
	initialize_llnode(&(tt_p->embed_node));
	return tt_p;
}

void* process_all_transformers(const linkedlist* tts_p, void* tuple, int* need_to_free_output)
{
	// a NULL tuple can never be processed by the tuple_transformer
	if(tuple == NULL)
	{
		(*need_to_free_output) = 0;
		return NULL;
	}

	// ther is no transformation to be done, so return the input tuple as is
	// we mark the tuple with need_to_free_output = 0, as the caller will need to control if the input tuple needs to be freed
	if(is_empty_linkedlist(tts_p))
	{
		(*need_to_free_output) = 0;
		return tuple;
	}

	// local variable that flags if the tuple for the current iteration needs to be freed
	// input parameter tuple, will never need freeing, so it starts with a value of 0
	int need_to_free_tuple = 0;
	tuple_transformer* tt_p = (tuple_transformer*) get_head_of_linkedlist(tts_p);
	do
	{
		void* output_tuple = tt_p->process(tt_p, tuple);

		// if output is same as input no freeing needed, and just continue
		if(output_tuple == tuple)
		{
			// below 2 are NO-OP tasks
			// need_to_free_tuple = need_to_free_tuple;
			// tuple = output_tuple; // the pointers are already the same
			tt_p = (tuple_transformer*) get_next_of_in_linkedlist(tts_p, tt_p);
			continue;
		}

		// if the selection failed, returned NULL, then first free the tuple if required
		// return NULL, while asking the user to not free the output returned, as it is NULL
		if(output_tuple == NULL)
		{
			if(need_to_free_tuple)
				free(tuple);
			(*need_to_free_output) = 0; // can not free a NULL output
			return NULL;
		}

		// else this is projection, with a non-in-place update
		{
			if(need_to_free_tuple)
				free(tuple);
			tuple = output_tuple; // prepare for the next iteration
			need_to_free_tuple = 1; // the new tuple, needs freeing as it is different from the tuple
			tt_p = (tuple_transformer*) get_next_of_in_linkedlist(tts_p, tt_p);
			continue;
		}
	}while(tt_p != get_head_of_linkedlist(tts_p));

	// tuple is required to be returned, let the caller know if they need to free this pointer
	(*need_to_free_output) = need_to_free_tuple;
	return tuple;
}

const tuple_def* get_input_def_all_transformers(const linkedlist* tts_p)
{
	if(is_empty_linkedlist(tts_p))
		return NULL;

	return ((const tuple_transformer*)(get_head_of_linkedlist(tts_p)))->input_def;
}

const tuple_def* get_output_def_all_transformers(const linkedlist* tts_p)
{
	if(is_empty_linkedlist(tts_p))
		return NULL;

	return ((const tuple_transformer*)(get_tail_of_linkedlist(tts_p)))->output_def;
}

static void destroy_transformer_on_remove_all_callback(void* _NULL, const void* data)
{
	tuple_transformer* tt_p = (tuple_transformer*)data;
	tt_p->destroy(tt_p);
	free(tt_p);
}

void destroy_all_transformers(linkedlist* tts_p)
{
	remove_all_from_linkedlist(tts_p, &((notifier_interface){NULL, destroy_transformer_on_remove_all_callback}));
}