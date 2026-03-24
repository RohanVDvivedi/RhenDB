#include<rhendb/tuple_transformer.h>

#include<stdlib.h>

void* process_all_transformers(tuple_transformer* tt_p, void* tuple, int* need_to_free_output)
{
	// a NULL tuple can never be processed by the tuple_transformer
	if(tuple == NULL)
	{
		(*need_to_free_output) = 0;
		return NULL;
	}

	// local variable that flags if the tuple for the current iteration needs to be freed
	// input parameter tuple, will never need freeing, so it starts with a value of 0
	int need_to_free_tuple = 0;
	while(tt_p != NULL && tuple != NULL)
	{
		void* output_tuple = tt_p->process(tt_p, tuple);

		// if output is same as input no freeing needed, and just continue
		if(output_tuple == tuple)
		{
			// below 2 are NO-OP tasks
			// need_to_free_tuple = need_to_free_tuple;
			// tuple = output_tuple; // the pointers are already the same
			tt_p = tt_p->next_tt_p;
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
			tuple = output_tuple; // prepare for the next iteration so that tt_p->next_tt_p process the output_tuple
			need_to_free_tuple = 1; // the new tuple, needs freeing as it is different from the tuple
			tt_p = tt_p->next_tt_p;
			continue;
		}
	}

	// tuple is required to be returned, let the caller know if they need to free this pointer
	(*need_to_free_output) = need_to_free_tuple;
	return tuple;
}

void destroy_all_transformers(tuple_transformer* tt_p)
{
	while(tt_p != NULL)
	{
		tuple_transformer* next_tt_p = tt_p->next_tt_p;
		tt_p->destroy(tt_p);
		tt_p = next_tt_p;
	}
}