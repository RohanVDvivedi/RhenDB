#include<rhendb/query_plan.h>

#include<rhendb/function_compare.h>

#include<tuplestore/tuple.h>
#include<tuplestore/datum.h>

#include<cutlery/pheap.h>

#include<stdlib.h>

typedef struct mergeable_tuple mergeable_tuple;
struct mergeable_tuple
{
	operator* source;

	void* tuple;

	datum* keys;

	phpnode embed_node;
};

typedef struct op_state op_state;
struct op_state
{
	pheap merge_heap;

	const data_type_info** key_dtis;

	// backing allocations
	datum* keys;              // key_element_count * input_operators_count
	mergeable_tuple* mtuples; // input_operators_count

	// the heap is primed (one tuple pulled from each input) lazily on the first next_tuple,
	// so that next is never called from open
	int primed;
};

static int compare_mergeable_tuples(const void* context_p, const void* mt1_vp, const void* mt2_vp)
{
	const operator* o = context_p;
	const op_state* this_state = o->state;

	const mergeable_tuple* mt1 = mt1_vp;
	const mergeable_tuple* mt2 = mt2_vp;

	return compare_datums3_rhendb(mt1->keys, mt2->keys, this_state->key_dtis, o->desc->params.merge_sorted_inputs.key_compare_direction, o->desc->params.merge_sorted_inputs.key_element_count, o->self_query_plan->desc->curr_tx);
}

static void materialize_keys(operator* o, mergeable_tuple* mt)
{
	uint32_t key_element_count = o->desc->params.merge_sorted_inputs.key_element_count;
	const positional_accessor* key_element_ids = o->desc->params.merge_sorted_inputs.key_element_ids;

	for(uint32_t j = 0; j < key_element_count; j++)
	{
		const data_type_info* dti_temp;
		if(!get_value_from_element_from_tuple(&(mt->keys[j]), &dti_temp, o->desc->output_tuple_def, key_element_ids[j], mt->tuple))
			mt->keys[j] = (*NULL_DATUM);
	}
}

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	// lazy priming : pull the first tuple from every input and build the heap
	if(!this_state->primed)
	{
		uint64_t input_operators_count = o->desc->params.merge_sorted_inputs.input_operators_count;

		for(uint64_t i = 0; i < input_operators_count; i++)
		{
			mergeable_tuple* mt = &(this_state->mtuples[i]);

			void* tuple = mt->source->next_tuple(mt->source, error_code);
			if(tuple == NULL)
			{
				if(*error_code)
				{
					o->destroy_state(o);
					return NULL;
				}
				continue; // this input was empty, keep it out of the heap
			}

			mt->tuple = tuple;
			materialize_keys(o, mt);
			push_to_pheap(&(this_state->merge_heap), mt);
		}

		this_state->primed = 1;
	}

	// every input is exhausted -> we are done
	if(is_empty_pheap(&(this_state->merge_heap)))
	{
		o->destroy_state(o);
		return NULL;
	}

	// the smallest head tuple across all inputs is the next output
	mergeable_tuple* mt = (mergeable_tuple*) get_top_of_pheap(&(this_state->merge_heap));
	void* out = mt->tuple;

	// advance the input that produced the output tuple
	void* next = mt->source->next_tuple(mt->source, error_code);
	if(next == NULL)
	{
		if(*error_code)
		{
			free(out);
			mt->tuple = NULL;
			pop_from_pheap(&(this_state->merge_heap));
			o->destroy_state(o);
			return NULL;
		}

		// this input is drained -> remove it from the heap
		mt->tuple = NULL;
		pop_from_pheap(&(this_state->merge_heap));
		mt->source->destroy_state(mt->source);
		mt->source = NULL;
	}
	else
	{
		// replace this input's head with its next tuple and restore the heap order
		mt->tuple = next;
		materialize_keys(o, mt);
		heapify_for_in_pheap(&(this_state->merge_heap), mt);
	}

	return out;
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		uint64_t input_operators_count = o->desc->params.merge_sorted_inputs.input_operators_count;

		for(uint64_t i = 0; i < input_operators_count; i++)
		{
			if(this_state->mtuples[i].source != NULL)
				this_state->mtuples[i].source->destroy_state(this_state->mtuples[i].source);
			if(this_state->mtuples[i].tuple != NULL)
				free(this_state->mtuples[i].tuple);
		}

		free(this_state->key_dtis);
		free(this_state->keys);
		free(this_state->mtuples);
		free(this_state);
	}
	o->state = NULL;
}

void open_merge_sorted_inputs_operator(operator* o)
{
	uint64_t input_operators_count = o->desc->params.merge_sorted_inputs.input_operators_count;
	uint32_t key_element_count = o->desc->params.merge_sorted_inputs.key_element_count;

	op_state* this_state = malloc(sizeof(op_state));
	o->state = this_state;

	this_state->primed = 0;
	this_state->key_dtis = malloc(sizeof(data_type_info*) * key_element_count);
	this_state->keys = malloc(sizeof(datum) * key_element_count * input_operators_count);
	this_state->mtuples = malloc(sizeof(mergeable_tuple) * input_operators_count);

	// key column types come from the shared output schema (all inputs produce identical tuples)
	for(uint32_t j = 0; j < key_element_count; j++)
		this_state->key_dtis[j] = get_type_info_for_element_from_tuple_def(o->desc->output_tuple_def, o->desc->params.merge_sorted_inputs.key_element_ids[j]);

	// one heap node per input operator; no tuples are fetched here (next is never called from open)
	for(uint64_t i = 0; i < input_operators_count; i++)
	{
		mergeable_tuple* mt = &(this_state->mtuples[i]);

		mt->source = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.merge_sorted_inputs.input_operator_indices[i]);
		mt->tuple = NULL;
		mt->keys = &(this_state->keys[i * key_element_count]);
		initialize_phpnode(&(mt->embed_node));
	}

	initialize_pheap(&(this_state->merge_heap), MIN_HEAP, LEFTIST, &contexted_comparator(o, compare_mergeable_tuples), offsetof(mergeable_tuple, embed_node));

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}