#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

#include<rhendb/function_compare.h>

#include<tuplestore/tuple.h>
#include<tuplestore/datum.h>

#include<cutlery/singlylist.h>
#include<cutlery/pheap.h>

#include<stdlib.h>

typedef struct mergeable_tuple mergeable_tuple;
struct mergeable_tuple
{
	operator* source;

	void* tuple;

	datum* keys;

	php_node embed_node;
};

typedef struct op_state op_state;
struct op_state
{
	pheap input_tuples;

	singlylist waiting_input_iterators;

	const data_type_info** key_dtis;

	// memory allocation holders
	datum* keys;
	mergeable_tuples* mtuples;
};

static int compare_mergeable_tuples(const void* context_p, const void* mt1_vp, const void* mt2_vp)
{
	const operator* o = context_p;
	const op_state* this_state = o->state;

	const mergeable_tuple* mt1 = mt1_vp;
	const mergeable_tuple* mt2 = mt2_vp;

	return compare_datums3_rhendb(mt1->keys, mt2->keys, this_state->key_dtis, o->desc->params.merge_sorted_inputs.key_compare_direction, o->desc->params.merge_sorted_inputs.key_element_count, o->self_query_plan->curr_tx);
}

static void revise_materialized_keys_in_mergeable_tuple(operator* o, mergeable_tuple* mt)
{
	const op_state* this_state = o->state;

	for(uint32_t j = 0; j < inputs->key_element_count; j++)
	{
		const data_type_info* dti_temp;
		if(!get_value_from_element_from_tuple(mt1->keys[j], &dti_temp,  o->desc->output_tuple_def, o->desc->params.merge_sorted_inputs.key_element_ids[j], mt->tuple))
			mt1->keys[j] = (*NULL_DATUM);
	}
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	// iterate over all waiting_input_iterators and fetch their next tuples and store it in embed_ptrs[0]
	// and also move it friom waiting_input_iterators to ready_input_iterators
	while(!is_empty_singlylist(&(inputs->waiting_input_iterators)))
	{
		consumption_iterator* cit_p = (consumption_iterator*) get_head_of_singlylist(&(inputs->waiting_input_iterators));

		int no_more_data = 0;
		cit_p->embed_ptrs[0] = (void*) consume_for_consumption_iterator(cit_p, &no_more_data);
		if(no_more_data)
		{
			remove_head_from_singlylist(&(inputs->waiting_input_iterators));
			destroy_consumption_iterator(cit_p);
			continue;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
			return ;
		}

		if(cit_p->embed_ptrs[0] != NULL)
		{
			remove_head_from_singlylist(&(inputs->waiting_input_iterators));
			revise_materialized_keys_in_consumption_iterator(o, cit_p);
			push_to_pheap(&(inputs->ready_input_iterators), cit_p);
			continue;
		}
		else
		{
			return;
		}
	}

	// now we know for sure that we are not waiting for input tuples from any input_operators

	// so iterate over the ready_input_iterators and pick the smallest and produce it, as long as the waiting_input_iteratros is empty
	while(!is_empty_pheap(&(inputs->ready_input_iterators)))
	{
		// fetch the top of the min pheap
		consumption_iterator* cit_p = (consumption_iterator*) get_top_of_pheap(&(inputs->ready_input_iterators));

		const void* tuple = cit_p->embed_ptrs[0];
		cit_p->embed_ptrs[0] = NULL;

		// produce the top tuple
		{
			int produced = produce_tuple_from_operator(o, (void*)tuple);
			if(!produced)
			{
				kill_signal_for_self_operator(o, OPERATOR_COULD_NOT_PRODUCE_REASON);
				return ;
			}
		}

		// fetch the next tuple, if it is successfully fetch then heapify and continue
		// else move it into waiting_input_iterators
		int no_more_data = 0;
		cit_p->embed_ptrs[0] = (void*) consume_for_consumption_iterator(cit_p, &no_more_data);

		if(no_more_data)
		{
			remove_from_pheap(&(inputs->ready_input_iterators), cit_p);
			destroy_consumption_iterator(cit_p);
			continue;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
			return ;
		}

		if(cit_p->embed_ptrs[0] != NULL)
		{
			revise_materialized_keys_in_consumption_iterator(o, cit_p);
			heapify_for_in_pheap(&(inputs->ready_input_iterators), cit_p);
			continue;
		}
		else
		{
			remove_from_pheap(&(inputs->ready_input_iterators), cit_p);
			insert_tail_in_singlylist(&(inputs->waiting_input_iterators), cit_p);

			// waiting_input_iterators now has something so we can no longer proceed with this loop
			return;
		}
	}

	// if there is nothing ready (there already is nothing in waiting also), we are done, we quit with success
	kill_signal_for_self_operator(o, OPERATOR_SUCCESS_REASON);
	return ;
}

static void destroy_state(operator* o)
{
	
}

void open_merge_sorted_inputs_operator(operator* o)
{
	if(input_operators_count == 0)
	{
		printf("input_operators can not be 0 for merge_sorted_inputs_operator\n");
		exit(-1);
	}

	const tuple_def* record_def = input_operators[0]->output_tuple_def;
	for(uint32_t i = 1; i < input_operators_count; i++)
	{
		if(!are_accessibly_equivalent_type_info(record_def->type_info, input_operators[i]->output_tuple_def->type_info)) // if they are accessibly_equivalent, then they must be comparable
		{
			printf("input_operators must produce identical outputs for merge_sorted_inputs_operator\n");
			exit(-1);
		}
	}

	if(key_element_count == 0)
	{
		printf("key_element_count can not be 0 for merge_sorted_inputs_operator\n");
		exit(-1);
	}

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	// it is an union operator (identity-like), produces the same thing as it consumes
	o->output_tuple_def = record_def;

	o->inputs = malloc(sizeof(input_values));
	input_values* inputs = o->inputs;
	*inputs = (input_values){
		.record_def = record_def,
		.key_element_count = key_element_count,
		.key_element_ids = key_element_ids,
		.key_compare_direction = key_compare_direction,
		.key_dtis = malloc(sizeof(data_type_info*) * key_element_count),
		.keys = malloc(sizeof(datum) * key_element_count * input_operators_count),
	};

	for(uint32_t j = 0; j < key_element_count; j++)
		inputs->key_dtis[j] = get_type_info_for_element_from_tuple_def(inputs->record_def, key_element_ids[j]);

	initialize_pheap(&(inputs->ready_input_iterators), MIN_HEAP, LEFTIST, &contexted_comparator(o, compare_consumption_iterators), offsetof(consumption_iterator, embed_node_php));
	initialize_singlylist(&(inputs->waiting_input_iterators), offsetof(consumption_iterator, embed_node_sl));

	for(uint32_t i = 0; i < input_operators_count; i++)
	{
		consumption_iterator* cit_p = create_consumption_iterator(input_operators[i], o, NULL, NULL);
		cit_p->embed_ptrs[0] = NULL;
		cit_p->embed_ptrs[1] = &(inputs->keys[i * key_element_count]);
		insert_tail_in_singlylist(&(inputs->waiting_input_iterators), cit_p);
	}
}