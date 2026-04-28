#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

#include<rhendb/function_compare.h>

#include<tuplestore/tuple.h>
#include<tuplestore/datum.h>

#include<cutlery/singlylist.h>
#include<cutlery/pheap.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	// extracted from input_operator
	const tuple_def* record_def;
	uint32_t key_element_count;
	const positional_accessor* key_element_ids;
	const compare_direction* key_compare_direction;

	pheap ready_input_iterators;

	singlylist waiting_input_iterators;

	const data_type_info** key_dtis;
	datum* keys;
};

static int compare_consumption_iterators(const void* context_p, const void* cit1_vp, const void* cit2_vp)
{
	const operator* o = context_p;
	const input_values* inputs = o->inputs;

	const consumption_iterator* cit1_p = cit1_vp;
	const consumption_iterator* cit2_p = cit2_vp;

	int abort_error = 0;
	return compare_datums3_rhendb(cit1_p->embed_ptrs[1], cit2_p->embed_ptrs[1], inputs->key_dtis, inputs->key_compare_direction, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine), NULL, &abort_error);
}

// we materialize, the keys in cit_p->embed_ptrs[1], using it as datum[] having inputs->key_element_count elements long
static void revise_materialized_keys_in_consumption_iterator(operator* o, consumption_iterator* cit_p)
{
	input_values* inputs = o->inputs;

	for(uint32_t j = 0; j < inputs->key_element_count; j++)
		if(!get_value_from_element_from_tuple(&(((datum*)(cit_p->embed_ptrs[1]))[j]), inputs->record_def, inputs->key_element_ids[j], cit_p->embed_ptrs[0]))
			((datum*)(cit_p->embed_ptrs[1]))[j] = (*NULL_DATUM);
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	// iterate over all waiting_input_iterators and fetch their next tuples and stor it in embed_ptrs[0]
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
			remove_all_from_pheap(&(inputs->ready_input_iterators), DELETE_ON_NOTIFY_FOR_CONSUMPTION_ITERATOR);
			remove_all_from_singlylist(&(inputs->waiting_input_iterators), DELETE_ON_NOTIFY_FOR_CONSUMPTION_ITERATOR);

			kill_signal_for_self_operator(o, kill_reason); return ;
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
				remove_all_from_pheap(&(inputs->ready_input_iterators), DELETE_ON_NOTIFY_FOR_CONSUMPTION_ITERATOR);
				remove_all_from_singlylist(&(inputs->waiting_input_iterators), DELETE_ON_NOTIFY_FOR_CONSUMPTION_ITERATOR);

				kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
				kill_signal_for_self_operator(o, kill_reason); return ;
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
			remove_all_from_pheap(&(inputs->ready_input_iterators), DELETE_ON_NOTIFY_FOR_CONSUMPTION_ITERATOR);
			remove_all_from_singlylist(&(inputs->waiting_input_iterators), DELETE_ON_NOTIFY_FOR_CONSUMPTION_ITERATOR);

			kill_signal_for_self_operator(o, kill_reason); return ;
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
	remove_all_from_pheap(&(inputs->ready_input_iterators), DELETE_ON_NOTIFY_FOR_CONSUMPTION_ITERATOR);
	remove_all_from_singlylist(&(inputs->waiting_input_iterators), DELETE_ON_NOTIFY_FOR_CONSUMPTION_ITERATOR);
	kill_signal_for_self_operator(o, kill_reason); return ;
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	free(inputs->keys);
	free(inputs->key_dtis);
	free(inputs);
}

void setup_sorted_inputs_operator(operator* o, operator** input_operators, uint32_t input_operators_count, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction)
{
	if(input_operators_count == 0)
	{
		printf("union operator created with 0 input_operators\n");
		exit(-1);
	}

	const tuple_def* record_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operators[0]);
	for(uint32_t i = 1; i < input_operators_count; i++)
	{
		if(!are_identical_type_info(record_def->type_info, get_tuple_def_for_tuples_to_be_consumed_from(input_operators[i])->type_info))
		{
			printf("union operator created with non-identical input_operators\n");
			exit(-1);
		}
	}

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = free_resources;

	// it is an union operator (identity-like), produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), record_def);

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