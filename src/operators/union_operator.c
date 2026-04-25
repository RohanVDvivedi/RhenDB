#include<rhendb/query_plan.h>

#include<cutlery/linkedlist.h>

#include<stdlib.h>

#define IS_READY 0
#define IS_WAITING 1
#define IS_DELETED 2

typedef struct input_values input_values;
struct input_values
{
	uint32_t input_operators_count;

	const tuple_def* input_tuple_def;

	pthread_mutex_t input_iterators_list_lock;

	linkedlist ready_input_iterators;

	linkedlist waiting_input_iterators;
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	while(1)
	{
		consumption_iterator* cit_p = NULL;

		// pick any run that is ready, and move it to waiting ones
		pthread_mutex_lock(&(inputs->input_iterators_list_lock));

		cit_p = (consumption_iterator*) get_head_of_linkedlist(&(inputs->ready_input_iterators));
		if(cit_p != NULL)
		{
			remove_from_linkedlist(&(inputs->ready_input_iterators), cit_p);
			cit_p->embed_uints[0] = IS_WAITING;
			insert_tail_in_linkedlist(&(inputs->waiting_input_iterators), cit_p);
		}

		pthread_mutex_unlock(&(inputs->input_iterators_list_lock));

		// fail if there ie nothing left to consume from, i.e. nothing is ready
		if(cit_p == NULL)
			break;

		// consume from this selected cit_p until there is nothing more produced
		while(1)
		{
			int no_more_data = 0;
			const void* tuple = consume_for_consumption_iterator(cit_p, &no_more_data);
			if(no_more_data)
			{
				// this imples that this cit_p will not produce anything more, and must be destroyed

				pthread_mutex_lock(&(inputs->input_iterators_list_lock));

				if(cit_p->embed_uints[0] == IS_WAITING)
					remove_from_linkedlist(&(inputs->waiting_input_iterators), cit_p);
				else if(cit_p->embed_uints[0] == IS_READY)
					remove_from_linkedlist(&(inputs->ready_input_iterators), cit_p);

				cit_p->embed_uints[0] = IS_DELETED;

				pthread_mutex_unlock(&(inputs->input_iterators_list_lock));

				destroy_consumption_iterator(cit_p);

				inputs->input_operators_count--;

				if(inputs->input_operators_count == 0)
				{
					kill_signal_for_self_operator(o, kill_reason); return ;
				}

				break;
			}
			if(can_not_proceed_for_execution_operator(o))
			{
				kill_signal_for_self_operator(o, kill_reason); return ;
			}

			if(tuple != NULL) // if there was a tuple, produce it
			{
				int produced = produce_tuple_from_operator(o, (void*)tuple);
				if(!produced)
				{
					kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
					kill_signal_for_self_operator(o, kill_reason); return ;
				}
			}
			else // else we break out of this loop and return, and pick the next ready_input_iterator
				break;
		}
	}

	return ;
}

// this call back ensures that on a trigger, we move the respective cit_p from waiting to ready
static void notify_callback(operator* o, consumption_iterator* cit_p)
{
	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->input_iterators_list_lock));

	if(cit_p->embed_uints[0] == IS_WAITING)
	{
		remove_from_linkedlist(&(inputs->waiting_input_iterators), cit_p);
		cit_p->embed_uints[0] = IS_READY;
		insert_tail_in_linkedlist(&(inputs->ready_input_iterators), cit_p);
	}

	pthread_mutex_unlock(&(inputs->input_iterators_list_lock));
}

void setup_union_operator(operator* o, operator** input_operators, uint32_t input_operators_count)
{
	if(input_operators_count == 0)
	{
		printf("union operator created with 0 input_operators\n");
		exit(-1);
	}

	const tuple_def* input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operators[0]);
	for(uint32_t i = 1; i < input_operators_count; i++)
	{
		if(!are_identical_type_info(input_tuple_def->type_info, get_tuple_def_for_tuples_to_be_consumed_from(input_operators[i])->type_info))
		{
			printf("union operator created with non-identical input_operators\n");
			exit(-1);
		}
	}

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	// it is an union operator (identity-like), produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), input_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	input_values* inputs = o->inputs;
	*inputs = (input_values){
		.input_operators_count = input_operators_count,
		.input_tuple_def = input_tuple_def,
		.input_iterators_list_lock = PTHREAD_MUTEX_INITIALIZER,
	};

	initialize_linkedlist(&(inputs->ready_input_iterators), offsetof(consumption_iterator, embed_node_ll));
	initialize_linkedlist(&(inputs->waiting_input_iterators), offsetof(consumption_iterator, embed_node_ll));

	for(uint32_t i = 0; i < input_operators_count; i++)
	{
		consumption_iterator* cit_p = create_consumption_iterator(input_operators[i], o, notify_callback, NULL);
		cit_p->embed_uints[0] = IS_READY;
		insert_tail_in_linkedlist(&(inputs->ready_input_iterators), cit_p);
	}
}