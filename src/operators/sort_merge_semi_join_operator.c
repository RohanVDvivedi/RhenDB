#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/function_compare.h>

#include<rhendb/interim_tuple_store.h>

#include<rhendb/join_type.h>

#include<rhendb/nullable_type_info_maker.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	uint32_t key_element_count;
	const compare_direction* key_compare_direction;

	consumption_iterator* left_input_iterator;
	const tuple_def* left_input_tuple_def;
	const positional_accessor* left_key_element_ids;

	consumption_iterator* right_input_iterator;
	const tuple_def* right_input_tuple_def;
	const positional_accessor* right_key_element_ids;

	semi_join_type stype;
	uint32_t min_block_size;
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	while(1)
	{
		// fetch left side tuple if possible
		if(inputs->left_input_iterator != NULL)
		{
			if(inputs->left_input_iterator->embed_ptrs[0] == NULL)
			{
				int no_more_data = 0;
				inputs->left_input_iterator->embed_ptrs[0] = (void*) consume_for_consumption_iterator(inputs->left_input_iterator, &no_more_data);
				if(no_more_data)
				{
					destroy_consumption_iterator(inputs->left_input_iterator);
					inputs->left_input_iterator = NULL;
					goto LEFT_TUPLE_FETCHED;
				}
				if(can_not_proceed_for_execution_operator(o))
				{
					kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
					return ;
				}
				if(inputs->left_input_iterator->embed_ptrs[0] == NULL)
					return;
			}
			LEFT_TUPLE_FETCHED:;
		}

		// fetch right side tuple if possible
		if(inputs->right_input_iterator != NULL)
		{
			if(inputs->right_input_iterator->embed_ptrs[0] == NULL)
			{
				int no_more_data = 0;
				inputs->right_input_iterator->embed_ptrs[0] = (void*) consume_for_consumption_iterator(inputs->right_input_iterator, &no_more_data);
				if(no_more_data)
				{
					destroy_consumption_iterator(inputs->right_input_iterator);
					inputs->right_input_iterator = NULL;
					goto RIGHT_TUPLE_FETCHED;
				}
				if(can_not_proceed_for_execution_operator(o))
				{
					kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
					return ;
				}
				if(inputs->right_input_iterator->embed_ptrs[0] == NULL)
					return;
			}
			RIGHT_TUPLE_FETCHED:;
		}

		// if there are no more tuples on the left side, then return success
		// as we are only producing left side tuples, we need left tuples to continue operating
		if(inputs->left_input_iterator == NULL)
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
			return ;
		}

		// if left side tuples are left, but there are none remaining to be read on the right side
		// this means all the left side tuples will remian un-matched
		// this means if we are not meant to produce any unmatched tuples then quit
		if(inputs->right_input_iterator == NULL && (!(DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(inputs->stype))))
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
			return ;
		}

		{
			int produce_left = 0;
			int consume_left = 0;
			int consume_right = 0;

			if(inputs->right_input_iterator == NULL)
			{
				produce_left = DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(inputs->stype);
				consume_left = 1;
			}
			else
			{
				int cmp = compare_tuples_rhendb(inputs->left_input_iterator->embed_ptrs[0], inputs->left_input_tuple_def, inputs->left_key_element_ids,
												inputs->right_input_iterator->embed_ptrs[0], inputs->right_input_tuple_def, inputs->right_key_element_ids,
							inputs->key_compare_direction, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine));

				if(cmp == -1)
				{
					produce_left = DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(inputs->stype);
					consume_left = 1;
				}
				else if(cmp == 1)
				{
					consume_right = 1;
				}
				else
				{
					produce_left = DOES_PRODUCE_MATCHED_LEFT_TUPLES(inputs->stype);
					consume_left = 1;
				}
			}

			if(produce_left)
			{
				if(!produce_tuple_from_operator(o, inputs->left_input_iterator->embed_ptrs[0]))
				{
					kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
					return;
				}
			}

			if(consume_left)
				inputs->left_input_iterator->embed_ptrs[0] = NULL;

			if(consume_right)
				inputs->right_input_iterator->embed_ptrs[0] = NULL;

			continue;
		}
	}

	return ;
}

static void clean_up_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->right_input_iterator != NULL)
	{
		destroy_consumption_iterator(inputs->right_input_iterator);
		inputs->right_input_iterator = NULL;
	}
	if(inputs->left_input_iterator != NULL)
	{
		destroy_consumption_iterator(inputs->left_input_iterator);
		inputs->left_input_iterator = NULL;
	}
}

operator_resource_counter setup_sort_merge_semi_join_operator(operator* o, operator* left_input_operator, const positional_accessor* left_key_element_ids, operator* right_input_operator, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, semi_join_type stype, uint32_t min_block_size)
{
	if(key_element_count == 0)
	{
		printf("key_element_count must not be 0 for sort_merge_semi_join_operator\n");
		exit(-1);
	}

	if(min_block_size == 0)
	{
		printf("min_block_size can not be 0 for sort_merge_semi_join_operator\n");
		exit(-1);
	}

	const tuple_def* left_input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(left_input_operator);
	const tuple_def* right_input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(right_input_operator);

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		if(!can_compare_datum_rhendb(get_type_info_for_element_from_tuple_def(left_input_tuple_def, left_key_element_ids[i]), get_type_info_for_element_from_tuple_def(right_input_tuple_def, right_key_element_ids[i])))
		{
			printf("input_operators must produce comparable key outputs for inputs to sort_merge_join_operator\n");
			exit(-1);
		}
	}

	operator_resource_counter result = {.buffer_counter = has_extended_type_info3(left_input_tuple_def, key_element_count, left_key_element_ids, PERSISTENT_EXT_SUB_TYPE) + has_extended_type_info3(right_input_tuple_def, key_element_count, right_key_element_ids, PERSISTENT_EXT_SUB_TYPE), .job_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	init_tuple_transformers(&(o->output_tuple_transformers), left_input_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.key_element_count = key_element_count,
		.key_compare_direction = key_compare_direction,

		.left_input_iterator = create_consumption_iterator(left_input_operator, o, NULL, NULL),
		.left_input_tuple_def = left_input_tuple_def,
		.left_key_element_ids = left_key_element_ids,

		.right_input_iterator = create_consumption_iterator(right_input_operator, o, NULL, NULL),
		.right_input_tuple_def = right_input_tuple_def,
		.right_key_element_ids = right_key_element_ids,

		.stype = stype,
		.min_block_size = min_block_size,
	};

	return result;
}