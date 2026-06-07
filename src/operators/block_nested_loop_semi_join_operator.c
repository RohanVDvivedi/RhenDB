#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/interim_tuple_store.h>

#include<rhendb/join_type.h>

#include<rhendb/nullable_type_info_maker.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* left_input_iterator;
	const tuple_def* left_input_tuple_def;
	interim_tuple_store* batched_left_side_tuples; // only ciontains last atmost max_block_size bytes of tuples

	consumption_iterator* right_input_iterator;
	const tuple_def* right_input_tuple_def;
	interim_tuple_store* right_side_tuples; // contains all the tuples

	// it becomes a cross join if the join_matcher is NULL
	const void* join_matcher_context_p;
	int (*join_matcher)(const void* join_match_context_p, const void* left_tuple, const tuple_def* left_tuple_def, const void* right_tuple, const tuple_def* right_tuple_def);

	semi_join_type stype;
	uint32_t max_block_size;
};

// returns 0 on error
static int produce_batched_left_block_loop_over_all_right(operator* o)
{
	input_values* inputs = o->inputs;

	void* left_tuple_matched_bitmap = malloc(UINT_ALIGN_UP(inputs->batched_left_side_tuples->tuples_count, 8) / 8);
	memory_set(left_tuple_matched_bitmap, 0, UINT_ALIGN_UP(inputs->batched_left_side_tuples->tuples_count, 8) / 8);
	uint32_t left_tuples_matched_count = 0;

	uint64_t pairs_checked = 0;

	// iterate over all the right_side_tuples
	FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(right_tuple, right_tuple_index, right_tuple_offset, (&(inputs->right_input_tuple_def->size_def)), inputs->right_side_tuples, inputs->max_block_size,
	{
		// if all the left tuples are matched break out of this loop
		if(left_tuples_matched_count == inputs->batched_left_side_tuples->tuples_count)
			break;

		void* left_tuple = NULL;
		uint64_t left_tuple_index = 0;
		uint64_t left_tuple_offset = 0;
		while(mmap_for_reading_tuple(inputs->batched_left_side_tuples, &(inputs->batched_left_side_tuples->embed_regions[0]), left_tuple_offset, &(inputs->left_input_tuple_def->size_def), get_total_bytes_in_interim_tuple_store(inputs->batched_left_side_tuples)))
		{
			// after every 1000 tuples ensure that the operator is still allowed to progress
			if((pairs_checked % 1024 == 0) && can_not_proceed_for_execution_operator(o))
			{
				free(left_tuple_matched_bitmap);

				// region used for the outer loop
				unmap_for_interim_tuple_region(&_temp_tuple_region);

				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("block_nested_loop_join_killed_abruptly"));
				return 0;
			}pairs_checked++;

			left_tuple = inputs->batched_left_side_tuples->embed_regions[0].tuple;

			// if all the left tuples are matched break out of this loop
			if(left_tuples_matched_count == inputs->batched_left_side_tuples->tuples_count)
				break;

			// if this left side tuple already matched, then skip to next tuple, without the check
			if(get_bit(left_tuple_matched_bitmap, left_tuple_index) == 1)
				continue;

			int matched = 1;
			if(inputs->join_matcher != NULL)
				matched = inputs->join_matcher(inputs->join_matcher_context_p, left_tuple, inputs->left_input_tuple_def, right_tuple, inputs->right_input_tuple_def);

			if(matched == -1)
			{
				free(left_tuple_matched_bitmap);

				// region used for the outer loop
				unmap_for_interim_tuple_region(&_temp_tuple_region);

				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("block_nested_loop_join_matcher_errored"));
				return 0;
			}

			// mark this left_tuple matched, so we could skip it next time
			if(matched)
			{
				set_bit(left_tuple_matched_bitmap, left_tuple_index);
				left_tuples_matched_count++;
			}

			left_tuple_offset = next_tuple_offset_for_interim_tuple_region(&(inputs->batched_left_side_tuples->embed_regions[0]));
			left_tuple_index++;
		}
	});

	// produce all left tuples, based on the stype
	{
		void* left_tuple = NULL;
		uint64_t left_tuple_index = 0;
		uint64_t left_tuple_offset = 0;
		while(mmap_for_reading_tuple(inputs->batched_left_side_tuples, &(inputs->batched_left_side_tuples->embed_regions[0]), left_tuple_offset, &(inputs->left_input_tuple_def->size_def), get_total_bytes_in_interim_tuple_store(inputs->batched_left_side_tuples)))
		{
			left_tuple = inputs->batched_left_side_tuples->embed_regions[0].tuple;

			// if this left_tuple_index matched
			if(get_bit(left_tuple_matched_bitmap, left_tuple_index) == 1)
			{
				if(DOES_PRODUCE_MATCHED_LEFT_TUPLES(inputs->stype))
				{
					if(!produce_tuple_from_operator(o, left_tuple))
					{
						free(left_tuple_matched_bitmap);

						kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
						return 0;
					}
				}
			}
			else // if it never matched
			{
				if(DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(inputs->stype))
				{
					if(!produce_tuple_from_operator(o, left_tuple))
					{
						free(left_tuple_matched_bitmap);

						kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
						return 0;
					}
				}
			}

			left_tuple_offset = next_tuple_offset_for_interim_tuple_region(&(inputs->batched_left_side_tuples->embed_regions[0]));
			left_tuple_index++;
		}
	}

	return 1;
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	// first consume all of the right side first
	if(inputs->right_input_iterator != NULL)
	{
		while(1)
		{
			int no_more_data = 0;
			const void* tuple = consume_for_consumption_iterator(inputs->right_input_iterator, &no_more_data);
			if(no_more_data)
			{
				destroy_consumption_iterator(inputs->right_input_iterator);
				inputs->right_input_iterator = NULL;
				break;
			}
			if(can_not_proceed_for_execution_operator(o))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
				return ;
			}

			if(tuple != NULL)
			{
				append_tuple_to_interim_tuple_store2(inputs->right_side_tuples, &(inputs->right_side_tuples->embed_regions[0]), tuple, &(inputs->right_input_tuple_def->size_def), inputs->max_block_size);
			}
			else
				return;
		}
	}

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->left_input_iterator, &no_more_data);
		if(no_more_data)
		{
			produce_batched_left_block_loop_over_all_right(o);

			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
			return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
			return ;
		}

		if(tuple != NULL)
		{
			append_tuple_to_interim_tuple_store2(inputs->batched_left_side_tuples, &(inputs->batched_left_side_tuples->embed_regions[0]), tuple, &(inputs->left_input_tuple_def->size_def), inputs->max_block_size);

			if(get_total_bytes_in_interim_tuple_store(inputs->batched_left_side_tuples) > inputs->max_block_size)
			{
				if(!produce_batched_left_block_loop_over_all_right(o)) // returns 0, and fails
				{
					kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("failed_cross_join_on_batches"));
					return ;
				}

				unmap_all_embed_regions_in_interim_tuple_store(inputs->batched_left_side_tuples);
				unmap_all_embed_regions_in_interim_tuple_store(inputs->right_side_tuples);

				reinitialize_interim_tuple_store(inputs->batched_left_side_tuples, inputs->max_block_size);
			}
		}
		else
			return;
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

	if(inputs->batched_left_side_tuples != NULL)
	{
		delete_interim_tuple_store(inputs->batched_left_side_tuples);
		inputs->batched_left_side_tuples = NULL;
	}
	if(inputs->right_side_tuples != NULL)
	{
		delete_interim_tuple_store(inputs->right_side_tuples);
		inputs->right_side_tuples = NULL;
	}
}

operator_resource_counter setup_block_nested_loop_semi_join_operator(operator* o, operator* left_input_operator, operator* right_input_operator, const void* join_matcher_context_p, int (*join_matcher)(const void* join_match_context_p, const void* left_tuple, const tuple_def* left_tuple_def, const void* right_tuple, const tuple_def* right_tuple_def), semi_join_type stype, uint32_t max_block_size)
{
	if(max_block_size == 0)
	{
		printf("max_block_size can not be 0 for block_nested_loop_join_operator\n");
		exit(-1);
	}

	operator_resource_counter result = {.job_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	const tuple_def* left_input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(left_input_operator);
	const tuple_def* right_input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(right_input_operator);

	init_tuple_transformers(&(o->output_tuple_transformers), left_input_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.left_input_iterator = create_consumption_iterator(left_input_operator, o, NULL, NULL),
		.left_input_tuple_def = left_input_tuple_def,
		.batched_left_side_tuples = get_new_interim_tuple_store(max_block_size),

		.right_input_iterator = create_consumption_iterator(right_input_operator, o, NULL, NULL),
		.right_input_tuple_def = right_input_tuple_def,
		.right_side_tuples = get_new_interim_tuple_store(max_block_size),

		.join_matcher_context_p = join_matcher_context_p,
		.join_matcher = join_matcher,

		.stype = stype,
		.max_block_size = max_block_size,
	};

	return result;
}