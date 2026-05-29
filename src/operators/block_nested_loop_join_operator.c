#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/interim_tuple_store.h>

#include<rhendb/join_preserve_type.h>

#include<rhendb/nullable_type_info_maker.h>

#include<stdlib.h>

/*
	idnetity operator, primarily used for adding tuple_transformers to its produce
*/

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* left_input_iterator;
	const tuple_def* left_input_tuple_def;
	interim_tuple_store* batched_left_side_tuples; // only ciontains last atmost max_block_size bytes of tuples

	consumption_iterator* right_input_iterator;
	const tuple_def* right_input_tuple_def;
	interim_tuple_store* right_side_tuples; // contains all the tuples

	const tuple_def* output_tuple_def;

	// it becomes a cross join if the join_matcher is NULL
	const void* join_matcher_context_p;
	int (*join_matcher)(const void* join_match_context_p, const void* left_tuple, const tuple_def* left_tuple_def, const void* right_tuple, const tuple_def* right_tuple_def);

	// ptype can not be PRESERVE_RIGHT or PRESERVE_BOTH
	join_preserve_type ptype;
	uint32_t max_block_size;
};

static void clean_up_before_killing_self(operator* o)
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

// returns 0 on error
static int produce_batched_left_block_loop_over_all_right(operator* o)
{
	input_values* inputs = o->inputs;

	void* left_tuple_matched_bitmap = NULL;
	if(DOES_IT_PRESERVE_LEFT(inputs->ptype))
	{
		left_tuple_matched_bitmap = malloc(UINT_ALIGN_UP(inputs->batched_left_side_tuples->tuples_count, 8) / 8);
		memory_set(left_tuple_matched_bitmap, 0, UINT_ALIGN_UP(inputs->batched_left_side_tuples->tuples_count, 8) / 8);
	}

	// iterate over all the right_side_tuples
	FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(right_tuple, right_tuple_index, right_tuple_offset, (&(inputs->right_input_tuple_def->size_def)), inputs->right_side_tuples, inputs->max_block_size,
	{
		void* left_tuple = NULL;
		uint64_t left_tuple_index = 0;
		uint64_t left_tuple_offset = 0;
		while(mmap_for_reading_tuple(inputs->batched_left_side_tuples, &(inputs->batched_left_side_tuples->embed_regions[0]), left_tuple_offset, &(inputs->left_input_tuple_def->size_def), get_total_bytes_in_interim_tuple_store(inputs->batched_left_side_tuples)))
		{
			left_tuple = inputs->batched_left_side_tuples->embed_regions[0].tuple;

			int matched = 1;
			if(inputs->join_matcher != NULL)
				matched = inputs->join_matcher(inputs->join_matcher_context_p, left_tuple, inputs->left_input_tuple_def, right_tuple, inputs->right_input_tuple_def);

			if(matched == -1)
			{
				if(left_tuple_matched_bitmap != NULL)
					free(left_tuple_matched_bitmap);

				clean_up_before_killing_self(o);

				dstring kill_reason = get_dstring_pointing_to_literal_cstring("block_nested_loop_join_matcher_errored");
				kill_signal_for_self_operator(o, kill_reason); return 0;
			}

			if(matched)
			{
				if(left_tuple_matched_bitmap != NULL)
				{
					set_bit(left_tuple_matched_bitmap, left_tuple_index);
				}
			}

			if(matched)
			{
				uint32_t output_tuple_capacity = 32 + get_tuple_size(inputs->left_input_tuple_def, left_tuple) + get_tuple_size(inputs->right_input_tuple_def, right_tuple);
				void* output_tuple = malloc(output_tuple_capacity);

				init_tuple(inputs->output_tuple_def, output_tuple);

				set_element_in_tuple_from_tuple(inputs->output_tuple_def, STATIC_POSITION(0), output_tuple, inputs->left_input_tuple_def, SELF, left_tuple, UINT32_MAX);
				set_element_in_tuple_from_tuple(inputs->output_tuple_def, STATIC_POSITION(1), output_tuple, inputs->right_input_tuple_def, SELF, right_tuple, UINT32_MAX);

				// produce output_tuple
				int produced = produce_tuple_from_operator(o, output_tuple);
				free(output_tuple);
				if(!produced)
				{
					if(left_tuple_matched_bitmap != NULL)
						free(left_tuple_matched_bitmap);

					clean_up_before_killing_self(o);

					dstring kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
					kill_signal_for_self_operator(o, kill_reason); return 0;
				}
			}

			left_tuple_offset = next_tuple_offset_for_interim_tuple_region(&(inputs->batched_left_side_tuples->embed_regions[0]));
			left_tuple_index++;
		}
	});

	if(left_tuple_matched_bitmap != NULL)
	{
		void* left_tuple = NULL;
		uint64_t left_tuple_index = 0;
		uint64_t left_tuple_offset = 0;
		while(mmap_for_reading_tuple(inputs->batched_left_side_tuples, &(inputs->batched_left_side_tuples->embed_regions[0]), left_tuple_offset, &(inputs->left_input_tuple_def->size_def), get_total_bytes_in_interim_tuple_store(inputs->batched_left_side_tuples)))
		{
			left_tuple = inputs->batched_left_side_tuples->embed_regions[0].tuple;

			// if this left_tuple_index never matched
			if(get_bit(left_tuple_matched_bitmap, left_tuple_index) == 0)
			{
				uint32_t output_tuple_capacity = 32 + get_tuple_size(inputs->left_input_tuple_def, left_tuple);
				void* output_tuple = malloc(output_tuple_capacity);

				init_tuple(inputs->output_tuple_def, output_tuple);

				set_element_in_tuple_from_tuple(inputs->output_tuple_def, STATIC_POSITION(0), output_tuple, inputs->left_input_tuple_def, SELF, left_tuple, UINT32_MAX);

				// produce output_tuple
				int produced = produce_tuple_from_operator(o, output_tuple);
				free(output_tuple);
				if(!produced)
				{
					if(left_tuple_matched_bitmap != NULL)
						free(left_tuple_matched_bitmap);

					clean_up_before_killing_self(o);

					dstring kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
					kill_signal_for_self_operator(o, kill_reason); return 0;
				}
			}

			left_tuple_offset = next_tuple_offset_for_interim_tuple_region(&(inputs->batched_left_side_tuples->embed_regions[0]));
			left_tuple_index++;
		}
	}

	if(left_tuple_matched_bitmap != NULL)
		free(left_tuple_matched_bitmap);

	return 1;
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	// first consume all of the right side first
	if(inputs->right_input_iterator != NULL)
	{
		while(1)
		{
			int no_more_data = 0;
			const void* tuple = consume_for_consumption_iterator(inputs->right_input_iterator, &no_more_data);
			if(no_more_data)
			{
				destroy_consumption_iterator(inputs->right_input_iterator); inputs->right_input_iterator = NULL;
				break;
			}
			if(can_not_proceed_for_execution_operator(o))
			{
				clean_up_before_killing_self(o);

				kill_signal_for_self_operator(o, kill_reason); return ;
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
			// close left side operator
			destroy_consumption_iterator(inputs->left_input_iterator); inputs->left_input_iterator = NULL;

			produce_batched_left_block_loop_over_all_right(o);

			clean_up_before_killing_self(o);

			kill_signal_for_self_operator(o, kill_reason); return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			clean_up_before_killing_self(o);

			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		if(tuple != NULL)
		{
			append_tuple_to_interim_tuple_store2(inputs->batched_left_side_tuples, &(inputs->batched_left_side_tuples->embed_regions[0]), tuple, &(inputs->left_input_tuple_def->size_def), inputs->max_block_size);

			if(get_total_bytes_in_interim_tuple_store(inputs->batched_left_side_tuples) > inputs->max_block_size)
			{
				if(!produce_batched_left_block_loop_over_all_right(o)) // returns 0, and fails
				{
					clean_up_before_killing_self(o);

					kill_signal_for_self_operator(o, kill_reason); return ;
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

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->batched_left_side_tuples)
		delete_interim_tuple_store(inputs->batched_left_side_tuples);
	if(inputs->right_side_tuples)
		delete_interim_tuple_store(inputs->right_side_tuples);

	// all key_dtis were made into nullable types, so free them first
	for(uint32_t i = 0; i < 2; i++)
		free((data_type_info*)(inputs->output_tuple_def->type_info->containees[i].al.type_info));
	free((data_type_info*)(inputs->output_tuple_def->type_info));
	free((tuple_def*)(inputs->output_tuple_def));

	free(inputs);
}

operator_resource_counter setup_block_nested_loop_join_operator(operator* o, operator* left_input_operator, operator* right_input_operator, const void* join_matcher_context_p, int (*join_matcher)(const void* join_match_context_p, const void* left_tuple, const tuple_def* left_tuple_def, const void* right_tuple, const tuple_def* right_tuple_def), join_preserve_type ptype, uint32_t max_block_size)
{
	if(max_block_size == 0)
	{
		printf("max_block_size can not be 0 for block_nested_loop_join_operator\n");
		exit(-1);
	}

	if(DOES_IT_PRESERVE_RIGHT(ptype))
	{
		printf("can not be preserve right for block_nested_loop_join_operator\n");
		exit(-1);
	}

	operator_resource_counter result = {.job_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = free_resources;

	const tuple_def* left_input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(left_input_operator);
	const tuple_def* right_input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(right_input_operator);

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(2));
	uint32_t max_output_tuple_size = 0;

	{
		data_type_info* left_dti = left_input_tuple_def->type_info;

		if(left_dti == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += left_dti->is_variable_sized ? (8 + left_dti->max_size) : (1 + left_dti->size);

		strcpy(output_dti->containees[0].field_name, "left");
		output_dti->containees[0].al.type_info = shallow_clone_into_nullable_type(left_dti);
	}

	{
		data_type_info* right_dti = right_input_tuple_def->type_info;

		if(right_dti == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += right_dti->is_variable_sized ? (8 + right_dti->max_size) : (1 + right_dti->size);

		strcpy(output_dti->containees[1].field_name, "right");
		output_dti->containees[1].al.type_info = shallow_clone_into_nullable_type(right_dti);
	}

	initialize_tuple_data_type_info(output_dti, "join", 0, max_output_tuple_size, 2);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);


	// it is an identity operator, produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.left_input_iterator = create_consumption_iterator(left_input_operator, o, NULL, NULL),
		.left_input_tuple_def = left_input_tuple_def,
		.batched_left_side_tuples = get_new_interim_tuple_store(max_block_size),

		.right_input_iterator = create_consumption_iterator(right_input_operator, o, NULL, NULL),
		.right_input_tuple_def = right_input_tuple_def,
		.right_side_tuples = get_new_interim_tuple_store(max_block_size),

		.output_tuple_def = output_tuple_def,

		.join_matcher_context_p = join_matcher_context_p,
		.join_matcher = join_matcher,

		.ptype = ptype,
		.max_block_size = max_block_size,
	};

	return result;
}