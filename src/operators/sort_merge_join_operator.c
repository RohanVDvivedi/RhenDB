#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/function_compare.h>

#include<rhendb/interim_tuple_store.h>

#include<rhendb/join_preserve_type.h>

#include<stdlib.h>

/*
	idnetity operator, primarily used for adding tuple_transformers to its produce
*/

typedef struct input_values input_values;
struct input_values
{
	uint32_t key_element_count;
	const compare_direction* key_compare_direction;

	consumption_iterator* left_input_iterator;
	const tuple_def* left_input_tuple_def;
	const positional_accessor* left_key_element_ids;
	interim_tuple_store* left_side_equal_tuples_batch; // contains all the tuples that match with current batch of right_side_equal_tuples_batch, for the right_key_element_ids

	consumption_iterator* right_input_iterator;
	const tuple_def* right_input_tuple_def;
	const positional_accessor* right_key_element_ids;
	interim_tuple_store* right_side_equal_tuples_batch; // contains all the tuples that match with current batch of left_side_equal_tuples_batch, for the left_key_element_ids

	const tuple_def* output_tuple_def;

	// ptype can not be PRESERVE_RIGHT or PRESERVE_BOTH
	join_preserve_type ptype;
	uint32_t max_block_size;
};

static int produce_join_result(operator* o, const void* left_tuple, const void* right_tuple)
{
	input_values* inputs = o->inputs;

	uint32_t output_tuple_capacity = 32 + ((left_tuple != NULL) ? get_tuple_size(inputs->left_input_tuple_def, left_tuple) : 0) + ((right_tuple != NULL) ? get_tuple_size(inputs->right_input_tuple_def, right_tuple) : 0);

	void* output_tuple = malloc(output_tuple_capacity);

	init_tuple(inputs->output_tuple_def, output_tuple);

	if(left_tuple)
		set_element_in_tuple_from_tuple(inputs->output_tuple_def, STATIC_POSITION(0), output_tuple, inputs->left_input_tuple_def, SELF, left_tuple, UINT32_MAX);

	if(right_tuple)
		set_element_in_tuple_from_tuple(inputs->output_tuple_def, STATIC_POSITION(1), output_tuple, inputs->right_input_tuple_def, SELF, right_tuple, UINT32_MAX);

	// produce output_tuple
	int produced = produce_tuple_from_operator(o, output_tuple);
	free(output_tuple);

	return produced;
}

// returns 0 on error
static int cross_product_equal_tuples_on_both_sides(operator* o)
{
	input_values* inputs = o->inputs;

	// simple nlj over the left and right tuples
	// TODO implement bnlj
	FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(left_tuple, left_tuple_index, left_tuple_offset, &(inputs->left_input_tuple_def->size_def), inputs->left_side_equal_tuples_batch, inputs->max_block_size, {
		FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(right_tuple, right_tuple_index, right_tuple_offset, &(inputs->right_input_tuple_def->size_def), inputs->right_side_equal_tuples_batch, inputs->max_block_size, {
			if(!produce_join_result(o, left_tuple, right_tuple))
				return 0;
		});
	});

	return 1;
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

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
					destroy_consumption_iterator(inputs->left_input_iterator); inputs->left_input_iterator = NULL;
					goto LEFT_TUPLE_FETCHED;
				}
				if(can_not_proceed_for_execution_operator(o))
				{
					destroy_consumption_iterator(inputs->left_input_iterator); inputs->left_input_iterator = NULL;
					if(inputs->right_input_iterator != NULL)
					{
						destroy_consumption_iterator(inputs->right_input_iterator); inputs->right_input_iterator = NULL;
					}

					delete_interim_tuple_store(inputs->left_side_equal_tuples_batch); inputs->left_side_equal_tuples_batch = NULL;
					delete_interim_tuple_store(inputs->right_side_equal_tuples_batch); inputs->right_side_equal_tuples_batch = NULL;

					kill_signal_for_self_operator(o, kill_reason); return ;
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
					destroy_consumption_iterator(inputs->right_input_iterator); inputs->right_input_iterator = NULL;
					goto RIGHT_TUPLE_FETCHED;
				}
				if(can_not_proceed_for_execution_operator(o))
				{
					destroy_consumption_iterator(inputs->right_input_iterator); inputs->right_input_iterator = NULL;
					if(inputs->left_input_iterator != NULL)
					{
						destroy_consumption_iterator(inputs->left_input_iterator); inputs->left_input_iterator = NULL;
					}

					delete_interim_tuple_store(inputs->left_side_equal_tuples_batch); inputs->left_side_equal_tuples_batch = NULL;
					delete_interim_tuple_store(inputs->right_side_equal_tuples_batch); inputs->right_side_equal_tuples_batch = NULL;

					kill_signal_for_self_operator(o, kill_reason); return ;
				}
				if(inputs->right_input_iterator->embed_ptrs[0] == NULL)
					return;
			}
			RIGHT_TUPLE_FETCHED:;
		}

		// if there are no more tuples left on any side, then return success
		if(inputs->left_input_iterator == NULL && inputs->right_input_iterator == NULL)
		{
			delete_interim_tuple_store(inputs->left_side_equal_tuples_batch); inputs->left_side_equal_tuples_batch = NULL;
			delete_interim_tuple_store(inputs->right_side_equal_tuples_batch); inputs->right_side_equal_tuples_batch = NULL;

			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		if(get_total_bytes_in_interim_tuple_store(inputs->left_side_equal_tuples_batch) == 0) // if the batch of equal tuples is not yet started
		{
			int consume_left = 0;
			int consume_right = 0;

			if(inputs->right_input_iterator == NULL)
				consume_left = 1;
			else if(inputs->left_input_iterator == NULL)
				consume_right = 1;
			else
			{
				int cmp = compare_tuples_rhendb(inputs->left_input_iterator->embed_ptrs[0], inputs->left_input_tuple_def, inputs->left_key_element_ids,
												inputs->right_input_iterator->embed_ptrs[0], inputs->right_input_tuple_def, inputs->right_key_element_ids,
							inputs->key_compare_direction, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine));

				if(cmp == -1)
				{
					consume_left = 1;
				}
				else if(cmp == 1)
				{
					consume_right = 1;
				}
				else
				{
					consume_left = 1;
					consume_right = 1;
				}
			}

			if(consume_left && (!consume_right))
			{
				if(DOES_IT_PRESERVE_LEFT(inputs->ptype))
				{
					if(!produce_join_result(o, inputs->left_input_iterator->embed_ptrs[0], NULL))
					{
						destroy_consumption_iterator(inputs->left_input_iterator); inputs->left_input_iterator = NULL;

						delete_interim_tuple_store(inputs->left_side_equal_tuples_batch); inputs->left_side_equal_tuples_batch = NULL;
						delete_interim_tuple_store(inputs->right_side_equal_tuples_batch); inputs->right_side_equal_tuples_batch = NULL;

						kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
						kill_signal_for_self_operator(o, kill_reason); return ;
					}
				}
				inputs->left_input_iterator->embed_ptrs[0] = NULL;
			}
			else if(consume_right && (!consume_left))
			{
				if(DOES_IT_PRESERVE_RIGHT(inputs->ptype))
				{
					if(!produce_join_result(o, NULL, inputs->right_input_iterator->embed_ptrs[0]))
					{
						destroy_consumption_iterator(inputs->right_input_iterator); inputs->right_input_iterator = NULL;

						delete_interim_tuple_store(inputs->left_side_equal_tuples_batch); inputs->left_side_equal_tuples_batch = NULL;
						delete_interim_tuple_store(inputs->right_side_equal_tuples_batch); inputs->right_side_equal_tuples_batch = NULL;

						kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
						kill_signal_for_self_operator(o, kill_reason); return ;
					}
				}
				inputs->right_input_iterator->embed_ptrs[0] = NULL;
			}
			else
			{
				// insert this as left tuple and mmap this first tuple in embed_regions[1]
				append_tuple_to_interim_tuple_store2(inputs->left_side_equal_tuples_batch, &(inputs->left_side_equal_tuples_batch->embed_regions[0]), inputs->left_input_iterator->embed_ptrs[0], &(inputs->left_input_tuple_def->size_def), inputs->max_block_size);
				mmap_for_reading_tuple(inputs->left_side_equal_tuples_batch, &(inputs->left_side_equal_tuples_batch->embed_regions[1]), 0, &(inputs->left_input_tuple_def->size_def), inputs->max_block_size);
				inputs->left_side_equal_tuples_batch->embed_uints[0] = 1;

				// insert this as right tuple and mmap this first tuple in embed_regions[1]
				append_tuple_to_interim_tuple_store2(inputs->right_side_equal_tuples_batch, &(inputs->right_side_equal_tuples_batch->embed_regions[0]), inputs->right_input_iterator->embed_ptrs[0], &(inputs->left_input_tuple_def->size_def), inputs->max_block_size);
				mmap_for_reading_tuple(inputs->right_side_equal_tuples_batch, &(inputs->right_side_equal_tuples_batch->embed_regions[1]), 0, &(inputs->right_input_tuple_def->size_def), inputs->max_block_size);
				inputs->right_side_equal_tuples_batch->embed_uints[0] = 1;

				// embed_uints[0] = 1, implies previous tuple matched so attempt to compare it with the next

				inputs->left_input_iterator->embed_ptrs[0] = NULL;
				inputs->right_input_iterator->embed_ptrs[0] = NULL;
			}

			continue;
		}
		else
		{
			if(inputs->left_side_equal_tuples_batch->embed_uints[0])
			{
				if(inputs->left_input_iterator == NULL)
					inputs->left_side_equal_tuples_batch->embed_uints[0] = 0;
				else
					inputs->left_side_equal_tuples_batch->embed_uints[0] = (0 == compare_tuples2_rhendb(inputs->left_input_iterator->embed_ptrs[0], inputs->left_side_equal_tuples_batch->embed_regions[1].tuple,
															inputs->left_input_tuple_def, inputs->left_key_element_ids,
							inputs->key_compare_direction, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine)));
			}

			if(inputs->right_side_equal_tuples_batch->embed_uints[0])
			{
				if(inputs->right_input_iterator == NULL)
					inputs->right_side_equal_tuples_batch->embed_uints[0] = 0;
				else
					inputs->right_side_equal_tuples_batch->embed_uints[0] = (0 == compare_tuples2_rhendb(inputs->right_input_iterator->embed_ptrs[0], inputs->right_side_equal_tuples_batch->embed_regions[1].tuple,
															inputs->right_input_tuple_def, inputs->right_key_element_ids,
							inputs->key_compare_direction, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine)));
			}

			if(inputs->left_side_equal_tuples_batch->embed_uints[0])
			{
				append_tuple_to_interim_tuple_store2(inputs->left_side_equal_tuples_batch, &(inputs->left_side_equal_tuples_batch->embed_regions[0]), inputs->left_input_iterator->embed_ptrs[0], &(inputs->left_input_tuple_def->size_def), inputs->max_block_size);
				inputs->left_input_iterator->embed_ptrs[0] = NULL;
			}

			if(inputs->right_side_equal_tuples_batch->embed_uints[0])
			{
				append_tuple_to_interim_tuple_store2(inputs->right_side_equal_tuples_batch, &(inputs->right_side_equal_tuples_batch->embed_regions[0]), inputs->right_input_iterator->embed_ptrs[0], &(inputs->left_input_tuple_def->size_def), inputs->max_block_size);
				inputs->right_input_iterator->embed_ptrs[0] = NULL;
			}

			if((inputs->left_side_equal_tuples_batch->embed_uints[0] == 0) && (inputs->right_side_equal_tuples_batch->embed_uints[0] == 0))
			{
				if(!cross_product_equal_tuples_on_both_sides(o))
				{
					if(inputs->left_input_iterator != NULL)
					{
						destroy_consumption_iterator(inputs->left_input_iterator); inputs->left_input_iterator = NULL;
					}
					if(inputs->right_input_iterator != NULL)
					{
						destroy_consumption_iterator(inputs->right_input_iterator); inputs->right_input_iterator = NULL;
					}

					delete_interim_tuple_store(inputs->left_side_equal_tuples_batch); inputs->left_side_equal_tuples_batch = NULL;
					delete_interim_tuple_store(inputs->right_side_equal_tuples_batch); inputs->right_side_equal_tuples_batch = NULL;

					kill_reason = get_dstring_pointing_to_literal_cstring("could_not_cross_for_equal_tuples");
					kill_signal_for_self_operator(o, kill_reason); return ;
				}

				unmap_all_embed_regions_in_interim_tuple_store(inputs->left_side_equal_tuples_batch);
				unmap_all_embed_regions_in_interim_tuple_store(inputs->right_side_equal_tuples_batch);

				reinitialize_interim_tuple_store(inputs->left_side_equal_tuples_batch, inputs->max_block_size);
				reinitialize_interim_tuple_store(inputs->right_side_equal_tuples_batch, inputs->max_block_size);
			}

			continue;
		}
	}

	return ;
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->left_side_equal_tuples_batch)
		delete_interim_tuple_store(inputs->left_side_equal_tuples_batch);
	if(inputs->right_side_equal_tuples_batch)
		delete_interim_tuple_store(inputs->right_side_equal_tuples_batch);

	// all key_dtis were made into nullable types, so free them first
	for(uint32_t i = 0; i < 2; i++)
		free((data_type_info*)(inputs->output_tuple_def->type_info->containees[i].al.type_info));
	free((data_type_info*)(inputs->output_tuple_def->type_info));
	free((tuple_def*)(inputs->output_tuple_def));

	free(inputs);
}

static data_type_info* shallow_clone_into_nullable_type(const data_type_info* input_type_info)
{
	// figure out the number of bytes to shallow copy input_type_info
	size_t bytes_to_shallow_copy = get_shallow_copy_struct_size_for_data_type_info(input_type_info);

	// make shallow copy, mark it nullable, and finalize this type
	data_type_info* output_type_info = malloc(bytes_to_shallow_copy);
	memory_move(output_type_info, input_type_info, bytes_to_shallow_copy);
	output_type_info->is_nullable = 1;
	finalize_type_info(output_type_info);

	// return it
	return output_type_info;
}

operator_resource_counter setup_sort_merge_join_operator(operator* o, operator* left_input_operator, const positional_accessor* left_key_element_ids, operator* right_input_operator, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, join_preserve_type ptype, uint32_t max_block_size)
{
	if(max_block_size == 0)
	{
		printf("max_block_size can not be 0 for block_nested_loop_join_operator\n");
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

	operator_resource_counter result = {.buffer_counter = has_extended_type_info3(left_input_tuple_def, key_element_count, left_key_element_ids) + has_extended_type_info3(right_input_tuple_def, key_element_count, right_key_element_ids), .job_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = free_resources;

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
		.key_element_count = key_element_count,
		.key_compare_direction = key_compare_direction,

		.left_input_iterator = create_consumption_iterator(left_input_operator, o, NULL, NULL),
		.left_input_tuple_def = left_input_tuple_def,
		.left_key_element_ids = left_key_element_ids,
		.left_side_equal_tuples_batch = get_new_interim_tuple_store(max_block_size),

		.right_input_iterator = create_consumption_iterator(right_input_operator, o, NULL, NULL),
		.right_input_tuple_def = right_input_tuple_def,
		.right_key_element_ids = right_key_element_ids,
		.right_side_equal_tuples_batch = get_new_interim_tuple_store(max_block_size),

		.output_tuple_def = output_tuple_def,

		.ptype = ptype,
		.max_block_size = max_block_size,
	};

	return result;
}