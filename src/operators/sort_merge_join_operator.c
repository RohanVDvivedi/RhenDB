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
	interim_tuple_store* left_side_equal_tuples_batch; // contains all the tuples that match with current batch of right_side_equal_tuples_batch, for the right_key_element_ids

	consumption_iterator* right_input_iterator;
	const tuple_def* right_input_tuple_def;
	const positional_accessor* right_key_element_ids;
	interim_tuple_store* right_side_equal_tuples_batch; // contains all the tuples that match with current batch of left_side_equal_tuples_batch, for the left_key_element_ids

	const tuple_def* output_tuple_def;

	join_preserve_type ptype;
	uint32_t min_block_size;
};

static int produce_join_result(operator* o, const void* left_tuple, const void* right_tuple)
{
	input_values* inputs = o->inputs;

	uint32_t output_tuple_capacity = get_minimum_tuple_size(inputs->output_tuple_def);
	uint32_t output_tuple_size = output_tuple_capacity;

	void* output_tuple = malloc(output_tuple_capacity);

	init_tuple(inputs->output_tuple_def, output_tuple);

	if(left_tuple)
	{
		datum left_datum;
		get_value_from_element_from_tuple(&left_datum, inputs->left_input_tuple_def, SELF, left_tuple);
		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(0), output_tuple, &left_datum, output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity += get_tuple_size(inputs->left_input_tuple_def, left_tuple);
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);
	}

	if(right_tuple)
	{
		datum right_datum;
		get_value_from_element_from_tuple(&right_datum, inputs->right_input_tuple_def, SELF, right_tuple);
		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(1), output_tuple, &right_datum, output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity += get_tuple_size(inputs->right_input_tuple_def, right_tuple);
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);
	}

	// produce output_tuple
	int produced = produce_tuple_from_operator(o, output_tuple);
	free(output_tuple);

	return produced;
}

// returns 0 on error
static int cross_product_equal_tuples_on_both_sides(operator* o)
{
	input_values* inputs = o->inputs;

	int fail = 0;

	// simple nlj over the left and right tuples, UNOPTIMIZED
	/*FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(left_tuple, left_tuple_index, left_tuple_offset, &(inputs->left_input_tuple_def->size_def), inputs->left_side_equal_tuples_batch, inputs->min_block_size, {
		FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(right_tuple, right_tuple_index, right_tuple_offset, &(inputs->right_input_tuple_def->size_def), inputs->right_side_equal_tuples_batch, inputs->min_block_size, {
			if(!produce_join_result(o, left_tuple, right_tuple))
				fail = 1;
			if(fail)
				break;
		});
		if(fail)
			break;
	});*/

	// block nested loop join over the left and right tuples
	uint64_t left_block_offset = 0;
	while(mmap_for_reading_tuple(inputs->left_side_equal_tuples_batch, &(inputs->left_side_equal_tuples_batch->embed_regions[0]), left_block_offset, &(inputs->left_input_tuple_def->size_def), inputs->min_block_size))
	{
		uint64_t next_left_block_offset = left_block_offset;

		uint64_t right_block_offset = 0;
		while(mmap_for_reading_tuple(inputs->right_side_equal_tuples_batch, &(inputs->right_side_equal_tuples_batch->embed_regions[0]), right_block_offset, &(inputs->right_input_tuple_def->size_def), inputs->min_block_size))
		{
			uint64_t next_right_block_offset = right_block_offset;

			uint64_t left_tuple_offset = left_block_offset;
			while(contains_tuple_at_offset_in_interim_tuple_store(inputs->left_side_equal_tuples_batch, &(inputs->left_side_equal_tuples_batch->embed_regions[0]), left_tuple_offset, &(inputs->left_input_tuple_def->size_def))
			 && mmap_for_reading_tuple(inputs->left_side_equal_tuples_batch, &(inputs->left_side_equal_tuples_batch->embed_regions[0]), left_tuple_offset, &(inputs->left_input_tuple_def->size_def), inputs->min_block_size))
			{
				const void* left_tuple = inputs->left_side_equal_tuples_batch->embed_regions[0].tuple;

				uint64_t right_tuple_offset = right_block_offset;
				while(contains_tuple_at_offset_in_interim_tuple_store(inputs->right_side_equal_tuples_batch, &(inputs->right_side_equal_tuples_batch->embed_regions[0]), right_tuple_offset, &(inputs->right_input_tuple_def->size_def))
				 && mmap_for_reading_tuple(inputs->right_side_equal_tuples_batch, &(inputs->right_side_equal_tuples_batch->embed_regions[0]), right_tuple_offset, &(inputs->right_input_tuple_def->size_def), inputs->min_block_size))
				{
					const void* right_tuple = inputs->right_side_equal_tuples_batch->embed_regions[0].tuple;

					if(!produce_join_result(o, left_tuple, right_tuple))
						fail = 1;
					if(fail)
						break;

					right_tuple_offset = next_tuple_offset_for_interim_tuple_region(&(inputs->right_side_equal_tuples_batch->embed_regions[0]));
				}
				if(fail)
					break;

				next_right_block_offset = max(right_tuple_offset, next_right_block_offset);

				left_tuple_offset = next_tuple_offset_for_interim_tuple_region(&(inputs->left_side_equal_tuples_batch->embed_regions[0]));
			}
			if(fail)
				break;

			right_block_offset = next_right_block_offset;

			next_left_block_offset =  max(left_tuple_offset, next_left_block_offset);
		}
		if(fail)
			break;

		left_block_offset = next_left_block_offset;
	}

	if(fail)
		return 0;

	return 1;
}

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

		// if there are no more tuples left on any side, then return success
		if(inputs->left_input_iterator == NULL && inputs->right_input_iterator == NULL)
		{
			// unmap the append regions embed_regions[1]
			unmap_for_interim_tuple_region(&(inputs->left_side_equal_tuples_batch->embed_regions[1]));
			unmap_for_interim_tuple_region(&(inputs->right_side_equal_tuples_batch->embed_regions[1]));

			// if there are any pending tuples process them
			if(!cross_product_equal_tuples_on_both_sides(o))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_cross_for_equal_tuples"));
				return ;
			}

			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
			return ;
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
						kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
						return ;
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
						kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
						return ;
					}
				}
				inputs->right_input_iterator->embed_ptrs[0] = NULL;
			}
			else
			{
				// insert this as left tuple and mmap this first tuple in embed_regions[0]
				append_tuple_to_interim_tuple_store2(inputs->left_side_equal_tuples_batch, &(inputs->left_side_equal_tuples_batch->embed_regions[0]), inputs->left_input_iterator->embed_ptrs[0], &(inputs->left_input_tuple_def->size_def), inputs->min_block_size);
				inputs->left_side_equal_tuples_batch->embed_uints[0] = 1;

				// insert this as right tuple and mmap this first tuple in embed_regions[0]
				append_tuple_to_interim_tuple_store2(inputs->right_side_equal_tuples_batch, &(inputs->right_side_equal_tuples_batch->embed_regions[0]), inputs->right_input_iterator->embed_ptrs[0], &(inputs->right_input_tuple_def->size_def), inputs->min_block_size);
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
					inputs->left_side_equal_tuples_batch->embed_uints[0] = (0 == compare_tuples2_rhendb(inputs->left_input_iterator->embed_ptrs[0], inputs->left_side_equal_tuples_batch->embed_regions[0].tuple,
															inputs->left_input_tuple_def, inputs->left_key_element_ids,
							inputs->key_compare_direction, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine)));
			}

			if(inputs->right_side_equal_tuples_batch->embed_uints[0])
			{
				if(inputs->right_input_iterator == NULL)
					inputs->right_side_equal_tuples_batch->embed_uints[0] = 0;
				else
					inputs->right_side_equal_tuples_batch->embed_uints[0] = (0 == compare_tuples2_rhendb(inputs->right_input_iterator->embed_ptrs[0], inputs->right_side_equal_tuples_batch->embed_regions[0].tuple,
															inputs->right_input_tuple_def, inputs->right_key_element_ids,
							inputs->key_compare_direction, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine)));
			}

			if(inputs->left_side_equal_tuples_batch->embed_uints[0])
			{
				// any further append uses embed_regions[1] in left_side_equal_tuples_batch
				append_tuple_to_interim_tuple_store2(inputs->left_side_equal_tuples_batch, &(inputs->left_side_equal_tuples_batch->embed_regions[1]), inputs->left_input_iterator->embed_ptrs[0], &(inputs->left_input_tuple_def->size_def), inputs->min_block_size);
				inputs->left_input_iterator->embed_ptrs[0] = NULL;
			}

			if(inputs->right_side_equal_tuples_batch->embed_uints[0])
			{
				append_tuple_to_interim_tuple_store2(inputs->right_side_equal_tuples_batch, &(inputs->right_side_equal_tuples_batch->embed_regions[1]), inputs->right_input_iterator->embed_ptrs[0], &(inputs->right_input_tuple_def->size_def), inputs->min_block_size);
				inputs->right_input_iterator->embed_ptrs[0] = NULL;
			}

			if((inputs->left_side_equal_tuples_batch->embed_uints[0] == 0) && (inputs->right_side_equal_tuples_batch->embed_uints[0] == 0))
			{
				// unmaop the append regions embed_regions[1]
				unmap_for_interim_tuple_region(&(inputs->left_side_equal_tuples_batch->embed_regions[1]));
				unmap_for_interim_tuple_region(&(inputs->right_side_equal_tuples_batch->embed_regions[1]));

				if(!cross_product_equal_tuples_on_both_sides(o))
				{
					kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_cross_for_equal_tuples"));
					return ;
				}

				unmap_all_embed_regions_in_interim_tuple_store(inputs->left_side_equal_tuples_batch);
				unmap_all_embed_regions_in_interim_tuple_store(inputs->right_side_equal_tuples_batch);

				reinitialize_interim_tuple_store(inputs->left_side_equal_tuples_batch, inputs->min_block_size);
				reinitialize_interim_tuple_store(inputs->right_side_equal_tuples_batch, inputs->min_block_size);
			}

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

	if(inputs->left_side_equal_tuples_batch != NULL)
	{
		delete_interim_tuple_store(inputs->left_side_equal_tuples_batch);
		inputs->left_side_equal_tuples_batch = NULL;
	}
	if(inputs->right_side_equal_tuples_batch != NULL)
	{
		delete_interim_tuple_store(inputs->right_side_equal_tuples_batch);
		inputs->right_side_equal_tuples_batch = NULL;
	}
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	// all key_dtis were made into nullable types, so free them first
	for(uint32_t i = 0; i < 2; i++)
		free((data_type_info*)(inputs->output_tuple_def->type_info->containees[i].al.type_info));
	free((data_type_info*)(inputs->output_tuple_def->type_info));
	free((tuple_def*)(inputs->output_tuple_def));

	free(inputs);
}

operator_resource_counter setup_sort_merge_join_operator(operator* o, operator* left_input_operator, const positional_accessor* left_key_element_ids, operator* right_input_operator, const positional_accessor* right_key_element_ids, const compare_direction* key_compare_direction, uint32_t key_element_count, join_preserve_type ptype, uint32_t min_block_size)
{
	if(min_block_size == 0)
	{
		printf("min_block_size can not be 0 for sort_merge_join_operator\n");
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
	o->clean_up_resources = clean_up_resources;
	o->free_resources = free_resources;

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(2));
	uint64_t max_output_tuple_size = 8;

	{
		data_type_info* left_dti = left_input_tuple_def->type_info;

		if(left_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += left_dti->is_variable_sized ? (8 + left_dti->max_size) : (1 + left_dti->size);

		strcpy(output_dti->containees[0].field_name, "left");
		output_dti->containees[0].al.type_info = shallow_clone_into_nullable_type(left_dti);
	}

	{
		data_type_info* right_dti = right_input_tuple_def->type_info;

		if(right_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += right_dti->is_variable_sized ? (8 + right_dti->max_size) : (1 + right_dti->size);

		strcpy(output_dti->containees[1].field_name, "right");
		output_dti->containees[1].al.type_info = shallow_clone_into_nullable_type(right_dti);
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for sort_merge_join_operator\n");
		exit(-1);
	}

	initialize_tuple_data_type_info(output_dti, "join", 0, max_output_tuple_size, 2);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.key_element_count = key_element_count,
		.key_compare_direction = key_compare_direction,

		.left_input_iterator = create_consumption_iterator(left_input_operator, o, NULL, NULL),
		.left_input_tuple_def = left_input_tuple_def,
		.left_key_element_ids = left_key_element_ids,
		.left_side_equal_tuples_batch = get_new_interim_tuple_store(min_block_size),

		.right_input_iterator = create_consumption_iterator(right_input_operator, o, NULL, NULL),
		.right_input_tuple_def = right_input_tuple_def,
		.right_key_element_ids = right_key_element_ids,
		.right_side_equal_tuples_batch = get_new_interim_tuple_store(min_block_size),

		.output_tuple_def = output_tuple_def,

		.ptype = ptype,
		.min_block_size = min_block_size,
	};

	return result;
}