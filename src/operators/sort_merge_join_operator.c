#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

#include<rhendb/function_compare.h>

#include<rhendb/interim_tuple_store.h>

#include<rhendb/join_type.h>

#include<stdlib.h>

typedef struct op_state op_state;
struct op_state
{
	operator* left_input_operator;
	operator* right_input_operator;

	const tuple_def* left_tuple_def;
	const tuple_def* right_tuple_def;

	// the current buffered tuple from each side, owned by us (NULL => fetch the next one)
	void* left_staged_tuple;
	void* right_staged_tuple;

	// embed_regions[1] is the append cursor
	interim_tuple_store* left_side_equal_tuples_batch;
	interim_tuple_store* right_side_equal_tuples_batch;

	uint64_t curr_left_block_offset;
	uint64_t curr_right_block_offset;

	uint64_t curr_left_offset;
	uint64_t curr_right_offset;
};

static void* produce_join_result(operator* o, const void* left_tuple, const void* right_tuple)
{
	op_state* this_state = o->state;

	void* output_tuple = malloc(get_maximum_tuple_size(o->desc->output_tuple_def));
	init_tuple(o->desc->output_tuple_def, output_tuple);

	if(left_tuple)
	{
		const data_type_info* dti_temp;
		datum left_datum;
		get_value_from_element_from_tuple(&left_datum, &dti_temp, this_state->left_tuple_def, SELF, left_tuple);
		set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(0), output_tuple, &left_datum, UINT32_MAX);
	}

	if(right_tuple)
	{
		const data_type_info* dti_temp;
		datum right_datum;
		get_value_from_element_from_tuple(&right_datum, &dti_temp, this_state->right_tuple_def, SELF, right_tuple);
		set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(1), output_tuple, &right_datum, UINT32_MAX);
	}

	output_tuple = realloc(output_tuple, get_tuple_size(o->desc->output_tuple_def, output_tuple));
	return output_tuple;
}

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	while(1)
	{
		if(get_total_bytes_in_interim_tuple_store(this_state->left_side_equal_tuples_batch) == 0) // nothing in the batches
		{
			// if left staged tuple is NULL and can be gotten, get it
			if(this_state->left_input_operator != NULL && this_state->left_staged_tuple == NULL)
			{
				this_state->left_staged_tuple = this_state->left_input_operator->next_tuple(this_state->left_input_operator, error_code);
				if(this_state->left_staged_tuple == NULL)
				{
					if(*error_code)
					{
						o->destroy_state(o);
						return NULL;
					}
					this_state->left_input_operator->destroy_state(this_state->left_input_operator);
					this_state->left_input_operator = NULL;
				}
			}

			// if right staged tuple is NULL and can be gotten, get it
			if(this_state->right_input_operator != NULL && this_state->right_staged_tuple == NULL)
			{
				this_state->right_staged_tuple = this_state->right_input_operator->next_tuple(this_state->right_input_operator, error_code);
				if(this_state->right_staged_tuple == NULL)
				{
					if(*error_code)
					{
						o->destroy_state(o);
						return NULL;
					}
					this_state->right_input_operator->destroy_state(this_state->right_input_operator);
					this_state->right_input_operator = NULL;
				}
			}

			// if both the operators have ended, exit
			if(this_state->left_staged_tuple == NULL && this_state->right_staged_tuple == NULL) // if both are NULL we are done
			{
				o->destroy_state(o);
				return NULL;
			}

			// else atleast 1 of the stages tuple is present
			{
				int consume_left = 0, consume_right = 0;
				if(this_state->right_staged_tuple == NULL) // left is surely non-NULL, consume left
					consume_left = 1;
				else if(this_state->left_staged_tuple == NULL) // right is surely non-NULL. consume right
					consume_right = 1;
				else // if both are present compare them
				{
					int cmp = compare_tuples_rhendb(this_state->left_staged_tuple, this_state->left_tuple_def, o->desc->params.sort_merge_join.left_key_element_ids,
													this_state->right_staged_tuple, this_state->right_tuple_def, o->desc->params.sort_merge_join.right_key_element_ids,
													o->desc->params.sort_merge_join.key_compare_direction, o->desc->params.sort_merge_join.key_element_count, o->self_query_plan->desc->curr_tx);

					// which ever is lesser has to be consumed, if both are equal consume both
					if(cmp < 0)
						consume_left = 1;
					else if(cmp > 0)
						consume_right = 1;
					else
					{
						consume_left = 1;
						consume_right = 1;
					}
				}

				if(consume_left && !consume_right)
				{
					void* out = NULL;
					if(DOES_IT_PRESERVE_LEFT(o->desc->params.sort_merge_join.ptype))
						out = produce_join_result(o, this_state->left_staged_tuple, NULL);
					free(this_state->left_staged_tuple);
					this_state->left_staged_tuple = NULL;
					if(out != NULL)
						return out;
					continue;
				}
				else if(consume_right && !consume_left)
				{
					void* out = NULL;
					if(DOES_IT_PRESERVE_RIGHT(o->desc->params.sort_merge_join.ptype))
						out = produce_join_result(o, NULL, this_state->right_staged_tuple);
					free(this_state->right_staged_tuple);
					this_state->right_staged_tuple = NULL;
					if(out != NULL)
						return out;
					continue;
				}
				else // consume both equal keys case
				{
					// insert staged tuples in the correct interim tuple stores
					append_tuple_to_interim_tuple_store2(this_state->left_side_equal_tuples_batch, &(this_state->left_side_equal_tuples_batch->embed_regions[0]), this_state->left_staged_tuple, &(this_state->left_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);
					append_tuple_to_interim_tuple_store2(this_state->right_side_equal_tuples_batch, &(this_state->right_side_equal_tuples_batch->embed_regions[0]), this_state->right_staged_tuple, &(this_state->right_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);

					// loop left operator while it's tuple compare equal to the left_staged_tuple, or it gets exhausted
					while(1)
					{
						void* left_tuple = this_state->left_input_operator->next_tuple(this_state->left_input_operator, error_code);
						if(left_tuple == NULL)
						{
							if(*error_code)
							{
								o->destroy_state(o);
								return NULL;
							}
							this_state->left_input_operator->destroy_state(this_state->left_input_operator);
							this_state->left_input_operator = NULL;
							free(this_state->left_staged_tuple);
							this_state->left_staged_tuple = left_tuple;
							break;
						}
						if(0 != compare_tuples_rhendb(this_state->left_staged_tuple, this_state->left_tuple_def, o->desc->params.sort_merge_join.left_key_element_ids,
													left_tuple, this_state->left_tuple_def, o->desc->params.sort_merge_join.left_key_element_ids,
													o->desc->params.sort_merge_join.key_compare_direction, o->desc->params.sort_merge_join.key_element_count, o->self_query_plan->desc->curr_tx))
						{
							free(this_state->left_staged_tuple);
							this_state->left_staged_tuple = left_tuple;
							break;
						}
						else
						{
							append_tuple_to_interim_tuple_store2(this_state->left_side_equal_tuples_batch, &(this_state->left_side_equal_tuples_batch->embed_regions[0]), left_tuple, &(this_state->left_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);
							free(left_tuple);
						}
					}

					// loop right operator while it's tuple compare equal to the right_staged_tuple, or it gets exhausted
					while(1)
					{
						void* right_tuple = this_state->right_input_operator->next_tuple(this_state->right_input_operator, error_code);
						if(right_tuple == NULL)
						{
							if(*error_code)
							{
								o->destroy_state(o);
								return NULL;
							}
							this_state->right_input_operator->destroy_state(this_state->right_input_operator);
							this_state->right_input_operator = NULL;
							free(this_state->right_staged_tuple);
							this_state->right_staged_tuple = right_tuple;
							break;
						}
						if(0 != compare_tuples_rhendb(this_state->right_staged_tuple, this_state->right_tuple_def, o->desc->params.sort_merge_join.right_key_element_ids,
													right_tuple, this_state->right_tuple_def, o->desc->params.sort_merge_join.right_key_element_ids,
													o->desc->params.sort_merge_join.key_compare_direction, o->desc->params.sort_merge_join.key_element_count, o->self_query_plan->desc->curr_tx))
						{
							free(this_state->right_staged_tuple);
							this_state->right_staged_tuple = right_tuple;
							break;
						}
						else
						{
							append_tuple_to_interim_tuple_store2(this_state->right_side_equal_tuples_batch, &(this_state->right_side_equal_tuples_batch->embed_regions[0]), right_tuple, &(this_state->right_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);
							free(right_tuple);
						}
					}

					// runs are built now we iterate over them

					this_state->curr_left_offset = 0;
					this_state->curr_left_block_offset = 0;

					mmap_for_reading_tuple(this_state->left_side_equal_tuples_batch, &(this_state->left_side_equal_tuples_batch->embed_regions[0]), this_state->curr_left_offset, &(this_state->left_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);

					this_state->curr_right_offset = 0;
					this_state->curr_right_block_offset = 0;

					mmap_for_reading_tuple(this_state->right_side_equal_tuples_batch, &(this_state->right_side_equal_tuples_batch->embed_regions[0]), this_state->curr_right_offset, &(this_state->right_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);
				}
			}
		}
		else
		{
			// for going next on the outer 2 loops
			uint64_t next_left_block_offset = 0;
			uint64_t next_right_block_offset = 0;

			// iterate looping
			while(this_state->curr_left_block_offset < get_total_bytes_in_interim_tuple_store(this_state->left_side_equal_tuples_batch))
			{
				while(this_state->curr_right_block_offset < get_total_bytes_in_interim_tuple_store(this_state->right_side_equal_tuples_batch))
				{
					while(contains_tuple_at_offset_in_interim_tuple_store(this_state->left_side_equal_tuples_batch, &(this_state->left_side_equal_tuples_batch->embed_regions[0]), this_state->curr_left_offset, &(this_state->left_tuple_def->size_def)))
					{
						const void* left_tuple = this_state->left_side_equal_tuples_batch->embed_regions[0].tuple;

						while(contains_tuple_at_offset_in_interim_tuple_store(this_state->right_side_equal_tuples_batch, &(this_state->right_side_equal_tuples_batch->embed_regions[0]), this_state->curr_right_offset, &(this_state->right_tuple_def->size_def)))
						{
							const void* right_tuple = this_state->right_side_equal_tuples_batch->embed_regions[0].tuple;

							void* output_tuple = produce_join_result(o, left_tuple, right_tuple);

							// next tuple in right region, mmap only if it is contained
							this_state->curr_right_offset = next_tuple_offset_for_interim_tuple_region(&(this_state->right_side_equal_tuples_batch->embed_regions[0]));
							if(contains_tuple_at_offset_in_interim_tuple_store(this_state->right_side_equal_tuples_batch, &(this_state->right_side_equal_tuples_batch->embed_regions[0]), this_state->curr_right_offset, &(this_state->right_tuple_def->size_def)))
								mmap_for_reading_tuple(this_state->right_side_equal_tuples_batch, &(this_state->right_side_equal_tuples_batch->embed_regions[0]), this_state->curr_right_offset, &(this_state->right_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);

							return output_tuple;
						}

						// reset right side, start from beginning of this block
						next_right_block_offset = this_state->curr_right_offset;
						this_state->curr_right_offset = this_state->curr_right_block_offset;
						mmap_for_reading_tuple(this_state->right_side_equal_tuples_batch, &(this_state->right_side_equal_tuples_batch->embed_regions[0]), this_state->curr_right_offset, &(this_state->right_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);
						// above mmap is almost no-op, as it is in the same block

						// next tuple in left region, mmap only if it is contained
						this_state->curr_left_offset = next_tuple_offset_for_interim_tuple_region(&(this_state->left_side_equal_tuples_batch->embed_regions[0]));
						if(contains_tuple_at_offset_in_interim_tuple_store(this_state->left_side_equal_tuples_batch, &(this_state->left_side_equal_tuples_batch->embed_regions[0]), this_state->curr_left_offset, &(this_state->left_tuple_def->size_def)))
							mmap_for_reading_tuple(this_state->left_side_equal_tuples_batch, &(this_state->left_side_equal_tuples_batch->embed_regions[0]), this_state->curr_left_offset, &(this_state->left_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);
					}

					// reset left side
					next_left_block_offset = this_state->curr_left_offset;
					this_state->curr_left_offset = this_state->curr_left_block_offset;
					mmap_for_reading_tuple(this_state->left_side_equal_tuples_batch, &(this_state->left_side_equal_tuples_batch->embed_regions[0]), this_state->curr_left_offset, &(this_state->left_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);
					// above mmap is almost no-op, as it is in the same block

					// next block in the right batch
					this_state->curr_right_block_offset = next_right_block_offset;
					this_state->curr_right_offset = next_right_block_offset;
					if(this_state->curr_right_offset < get_total_bytes_in_interim_tuple_store(this_state->right_side_equal_tuples_batch))
						mmap_for_reading_tuple(this_state->right_side_equal_tuples_batch, &(this_state->right_side_equal_tuples_batch->embed_regions[0]), this_state->curr_right_offset, &(this_state->right_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);
				}

				// reset right blocks to point to the first block, with the first offset
				this_state->curr_right_block_offset = 0;
				this_state->curr_right_offset = 0;
				mmap_for_reading_tuple(this_state->right_side_equal_tuples_batch, &(this_state->right_side_equal_tuples_batch->embed_regions[0]), this_state->curr_right_offset, &(this_state->right_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);
				// above mmap might cost us, extra on only the last iteration of the outer most loop

				// next block in the left batch
				this_state->curr_left_block_offset = next_left_block_offset;
				this_state->curr_left_offset = next_left_block_offset;
				if(this_state->curr_left_offset < get_total_bytes_in_interim_tuple_store(this_state->left_side_equal_tuples_batch))
					mmap_for_reading_tuple(this_state->left_side_equal_tuples_batch, &(this_state->left_side_equal_tuples_batch->embed_regions[0]), this_state->curr_left_offset, &(this_state->left_tuple_def->size_def), o->desc->params.sort_merge_join.min_block_size);
			}

			// both the batches have been completely exhausted, so unmap and reset to new size
			unmap_all_embed_regions_in_interim_tuple_store(this_state->left_side_equal_tuples_batch);
			unmap_all_embed_regions_in_interim_tuple_store(this_state->right_side_equal_tuples_batch);
			reinitialize_interim_tuple_store(this_state->left_side_equal_tuples_batch, o->desc->params.sort_merge_join.min_block_size);
			reinitialize_interim_tuple_store(this_state->right_side_equal_tuples_batch, o->desc->params.sort_merge_join.min_block_size);

			continue;
		}
	}

	o->destroy_state(o);
	return NULL;
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		if(this_state->left_input_operator != NULL)
			this_state->left_input_operator->destroy_state(this_state->left_input_operator);

		if(this_state->right_input_operator != NULL)
			this_state->right_input_operator->destroy_state(this_state->right_input_operator);

		if(this_state->left_staged_tuple != NULL)
			free(this_state->left_staged_tuple);
		if(this_state->right_staged_tuple != NULL)
			free(this_state->right_staged_tuple);

		if(this_state->left_side_equal_tuples_batch != NULL)
			delete_interim_tuple_store(this_state->left_side_equal_tuples_batch);
		if(this_state->right_side_equal_tuples_batch != NULL)
			delete_interim_tuple_store(this_state->right_side_equal_tuples_batch);

		free(this_state);
	}
	o->state = NULL;
}

void open_sort_merge_join_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->left_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.sort_merge_join.left_input_operator_index);
	this_state->right_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.sort_merge_join.right_input_operator_index);

	this_state->left_tuple_def = this_state->left_input_operator->desc->output_tuple_def;
	this_state->right_tuple_def = this_state->right_input_operator->desc->output_tuple_def;

	this_state->left_staged_tuple = NULL;
	this_state->right_staged_tuple = NULL;

	this_state->left_side_equal_tuples_batch = get_new_interim_tuple_store(o->desc->params.sort_merge_join.min_block_size);
	this_state->right_side_equal_tuples_batch = get_new_interim_tuple_store(o->desc->params.sort_merge_join.min_block_size);

	this_state->curr_left_block_offset = 0;
	this_state->curr_right_block_offset = 0;

	this_state->curr_left_offset = 0;
	this_state->curr_right_offset = 0;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}