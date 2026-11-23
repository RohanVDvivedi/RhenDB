#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

#include<rhendb/expression_evaluator.h>

#include<rhendb/interim_tuple_store.h>

#include<rhendb/join_type.h>

#include<cutlery/arraylist.h>
#include<cutlery/bitmap.h>
#include<cutlery/cutlery_stds.h>

typedef struct op_state op_state;
struct op_state
{
	operator* left_input_operator; // non-NULL until it dies

	operator* right_input_operator; // if non-NULL, collect all tuples from this operator, until it's tuples are exhausted

	// all right side tuples
	interim_tuple_store* right_side_tuples;
	const tuple_def* right_tuple_def;

	// current batch of left side tuples
	arraylist batched_left_side_tuples;

	// next (right, left) pair to process; both always point at the NEXT tuple to process
	uint64_t curr_right_offset;
	cy_uint curr_left_index;

	// preserve-left : bit set for every left tuple of the batch that matched atleast one right tuple
	char* left_tuple_matched_bitmap;
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
		get_value_from_element_from_tuple(&left_datum, &dti_temp, this_state->left_input_operator->desc->output_tuple_def, SELF, left_tuple);
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

	// COLLECTION OF RIGHT SIDE TUPLES PHASE (inferred by right_input_operator != NULL)
	if(this_state->right_input_operator != NULL)
	{
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
				break;
			}

			append_tuple_to_interim_tuple_store2(this_state->right_side_tuples, &(this_state->right_side_tuples->embed_regions[0]), right_tuple, &(this_state->right_tuple_def->size_def), o->desc->params.block_nested_loop_join.min_block_size);

			free(right_tuple);
		}
	}

	// PROBE PHASE
	while(1)
	{
		// an empty batch means the previous batch's iteration completed : pull the next batch of left tuples
		if(get_element_count_arraylist(&(this_state->batched_left_side_tuples)) == 0)
		{
			uint64_t batch_bytes = 0;
			while(batch_bytes < o->desc->params.block_nested_loop_join.min_block_size)
			{
				void* left_tuple = this_state->left_input_operator->next_tuple(this_state->left_input_operator, error_code);
				if(left_tuple == NULL)
				{
					if(*error_code)
					{
						o->destroy_state(o);
						return NULL;
					}
					break; // left side exhausted
				}

				if(is_full_arraylist(&(this_state->batched_left_side_tuples)) && !expand_arraylist(&(this_state->batched_left_side_tuples)))
					exit(-1);
				push_back_to_arraylist(&(this_state->batched_left_side_tuples), left_tuple);
				batch_bytes += get_tuple_size(this_state->left_input_operator->desc->output_tuple_def, left_tuple);
			}

			// nothing pulled : the left side is exhausted, so we are complete
			if(get_element_count_arraylist(&(this_state->batched_left_side_tuples)) == 0)
			{
				o->destroy_state(o);
				return NULL;
			}

			this_state->curr_right_offset = 0;
			this_state->curr_left_index = 0;
			if(DOES_IT_PRESERVE_LEFT(o->desc->params.block_nested_loop_join.ptype))
			{
				this_state->left_tuple_matched_bitmap = malloc(UINT_ALIGN_UP(get_element_count_arraylist(&(this_state->batched_left_side_tuples)), 8) / 8);
				memory_set(this_state->left_tuple_matched_bitmap, 0, UINT_ALIGN_UP(get_element_count_arraylist(&(this_state->batched_left_side_tuples)), 8) / 8);
			}
			mmap_for_reading_tuple(this_state->right_side_tuples, &(this_state->right_side_tuples->embed_regions[0]), this_state->curr_right_offset, &(this_state->right_tuple_def->size_def), o->desc->params.block_nested_loop_join.min_block_size);
		}

		// DOUBLE LOOP : right tuples on the outside, left tuples on the inside
		while(this_state->curr_right_offset < this_state->right_side_tuples->next_tuple_offset)
		{
			void* right_tuple = this_state->right_side_tuples->embed_regions[0].tuple;

			while(this_state->curr_left_index < get_element_count_arraylist(&(this_state->batched_left_side_tuples)))
			{
				void* left_tuple = (void*) get_from_arraylist(&(this_state->batched_left_side_tuples), this_state->curr_left_index);

				int join_match = 1;
				if(o->desc->params.block_nested_loop_join.join_expr != NULL)
				{
					set_input_tuples_in_context_for_rhendb_v(o->desc->expr_eval_context, 2, left_tuple, right_tuple);

					int expr_error_code = 0;
					join_match = select_using_evaluate_sql_expr_for_rhendb(o->desc->params.block_nested_loop_join.join_expr, o->desc->expr_eval_context, &expr_error_code);
					if(expr_error_code)
					{
						(*error_code) = EXPRESSION_ERRORED_IN_OPERATOR;
						o->destroy_state(o);
						return NULL;
					}
				}

				if(join_match)
				{
					if(this_state->left_tuple_matched_bitmap != NULL)
						set_bit(this_state->left_tuple_matched_bitmap, this_state->curr_left_index);

					this_state->curr_left_index++;
					return produce_join_result(o, left_tuple, right_tuple);
				}

				this_state->curr_left_index++;
			}

			this_state->curr_left_index = 0;
			this_state->curr_right_offset = next_tuple_offset_for_interim_tuple_region(&(this_state->right_side_tuples->embed_regions[0]));
			if(!mmap_for_reading_tuple(this_state->right_side_tuples, &(this_state->right_side_tuples->embed_regions[0]), this_state->curr_right_offset, &(this_state->right_tuple_def->size_def), o->desc->params.block_nested_loop_join.min_block_size))
				break;
		}

		// all right tuples consumed : emit the left tuples that never matched (preserve-left only)
		if(this_state->left_tuple_matched_bitmap != NULL)
		{
			while(this_state->curr_left_index < get_element_count_arraylist(&(this_state->batched_left_side_tuples)))
			{
				if(get_bit(this_state->left_tuple_matched_bitmap, this_state->curr_left_index) == 0) // this tuple never got picked
				{
					void* left_tuple = (void*) get_from_arraylist(&(this_state->batched_left_side_tuples), this_state->curr_left_index);
					this_state->curr_left_index++;
					return produce_join_result(o, left_tuple, NULL);
				}

				this_state->curr_left_index++;
			}

			free(this_state->left_tuple_matched_bitmap);
			this_state->left_tuple_matched_bitmap = NULL;
		}

		for(cy_uint i = 0; i < get_element_count_arraylist(&(this_state->batched_left_side_tuples)); i++)
			free((void*) get_from_arraylist(&(this_state->batched_left_side_tuples), i));
		remove_all_from_arraylist(&(this_state->batched_left_side_tuples));
	}
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

		for(cy_uint i = 0; i < get_element_count_arraylist(&(this_state->batched_left_side_tuples)); i++)
		{
			void* left_tuple = (void*) get_from_arraylist(&(this_state->batched_left_side_tuples), i);
			if(left_tuple != NULL)
				free(left_tuple);
		}
		deinitialize_arraylist(&(this_state->batched_left_side_tuples));

		if(this_state->left_tuple_matched_bitmap != NULL)
			free(this_state->left_tuple_matched_bitmap);

		if(this_state->right_side_tuples != NULL)
			delete_interim_tuple_store(this_state->right_side_tuples);

		free(this_state);
	}
	o->state = NULL;
}

void open_block_nested_loop_join_operator(operator* o)
{
	if(DOES_IT_PRESERVE_RIGHT(o->desc->params.block_nested_loop_join.ptype))
	{
		printf("can not be preserve right for block_nested_loop_join_operator\n");
		exit(-1);
	}

	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->left_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.block_nested_loop_join.left_input_operator_index);
	this_state->right_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.block_nested_loop_join.right_input_operator_index);

	this_state->right_side_tuples = get_new_interim_tuple_store(o->desc->params.block_nested_loop_join.min_block_size);
	this_state->right_tuple_def = this_state->right_input_operator->desc->output_tuple_def;

	initialize_arraylist(&(this_state->batched_left_side_tuples), 16);

	this_state->curr_right_offset = 0;
	this_state->curr_left_index = 0;

	this_state->left_tuple_matched_bitmap = NULL;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}