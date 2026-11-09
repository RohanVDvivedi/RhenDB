#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

#include<rhendb/expression_evaluator.h>

#include<rhendb/interim_tuple_store.h>

#include<rhendb/join_type.h>

#include<cutlery/arraylist.h>

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
};

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

			append_tuple_to_interim_tuple_store2(this_state->right_side_tuples, &(this_state->right_side_tuples->embed_regions[0]), right_tuple, &(this_state->right_tuple_def->size_def), o->desc->params.block_nested_loop_semi_join.min_block_size);

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
			while(batch_bytes < o->desc->params.block_nested_loop_semi_join.min_block_size)
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
			mmap_for_reading_tuple(this_state->right_side_tuples, &(this_state->right_side_tuples->embed_regions[0]), this_state->curr_right_offset, &(this_state->right_tuple_def->size_def), o->desc->params.block_nested_loop_semi_join.min_block_size);
		}

		// DOUBLE LOOP : right tuples on the outside, left tuples (not yet settled) on the inside
		while(this_state->curr_right_offset < this_state->right_side_tuples->next_tuple_offset)
		{
			void* right_tuple = this_state->right_side_tuples->embed_regions[0].tuple;

			while(this_state->curr_left_index < get_element_count_arraylist(&(this_state->batched_left_side_tuples)))
			{
				void* left_tuple = (void*) get_from_arraylist(&(this_state->batched_left_side_tuples), this_state->curr_left_index);
				if(left_tuple == NULL) // already settled on an earlier match
				{
					this_state->curr_left_index++;
					continue;
				}

				int join_match = 1;
				if(o->desc->params.block_nested_loop_semi_join.join_expr != NULL)
				{
					set_input_tuples_in_context_for_rhendb_v(o->desc->expr_eval_context, 2, left_tuple, right_tuple);

					int expr_error_code = 0;
					join_match = select_using_evaluate_sql_expr_for_rhendb(o->desc->params.block_nested_loop_semi_join.join_expr, o->desc->expr_eval_context, &expr_error_code);
					if(expr_error_code)
					{
						(*error_code) = EXPRESSION_ERRORED_IN_OPERATOR;
						o->destroy_state(o);
						return NULL;
					}
				}

				if(join_match)
				{
					// this left tuple is settled, take it out of its slot so it is not revisited
					set_in_arraylist(&(this_state->batched_left_side_tuples), NULL, this_state->curr_left_index);

					if(DOES_PRODUCE_MATCHED_LEFT_TUPLES(o->desc->params.block_nested_loop_semi_join.stype)) // LEFT_SEMI_JOIN : steal and emit the matched left tuple now
					{
						this_state->curr_left_index++;
						return left_tuple;
					}
					else                                        // LEFT_ANTI_JOIN : a matched left tuple is never produced
					{
						free(left_tuple);
						this_state->curr_left_index++;
					}
				}
			}

			this_state->curr_left_index = 0;
			this_state->curr_right_offset = next_tuple_offset_for_interim_tuple_region(&(this_state->right_side_tuples->embed_regions[0]));
			if(!mmap_for_reading_tuple(this_state->right_side_tuples, &(this_state->right_side_tuples->embed_regions[0]), this_state->curr_right_offset, &(this_state->right_tuple_def->size_def), o->desc->params.block_nested_loop_semi_join.min_block_size))
				break;
		}

		// all right tuples consumed : the remaining (non-NULL) left tuples never matched any right tuple
		while(this_state->curr_left_index < get_element_count_arraylist(&(this_state->batched_left_side_tuples)))
		{
			void* left_tuple = (void*) get_from_arraylist(&(this_state->batched_left_side_tuples), this_state->curr_left_index);
			if(left_tuple == NULL)
			{
				this_state->curr_left_index++;
				continue;
			}

			set_in_arraylist(&(this_state->batched_left_side_tuples), NULL, this_state->curr_left_index);

			if(DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(o->desc->params.block_nested_loop_semi_join.stype)) // LEFT_ANTI_JOIN : steal and emit the never matched left tuple
			{
				this_state->curr_left_index++;
				return left_tuple;
			}
			else                                           // LEFT_SEMI_JOIN : a never matched left tuple is not produced
			{
				free(left_tuple);
				this_state->curr_left_index++;
			}
		}
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

		if(this_state->right_side_tuples != NULL)
			delete_interim_tuple_store(this_state->right_side_tuples);

		free(this_state);
	}
	o->state = NULL;
}

void open_block_nested_loop_semi_join_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->left_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.block_nested_loop_semi_join.left_input_operator_index);
	this_state->right_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.block_nested_loop_semi_join.right_input_operator_index);

	this_state->right_side_tuples = get_new_interim_tuple_store(o->desc->params.block_nested_loop_semi_join.min_block_size);
	this_state->right_tuple_def = this_state->right_input_operator->desc->output_tuple_def;

	initialize_arraylist(&(this_state->batched_left_side_tuples), 16);

	this_state->curr_right_offset = 0;
	this_state->curr_left_index = 0;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}