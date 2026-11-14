#include<rhendb/query_plan.h>

#include<rhendb/function_compare.h>

#include<rhendb/join_type.h>

typedef struct op_state op_state;
struct op_state
{
	operator* left_input_operator;
	operator* right_input_operator;

	const tuple_def* left_tuple_def;
	const tuple_def* right_tuple_def;

	void* right_tuple;
	void* left_tuple;

	datum* left_tuple_key_datums;
	datum* right_tuple_key_datums;

	// derieved information
	const data_type_info** left_key_dtis;
	const data_type_info** right_key_dtis;
};

static void revise_key_datums_from_tuple(datum* key_datums, const void* tuple, const tuple_def* tpl_d, const positional_accessor* key_element_ids, uint32_t key_element_count)
{
	for(uint32_t i = 0; i < key_element_count; i++)
	{
		const data_type_info* dti_temp;
		if(!get_value_from_element_from_tuple(&(key_datums[i]), &dti_temp, tpl_d, key_element_ids[i], tuple))
			key_datums[i] = (*NULL_DATUM);
	}
}

static void revise_key_datums_for_left_input_tuple(operator* o, datum* key_datums, const void* tuple)
{
	op_state* this_state = o->state;
	revise_key_datums_from_tuple(key_datums, tuple, this_state->left_tuple_def, o->desc->params.sort_merge_semi_join.left_key_element_ids, o->desc->params.sort_merge_semi_join.key_element_count);
}

static void revise_key_datums_for_right_input_tuple(operator* o, datum* key_datums, const void* tuple)
{
	op_state* this_state = o->state;
	revise_key_datums_from_tuple(key_datums, tuple, this_state->right_tuple_def, o->desc->params.sort_merge_semi_join.right_key_element_ids, o->desc->params.sort_merge_semi_join.key_element_count);
}

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	void* output_tuple = NULL;

	while(output_tuple == NULL)
	{
		// fetch left side tuple if possible
		if(this_state->left_input_operator != NULL)
		{
			if(this_state->left_tuple == NULL)
			{
				this_state->left_tuple = this_state->left_input_operator->next_tuple(this_state->left_input_operator, error_code);
				if(this_state->left_tuple == NULL)
				{
					if(*error_code)
					{
						o->destroy_state(o);
						return NULL;
					}
					this_state->left_input_operator->destroy_state(this_state->left_input_operator);
					this_state->left_input_operator = NULL;
				}
				else
				{
					if(this_state->right_input_operator != NULL) // recompute datums only if we will need it
						revise_key_datums_for_left_input_tuple(o, this_state->left_tuple_key_datums, this_state->left_tuple);
				}
			}
		}

		// fetch right side tuple if possible
		if(this_state->right_input_operator != NULL)
		{
			if(this_state->right_tuple == NULL)
			{
				this_state->right_tuple = this_state->right_input_operator->next_tuple(this_state->right_input_operator, error_code);
				if(this_state->right_tuple == NULL)
				{
					if(*error_code)
					{
						o->destroy_state(o);
						return NULL;
					}
					this_state->right_input_operator->destroy_state(this_state->right_input_operator);
					this_state->right_input_operator = NULL;
				}
				{
					if(this_state->left_input_operator != NULL) // recompute datums only if we will need it
						revise_key_datums_for_right_input_tuple(o, this_state->right_tuple_key_datums, this_state->right_tuple);
				}
			}
		}

		// if there are no more tuples on the left side, then return success
		// as we are only producing left side tuples, we need left tuples to continue operating
		if(this_state->left_input_operator == NULL)
		{
			o->destroy_state(o);
			return NULL;
		}

		// if left side tuples are left, but there are none remaining to be read on the right side
		// this means all the left side tuples will remian un-matched
		// this means if we are not meant to produce any unmatched tuples then quit
		if(this_state->right_input_operator == NULL && (!(DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(o->desc->params.sort_merge_semi_join.stype))))
		{
			o->destroy_state(o);
			return NULL;
		}

		{
			int produce_left = 0;
			int consume_left = 0;
			int consume_right = 0;

			if(this_state->right_input_operator == NULL)
			{
				produce_left = DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(o->desc->params.sort_merge_semi_join.stype);
				consume_left = 1;
			}
			else
			{
				int cmp = compare_datums4_rhendb(this_state->left_tuple_key_datums, this_state->left_key_dtis,
												this_state->right_tuple_key_datums, this_state->right_key_dtis,
												o->desc->params.sort_merge_semi_join.key_compare_direction, o->desc->params.sort_merge_semi_join.key_element_count, o->self_query_plan->desc->curr_tx);

				if(cmp == -1)
				{
					produce_left = DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(o->desc->params.sort_merge_semi_join.stype);
					consume_left = 1;
				}
				else if(cmp == 1)
				{
					consume_right = 1;
				}
				else
				{
					produce_left = DOES_PRODUCE_MATCHED_LEFT_TUPLES(o->desc->params.sort_merge_semi_join.stype);
					consume_left = 1;
				}
			}

			if(produce_left) // steal left tuple
			{
				output_tuple = this_state->left_tuple;
				this_state->left_tuple = NULL;
			}

			if(consume_left) // if one exists, free it
			{
				if(this_state->left_tuple != NULL)
					free(this_state->left_tuple);
				this_state->left_tuple = NULL;
			}

			if(consume_right) // if one exists, free it
			{
				if(this_state->right_tuple != NULL)
					free(this_state->right_tuple);
				this_state->right_tuple = NULL;
			}
		}
	}

	return output_tuple;
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

		if(this_state->left_tuple != NULL)
			free(this_state->left_tuple);

		if(this_state->right_tuple != NULL)
			free(this_state->right_tuple);

		if(this_state->left_tuple_key_datums != NULL)
			free(this_state->left_tuple_key_datums);

		if(this_state->right_tuple_key_datums != NULL)
			free(this_state->right_tuple_key_datums);

		free(this_state->left_key_dtis);

		free(this_state->right_key_dtis);

		free(this_state);
	}
	o->state = NULL;
}

void open_sort_merge_semi_join_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->left_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.sort_merge_semi_join.left_input_operator_index);
	this_state->right_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.sort_merge_semi_join.right_input_operator_index);

	this_state->left_tuple_def = this_state->left_input_operator->desc->output_tuple_def;
	this_state->right_tuple_def = this_state->right_input_operator->desc->output_tuple_def;

	this_state->left_tuple = NULL;
	this_state->right_tuple = NULL;

	this_state->left_tuple_key_datums = malloc(sizeof(datum) * o->desc->params.sort_merge_semi_join.key_element_count);
	this_state->right_tuple_key_datums = malloc(sizeof(datum) * o->desc->params.sort_merge_semi_join.key_element_count);

	this_state->left_key_dtis = malloc(sizeof(data_type_info*) * o->desc->params.sort_merge_semi_join.key_element_count);
	for(uint32_t j = 0; j < o->desc->params.sort_merge_semi_join.key_element_count; j++)
		this_state->left_key_dtis[j] = get_type_info_for_element_from_tuple_def(this_state->left_input_operator->desc->output_tuple_def, o->desc->params.sort_merge_semi_join.left_key_element_ids[j]);

	this_state->right_key_dtis = malloc(sizeof(data_type_info*) * o->desc->params.sort_merge_semi_join.key_element_count);
	for(uint32_t j = 0; j < o->desc->params.sort_merge_semi_join.key_element_count; j++)
		this_state->right_key_dtis[j] = get_type_info_for_element_from_tuple_def(this_state->right_input_operator->desc->output_tuple_def, o->desc->params.sort_merge_semi_join.right_key_element_ids[j]);

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}