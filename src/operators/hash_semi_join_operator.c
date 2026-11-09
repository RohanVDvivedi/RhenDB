#include<rhendb/query_plan.h>

#include<rhendb/rash_table.h>

#include<rhendb/join_type.h>

#define INIT_BUCKET_COUNT 64
#define MAX_LOAD_FACTOR 1.3

typedef struct op_state op_state;
struct op_state
{
	operator* left_input_operator;

	operator* right_input_operator; // if non-NULL, we are in build phase, else we are in probe phase

	// right side build table
	rash_table_handle rth;
};

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	if(this_state->right_input_operator != NULL)
	{
		while(1)
		{
			// get right side tuple for building the hash table
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

			// insert into the rth, no need to insert the value, just insert the key of the right side tuple
			{
				// create rash table key
				rash_table_key rtk = get_new_rash_table_key(right_tuple, this_state->right_input_operator->desc->output_tuple_def, o->desc->params.hash_semi_join.right_key_element_ids, o->desc->params.hash_semi_join.key_element_count, o->self_query_plan->desc->curr_tx);

				// open rash table iterator for insertion/appending
				rash_table_iterator rti = find_equals_in_rash_table(&(this_state->rth), &rtk, 0);

				// we only insert entry, if it does not exist and no need to store the entire right_tuple like the regular join
				if(!(rti.pointing_to_rkey)) // result of exists check done above
				{
					// open write iterator on this key
					binary_write_iterator* bwi_p = open_for_writing_value_in_rash_table_iterator(&rti);

					// close the bwi_p
					close_and_write_value_in_hash_table_iterator(&rti, bwi_p);
				}

				// delete the insertion iterator
				delete_rash_table_iterator(&rti);

				// if the load factor went too high then expand the bucket_count for the partition
				if(get_load_factor_for_rash_table(&(this_state->rth)) > MAX_LOAD_FACTOR)
					expand_rash_table(&(this_state->rth));

				// destroy rash table key
				destroy_rash_table_key(&rtk);
			}

			free(right_tuple);
		}
	}

	void* output_tuple = NULL;
	while(output_tuple == NULL)
	{
		// get left side tuple for probing
		void* left_tuple = this_state->left_input_operator->next_tuple(this_state->left_input_operator, error_code);
		if(left_tuple == NULL)
		{
			o->destroy_state(o);
			return NULL;
		}

		// probe the right side table to see if the key from the left exists in it
		{
			// create rash table key
			rash_table_key rtk = get_new_rash_table_key(left_tuple, this_state->left_input_operator->desc->output_tuple_def, o->desc->params.hash_semi_join.left_key_element_ids, o->desc->params.hash_semi_join.key_element_count, o->self_query_plan->desc->curr_tx);

			// open rash table iterator for insertion/appending
			rash_table_iterator rti = find_equals_in_rash_table(&(this_state->rth), &rtk, 1);

			if(rti.pointing_to_rkey) // result of exists check done above
			{
				if(DOES_PRODUCE_MATCHED_LEFT_TUPLES(o->desc->params.hash_semi_join.stype))
					output_tuple = left_tuple;
			}
			else
			{
				if(DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(o->desc->params.hash_semi_join.stype))
					output_tuple = left_tuple;
			}

			// delete the insertion iterator
			delete_rash_table_iterator(&rti);

			// destroy rash table key
			destroy_rash_table_key(&rtk);
		}

		if(output_tuple != left_tuple)
			free(left_tuple);
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

		destroy_rash_table(&(this_state->rth));

		free(this_state);
	}
	o->state = NULL;
}

void open_hash_semi_join_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->left_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.hash_semi_join.left_input_operator_index);
	this_state->right_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.hash_semi_join.right_input_operator_index);
	this_state->rth = get_new_rash_table(INIT_BUCKET_COUNT, this_state->right_input_operator->desc->output_tuple_def, o->desc->params.hash_semi_join.right_key_element_ids, o->desc->params.hash_semi_join.key_element_count, o->self_query_plan->desc->curr_tx, o->self_query_plan->desc->curr_tx->rdb);

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}