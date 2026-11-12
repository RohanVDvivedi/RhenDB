#include<rhendb/query_plan.h>

#include<rhendb/rash_table.h>

#include<rhendb/join_type.h>

#define INIT_BUCKET_COUNT 64
#define MAX_LOAD_FACTOR 1.3

typedef struct op_state op_state;
struct op_state
{
	operator* right_input_operator; // if non-NULL, we are in build phase

	operator* left_input_operator; //  if non-NULL, we are in probe phase part1 (emit matched and left side un-match tuples)

	void* left_tuple; // preserve left side tuple to match with the rest of the right tuples
	rash_table_key rtk; // as alive as left_tuple

	// if both the above operators are NULL, we are in probe phase part2 (emit right side un-match tuples)
	int has_active_probe; // valid only in the probe phase part1 and part2
	rash_table_iterator rti;

	binary_read_iterator* bri_p; // must be alive only if it has an active probe

	// right side build table
	rash_table_handle rth;
	const tuple_def* rth_value_tuple_def; // same as right_input_iterator->desc->output_tuple_def
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
		get_value_from_element_from_tuple(&right_datum, &dti_temp, this_state->rth_value_tuple_def, SELF, right_tuple);
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
				rash_table_key rtk = get_new_rash_table_key(right_tuple, this_state->right_input_operator->desc->output_tuple_def, o->desc->params.hash_join.right_key_element_ids, o->desc->params.hash_join.key_element_count, o->self_query_plan->desc->curr_tx);

				// open rash table iterator for insertion/appending
				rash_table_iterator rti = find_equals_in_rash_table(&(this_state->rth), &rtk, 0);

				// open write iterator on this key
				binary_write_iterator* bwi_p = open_for_writing_value_in_rash_table_iterator(&rti);

				// perform append into this iterator storing in the complete tuple
				int abort_error_dummy = 0;
				append_to_binary_write_iterator(bwi_p, right_tuple, get_tuple_size(this_state->right_input_operator->desc->output_tuple_def, right_tuple), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(this_state->rth.htan)), NULL, &abort_error_dummy);

				// close the bwi_p
				close_and_write_value_in_hash_table_iterator(&rti, bwi_p);

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

	while(this_state->left_input_operator != NULL || this_state->left_tuple != NULL)
	{
		if(this_state->left_tuple == NULL)
		{
			// get left side tuple for probing the hash table
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
				break;
			}

			// create rash table key, from the left tuple
			this_state->rtk = get_new_rash_table_key(this_state->left_tuple, this_state->left_input_operator->desc->output_tuple_def, o->desc->params.hash_join.left_key_element_ids, o->desc->params.hash_join.key_element_count, o->self_query_plan->desc->curr_tx);
		}

		// only comes here, if we have a valid left_tuple

		if(!this_state->has_active_probe)
		{
			// open rash table iterator for insertion/appending
			this_state->rti = find_equals_in_rash_table(&(this_state->rth), &(this_state->rtk), 0);

			if(this_state->rti.pointing_to_rkey)
			{
				this_state->has_active_probe = 1;

				if(DOES_IT_PRESERVE_RIGHT(o->desc->params.hash_join.ptype))
					write_state_in_rash_table_iterator(&(this_state->rti), 1);

				// go ahead with producing with all left_tuple with right_tuple-s
			}
			else
			{
				delete_rash_table_iterator(&(this_state->rti));

				if(DOES_IT_PRESERVE_LEFT(o->desc->params.hash_join.ptype))
				{
					void* output_tuple = produce_join_result(o, this_state->left_tuple, NULL);

					// clear left_tuple so that next call picks a new left_tuple
					destroy_rash_table_key(&(this_state->rtk));
					free(this_state->left_tuple);
					this_state->left_tuple = NULL;

					return output_tuple;
				}

				destroy_rash_table_key(&(this_state->rtk));
				free(this_state->left_tuple);
				this_state->left_tuple = NULL;

				// go ahead with the next left_tuple
				continue;
			}
		}

		// only comes here if we have an active probe i.e. bri_p != NULL

		// iterate over all tuples on the right and produce join results
		{
			if(this_state->bri_p == NULL) // open a new value reader iterator
				this_state->bri_p = read_value_in_rash_table_iterator(&(this_state->rti));

			void* output_tuple = NULL;
			int abort_error_dummy = 0;
			consume_tuple_from_tuple_list(right_tuple, this_state->rth_value_tuple_def, this_state->bri_p, NULL, &abort_error_dummy,
			{
				if(right_tuple != NULL)
					output_tuple = produce_join_result(o, this_state->left_tuple, right_tuple);
			});

			if(output_tuple == NULL)
			{
				delete_binary_read_iterator(this_state->bri_p, NULL, &abort_error_dummy);
				this_state->bri_p = NULL;

				delete_rash_table_iterator(&(this_state->rti));
				this_state->has_active_probe = 0;

				destroy_rash_table_key(&(this_state->rtk));
				free(this_state->left_tuple);
				this_state->left_tuple = NULL;

				// new probe in next iteration, as this probe ended
				continue;
			}

			return output_tuple;
		}
	}

	if(DOES_IT_PRESERVE_RIGHT(o->desc->params.hash_join.ptype))
	{
		if(this_state->has_active_probe == 0)
		{
			this_state->rti = find_all_in_rash_table(&(this_state->rth), 1);
			this_state->has_active_probe = 1;
		}

		while(1)
		{
			// process it only if the entry exists, and it has not yet been used
			if(this_state->bri_p != NULL || (exists_in_rash_table_iterator(&(this_state->rti)) && read_state_in_rash_table_iterator(&(this_state->rti)) == 0))
			{
				if(this_state->bri_p == NULL)
					this_state->bri_p = read_value_in_rash_table_iterator(&(this_state->rti));

				int abort_error_dummy = 0;
				void* output_tuple = NULL;
				consume_tuple_from_tuple_list(right_tuple, this_state->rth_value_tuple_def, this_state->bri_p, NULL, &abort_error_dummy,
				{
					if(right_tuple != NULL)
						output_tuple = produce_join_result(o, NULL, right_tuple);
				});

				if(output_tuple == NULL)
				{
					delete_binary_read_iterator(this_state->bri_p, NULL, &abort_error_dummy);
					this_state->bri_p = NULL;

					// go to next entry and try again
				}
				else
					return output_tuple;
			}

			// if we can not go next break out
			if(!next_in_rash_table_iterator(&(this_state->rti)))
				break;
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

		if(this_state->left_tuple)
		{
			destroy_rash_table_key(&(this_state->rtk));
			free(this_state->left_tuple);
		}

		if(this_state->bri_p != NULL)
		{
			int abort_error_dummy = 0;
			delete_binary_read_iterator(this_state->bri_p, NULL, &abort_error_dummy);
		}

		if(this_state->has_active_probe)
			delete_rash_table_iterator(&(this_state->rti));

		destroy_rash_table(&(this_state->rth));

		free(this_state);
	}
	o->state = NULL;
}

void open_hash_join_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->right_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.hash_join.right_input_operator_index);
	this_state->left_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.hash_join.left_input_operator_index);
	this_state->left_tuple = NULL;
	this_state->has_active_probe = 0;
	this_state->bri_p = NULL;
	this_state->rth = get_new_rash_table(INIT_BUCKET_COUNT, this_state->right_input_operator->desc->output_tuple_def, o->desc->params.hash_join.right_key_element_ids, o->desc->params.hash_join.key_element_count, o->self_query_plan->desc->curr_tx);
	this_state->rth_value_tuple_def = this_state->right_input_operator->desc->output_tuple_def;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}