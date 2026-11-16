#include<rhendb/query_plan.h>

#include<rhendb/cash_table.h>
#include<rhendb/grash_table.h>

#include<rhendb/join_type.h>

typedef struct op_state op_state;
struct op_state
{
	operator* right_input_operator;
	operator* left_input_operator;

	const tuple_def* left_tuple_def;
	const tuple_def* right_tuple_def;

	// in_memory state

	cash_table* right_cash;
	cash_table_entry* right_it;
	void* left_tuple;

	// spilling state

	grash_table* right_grash;
	grash_table* left_grash;

	cash_table* right_grash_index;

	grash_table_node* right_curr_node;
	grash_table_node* left_curr_node;

	// probe key

	unsigned int is_valid_left_probe_key:1;
	cash_table_entry left_probe_key;
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

	// build phase
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

			// build
			{
				if(this_state->right_cash != NULL)
				{
					cash_table_entry* e = get_new_cash_table_value_own(this_state->right_cash, right_tuple, 0);
					insert_in_cash_table(this_state->right_cash, e);

					if(o->desc->params.hash_join.min_part_size < this_state->right_cash->memory_utilization)
					{
						this_state->right_grash = get_new_grash_table(this_state->right_tuple_def, o->desc->params.hash_join.right_key_element_ids, o->desc->params.hash_join.key_element_count, o->desc->params.hash_join.min_part_size, o->self_query_plan->desc->curr_tx);

						for(const cash_table_entry* e = find_all_in_cash_table(this_state->right_cash); e != NULL; e = next_all_in_cash_table(this_state->right_cash, e))
							insert_to_grash_table(this_state->right_grash, e->as_value.value, e->hash_value);

						delete_cash_table(this_state->right_cash);
						this_state->right_cash = NULL;
					}
				}
				else // else right grash must be present
				{
					insert_to_grash_table(this_state->right_grash, right_tuple, 0);

					free(right_tuple);
				}
			}
		}
	}

	// in_memory probe phase
	if(this_state->right_cash != NULL)
	{
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

				if(this_state->is_valid_left_probe_key)
					reset_cash_table_key(&(this_state->left_probe_key), this_state->right_cash, this_state->left_tuple, this_state->left_tuple_def, o->desc->params.hash_join.left_key_element_ids, 0);
				else
				{
					this_state->left_probe_key = get_new_cash_table_key(this_state->right_cash, this_state->left_tuple, this_state->left_tuple_def, o->desc->params.hash_join.left_key_element_ids, 0);
					this_state->is_valid_left_probe_key = 1;
				}

				this_state->right_it = find_equals_in_cash_table(this_state->right_cash, &(this_state->left_probe_key));

				if(this_state->right_it == NULL) // nothing matches this tuple
				{
					void* output_tuple = NULL;
					if(DOES_IT_PRESERVE_LEFT(o->desc->params.hash_join.ptype))
						output_tuple = produce_join_result(o, this_state->left_tuple, NULL);

					free(this_state->left_tuple);
					this_state->left_tuple = NULL;

					if(output_tuple)
						return output_tuple;

					continue;
				}
			}
			else
			{
				this_state->right_it = next_equals_in_cash_table(this_state->right_cash, this_state->right_it);
			}

			// only comes here, if we have a valid left_tuple and the right_it matching
			if(this_state->right_it != NULL)
			{
				this_state->right_it->as_value.state_u = 1;

				return produce_join_result(o, this_state->left_tuple, this_state->right_it->as_value.value);
			}
			else // we are done with this particular left side tuple
			{
				free(this_state->left_tuple);
				this_state->left_tuple = NULL;
			}
		}

		if(DOES_IT_PRESERVE_RIGHT(o->desc->params.hash_join.ptype))
		{
			if(this_state->right_it == NULL)
			{
				this_state->right_it = find_all_in_cash_table(this_state->right_cash);
			}
			else
			{
				this_state->right_it = next_equals_in_cash_table(this_state->right_cash, this_state->right_it);
			}

			while(this_state->right_it != NULL)
			{
				if(this_state->right_it->as_value.state_u == 0) // this right tuple was never picked
					return produce_join_result(o, NULL, this_state->right_it->as_value.value);
				else
					this_state->right_it = next_equals_in_cash_table(this_state->right_cash, this_state->right_it);
			}
		}

		// completed
		o->destroy_state(o);
		return NULL;
	}

	// build side spilled to right_grash, spill left side too
	// TODO

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

		if(this_state->right_cash != NULL)
			delete_cash_table(this_state->right_cash);

		if(this_state->left_tuple != NULL)
			free(this_state->left_tuple);

		if(this_state->right_grash_index != NULL)
			delete_cash_table(this_state->right_grash_index);

		if(this_state->right_grash != NULL)
			delete_grash_table(this_state->right_grash);

		if(this_state->left_grash != NULL)
			delete_grash_table(this_state->left_grash);

		if(this_state->is_valid_left_probe_key)
			delete_cash_table_entry(&(this_state->left_probe_key));

		free(this_state);
	}
	o->state = NULL;
}

void open_hash_join_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->left_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.hash_join.left_input_operator_index);
	this_state->right_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.hash_join.right_input_operator_index);

	this_state->left_tuple_def = this_state->left_input_operator->desc->output_tuple_def;
	this_state->right_tuple_def = this_state->right_input_operator->desc->output_tuple_def;

	this_state->right_cash = get_new_cash_table(100, this_state->right_tuple_def, o->desc->params.hash_join.right_key_element_ids, o->desc->params.hash_join.key_element_count, o->self_query_plan->desc->curr_tx);
	this_state->right_it = NULL;
	this_state->left_tuple = NULL;

	this_state->right_grash = NULL;
	this_state->left_grash = NULL;

	this_state->right_grash_index = NULL;

	this_state->right_curr_node = NULL;
	this_state->left_curr_node = NULL;

	this_state->is_valid_left_probe_key = 0;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}