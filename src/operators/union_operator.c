#include<rhendb/query_plan.h>

#include<cutlery/linkedlist.h>

#include<stdlib.h>

#define IS_READY 0
#define IS_WAITING 1
#define IS_DELETED 2

typedef struct op_state op_state;
struct op_state
{
	uint64_t curr_input_operator_index;
};

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	while(1)
	{
		if(this_state->curr_input_operator_index >= o->desc->params.union_op.input_operators_count) // no more operators
		{
			o->destroy_state(o);
			return NULL;
		}

		operator* input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.union_op.input_operator_indices[this_state->curr_input_operator_index]);

		void* tuple = input_operator->next_tuple(input_operator, error_code);
		if(tuple == NULL)
		{
			if(*error_code)
			{
				o->destroy_state(o);
				return NULL;
			}
			else
			{
				this_state->curr_input_operator_index++;
				continue;
			}
		}
		return tuple;
	}

	return NULL;
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		while(this_state->curr_input_operator_index < o->desc->params.union_op.input_operators_count)
		{
			operator* t = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.union_op.input_operator_indices[this_state->curr_input_operator_index++]);
			t->destroy_state(t);
		}

		free(this_state);
	}
	o->state = NULL;
}

void open_union_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->curr_input_operator_index = 0;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}