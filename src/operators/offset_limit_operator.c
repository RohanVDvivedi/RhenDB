#include<rhendb/query_plan.h>

#include<rhendb/tuples_down_counter.h>

typedef struct op_state op_state;
struct op_state
{
	operator* input_operator;

	tuples_down_counter offset_counter;

	tuples_down_counter limit_counter;
};

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	while(can_decrement_tuples_down_counter(&(this_state->offset_counter)))
	{
		void* tuple = this_state->input_operator->next_tuple(this_state->input_operator, error_code);
		if(tuple == NULL)
		{
			o->destroy_state(o);
			return NULL;
		}
		free(tuple);
		decrement_tuples_down_counter(&(this_state->offset_counter));
	}

	if(!can_decrement_tuples_down_counter(&(this_state->limit_counter)))
	{
		o->destroy_state(o);
		return NULL;
	}

	void* tuple = this_state->input_operator->next_tuple(this_state->input_operator, error_code);
	if(tuple == NULL)
	{
		o->destroy_state(o);
		return NULL;
	}
	decrement_tuples_down_counter(&(this_state->limit_counter));
	return tuple;
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		this_state->input_operator->destroy_state(this_state->input_operator);
	}
	o->state = NULL;
}

void open_offset_limit_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.offset_limit.input_operator_index);
	this_state->offset_counter = o->desc->params.offset_limit.offset_counter;
	this_state->limit_counter = o->desc->params.offset_limit.limit_counter;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}