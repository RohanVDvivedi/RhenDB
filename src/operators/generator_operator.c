#include<rhendb/query_plan.h>

typedef struct op_state op_state;
struct op_state
{
	void* curr_tuple;

	// return this from next_tuple
	rhendb_datum rtuple;
};

static const rhendb_datum* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	if(this_state->curr_tuple != NULL)
		free(this_state->curr_tuple);

	this_state->curr_tuple = o->desc->params.generator.generator(o->desc->params.generator.generator_context, o->desc->params.generator.generator_tuple_def);
	if(this_state->curr_tuple == NULL)
	{
		o->destroy_state(o);
		return NULL;
	}

	return &(this_state->rtuple);
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		if(this_state->curr_tuple)
			free(this_state->curr_tuple);

		free(this_state);
	}
	o->state = NULL;
}

void open_generator_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->curr_tuple = malloc(get_maximum_tuple_size(o->desc->params.stream_input.input_tuple_def));

	this_state->rtuple.type = RAW_DATUM;
	this_state->rtuple.as_raw_datum.value.tuple_value = this_state->curr_tuple;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}