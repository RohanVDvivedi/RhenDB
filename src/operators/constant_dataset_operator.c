#include<rhendb/query_plan.h>

#include<rhendb/interim_tuple_store.h>

typedef struct op_state op_state;
struct op_state
{
	interim_tuple_region current_region;
	uint64_t next_tuple_offset;

	// return this from next_tuple
	rhendb_datum rtuple;
};

#define MIN_BYTES_TO_MMAP 1024*1024

static const rhendb_datum* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	if(!mmap_for_reading_tuple(o->desc->params.constant_dataset.constant_dataset_p, &(this_state->current_region), this_state->next_tuple_offset, &(o->desc->params.constant_dataset.record_def->size_def), MIN_BYTES_TO_MMAP))
	{
		o->destroy_state(o);
		return NULL;
	}

	this_state->rtuple.type = RAW_DATUM;
	this_state->rtuple.as_raw_datum.value = (datum){.tuple_value = this_state->current_region.tuple};

	// compute the next_tuple_offset, but do not mmap
	this_state->next_tuple_offset = next_tuple_offset_for_interim_tuple_region(&(this_state->current_region));

	return &(this_state->rtuple);
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		if(!is_empty_interim_tuple_region(&(this_state->current_region)))
			unmap_for_interim_tuple_region(&(this_state->current_region));

		free(this_state);
	}
	o->state = NULL;
}

void open_constant_dataset_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->current_region = INIT_INTERIM_TUPLE_REGION;
	this_state->next_tuple_offset = 0;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}