#include<rhendb/query_plan.h>

#include<rhendb/interim_tuple_store.h>

/*
	source operator like the scans, but it iterates over an interim_tuple_store, tuple by tuple and then produces each tuple from it
*/

typedef struct op_state op_state;
struct op_state
{
	interim_tuple_region current_region;
	uint64_t next_tuple_offset;
};

#define MIN_BYTES_TO_MMAP 1024*1024

static void* next_tuple(operator* o, int* error_code)
{
	op_state* this_state = o->state;

	if(this_state->next_tuple_offset == o->desc->params.constant_dataset.constant_dataset_p->next_tuple_offset)
	{
		if(!is_empty_interim_tuple_region(&(this_state->current_region)))
			unmap_for_interim_tuple_region(&(this_state->current_region));
		return NULL;
	}

	mmap_for_reading_tuple(o->desc->params.constant_dataset.constant_dataset_p, &(this_state->current_region), this_state->next_tuple_offset, &(o->desc->params.constant_dataset.record_def->size_def), MIN_BYTES_TO_MMAP);

	uint32_t tuple_size = get_tuple_size((const tuple_def*)(&(o->desc->params.constant_dataset.record_def)), this_state->current_region.tuple);
	void* tuple = malloc(tuple_size);
	memcpy(tuple, this_state->current_region.tuple, tuple_size);

	this_state->next_tuple_offset += tuple_size;

	return tuple;
}

static void destroy_state(operator* o)
{
	op_state* this_state = o->state;

	unmap_for_interim_tuple_region(&(this_state->current_region));

	free(this_state);
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