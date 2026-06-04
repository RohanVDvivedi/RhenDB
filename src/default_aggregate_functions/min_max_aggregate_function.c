#include<rhendb/rhendb_functions.h>

#include<rhendb/function_compare.h>

#include<tuplelargetypes/common_extended.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<stdlib.h>

typedef struct min_max_state min_max_state;
struct min_max_state
{
	datum min_max_value;
	void* memory;
	uint32_t capacity;
};

static min_max_state create_min_max_state()
{
	return (min_max_state) {
		.min_max_value = (*NULL_DATUM),
		.memory = NULL,
		.capacity = 0,
	};
}

static void replace_min_max_state(min_max_state* ms, const datum* val, const data_type_info* dti)
{
	if(is_datum_NULL(val))
	{
		ms->min_max_value = (*NULL_DATUM);
		return;
	}

	switch(dti->type)
	{
		default :
		{
			ms->min_max_value = (*val);
			break;
		}
		case STRING :
		case BINARY :
		{
			if(ms->capacity < val->string_or_binary_size)
			{
				ms->capacity = val->string_or_binary_size;
				ms->memory = realloc(ms->memory, val->string_or_binary_size);
			}

			memory_move(ms->memory, val->string_or_binary_value, val->string_or_binary_size);

			ms->min_max_value = (datum){.string_or_binary_value = ms->memory, .string_or_binary_size = val->string_or_binary_size};
			break;
		}
		case ARRAY :
		{
			uint32_t array_memory_size = get_size_for_type_info(dti, val->array_value);

			if(ms->capacity < array_memory_size)
			{
				ms->capacity = array_memory_size;
				ms->memory = realloc(ms->memory, array_memory_size);
			}

			memory_move(ms->memory, val->array_value, array_memory_size);

			ms->min_max_value = (datum){.array_value = ms->memory};
			break;
		}
		case TUPLE :
		{
			uint32_t tuple_size = get_size_for_type_info(dti, val->tuple_value);

			if(ms->capacity < tuple_size)
			{
				ms->capacity = tuple_size;
				ms->memory = realloc(ms->memory, tuple_size);
			}

			memory_move(ms->memory, val->tuple_value, tuple_size);

			ms->min_max_value = (datum){.tuple_value = ms->memory};
			break;
		}
	}
}

static void destroy_min_max_state(min_max_state* ms)
{
	if(ms->memory)
		free(ms->memory);
	ms->min_max_value = (*NULL_DATUM);
	ms->memory = NULL;
	ms->capacity = 0;
}

typedef struct min_max_context min_max_context;
struct min_max_context
{
	rage_engine* persistent_acid_rage_engine;
	int is_min;
};

static int process_input(const rhendb_function* af_p, void** state_p, const datum inputs[])
{
	if((*state_p) == NULL)
	{
		(*state_p) = malloc(sizeof(min_max_state));
		**((min_max_state**)state_p) = create_min_max_state();
	}

	// process only if input is not NULL_DATUM
	if(!is_datum_NULL(&(inputs[0])))
	{
		int must_replace = 0;

		if(is_datum_NULL(&((*((min_max_state**)state_p))->min_max_value)))
			must_replace = 1;
		else
		{
			int compare = compare_datum_rhendb(&((*((min_max_state**)state_p))->min_max_value), af_p->output_type_info, &(inputs[0]), af_p->input_type_infos[0], ((min_max_context*)(af_p->context_p))->persistent_acid_rage_engine);

			if(((min_max_context*)(af_p->context_p))->is_min)
				must_replace = (compare > 0);
			else
				must_replace = (compare < 0);
		}

		if(must_replace)
			replace_min_max_state(*((min_max_state**)state_p), &(inputs[0]), af_p->output_type_info);
	}

	return 1;
}

static int produce_output(const rhendb_function* af_p, datum* output, void** state_p)
{
	(*output) = (*NULL_DATUM);

	if((*state_p) != NULL)
		(*output) = (*((min_max_state**)state_p))->min_max_value;

	return 1;
}

static void destroy_state(const rhendb_function* af_p, void** state_p)
{
	// NOP if the state_p is already NULL
	if((*state_p) == NULL)
		return;

	destroy_min_max_state(*((min_max_state**)state_p));

	free(*state_p);
	(*state_p) = NULL;
}

static void destroy_rhendb_function(rhendb_function* af_p)
{
	// if a new output_tuple_info was allocated to make it nullable then free it
	if(af_p->output_type_info != af_p->input_type_infos[0])
		free((void*)af_p->output_type_info);

	free((void*)af_p->context_p);
	free(af_p);
}

rhendb_function* get_min_max_aggregate_function(rhendb* rdb, const data_type_info* input_type_info, int is_min)
{
	rhendb_function* af_p = malloc(size_of_rhendb_function(1));

	// context stores rdb here, for this aggregate function
	af_p->context_p = malloc(sizeof(min_max_context));
	((min_max_context*)(af_p->context_p))->persistent_acid_rage_engine = &(rdb->persistent_acid_rage_engine);
	((min_max_context*)(af_p->context_p))->is_min = is_min;

	af_p->is_aggregate_function = 1;

	af_p->process_input = process_input;

	af_p->produce_output = produce_output;

	af_p->destroy_state = destroy_state;

	af_p->destroy_rhendb_function = destroy_rhendb_function;

	// if no data is present, the aggregate function returns NULL_DATUM, so here try to make the input_type_info's shallow copy to handle null values
	if(is_nullable_type_info(input_type_info))
		af_p->output_type_info = input_type_info;
	else
	{
		// figure out the number of bytes to shallow copy input_type_info
		size_t bytes_to_shallow_copy = get_shallow_copy_struct_size_for_data_type_info(input_type_info);

		// make shallow copy, mark it nullable, and finalize this type
		data_type_info* output_type_info = malloc(bytes_to_shallow_copy);
		memory_move(output_type_info, input_type_info, bytes_to_shallow_copy);
		output_type_info->is_nullable = 1;
		finalize_type_info(output_type_info);

		// assign it to the af_p
		af_p->output_type_info = output_type_info;
	}

	af_p->buffers_resource_count = has_extended_type_info(input_type_info);

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}