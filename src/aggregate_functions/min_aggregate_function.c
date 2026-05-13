#include<rhendb/aggregate_functions.h>

#include<rhendb/function_compare.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<stdlib.h>

typedef struct min_state min_state;
struct min_state
{
	datum min_value;
	void* memory;
	uint32_t capacity;
};

static min_state create_min_state()
{
	return (min_state) {
		.min_value = (*NULL_DATUM),
		.memory = NULL,
		.capacity = 0,
	};
}

static void replace_min_state(min_state* ms, const datum* val, const data_type_info* dti)
{
	if(is_datum_NULL(val))
	{
		ms->min_value = (*NULL_DATUM);
		return;
	}

	switch(dti->type)
	{
		default :
		{
			ms->min_value = (*val);
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

			ms->min_value = (datum){.string_or_binary_value = ms->memory, .string_or_binary_size = val->string_or_binary_size};

			break;
		}
		case ARRAY :
		case TUPLE :
		{
			uint32_t tuple_size = get_size_for_type_info(dti, val->tuple_value);

			if(ms->capacity < tuple_size)
			{
				ms->capacity = tuple_size;
				ms->memory = realloc(ms->memory, tuple_size);
			}

			memory_move(ms->memory, val->tuple_value, tuple_size);

			ms->min_value = (datum){.tuple_value = ms->memory};
			break;
		}
	}
}

static void destroy_min_state(min_state* ms)
{
	free(ms->memory);
	ms->min_value = (*NULL_DATUM);
	ms->memory = NULL;
	ms->capacity = 0;
}

static int process_input(const aggregate_function* af_p, void** state_p, const datum inputs[])
{
	if((*state_p) == NULL)
	{
		(*state_p) = malloc(sizeof(min_state));
		**((min_state**)state_p) = create_min_state();
	}

	// process only if input is not NULL_DATUM
	if(!is_datum_NULL(&(inputs[0])))
	{
		int abort_error = 0;
		int must_replace = is_datum_NULL(&((*((min_state**)state_p))->min_value)) || (compare_datum2_rhendb(&((*((min_state**)state_p))->min_value), &(inputs[0]), af_p->input_type_infos[0], (rage_engine*)(af_p->context_p), NULL, &abort_error) > 0);

		if(abort_error)
			return 0;

		if(must_replace)
			replace_min_state(*((min_state**)state_p), &(inputs[0]), af_p->input_type_infos[0]);
	}

	return 1;
}

static int produce_output(const aggregate_function* af_p, datum* output, void** state_p)
{
	(*output) = (*NULL_DATUM);

	if((*state_p) != NULL)
		(*output) = (*((min_state**)state_p))->min_value;

	return 1;
}

static void destroy_output(const aggregate_function* af_p, datum* output)
{
	// it is storing just simple datum or a datum that has pointer set to state
	(*output) = (*NULL_DATUM);
}

static void destroy_state(const aggregate_function* af_p, void** state_p)
{
	// NOP if the state_p is already NULL
	if((*state_p) == NULL)
		return;

	destroy_min_state(*((min_state**)state_p));

	free(*state_p);
	(*state_p) = NULL;
}

static void destroy_aggregate_function(aggregate_function* af_p)
{
	free(af_p);
}

aggregate_function* get_min_aggregate_function(rhendb* rdb, const data_type_info* input_type_info)
{
	aggregate_function* af_p = malloc(size_of_aggregate_function(1));

	// context stores rdb here, for this aggregate function
	af_p->context_p = &(rdb->persistent_acid_rage_engine);

	af_p->process_input = process_input;

	af_p->produce_output = produce_output;

	af_p->destroy_output = destroy_output;

	af_p->destroy_state = destroy_state;

	af_p->destroy_aggregate_function = destroy_aggregate_function;

	af_p->output_type_info = input_type_info;

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}