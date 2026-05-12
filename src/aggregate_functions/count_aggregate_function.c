#include<rhendb/aggregate_functions.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<stdlib.h>

static int process_input(const aggregate_function* af_p, void** state_p, const datum* inputs[])
{
	if((*state_p) == NULL)
		(*state_p) = calloc(sizeof(uint64_t), 1);

	if(!is_datum_NULL(inputs[0]))
		(**((uint64_t**)state_p))++;

	return 1;
}

static void destroy_state(const aggregate_function* af_p, void** state)
{
	free(*state);
	(*state) = NULL;
}

static int produce_output(const aggregate_function* af_p, datum* output, const void* state)
{
	(*output) = (datum){.uint_value = 0};

	if(state != NULL)
		output->uint_value = (*((const uint64_t*)state));

	return 1;
}

static void destroy_output(const aggregate_function* af_p, datum* output)
{
	// it is storing just a .uint_value
	(*output) = (*NULL_DATUM);
}

static void destroy_aggregate_function(aggregate_function* af_p)
{
	free(af_p);
}

aggregate_function* get_count_aggregate_function(const data_type_info* input_type_info)
{
	aggregate_function* af_p = malloc(size_of_aggregate_function(1));

	af_p->context_p = NULL;

	af_p->process_input = process_input;

	af_p->destroy_state = destroy_state;

	af_p->produce_output = produce_output;

	af_p->destroy_output = destroy_output;

	af_p->destroy_aggregate_function = destroy_aggregate_function;

	af_p->output_type_info = UINT_NON_NULLABLE[8];

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}