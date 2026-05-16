#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

#include<rhendb/aggregate_functions.h>

#include<rhendb/function_compare.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	// input_tuple_def for the aggregate functions
	const tuple_def* input_tuple_def;

	consumption_iterator* input_iterator;

	// keys for the aggregation
	uint32_t key_element_count;
	const positional_accessor* key_element_ids;

	uint32_t aggregate_functions_count;
	aggregate_function** aggregate_functions;

	// maximum number of input parameters to any of the aggregation functions
	uint32_t max_of_aggregation_function_input_params_count;

	// 2D array of positonal_accessors
	// aggregate_input_element_ids[i][j]; -> gives the position of the j-th input_param of the i-th aggregate functions
	// its size is [aggregate_functions_count] and [i][aggregate_functions[i]->input_type_infos_count]
	const positional_accessor** aggregate_input_element_ids;

	// consists of all the output_type_infos of all aggregate_functions
	const tuple_def* output_tuple_def;

	// rash_table partitions

		uint32_t partitions_count;
		// TODO

	// job params and input parasm list

		uint32_t max_concurrent_jobs_count;
		// TODO
};

// loops until there are jobs comming, and closes the iterator to already processed consumption iterators
static void insert_for_build_phase_job(operator* o, void* param)
{
	input_values* inputs = o->inputs;

	consumption_iterator* cit_p = param;
	const void* tuple = cit_p->embed_ptrs[0];

	// TODO
}

// gets a partitions to iterate over and produce the aggregates over each entry in the partition
static void produce_aggregate_for_partition_on_probe_phase_job(operator* o, void* param)
{
	input_values* inputs = o->inputs;

	uint32_t parttion_id = (uintptr_t)param;

	// TODO
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	// TODO
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	// TODO
}

void setup_hash_aggregation_operator(operator* o, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids, uint32_t partitions_count, uint32_t max_concurrent_jobs_count)
{
	// if key_element_count == 0 => simple aggregation
	// if aggregate_functions_count == 0 => find distinct
	// but if both are 0, it is a bug
	if(key_element_count == 0 && aggregate_functions_count == 0)
	{
		printf("key_element_count && aggregate_functions_count are both zeroes in hash_aggregation_operator\n");
		exit(-1);
	}

	if(partitions_count == 0)
	{
		printf("partitions_count can not be 0 for hash_aggregation_operator\n");
		exit(-1);
	}

	if(max_concurrent_jobs_count == 0)
	{
		printf("max_concurrent_jobs_count can not be 0 for hash_aggregation_operator\n");
		exit(-1);
	}

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = free_resources;

	const tuple_def* input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator);

	uint32_t input_datums_count = 0;
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(key_element_count + aggregate_functions_count));
	uint32_t max_output_tuple_size = 0;

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		data_type_info* key_dti = (data_type_info*) get_type_info_for_element_from_tuple_def(input_tuple_def, key_element_ids[i]);

		if(key_dti == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += key_dti->is_variable_sized ? (8 + key_dti->max_size) : (1 + key_dti->size);

		sprintf(output_dti->containees[i].field_name, "key_%u", i);
		output_dti->containees[i].al.type_info = key_dti;
	}

	for(uint32_t i = 0, j = key_element_count; i < aggregate_functions_count; i++, j++)
	{
		input_datums_count = max(input_datums_count, aggregate_functions[i]->input_type_infos_count);

		if(aggregate_functions[i]->output_type_info->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += aggregate_functions[i]->output_type_info->is_variable_sized ? (8 + aggregate_functions[i]->output_type_info->max_size) : (1 + aggregate_functions[i]->output_type_info->size);

		sprintf(output_dti->containees[j].field_name, "agg_%u", i);
		output_dti->containees[j].al.type_info = (data_type_info*) aggregate_functions[i]->output_type_info;
	}

	initialize_tuple_data_type_info(output_dti, "keyed_aggregates", 0, max_output_tuple_size, key_element_count + aggregate_functions_count);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	// it is an identity operator, produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	input_values* inputs = o->inputs;
	*inputs = (input_values){
		.input_tuple_def = input_tuple_def,
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.key_element_count = key_element_count,
		.key_element_ids = key_element_ids,
		.aggregate_functions_count = aggregate_functions_count,
		.aggregate_functions = malloc(sizeof(aggregate_function*) * aggregate_functions_count),
		.max_of_aggregation_function_input_params_count = input_datums_count,
		.aggregate_input_element_ids = malloc(sizeof(aggregate_function*) * aggregate_functions_count),
		.output_tuple_def = output_tuple_def,
		.partitions_count = partitions_count,
		.max_concurrent_jobs_count = max_concurrent_jobs_count,
	};

	memory_move(inputs->aggregate_functions, aggregate_functions, sizeof(aggregate_function*) * aggregate_functions_count);

	memory_move(inputs->aggregate_input_element_ids, aggregate_input_element_ids, sizeof(aggregate_function*) * aggregate_functions_count);

	// TODO
}