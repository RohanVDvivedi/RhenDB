#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

#include<rhendb/rash_table.h>

#include<rhendb/aggregate_functions.h>

#include<rhendb/function_compare.h>

#include<stdlib.h>

typedef struct rash_table_partition rash_table_partition;
struct rash_table_partition
{
	// build lock on the partition, to be taken for an insert
	// not required to be taken for porbe phase
	pthread_mutex_t build_lock;

	rash_table_handle rth;
};

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
	uint32_t bucket_count_per_parttion;
	rash_table_partition** partitions;

	pthread_mutex_t partition_to_aggregate_next_lock;
	uint32_t partition_id_to_aggregate_next;

	// job params and input parasm list

	uint32_t max_concurrent_jobs_count;
	uint32_t max_concurrent_jobs_queue_size;

	pthread_mutex_t insert_for_build_queue_lock;
	linkedlist tuple_pointers_to_insert;
	uint32_t tuple_pointers_to_insert_queue_size;
	uint32_t active_build_phase_job_count;
	int no_more_build_phase_data;
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

	for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
		inputs->aggregate_functions[i]->destroy_aggregate_function(inputs->aggregate_functions[i]);

	free(inputs->aggregate_functions);

	free(inputs->aggregate_input_element_ids);

	for(uint32_t i = 0; i < inputs->partitions_count; i++)
	{
		if(inputs->partitions[i] == NULL)
			continue;
		pthread_mutex_destroy(&(inputs->partitions[i]->build_lock));
		destroy_rash_table(&(inputs->partitions[i]->rth));
		free(inputs->partitions[i]);
		inputs->partitions[i] = NULL;
	}
	free(inputs->partitions);

	pthread_mutex_destroy(&(inputs->partition_to_aggregate_next_lock));

	while(!is_empty_linkedlist(&(inputs->tuple_pointers_to_insert)))
	{
		consumption_iterator* cit_p = get_head_of_linkedlist(&(inputs->tuple_pointers_to_insert));
		remove_head_from_linkedlist(&(inputs->tuple_pointers_to_insert));
		destroy_consumption_iterator(cit_p);
	}
	pthread_mutex_destroy(&(inputs->insert_for_build_queue_lock));

	free((data_type_info*)(inputs->output_tuple_def->type_info));
	free((tuple_def*)(inputs->output_tuple_def));

	free(inputs);
}

void setup_hash_aggregation_operator(operator* o, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids, uint32_t partitions_count, uint32_t bucket_count_per_parttion, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size)
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
		.bucket_count_per_parttion = bucket_count_per_parttion,
		.partitions = malloc(sizeof(rash_table_partition*) * partitions_count),
		.partition_to_aggregate_next_lock = PTHREAD_MUTEX_INITIALIZER,
		.partition_id_to_aggregate_next = 0,
		.max_concurrent_jobs_count = max_concurrent_jobs_count,
		.max_concurrent_jobs_queue_size = max_concurrent_jobs_queue_size,
		.insert_for_build_queue_lock = PTHREAD_MUTEX_INITIALIZER,
		.tuple_pointers_to_insert_queue_size = 0,
		.active_build_phase_job_count = 0,
		.no_more_build_phase_data = 0,
	};

	memory_move(inputs->aggregate_functions, aggregate_functions, sizeof(aggregate_function*) * aggregate_functions_count);

	memory_move(inputs->aggregate_input_element_ids, aggregate_input_element_ids, sizeof(aggregate_function*) * aggregate_functions_count);

	for(uint32_t i = 0; i < partitions_count; i++)
	{
		inputs->partitions[i] = malloc(sizeof(rash_table_partition));
		pthread_mutex_init(&(inputs->partitions[i]->build_lock), NULL);
		inputs->partitions[i]->rth = get_new_rash_table(bucket_count_per_parttion, input_tuple_def, key_element_ids, key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine), o->self_query_plan->curr_tx->db);
	}

	initialize_linkedlist(&(inputs->tuple_pointers_to_insert), offsetof(consumption_iterator, embed_node_ll));
}