#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

#include<rhendb/rash_table.h>
#include<rhendb/interim_tuple_store.h>

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
	uint32_t min_build_tuple_buffer_size;

	pthread_mutex_t insert_for_build_queue_lock;
	linkedlist tuple_buffers_to_insert;
	uint32_t tuple_buffers_to_insert_queue_size;
	uint32_t active_build_phase_job_count;
	uint32_t active_probe_phase_job_count;
	int probe_jobs_started;
	int no_more_build_phase_data;

	// fill up this buffer before moving it into the tuple_buffers_to_insert
	interim_tuple_store* pending_build_buffer;
};

// loops until there are jobs comming, and closes the iterator to already processed consumption iterators
static void insert_for_build_phase_job(operator* o, void* param)
{
	input_values* inputs = o->inputs;

	while(1)
	{
		pthread_mutex_lock(&(inputs->insert_for_build_queue_lock));
		interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_linkedlist(&(inputs->tuple_buffers_to_insert));
		if(its_p != NULL)
		{
			remove_head_from_linkedlist(&(inputs->tuple_buffers_to_insert));
			inputs->tuple_buffers_to_insert_queue_size--;
		}
		pthread_mutex_unlock(&(inputs->insert_for_build_queue_lock));

		// if no tuple buffer found exit
		if(its_p == NULL)
			break;

		// insert to the right partition if one exists
		FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(tuple, tuple_index, tuple_offset, &(inputs->input_tuple_def->size_def), its_p, get_total_bytes_in_interim_tuple_store(its_p), {

			// create rash table key
			rash_table_key rtk = get_new_rash_table_key(tuple, inputs->input_tuple_def, inputs->key_element_ids, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine));

			// find the parttion this key goes into
			uint32_t partition_id = get_hash_value_for_rash_table_key(&rtk) % inputs->partitions_count;

			// take build lock of the partition at parttion_id
			pthread_mutex_lock(&(inputs->partitions[partition_id]->build_lock));

			// open rash table iterator for insertion/appending
			rash_table_iterator rti = find_equals_in_rash_table(&(inputs->partitions[partition_id]->rth), &rtk, 0);

			// open write iterator on this key
			binary_write_iterator* bwi_p = open_for_writing_value_in_rash_table_iterator(&rti);

			// perform append into this iterator storing in the complete tuple
			int abort_error_dummy = 0;
			append_to_binary_write_iterator(bwi_p, tuple, get_tuple_size(inputs->input_tuple_def, tuple), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->partitions[partition_id]->rth.htan)), NULL, &abort_error_dummy);

			// close the bwi_p
			close_and_write_value_in_hash_table_iterator(&rti, bwi_p);

			// delete the insertion iterator
			delete_rash_table_iterator(&rti);

			// release build lock on the partition
			pthread_mutex_unlock(&(inputs->partitions[partition_id]->build_lock));

			// destroy rash table key
			destroy_rash_table_key(&rtk);
		});

		delete_interim_tuple_store(its_p);
	}

	// decrement active build phas jobs count
	pthread_mutex_lock(&(inputs->insert_for_build_queue_lock));
	inputs->active_build_phase_job_count--;
	pthread_mutex_unlock(&(inputs->insert_for_build_queue_lock));

	trigger_execution_on_operator(o);
}

static void probe_for_aggregation_phase_job(operator* o, void* param)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	// this flag can be used to make this nested loop to stopmprocessing and instead just delete the remaining partitions
	// do this when either the udaf fails or the produce function fails
	int process = 1;

	// allocate pointers for states
	void** states = calloc(sizeof(void*), inputs->aggregate_functions_count);

	// allocate pointers for input params to these aggregate funtions
	datum* input_datums = calloc(sizeof(datum), inputs->max_of_aggregation_function_input_params_count);

	while(1)
	{
		uint32_t partition_id;

		// fetch the parttion to process next
		pthread_mutex_lock(&(inputs->partition_to_aggregate_next_lock));
		partition_id = inputs->partition_id_to_aggregate_next;
		if(partition_id < inputs->partitions_count)
			inputs->partition_id_to_aggregate_next++;
		pthread_mutex_unlock(&(inputs->partition_to_aggregate_next_lock));

		// if out of bounds break out of the loop
		if(partition_id >= inputs->partitions_count)
			break;

		// process the parttion, no need for the build lock, we are just probing it
		// if all you want to do is delete the remaining, then set process falg to 0
		if(process)
		{
			// create iterator to iterate over all the entries
			rash_table_iterator rti = find_all_in_rash_table(&(inputs->partitions[partition_id]->rth), 1);

			// loop over all entries
			while(process)
			{
				// process it only if the entry exists
				if(exists_in_rash_table_iterator(&rti))
				{
					int first_tuple = 1;

					// prepare the output
					uint32_t output_tuple_size = get_minimum_tuple_size(inputs->output_tuple_def);
					uint32_t output_tuple_capacity = output_tuple_size;
					void* output_tuple = malloc(output_tuple_capacity);
					init_tuple(inputs->output_tuple_def, output_tuple);

					// open an iterator to read values in every entry
					binary_read_iterator* value_bri_p = read_value_in_rash_table_iterator(&rti);

					int abort_error_dummy = 0;
					while(process)
					{
						int finish = 0;
						consume_tuple_from_tuple_list(tuple, inputs->input_tuple_def, value_bri_p, NULL, &abort_error_dummy,
						{
							if(tuple != NULL)
							{
								if(first_tuple) // set the keys in output
								{
									first_tuple = 0;

									for(uint32_t i = 0; i < inputs->key_element_count; i++)
									{
										datum key_val;
										if(!get_value_from_element_from_tuple(&key_val, inputs->input_tuple_def, inputs->key_element_ids[i], tuple))
											key_val = (*NULL_DATUM);

										// ensure there are enopugh bytes in the output_tuple
										while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(i), output_tuple, &key_val, output_tuple_capacity - output_tuple_size))
										{
											output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
											output_tuple = realloc(output_tuple, output_tuple_capacity);
										}

										// recompute tuple_size
										output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);
									}
								}

								// process the aggregate function calls for this tuple
								for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
								{
									// generate input params to the i-th udaf
									for(uint32_t j = 0; j < inputs->aggregate_functions[i]->input_type_infos_count; j++)
									{
										if(!get_value_from_element_from_tuple(&(input_datums[j]), inputs->input_tuple_def, inputs->aggregate_input_element_ids[i][j], tuple))
											input_datums[j] = (*NULL_DATUM);
									}

									// process_input for the udaf, if it fails kill the operator
									if(!inputs->aggregate_functions[i]->process_input(inputs->aggregate_functions[i], &(states[i]), input_datums))
									{
										kill_reason = get_dstring_pointing_to_literal_cstring("process_input_of_udaf_failed");
										kill_signal_for_self_operator(o, kill_reason);

										process = 0;
										finish = 1;
										goto FAILED_EXIT;
									}
								}
							}
							else
							{
								finish = 1;
							}

							FAILED_EXIT:;
						});
						if(finish)
							break;
					}

					if(process)
					{
						for(uint32_t i = 0, j = inputs->key_element_count; i < inputs->aggregate_functions_count; i++, j++)
						{
							// produce the output_uval the output of the i-th aggregate function
							datum output_uval;
							if(!inputs->aggregate_functions[i]->produce_output(inputs->aggregate_functions[i], &output_uval, &(states[i])))
							{
								kill_reason = get_dstring_pointing_to_literal_cstring("produce_output_of_udaf_failed");
								kill_signal_for_self_operator(o, kill_reason);

								process = 0;
							}

							// ensure there are enopugh bytes in the output_tuple
							while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(j), output_tuple, &output_uval, output_tuple_capacity - output_tuple_size))
							{
								output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
								output_tuple = realloc(output_tuple, output_tuple_capacity);
							}

							// recompute tuple_size
							output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);
						}
					}

					// produce output_tuple
					if(process)
					{
						int produced = produce_tuple_from_operator(o, output_tuple);
						if(!produced)
						{
							kill_reason = get_dstring_pointing_to_literal_cstring("could_not_produce");
							kill_signal_for_self_operator(o, kill_reason);

							process = 0;
						}
					}

					// once read all tuple close the read iterator
					abort_error_dummy = 0;
					delete_binary_read_iterator(value_bri_p, NULL, &abort_error_dummy);

					// clear output_tuple
					free(output_tuple);

					// destroy any states left over, after the tuple has been processed
					for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
						inputs->aggregate_functions[i]->destroy_state(inputs->aggregate_functions[i], &(states[i]));
				}

				// if we can not go next break out
				if(!next_in_rash_table_iterator(&rti))
					break;
			}

			// delete the fina_all iterator
			delete_rash_table_iterator(&rti);
		}

		// destroy the parttion
		{
			pthread_mutex_destroy(&(inputs->partitions[partition_id]->build_lock));
			destroy_rash_table(&(inputs->partitions[partition_id]->rth));
			free(inputs->partitions[partition_id]);
			inputs->partitions[partition_id] = NULL;
		}
	}

	// destroy any states left over
	for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
		inputs->aggregate_functions[i]->destroy_state(inputs->aggregate_functions[i], &(states[i]));

	free(states);
	free(input_datums);

	// decrement active build phas jobs count
	pthread_mutex_lock(&(inputs->insert_for_build_queue_lock));
	inputs->active_probe_phase_job_count--;
	pthread_mutex_unlock(&(inputs->insert_for_build_queue_lock));

	trigger_execution_on_operator(o);
}

static int should_produce_more_for_build_inserts_queue(operator* o)
{
	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->insert_for_build_queue_lock));
	int produce_more = (inputs->tuple_buffers_to_insert_queue_size < inputs->max_concurrent_jobs_queue_size);
	pthread_mutex_unlock(&(inputs->insert_for_build_queue_lock));

	return produce_more;
}

static int should_kill_with_success(operator* o)
{
	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->insert_for_build_queue_lock));
	int should_kill_with_success = (inputs->probe_jobs_started && (inputs->active_probe_phase_job_count == 0));
	pthread_mutex_unlock(&(inputs->insert_for_build_queue_lock));

	return should_kill_with_success;
}

static void start_build_jobs(operator* o)
{
	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->insert_for_build_queue_lock));
	if(inputs->tuple_buffers_to_insert_queue_size > 0)
	{
		uint32_t new_build_jobs = min(inputs->tuple_buffers_to_insert_queue_size, inputs->max_concurrent_jobs_count - inputs->active_build_phase_job_count);
		while(new_build_jobs > 0)
		{
			if(!run_concurrent_job_for_operator(o, NULL, insert_for_build_phase_job))
				break;
			inputs->active_build_phase_job_count++;
			new_build_jobs--;
		}
	}
	pthread_mutex_unlock(&(inputs->insert_for_build_queue_lock));
}

static void start_probe_jobs(operator* o)
{
	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->insert_for_build_queue_lock));
	if(inputs->tuple_buffers_to_insert_queue_size == 0 && inputs->active_build_phase_job_count == 0 && (!(inputs->probe_jobs_started)))
	{
		inputs->probe_jobs_started = 1;
		uint32_t new_probe_jobs = inputs->max_concurrent_jobs_count;
		while(new_probe_jobs > 0)
		{
			if(!run_concurrent_job_for_operator(o, NULL, probe_for_aggregation_phase_job))
				break;
			inputs->active_probe_phase_job_count++;
			new_probe_jobs--;
		}
	}
	pthread_mutex_unlock(&(inputs->insert_for_build_queue_lock));
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	// the operator has been woken up after possibly end of data from the producer, so start the probe jobs
	if(inputs->input_iterator == NULL)
	{
		start_probe_jobs(o);

		if(should_kill_with_success(o))
			kill_signal_for_self_operator(o, kill_reason);

		return;
	}

	while(should_produce_more_for_build_inserts_queue(o))
	{
		start_build_jobs(o);

		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data)
		{
			destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

			if(inputs->pending_build_buffer != NULL)
			{
				// its ownership for inputs->pending_build_buffer, is changing, so unmap it's embed_regions
				unmap_all_embed_regions_in_interim_tuple_store(inputs->pending_build_buffer);

				// insert pending_build_buffer in the tuple_buffers_to_insert linkedlist
				pthread_mutex_lock(&(inputs->insert_for_build_queue_lock));
				insert_tail_in_linkedlist(&(inputs->tuple_buffers_to_insert), inputs->pending_build_buffer);
				inputs->tuple_buffers_to_insert_queue_size++;
				pthread_mutex_unlock(&(inputs->insert_for_build_queue_lock));
				inputs->pending_build_buffer = NULL;

				// start build jobs if any could be started
				start_build_jobs(o);
			}

			start_probe_jobs(o);

			return;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		if(tuple != NULL)
		{
			if(inputs->pending_build_buffer == NULL)
				inputs->pending_build_buffer = get_new_interim_tuple_store(inputs->min_build_tuple_buffer_size);

			append_tuple_to_interim_tuple_store2(inputs->pending_build_buffer, &(inputs->pending_build_buffer->embed_regions[0]), (void*)tuple, &(inputs->input_tuple_def->size_def), inputs->min_build_tuple_buffer_size);

			if(get_total_bytes_in_interim_tuple_store(inputs->pending_build_buffer) >= inputs->min_build_tuple_buffer_size)
			{
				// its ownership for inputs->pending_build_buffer, is changing, so unmap it's embed_regions
				unmap_all_embed_regions_in_interim_tuple_store(inputs->pending_build_buffer);

				// insert pending_build_buffer in the tuple_buffers_to_insert linkedlist
				pthread_mutex_lock(&(inputs->insert_for_build_queue_lock));
				insert_tail_in_linkedlist(&(inputs->tuple_buffers_to_insert), inputs->pending_build_buffer);
				inputs->tuple_buffers_to_insert_queue_size++;
				pthread_mutex_unlock(&(inputs->insert_for_build_queue_lock));
				inputs->pending_build_buffer = NULL;

				// start build jobs if any could be started
				start_build_jobs(o);
			}
		}
		else
			break;
	}

	return ;
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->pending_build_buffer != NULL)
		delete_interim_tuple_store(inputs->pending_build_buffer);

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

	remove_all_from_linkedlist(&(inputs->tuple_buffers_to_insert), DELETE_ON_NOTIFY_FOR_INTERIM_TUPLE_STORE);
	pthread_mutex_destroy(&(inputs->insert_for_build_queue_lock));

	free((data_type_info*)(inputs->output_tuple_def->type_info));
	free((tuple_def*)(inputs->output_tuple_def));

	free(inputs);
}

void setup_hash_aggregation_operator(operator* o, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids, uint32_t partitions_count, uint32_t bucket_count_per_parttion, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_build_tuple_buffer_size)
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
		.min_build_tuple_buffer_size = min_build_tuple_buffer_size,
		.insert_for_build_queue_lock = PTHREAD_MUTEX_INITIALIZER,
		.tuple_buffers_to_insert_queue_size = 0,
		.active_build_phase_job_count = 0,
		.probe_jobs_started = 0,
		.no_more_build_phase_data = 0,
		.pending_build_buffer = NULL,
	};

	memory_move(inputs->aggregate_functions, aggregate_functions, sizeof(aggregate_function*) * aggregate_functions_count);

	memory_move(inputs->aggregate_input_element_ids, aggregate_input_element_ids, sizeof(aggregate_function*) * aggregate_functions_count);

	for(uint32_t i = 0; i < partitions_count; i++)
	{
		inputs->partitions[i] = malloc(sizeof(rash_table_partition));
		pthread_mutex_init(&(inputs->partitions[i]->build_lock), NULL);
		inputs->partitions[i]->rth = get_new_rash_table(bucket_count_per_parttion, input_tuple_def, key_element_ids, key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine), o->self_query_plan->curr_tx->db);
	}

	initialize_linkedlist(&(inputs->tuple_buffers_to_insert), offsetof(interim_tuple_store, embed_node_ll));
}