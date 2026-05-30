#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<rhendb/rash_table.h>
#include<rhendb/interim_tuple_store.h>

#include<rhendb/function_hash.h>

#include<rhendb/join_preserve_type.h>

#include<rhendb/nullable_type_info_maker.h>

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
typedef struct input_values input_values;
struct input_values
{
	uint32_t key_element_count;

	consumption_iterator* left_input_iterator;
	const tuple_def* left_input_tuple_def;
	const positional_accessor* left_key_element_ids;

	consumption_iterator* right_input_iterator;
	const tuple_def* right_input_tuple_def;
	const positional_accessor* right_key_element_ids;

	const tuple_def* output_tuple_def;

	join_preserve_type ptype;

	// fill up this buffer before moving it into the tuple_buffers_to_insert
	interim_tuple_store* pending_buffer;

	// rash_table partitions for the right side
	uint32_t partitions_count;
	uint32_t bucket_count_per_parttion;
	rash_table_partition** partitions;

	// to be used in the last phase to produce tuples on the right side that do not have a match on the left
	// when DOES_IT_PRESERVE_RIGHT(ptype) == 1
	pthread_mutex_t partition_to_right_only_join_next_lock;
	uint32_t partition_to_right_only_join_next;

	// job params and input params list

	uint32_t max_concurrent_jobs_count;
	uint32_t max_concurrent_jobs_queue_size;
	uint32_t min_pending_buffer_size;

	pthread_mutex_t buffers_queue_lock;
	linkedlist buffers_queue;
	uint32_t buffers_queue_size;

	uint32_t active_build_phase_job_count;
	uint32_t active_probe_phase_job_count;
	uint32_t active_right_only_probe_phase_job_count;

	int phase; // 0, 1, 2
};

static int produce_join_result(operator* o, const void* left_tuple, const void* right_tuple)
{
	input_values* inputs = o->inputs;

	uint32_t output_tuple_capacity = 32 + ((left_tuple != NULL) ? get_tuple_size(inputs->left_input_tuple_def, left_tuple) : 0) + ((right_tuple != NULL) ? get_tuple_size(inputs->right_input_tuple_def, right_tuple) : 0);

	void* output_tuple = malloc(output_tuple_capacity);

	init_tuple(inputs->output_tuple_def, output_tuple);

	if(left_tuple)
		set_element_in_tuple_from_tuple(inputs->output_tuple_def, STATIC_POSITION(0), output_tuple, inputs->left_input_tuple_def, SELF, left_tuple, UINT32_MAX);

	if(right_tuple)
		set_element_in_tuple_from_tuple(inputs->output_tuple_def, STATIC_POSITION(1), output_tuple, inputs->right_input_tuple_def, SELF, right_tuple, UINT32_MAX);

	// produce output_tuple
	int produced = produce_tuple_from_operator(o, output_tuple);
	free(output_tuple);

	return produced;
}

static void build_right_side_partitions(operator* o, void* param)
{
	input_values* inputs = o->inputs;

	while(1)
	{
		pthread_mutex_lock(&(inputs->buffers_queue_lock));
		interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_linkedlist(&(inputs->buffers_queue));
		if(its_p != NULL)
		{
			remove_head_from_linkedlist(&(inputs->buffers_queue));
			inputs->buffers_queue_size--;
		}
		pthread_mutex_unlock(&(inputs->buffers_queue_lock));

		// if no tuple buffer found exit
		if(its_p == NULL)
			break;

		// insert to the right partition if one exists
		FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(tuple, tuple_index, tuple_offset, &(inputs->right_input_tuple_def->size_def), its_p, get_total_bytes_in_interim_tuple_store(its_p), {

			// create rash table key
			rash_table_key rtk = get_new_rash_table_key(tuple, inputs->right_input_tuple_def, inputs->right_key_element_ids, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine));

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
			append_to_binary_write_iterator(bwi_p, tuple, get_tuple_size(inputs->right_input_tuple_def, tuple), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(inputs->partitions[partition_id]->rth.htan)), NULL, &abort_error_dummy);

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

	// decrement active build phase jobs count
	pthread_mutex_lock(&(inputs->buffers_queue_lock));
	inputs->active_build_phase_job_count--;
	pthread_mutex_unlock(&(inputs->buffers_queue_lock));

	trigger_execution_on_operator(o);
}

static void probe_right_side_partitions_using_left_tuples(operator* o, void* param)
{
	input_values* inputs = o->inputs;

	int failed = 0;






	// decrement active build phas jobs count
	pthread_mutex_lock(&(inputs->buffers_queue_lock));
	inputs->active_probe_phase_job_count--;
	pthread_mutex_unlock(&(inputs->buffers_queue_lock));

	if(!failed)
		trigger_execution_on_operator(o);
}

static void probe_right_side_partitions_for_right_only_tuples(operator* o, void* param)
{
	input_values* inputs = o->inputs;

	int failed = 0;






	// decrement active build phas jobs count
	pthread_mutex_lock(&(inputs->buffers_queue_lock));
	inputs->active_right_only_probe_phase_job_count--;
	pthread_mutex_unlock(&(inputs->buffers_queue_lock));

	if(!failed)
		trigger_execution_on_operator(o);
}

static void start_right_side_build_jobs(operator* o)
{
	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->buffers_queue_lock));
	if(inputs->buffers_queue_size > 0)
	{
		uint32_t new_jobs = min(inputs->buffers_queue_size, inputs->max_concurrent_jobs_count - inputs->active_build_phase_job_count);
		while(new_jobs > 0)
		{
			if(!run_concurrent_job_for_operator(o, NULL, build_right_side_partitions))
				break;
			inputs->active_build_phase_job_count++;
			new_jobs--;
		}
	}
	pthread_mutex_unlock(&(inputs->buffers_queue_lock));
}

static void start_right_side_probe_for_left_tupled_jobs(operator* o)
{
	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->buffers_queue_lock));
	if(inputs->buffers_queue_size > 0)
	{
		uint32_t new_jobs = min(inputs->buffers_queue_size, inputs->max_concurrent_jobs_count - inputs->active_probe_phase_job_count);
		while(new_jobs > 0)
		{
			if(!run_concurrent_job_for_operator(o, NULL, probe_right_side_partitions_using_left_tuples))
				break;
			inputs->active_probe_phase_job_count++;
			new_jobs--;
		}
	}
	pthread_mutex_unlock(&(inputs->buffers_queue_lock));
}

static void start_right_side_only_probe_jobs(operator* o)
{
	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->buffers_queue_lock));
	uint32_t new_jobs = inputs->max_concurrent_jobs_count;
	while(new_jobs > 0)
	{
		if(!run_concurrent_job_for_operator(o, NULL, probe_right_side_partitions_for_right_only_tuples))
			break;
		inputs->active_right_only_probe_phase_job_count++;
		new_jobs--;
	}
	pthread_mutex_unlock(&(inputs->buffers_queue_lock));
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	// accumulate right_tuple-s into pending_buffer and queue it to build right side partitions
	if(inputs->right_input_iterator != NULL)
	{
		while(1)
		{
			int no_more_data = 0;
			const void* tuple = consume_for_consumption_iterator(inputs->right_input_iterator, &no_more_data);
			if(no_more_data)
			{
				// this signals completion of build phase
				destroy_consumption_iterator(inputs->right_input_iterator);
				inputs->right_input_iterator = NULL;

				if(inputs->pending_buffer != NULL)
				{
					// its ownership for inputs->pending_buffer, is changing, so unmap it's embed_regions
					unmap_all_embed_regions_in_interim_tuple_store(inputs->pending_buffer);

					// insert pending_build_buffer in the buffers_queue linkedlist
					pthread_mutex_lock(&(inputs->buffers_queue_lock));
					insert_tail_in_linkedlist(&(inputs->buffers_queue), inputs->pending_buffer);
					inputs->buffers_queue_size++;
					pthread_mutex_unlock(&(inputs->buffers_queue_lock));
					inputs->pending_buffer = NULL;

					// start build jobs if any could be started
					start_right_side_build_jobs(o);
				}

				break ;
			}
			if(can_not_proceed_for_execution_operator(o))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
				return ;
			}

			if(tuple != NULL)
			{
				if(inputs->pending_buffer == NULL)
					inputs->pending_buffer = get_new_interim_tuple_store(inputs->min_pending_buffer_size);

				append_tuple_to_interim_tuple_store2(inputs->pending_buffer, &(inputs->pending_buffer->embed_regions[0]), tuple, &(inputs->right_input_tuple_def->size_def), inputs->min_pending_buffer_size);

				if(get_total_bytes_in_interim_tuple_store(inputs->pending_buffer) >= inputs->min_pending_buffer_size)
				{
					// its ownership for inputs->pending_buffer, is changing, so unmap it's embed_regions
					unmap_all_embed_regions_in_interim_tuple_store(inputs->pending_buffer);

					// insert pending_build_buffer in the buffers_queue linkedlist
					pthread_mutex_lock(&(inputs->buffers_queue_lock));
					insert_tail_in_linkedlist(&(inputs->buffers_queue), inputs->pending_buffer);
					inputs->buffers_queue_size++;
					pthread_mutex_unlock(&(inputs->buffers_queue_lock));
					inputs->pending_buffer = NULL;

					// start build jobs if any could be started
					start_right_side_build_jobs(o);
				}
			}
			else
				return ;
		}
	}

	// accumulate left_tuple-s into pending_buffer and queue it to probe right side partitions
	if(inputs->right_input_iterator == NULL && inputs->left_input_iterator != NULL)
	{
		if(inputs->phase == 0)
		{
			pthread_mutex_lock(&(inputs->buffers_queue_lock));
			if(inputs->buffers_queue_size == 0 && inputs->active_build_phase_job_count == 0)
				inputs->phase = 1;
			pthread_mutex_unlock(&(inputs->buffers_queue_lock));

			if(inputs->phase == 0) // if the phase is still 0, return immediately
				return ;
		}

		while(1)
		{
			int no_more_data = 0;
			const void* tuple = consume_for_consumption_iterator(inputs->left_input_iterator, &no_more_data);
			if(no_more_data)
			{
				// this signals completion of probe phase
				destroy_consumption_iterator(inputs->left_input_iterator);
				inputs->left_input_iterator = NULL;

				if(inputs->pending_buffer != NULL)
				{
					// its ownership for inputs->pending_buffer, is changing, so unmap it's embed_regions
					unmap_all_embed_regions_in_interim_tuple_store(inputs->pending_buffer);

					// insert pending_build_buffer in the buffers_queue linkedlist
					pthread_mutex_lock(&(inputs->buffers_queue_lock));
					insert_tail_in_linkedlist(&(inputs->buffers_queue), inputs->pending_buffer);
					inputs->buffers_queue_size++;
					pthread_mutex_unlock(&(inputs->buffers_queue_lock));
					inputs->pending_buffer = NULL;

					// start build jobs if any could be started
					start_right_side_probe_for_left_tupled_jobs(o);
				}

				break ;
			}
			if(can_not_proceed_for_execution_operator(o))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
				return ;
			}

			if(tuple != NULL)
			{
				if(inputs->pending_buffer == NULL)
					inputs->pending_buffer = get_new_interim_tuple_store(inputs->min_pending_buffer_size);

				append_tuple_to_interim_tuple_store2(inputs->pending_buffer, &(inputs->pending_buffer->embed_regions[0]), tuple, &(inputs->left_input_tuple_def->size_def), inputs->min_pending_buffer_size);

				if(get_total_bytes_in_interim_tuple_store(inputs->pending_buffer) >= inputs->min_pending_buffer_size)
				{
					// its ownership for inputs->pending_buffer, is changing, so unmap it's embed_regions
					unmap_all_embed_regions_in_interim_tuple_store(inputs->pending_buffer);

					// insert pending_build_buffer in the buffers_queue linkedlist
					pthread_mutex_lock(&(inputs->buffers_queue_lock));
					insert_tail_in_linkedlist(&(inputs->buffers_queue), inputs->pending_buffer);
					inputs->buffers_queue_size++;
					pthread_mutex_unlock(&(inputs->buffers_queue_lock));
					inputs->pending_buffer = NULL;

					// start build jobs if any could be started
					start_right_side_probe_for_left_tupled_jobs(o);
				}
			}
			else
				return ;
		}
	}

	// queue several jobs to just scan for right-only unmatched entries and produce them
	if(inputs->right_input_iterator == NULL && inputs->left_input_iterator == NULL)
	{
		if(inputs->phase == 1)
		{
			pthread_mutex_lock(&(inputs->buffers_queue_lock));
			if(inputs->buffers_queue_size == 0 && inputs->active_probe_phase_job_count == 0)
				inputs->phase = 2;
			pthread_mutex_unlock(&(inputs->buffers_queue_lock));

			if(inputs->phase == 1) // if the phase is still 1, return immediately
				return ;
		}

		if(inputs->phase == 2)
		{
			if(!DOES_IT_PRESERVE_RIGHT(inputs->ptype))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
				return;
			}
			inputs->phase = 3;
			start_right_side_only_probe_jobs(o);
		}

		if(inputs->phase == 3)
		{
			pthread_mutex_lock(&(inputs->buffers_queue_lock));
			if(inputs->active_right_only_probe_phase_job_count == 0)
				inputs->phase = 4;
			pthread_mutex_unlock(&(inputs->buffers_queue_lock));

			if(inputs->phase == 3)
				return;
		}

		if(inputs->phase == 4)
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
			return;
		}
	}
}

static void clean_up_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->right_input_iterator != NULL)
	{
		destroy_consumption_iterator(inputs->right_input_iterator);
		inputs->right_input_iterator = NULL;
	}
	if(inputs->left_input_iterator != NULL)
	{
		destroy_consumption_iterator(inputs->left_input_iterator);
		inputs->left_input_iterator = NULL;
	}

	if(inputs->pending_buffer != NULL)
	{
		delete_interim_tuple_store(inputs->pending_buffer);
		inputs->pending_buffer = NULL;
	}

	for(uint32_t i = 0; i < inputs->partitions_count; i++)
	{
		if(inputs->partitions[i] == NULL)
			continue;
		destroy_rash_table(&(inputs->partitions[i]->rth));
		free(inputs->partitions[i]);
		inputs->partitions[i] = NULL;
	}
	free(inputs->partitions);

	remove_all_from_linkedlist(&(inputs->buffers_queue), DELETE_ON_NOTIFY_FOR_INTERIM_TUPLE_STORE);
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	// all key_dtis were made into nullable types, so free them first
	for(uint32_t i = 0; i < 2; i++)
		free((data_type_info*)(inputs->output_tuple_def->type_info->containees[i].al.type_info));
	free((data_type_info*)(inputs->output_tuple_def->type_info));
	free((tuple_def*)(inputs->output_tuple_def));

	free(inputs);
}

operator_resource_counter setup_hash_join_operator(operator* o, operator* left_input_operator, const positional_accessor* left_key_element_ids, operator* right_input_operator, const positional_accessor* right_key_element_ids, uint32_t key_element_count, join_preserve_type ptype, uint32_t partitions_count, uint32_t bucket_count_per_parttion, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size)
{
	const tuple_def* left_input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(left_input_operator);
	const tuple_def* right_input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(right_input_operator);

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		if(!do_these_types_on_being_equal_hash_to_same_value(get_type_info_for_element_from_tuple_def(left_input_tuple_def, left_key_element_ids[i]), get_type_info_for_element_from_tuple_def(right_input_tuple_def, right_key_element_ids[i])))
		{
			printf("input_operators must produce comparably hash equal on having equal key for inputs to hash_join_operator\n");
			exit(-1);
		}
	}

	operator_resource_counter result = {.buffer_counter = has_extended_type_info3(left_input_tuple_def, key_element_count, left_key_element_ids) + has_extended_type_info3(right_input_tuple_def, key_element_count, right_key_element_ids), .job_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = free_resources;

	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(2));
	uint32_t max_output_tuple_size = 0;

	{
		data_type_info* left_dti = left_input_tuple_def->type_info;

		if(left_dti == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += left_dti->is_variable_sized ? (8 + left_dti->max_size) : (1 + left_dti->size);

		strcpy(output_dti->containees[0].field_name, "left");
		output_dti->containees[0].al.type_info = shallow_clone_into_nullable_type(left_dti);
	}

	{
		data_type_info* right_dti = right_input_tuple_def->type_info;

		if(right_dti == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += right_dti->is_variable_sized ? (8 + right_dti->max_size) : (1 + right_dti->size);

		strcpy(output_dti->containees[1].field_name, "right");
		output_dti->containees[1].al.type_info = shallow_clone_into_nullable_type(right_dti);
	}

	initialize_tuple_data_type_info(output_dti, "join", 0, max_output_tuple_size, 2);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);


	// it is an identity operator, produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	input_values* inputs = o->inputs;
	*inputs = (input_values){
		.key_element_count = key_element_count,

		.left_input_iterator = create_consumption_iterator(left_input_operator, o, NULL, NULL),
		.left_input_tuple_def = left_input_tuple_def,
		.left_key_element_ids = left_key_element_ids,

		.right_input_iterator = create_consumption_iterator(right_input_operator, o, NULL, NULL),
		.right_input_tuple_def = right_input_tuple_def,
		.right_key_element_ids = right_key_element_ids,

		.output_tuple_def = output_tuple_def,

		.ptype = ptype,

		.pending_buffer = NULL,

		.partitions_count = partitions_count,
		.bucket_count_per_parttion = bucket_count_per_parttion,

		.partition_to_right_only_join_next_lock = PTHREAD_MUTEX_INITIALIZER,
		.partition_to_right_only_join_next = 0,

		.max_concurrent_jobs_count = max_concurrent_jobs_count,
		.max_concurrent_jobs_queue_size = max_concurrent_jobs_queue_size,
		.min_pending_buffer_size = min_pending_buffer_size,

		.buffers_queue_lock = PTHREAD_MUTEX_INITIALIZER,
		.buffers_queue_size = 0,

		.active_build_phase_job_count = 0,
		.active_probe_phase_job_count = 0,
		.active_right_only_probe_phase_job_count = 0,

		.phase = 0, // always start with phase 0
	};

	for(uint32_t i = 0; i < partitions_count; i++)
	{
		inputs->partitions[i] = malloc(sizeof(rash_table_partition));
		pthread_mutex_init(&(inputs->partitions[i]->build_lock), NULL);
		inputs->partitions[i]->rth = get_new_rash_table(bucket_count_per_parttion, right_input_tuple_def, left_key_element_ids, key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine), o->self_query_plan->curr_tx->db);
	}

	initialize_linkedlist(&(inputs->buffers_queue), offsetof(interim_tuple_store, embed_node_ll));

	return result;
}