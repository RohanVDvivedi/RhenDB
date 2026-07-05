#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<rhendb/rash_table.h>
#include<rhendb/interim_tuple_store.h>

#include<rhendb/function_hash.h>

#include<rhendb/join_type.h>

#include<rhendb/nullable_type_info_maker.h>

#include<stdlib.h>

#define INIT_BUCKET_COUNT 64
#define MAX_LOAD_FACTOR 2.5

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

	semi_join_type stype;

	// fill up this buffer before moving it into the tuple_buffers_to_insert
	interim_tuple_store* pending_buffer;

	// rash_table partitions for the right side
	uint32_t partitions_count;
	rash_table_partition** partitions;

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

		// insert all to the right partition
		FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(tuple, tuple_index, tuple_offset, &(inputs->right_input_tuple_def->size_def), its_p, get_total_bytes_in_interim_tuple_store(its_p), {
			// create rash table key
			rash_table_key rtk = get_new_rash_table_key(tuple, inputs->right_input_tuple_def, inputs->right_key_element_ids, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine));

			// find the parttion this key goes into
			uint32_t partition_id = get_hash_value_for_rash_table_key(&rtk) % inputs->partitions_count;

			// take build lock of the partition at parttion_id
			pthread_mutex_lock(&(inputs->partitions[partition_id]->build_lock));

			// open rash table iterator for insertion/appending
			rash_table_iterator rti = find_equals_in_rash_table(&(inputs->partitions[partition_id]->rth), &rtk, 0);

			// we only insert entry, if it does not exist and no need to store the entire right_tuple like the regular join
			if(!(rti.pointing_to_rkey)) // result of exists check done above
			{
				// open write iterator on this key
				binary_write_iterator* bwi_p = open_for_writing_value_in_rash_table_iterator(&rti);

				// close the bwi_p
				close_and_write_value_in_hash_table_iterator(&rti, bwi_p);
			}

			// delete the insertion iterator
			delete_rash_table_iterator(&rti);

			// if the load factor went too high then expand the bucket_count for the partition
			if(get_load_factor_for_rash_table(&(inputs->partitions[partition_id]->rth)) > MAX_LOAD_FACTOR)
				expand_rash_table(&(inputs->partitions[partition_id]->rth));

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
	while(!failed)
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

		// probe all left_tuple-s to the right partition if one exists
		FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(tuple, tuple_index, tuple_offset, &(inputs->left_input_tuple_def->size_def), its_p, get_total_bytes_in_interim_tuple_store(its_p), {
			// create rash table key
			rash_table_key rtk = get_new_rash_table_key(tuple, inputs->left_input_tuple_def, inputs->left_key_element_ids, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine));

			// find the parttion this key goes into
			uint32_t partition_id = get_hash_value_for_rash_table_key(&rtk) % inputs->partitions_count;

			// open rash table iterator for insertion/appending
			rash_table_iterator rti = find_equals_in_rash_table(&(inputs->partitions[partition_id]->rth), &rtk, 1);

			if(rti.pointing_to_rkey) // result of exists check done above
			{
				if(DOES_PRODUCE_MATCHED_LEFT_TUPLES(inputs->stype))
				{
					if(!produce_tuple_from_operator(o, tuple))
					{
						kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
						failed = 1;
					}
				}
			}
			else
			{
				if(DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(inputs->stype))
				{
					if(!produce_tuple_from_operator(o, tuple))
					{
						kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
						failed = 1;
					}
				}
			}

			// delete the insertion iterator
			delete_rash_table_iterator(&rti);

			// destroy rash table key
			destroy_rash_table_key(&rtk);

			if(failed)
				break;
		});

		delete_interim_tuple_store(its_p);
	}


	// decrement active build phas jobs count
	pthread_mutex_lock(&(inputs->buffers_queue_lock));
	inputs->active_probe_phase_job_count--;
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

static int should_produce_more_for_buffers_queue(operator* o)
{
	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->buffers_queue_lock));
	int produce_more = (inputs->buffers_queue_size < inputs->max_concurrent_jobs_queue_size);
	pthread_mutex_unlock(&(inputs->buffers_queue_lock));

	return produce_more;
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	// accumulate right_tuple-s into pending_buffer and queue it to build right side partitions
	if(inputs->right_input_iterator != NULL)
	{
		while(1)
		{
			if(!should_produce_more_for_buffers_queue(o))
				return;

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
			if(!should_produce_more_for_buffers_queue(o))
				return;

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
		pthread_mutex_destroy(&(inputs->partitions[i]->build_lock));
		destroy_rash_table(&(inputs->partitions[i]->rth));
		free(inputs->partitions[i]);
		inputs->partitions[i] = NULL;
	}
	free(inputs->partitions);

	remove_all_from_linkedlist(&(inputs->buffers_queue), DELETE_ON_NOTIFY_FOR_INTERIM_TUPLE_STORE);

	pthread_mutex_destroy(&(inputs->buffers_queue_lock));
}

operator_resource_counter setup_hash_semi_join_operator(operator* o, operator* left_input_operator, const positional_accessor* left_key_element_ids, operator* right_input_operator, const positional_accessor* right_key_element_ids, uint32_t key_element_count, semi_join_type stype, uint32_t partitions_count, uint32_t max_concurrent_jobs_count, uint32_t max_concurrent_jobs_queue_size, uint32_t min_pending_buffer_size)
{
	if(key_element_count == 0)
	{
		printf("key_element_count must not be 0 for hash_semi_join_operator\n");
		exit(-1);
	}

	if(partitions_count == 0)
	{
		printf("partitions_count can not be 0 for hash_semi_join_operator\n");
		exit(-1);
	}

	if(max_concurrent_jobs_count == 0)
	{
		printf("max_concurrent_jobs_count can not be 0 for hash_semi_join_operator\n");
		exit(-1);
	}

	if(max_concurrent_jobs_queue_size == 0)
	{
		printf("max_concurrent_jobs_queue_size can not be 0 for hash_semi_join_operator\n");
		exit(-1);
	}

	if(min_pending_buffer_size == 0)
	{
		printf("min_pending_buffer_size can not be 0 for hash_semi_join_operator\n");
		exit(-1);
	}

	const tuple_def* left_input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(left_input_operator);
	const tuple_def* right_input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(right_input_operator);

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		if(!do_these_types_on_being_equal_hash_to_same_value(get_type_info_for_element_from_tuple_def(left_input_tuple_def, left_key_element_ids[i]), get_type_info_for_element_from_tuple_def(right_input_tuple_def, right_key_element_ids[i])))
		{
			printf("input_operators must produce comparably hash equal on having equal key for inputs to hash_semi_join_operator\n");
			exit(-1);
		}
	}

	uint64_t left_side_buffers = has_extended_type_info3(left_input_tuple_def, key_element_count, left_key_element_ids);
	uint64_t right_side_buffers = has_extended_type_info3(right_input_tuple_def, key_element_count, right_key_element_ids);

	operator_resource_counter result = {.buffer_counter = max_concurrent_jobs_count * (max(2 * right_side_buffers, left_side_buffers + right_side_buffers)), .job_counter = max_concurrent_jobs_count + 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	init_tuple_transformers(&(o->output_tuple_transformers), left_input_tuple_def);

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

		.stype = stype,

		.pending_buffer = NULL,

		.partitions_count = partitions_count,
		.partitions = malloc(sizeof(rash_table_partition*) * partitions_count),

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
		inputs->partitions[i]->rth = get_new_rash_table(INIT_BUCKET_COUNT, right_input_tuple_def, right_key_element_ids, key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine), o->self_query_plan->curr_tx->db);
	}

	initialize_linkedlist(&(inputs->buffers_queue), offsetof(interim_tuple_store, embed_node_ll));

	return result;
}