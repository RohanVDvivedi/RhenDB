#include<rhendb/query_plan_interface.h>

#include<rhendb/transaction.h>

#include<rhendb/function_compare.h>

#include<rhendb/interim_tuple_store_sort.h>

/*
	TEMPLATE FOR INTERMEDIATE OPERATORS (sorting(ordering), joins(hash_joins), aggregations(groupby->aggregates))
*/

#define MAX_LEVELS 128

typedef struct input_values input_values;
struct input_values
{
	consumption_iterator* input_iterator;
	interim_tuple_store* input_un_sorted_run;

	// extracted from input_operator
	const tuple_def* record_def;
	uint32_t key_element_count;
	const positional_accessor* key_element_ids;
	const compare_direction* key_compare_direction;

	uint64_t minimum_run_size;
	uint32_t N_way_sort;
	uint32_t max_concurrent_jobs_count;

	pthread_mutex_t runs_lock;

	int flag_no_new_un_sorted_runs;

	singlylist un_sorted_runs;
	uint64_t un_sorted_runs_count;

	singlylist sorted_runs[MAX_LEVELS];
	uint64_t sorted_runs_count[MAX_LEVELS];
	uint64_t total_sorted_runs_count;

	uint32_t total_concurrent_jobs_count;
};

static void request_to_process_some_jobs(operator* o);

static void sort_job(operator* o, void* _param)
{
	input_values* inputs = o->inputs;

	// fetch the next un_sorted_run, that e could sort
	pthread_mutex_lock(&(inputs->runs_lock));
	interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&(inputs->un_sorted_runs));
	if(its_p != NULL)
	{
		remove_head_from_singlylist(&(inputs->un_sorted_runs));
		inputs->un_sorted_runs_count--;
	}
	pthread_mutex_unlock(&(inputs->runs_lock));

	if(its_p == NULL)
	{
		pthread_mutex_lock(&(inputs->runs_lock));
		inputs->total_concurrent_jobs_count--;
		pthread_mutex_unlock(&(inputs->runs_lock));

		// request some new jobs to start
		request_to_process_some_jobs(o);
		return;
	}

	// sort its_p
	int abort_error = 0;
	interim_tuple_store* ots_p = sort_interim_tuples(its_p, inputs->minimum_run_size, inputs->record_def, inputs->key_element_ids, inputs->key_compare_direction, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine), NULL, &abort_error);
	delete_interim_tuple_store(its_p);

	// insert sorted run back into the sorted_runs[0], the smallest most level
	pthread_mutex_lock(&(inputs->runs_lock));
	insert_tail_in_singlylist(&(inputs->sorted_runs[0]), ots_p);
	inputs->sorted_runs_count[0]++;
	inputs->total_sorted_runs_count++;
	inputs->total_concurrent_jobs_count--;
	pthread_mutex_unlock(&(inputs->runs_lock));

	// request some new jobs to start
	request_to_process_some_jobs(o);
}

static int compare_interim_tuple_stores_for_pheap_runs(const void* o_vp, const void* its1_vp, const void* its2_vp)
{
	const operator* o = o_vp;
	const input_values* inputs = o->inputs;

	const interim_tuple_store* its1_p = its1_vp;
	const interim_tuple_store* its2_p = its2_vp;

	int abort_error = 0;
	return comare_tuples2_rhendb(its1_p->embed_regions[0].tuple, its2_p->embed_regions[0].tuple, inputs->record_def, inputs->key_element_ids, inputs->key_compare_direction, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine), NULL, &abort_error);
}

static void merge_job(operator* o, void* _param)
{
	input_values* inputs = o->inputs;
	uint64_t total_output_size_in_bytes = 0;

	int level = (intptr_t)(_param);

	pheap mergeable_open_runs;
	initialize_pheap(&mergeable_open_runs, MIN_HEAP, LEFTIST, &contexted_comparator(o, compare_interim_tuple_stores_for_pheap_runs), offsetof(interim_tuple_store, embed_node_php));

	// populate runs in mergeable_open_runs
	{
		singlylist mergeable_runs;
		initialize_singlylist(&mergeable_runs, offsetof(interim_tuple_store, embed_node_sl));
		uint32_t mergeable_runs_count = 0;

		pthread_mutex_lock(&(inputs->runs_lock));
		while(mergeable_runs_count < inputs->N_way_sort && !is_empty_singlylist(&(inputs->sorted_runs[level])))
		{
			interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&(inputs->sorted_runs[level]));
			if(its_p != NULL)
			{
				remove_head_from_singlylist(&(inputs->sorted_runs[level]));
				insert_tail_in_singlylist(&mergeable_runs, its_p);
				inputs->sorted_runs_count[level]--;
				inputs->total_sorted_runs_count--;
				mergeable_runs_count++;
			}
			else
				break;
		}
		pthread_mutex_unlock(&(inputs->runs_lock));

		if(mergeable_runs_count == 0)
		{
			pthread_mutex_lock(&(inputs->runs_lock));
			inputs->total_concurrent_jobs_count--;
			pthread_mutex_unlock(&(inputs->runs_lock));

			// request some new jobs to start
			request_to_process_some_jobs(o);
			return;
		}
		else if(mergeable_runs_count == 1)
		{
			pthread_mutex_lock(&(inputs->runs_lock));
			insert_all_at_tail_in_singlylist(&(inputs->sorted_runs[level+1]), &mergeable_runs);
			inputs->sorted_runs_count[level+1] += mergeable_runs_count;
			inputs->total_sorted_runs_count += mergeable_runs_count;
			inputs->total_concurrent_jobs_count--;
			pthread_mutex_unlock(&(inputs->runs_lock));

			// request some new jobs to start
			request_to_process_some_jobs(o);
			return;
		}
		else
		{
			while(!is_empty_singlylist(&mergeable_runs))
			{
				interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&mergeable_runs);
				remove_head_from_singlylist(&mergeable_runs);
				if(!mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), 0, &(inputs->record_def->size_def), inputs->minimum_run_size))
					delete_interim_tuple_store(its_p);
				else
				{
					push_to_pheap(&mergeable_open_runs, its_p);
					total_output_size_in_bytes += get_total_bytes_in_interim_tuple_store(its_p);
				}
			}
		}
	}

	// create output run
	interim_tuple_store* output_its_p = get_new_interim_tuple_store(".");
	extend_interim_tuple_store(output_its_p, total_output_size_in_bytes);

	// merge all one by one from the top into output_its_p
	while(!is_empty_pheap(&mergeable_open_runs))
	{
		interim_tuple_store* its_p = (interim_tuple_store*) get_top_of_pheap(&mergeable_open_runs);

		// copy the top tuple of its_p into (append it to) output_its_p
		append_tuple_to_interim_tuple_store2(output_its_p, &(output_its_p->embed_regions[0]), its_p->embed_regions[0].tuple, &(inputs->record_def->size_def), inputs->minimum_run_size);

		// go next on its_p, and insert it back into mergeable_open_runs
		{
			uint64_t next_tuple_offset = next_tuple_offset_for_interim_tuple_region(&(its_p->embed_regions[0]));
			if(!mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), next_tuple_offset, &(inputs->record_def->size_def), inputs->minimum_run_size))
			{
				remove_from_pheap(&mergeable_open_runs, its_p);
				unmap_all_embed_regions_in_interim_tuple_store(its_p);
				delete_interim_tuple_store(its_p);
			}
			else
				heapify_for_in_pheap(&mergeable_open_runs, its_p);
		}
	}

	// unmap the write-side region
	unmap_all_embed_regions_in_interim_tuple_store(output_its_p);

	// insert output_its_p back into the next level
	pthread_mutex_lock(&(inputs->runs_lock));
	insert_tail_in_singlylist(&(inputs->sorted_runs[level+1]), output_its_p);
	inputs->sorted_runs_count[level+1]++;
	inputs->total_sorted_runs_count++;
	inputs->total_concurrent_jobs_count--;
	pthread_mutex_unlock(&(inputs->runs_lock));

	// request some new jobs to start
	request_to_process_some_jobs(o);
}

static void request_to_process_some_jobs(operator* o)
{
	if(can_not_proceed_for_execution_operator(o))
		return;

	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->runs_lock));

	if(inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count)
	{
		for(int t = 0; t < inputs->un_sorted_runs_count
			&& (inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count); t++)
		{
			if(!run_concurrent_job_for_operator(o, NULL, sort_job) && can_not_proceed_for_execution_operator(o))
				goto EXIT;
			inputs->total_concurrent_jobs_count++;
		}
	}

	if(inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count)
	{
		for(int i = 0; i < MAX_LEVELS && (inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count); i++)
		{
			for(int t = 0; t < UINT_ALIGN_DOWN(inputs->sorted_runs_count[i], inputs->N_way_sort) / inputs->N_way_sort
				&& (inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count); t++)
			{
				if(!run_concurrent_job_for_operator(o, (void*)((intptr_t)i), merge_job) && can_not_proceed_for_execution_operator(o))
					goto EXIT;
				inputs->total_concurrent_jobs_count++;
			}
		}
	}

	if(inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count)
	{
		if(inputs->flag_no_new_un_sorted_runs && inputs->total_sorted_runs_count > 1)
		{
			uint32_t runs_seen = 0;
			for(int i = 0; i < MAX_LEVELS && (inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count); i++)
			{
				runs_seen += inputs->sorted_runs_count[i];
				if((runs_seen < inputs->total_sorted_runs_count) || (inputs->sorted_runs_count[i] > 1))
				{
					for(int t = 0; t < UINT_ALIGN_UP(inputs->sorted_runs_count[i], inputs->N_way_sort) / inputs->N_way_sort
						&& (inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count); t++)
					{
						if(!run_concurrent_job_for_operator(o, (void*)((intptr_t)i), merge_job) && can_not_proceed_for_execution_operator(o))
							goto EXIT;
						inputs->total_concurrent_jobs_count++;
					}
				}
			}
		}
	}

	if(inputs->flag_no_new_un_sorted_runs && inputs->total_concurrent_jobs_count == 0 && inputs->total_sorted_runs_count <= 1)
	{
		for(int i = 0; i < MAX_LEVELS; i++)
		{
			while(!is_empty_singlylist(&(inputs->sorted_runs[i])))
			{
				interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&(inputs->sorted_runs[i]));
				remove_head_from_singlylist(&(inputs->sorted_runs[i]));
				int produced = 0;
				{
					FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(tuple, tuple_index, tuple_offset, &(inputs->record_def->size_def), its_p, inputs->minimum_run_size, {
						produced = produce_tuple_from_operator(o, tuple);
						if(!produced)
							break;
					})
				}
				delete_interim_tuple_store(its_p);
				if(!produced)
				{
					kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("pushed_failed_from_identity_oerator_and_so_killed"));
					pthread_mutex_unlock(&(inputs->runs_lock));
					return ;
				}
			}
		}
		kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
	}

	int need_to_produce_more_runs = (inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count);
	if(inputs->flag_no_new_un_sorted_runs)
		need_to_produce_more_runs = 0;

	EXIT:;
	pthread_mutex_unlock(&(inputs->runs_lock));

	// if so trigger your on self
	if(need_to_produce_more_runs)
		trigger_execution_on_operator(o);
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	pthread_mutex_lock(&(inputs->runs_lock));
	int need_to_produce_more_runs = (inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count);
	if(inputs->flag_no_new_un_sorted_runs)
		need_to_produce_more_runs = 0;
	pthread_mutex_unlock(&(inputs->runs_lock));

	while(need_to_produce_more_runs)
	{
		if(!need_to_produce_more_runs)
			break;

		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data)
		{
			pthread_mutex_lock(&(inputs->runs_lock));
			if(inputs->input_un_sorted_run != NULL)
			{
				insert_tail_in_singlylist(&(inputs->un_sorted_runs), inputs->input_un_sorted_run);
				inputs->un_sorted_runs_count++;
			}
			inputs->flag_no_new_un_sorted_runs = 1;
			pthread_mutex_unlock(&(inputs->runs_lock));

			inputs->input_un_sorted_run = NULL;

			request_to_process_some_jobs(o);
			return;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		if(tuple != NULL)
		{
			if(inputs->input_un_sorted_run == NULL)
			{
				inputs->input_un_sorted_run = get_new_interim_tuple_store(".");
				extend_interim_tuple_store(inputs->input_un_sorted_run, inputs->minimum_run_size);
			}

			append_tuple_to_interim_tuple_store(inputs->input_un_sorted_run, (void*)tuple, &(inputs->record_def->size_def));

			if(get_total_bytes_in_interim_tuple_store(inputs->input_un_sorted_run) >= inputs->minimum_run_size)
			{
				pthread_mutex_lock(&(inputs->runs_lock));
				insert_tail_in_singlylist(&(inputs->un_sorted_runs), inputs->input_un_sorted_run);
				inputs->un_sorted_runs_count++;
				pthread_mutex_unlock(&(inputs->runs_lock));
				inputs->input_un_sorted_run = NULL;
			}

			request_to_process_some_jobs(o);

			if(inputs->input_un_sorted_run == NULL)
			{
				pthread_mutex_lock(&(inputs->runs_lock));
				need_to_produce_more_runs = (inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count);
				pthread_mutex_unlock(&(inputs->runs_lock));
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

	if(inputs->input_un_sorted_run != NULL)
		delete_interim_tuple_store(inputs->input_un_sorted_run);

	pthread_mutex_lock(&(inputs->runs_lock));

	for(int i = 0; i < MAX_LEVELS; i++)
	{
		while(!is_empty_singlylist(&(inputs->sorted_runs[i])))
		{
			interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&(inputs->sorted_runs[i]));
			remove_head_from_singlylist(&(inputs->sorted_runs[i]));
			delete_interim_tuple_store(its_p);
			inputs->sorted_runs_count[i]--;
			inputs->total_sorted_runs_count--;
		}
	}

	while(!is_empty_singlylist(&(inputs->un_sorted_runs)))
	{
		interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&(inputs->un_sorted_runs));
		remove_head_from_singlylist(&(inputs->un_sorted_runs));
		delete_interim_tuple_store(its_p);
		inputs->un_sorted_runs_count--;
	}

	pthread_mutex_unlock(&(inputs->runs_lock));

	free(inputs);
}

#include<rhendb/tuple_transformers.h>

void setup_external_sort_operator(operator* o, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint64_t minimum_run_size, uint32_t N_way_sort, uint32_t max_concurrent_jobs_count)
{
	// force the input operator to produce tuples one by one
	append_tuple_transformer(&(input_operator->output_tuple_transformers), get_new_identity_tuple_transformer(get_tuple_def_for_tuples_to_be_consumed_from(input_operator)));

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = free_resources;

	// it is an identity operator, produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), get_tuple_def_for_tuples_to_be_consumed_from(input_operator));

	o->inputs = malloc(sizeof(input_values));
	input_values* inputs = o->inputs;
	*inputs = (input_values){
		.input_iterator = create_consumption_iterator(input_operator, o, NULL),
		.input_un_sorted_run = NULL,
		.record_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator),
		.key_element_count = key_element_count,
		.key_element_ids = key_element_ids,
		.key_compare_direction = key_compare_direction,
		.minimum_run_size = minimum_run_size,
		.N_way_sort = N_way_sort,
		.max_concurrent_jobs_count = max_concurrent_jobs_count,
		.runs_lock = PTHREAD_MUTEX_INITIALIZER,
		.flag_no_new_un_sorted_runs = 0,
		.un_sorted_runs_count = 0,
		.sorted_runs_count = {},
		.total_sorted_runs_count = 0,
		.total_concurrent_jobs_count = 0,
	};

	for(int i = 0; i < MAX_LEVELS; i++)
		initialize_singlylist(&(inputs->sorted_runs[i]), offsetof(interim_tuple_store, embed_node_sl));
	initialize_singlylist(&(inputs->un_sorted_runs), offsetof(interim_tuple_store, embed_node_sl));
}