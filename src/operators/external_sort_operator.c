#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

#include<rhendb/function_compare.h>

#include<rhendb/interim_tuple_store_sort.h>

#include<rhendb/tuples_down_counter.h>

typedef struct tuple_runs tuple_runs;
struct tuple_runs
{
	// number of runs in runs singlylist
	uint64_t runs_count;

	singlylist runs;

	// embed_node to link all job_params in queue
	llnode embed_node;

	// only for merge operation
	int level_for_merged_run;
};

static void initialize_tuple_runs(tuple_runs* truns_p)
{
	truns_p->runs_count = 0;
	initialize_singlylist(&(truns_p->runs), offsetof(interim_tuple_store, embed_node_sl));
	initialize_llnode(&(truns_p->embed_node));
}

static void push_run_in_tuple_runs(tuple_runs* truns_p, interim_tuple_store* its_p)
{
	truns_p->runs_count++;
	insert_tail_in_singlylist(&(truns_p->runs), its_p);
}

static void push_all_runs_in_tuple_runs(tuple_runs* truns_p, tuple_runs* copy_from_truns_p)
{
	truns_p->runs_count += copy_from_truns_p->runs_count;
	copy_from_truns_p->runs_count = 0;
	insert_all_at_tail_in_singlylist(&(truns_p->runs), &(copy_from_truns_p->runs));
}

static interim_tuple_store* pop_run_from_tuple_runs(tuple_runs* truns_p)
{
	interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_singlylist(&(truns_p->runs));
	if(its_p == NULL)
		return NULL;
	truns_p->runs_count--;
	remove_head_from_singlylist(&(truns_p->runs));
	return its_p;
}

static void delete_all_runs_in_tuple_runs(tuple_runs* truns_p)
{
	for(interim_tuple_store* its_p = pop_run_from_tuple_runs(truns_p); its_p != NULL; its_p = pop_run_from_tuple_runs(truns_p))
		delete_interim_tuple_store(its_p);
}

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

	// derieved information
	const data_type_info** key_dtis;

	// below are the properties of this operator
	uint64_t minimum_run_size;
	uint32_t N_way_sort;
	uint32_t max_concurrent_jobs_count;

	// numbers of results to be produced
	// if infinity this operator sorts everything
	// else if finite, only these many outputs will be produced
	tuples_down_counter result_counter;

	// potects everything underneath
	pthread_mutex_t runs_lock;

	int flag_no_new_un_sorted_runs;

	tuple_runs un_sorted_runs;

	tuple_runs sorted_runs[MAX_LEVELS];

	uint64_t total_sorted_runs_count;

	uint32_t total_concurrent_jobs_count;

	linkedlist job_param_list;
	linkedlist job_param_free_list;
};

static void request_to_process_some_jobs(operator* o);

static void sort_job(operator* o, void* param)
{
	input_values* inputs = o->inputs;
	tuples_down_counter result_counter = inputs->result_counter;

	tuple_runs* input_param = param;

	uint32_t total_runs_to_process = input_param->runs_count;
	for(uint64_t i = 0; i < total_runs_to_process && !can_not_proceed_for_execution_operator(o); i++)
	{
		// pop one from the front
		interim_tuple_store* its_p = pop_run_from_tuple_runs(input_param);

		// sort it into a ne run
		int abort_error = 0;
		interim_tuple_store* ots_p = sort_interim_tuples(its_p, result_counter, inputs->record_def, inputs->key_element_ids, inputs->key_compare_direction, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine), NULL, &abort_error);

		// delete the input run
		delete_interim_tuple_store(its_p);

		// handling abort_error
		if(ots_p == NULL) // case for handling possibly an abort error
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_sort_possibly_abort_error_on_comparing_extended_type"));
			return;
		}

		// push ots_p at the back
		push_run_in_tuple_runs(input_param, ots_p);
	}

	// insert sorted run back into the sorted_runs[0], the smallest most level
	// also add input_param back to free list
	pthread_mutex_lock(&(inputs->runs_lock));
	remove_from_linkedlist(&(inputs->job_param_list), input_param);
	inputs->total_sorted_runs_count += input_param->runs_count;
	push_all_runs_in_tuple_runs(&(inputs->sorted_runs[0]), input_param);
	insert_tail_in_linkedlist(&(inputs->job_param_free_list), input_param);
	inputs->total_concurrent_jobs_count--;
	pthread_mutex_unlock(&(inputs->runs_lock));

	// request some new jobs to start
	request_to_process_some_jobs(o);
}

// we materialize, the keys in its_p->embed_ptrs[0], using it as datum[] having inputs->key_element_count elements long
static void revise_materialized_keys_in_interim_tuple_store(operator* o, interim_tuple_store* its_p)
{
	input_values* inputs = o->inputs;

	for(uint32_t j = 0; j < inputs->key_element_count; j++)
		if(!get_value_from_element_from_tuple(&(((datum*)(its_p->embed_ptrs[0]))[j]), inputs->record_def, inputs->key_element_ids[j], its_p->embed_regions[0].tuple))
			((datum*)(its_p->embed_ptrs[0]))[j] = (*NULL_DATUM);
}

// comparator to compare materialized tuples at its_p->embed_ptrs[0]
static int compare_interim_tuple_stores_for_pheap_runs(const void* o_vp, const void* its1_vp, const void* its2_vp)
{
	const operator* o = o_vp;
	const input_values* inputs = o->inputs;

	const interim_tuple_store* its1_p = its1_vp;
	const interim_tuple_store* its2_p = its2_vp;

	int abort_error = 0;
	return compare_datums3_rhendb(its1_p->embed_ptrs[0], its2_p->embed_ptrs[0], inputs->key_dtis, inputs->key_compare_direction, inputs->key_element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine), NULL, &abort_error);
}

static void merge_into_run_job(operator* o, void* param)
{
	input_values* inputs = o->inputs;
	tuples_down_counter result_counter = inputs->result_counter;

	tuple_runs* input_param = param;

	// malloc keys a datum[], to be used by all the runs existing in pheap until they are all merged and destroyed
	datum* keys = malloc(inputs->N_way_sort * inputs->key_element_count * sizeof(datum));

	// open runs
	pheap mergeable_open_runs;
	initialize_pheap(&mergeable_open_runs, MIN_HEAP, LEFTIST, &contexted_comparator(o, compare_interim_tuple_stores_for_pheap_runs), offsetof(interim_tuple_store, embed_node_php));

	uint64_t total_output_size_in_bytes = 0;
	uint64_t total_input_runs_count = 0;

	// populate runs in mergeable_open_runs
	for(interim_tuple_store* its_p = pop_run_from_tuple_runs(input_param); its_p != NULL; its_p = pop_run_from_tuple_runs(input_param))
	{
		if(!mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), 0, &(inputs->record_def->size_def), inputs->minimum_run_size))
			delete_interim_tuple_store(its_p);
		else
		{
			// comes here only if mmap succeeds, will happen always, unless the run is empty

			// embed_ptrs[0] is used to store the materialized keys
			its_p->embed_ptrs[0] = &(keys[(total_input_runs_count++) * (inputs->key_element_count)]);

			// revise materialized keys before pushing it to mergeable_open_runs
			revise_materialized_keys_in_interim_tuple_store(o, its_p);

			push_to_pheap(&mergeable_open_runs, its_p);
			total_output_size_in_bytes += get_total_bytes_in_interim_tuple_store(its_p);
		}
	}

	// create output run
	interim_tuple_store* output_its_p = get_new_interim_tuple_store(total_output_size_in_bytes);

	// merge all one by one from the top into output_its_p or produce them if possible
	while(!is_empty_pheap(&mergeable_open_runs) && can_decrement_tuples_down_counter(&result_counter))
	{
		// every 1000 tuples, make sue that the operator is not killed
		if(((output_its_p->tuples_count % 1000) == 0) && can_not_proceed_for_execution_operator(o))
			break;

		interim_tuple_store* its_p = (interim_tuple_store*) get_top_of_pheap(&mergeable_open_runs);

		// copy the top tuple of its_p into (append it to) output_its_p
		decrement_tuples_down_counter(&result_counter);
		append_tuple_to_interim_tuple_store2(output_its_p, &(output_its_p->embed_regions[0]), its_p->embed_regions[0].tuple, &(inputs->record_def->size_def), inputs->minimum_run_size);

		// go next on its_p, and insert it back into mergeable_open_runs
		{
			uint64_t next_tuple_offset = next_tuple_offset_for_interim_tuple_region(&(its_p->embed_regions[0]));
			if(!mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), next_tuple_offset, &(inputs->record_def->size_def), inputs->minimum_run_size))
			{
				// implies there are no more tuples

				total_input_runs_count--;
				remove_from_pheap(&mergeable_open_runs, its_p);

				unmap_all_embed_regions_in_interim_tuple_store(its_p);
				delete_interim_tuple_store(its_p);
			}
			else
			{
				// revise materialized keys before heapify-ing it in mergeable_open_runs
				revise_materialized_keys_in_interim_tuple_store(o, its_p);

				heapify_for_in_pheap(&mergeable_open_runs, its_p);
			}
		}
	}

	// destroy if any is still remaining
	if(!is_empty_pheap(&mergeable_open_runs))
		remove_all_from_pheap(&mergeable_open_runs, DELETE_ON_NOTIFY_FOR_INTERIM_TUPLE_STORE);

	// free the keys
	free(keys);

	// unmap the write-side region
	unmap_all_embed_regions_in_interim_tuple_store(output_its_p);

	// insert sorted run back into the sorted_runs[level_for_merged_run]
	// also add input_param back to free list
	pthread_mutex_lock(&(inputs->runs_lock));
	remove_from_linkedlist(&(inputs->job_param_list), input_param);
	inputs->total_sorted_runs_count += 1;
	push_run_in_tuple_runs(&(inputs->sorted_runs[input_param->level_for_merged_run]), output_its_p);
	insert_tail_in_linkedlist(&(inputs->job_param_free_list), input_param);
	inputs->total_concurrent_jobs_count--;
	pthread_mutex_unlock(&(inputs->runs_lock));

	// request some new jobs to start
	request_to_process_some_jobs(o);
}

static void merge_into_produce_job(operator* o, void* param)
{
	input_values* inputs = o->inputs;
	tuples_down_counter result_counter = inputs->result_counter;

	tuple_runs* input_param = param;

	// malloc keys a datum[], to be used by all the runs existing in pheap until they are all merged and destroyed
	datum* keys = malloc(inputs->N_way_sort * inputs->key_element_count * sizeof(datum));

	// open runs
	pheap mergeable_open_runs;
	initialize_pheap(&mergeable_open_runs, MIN_HEAP, LEFTIST, &contexted_comparator(o, compare_interim_tuple_stores_for_pheap_runs), offsetof(interim_tuple_store, embed_node_php));

	uint64_t total_input_runs_count = 0;

	// populate runs in mergeable_open_runs
	for(interim_tuple_store* its_p = pop_run_from_tuple_runs(input_param); its_p != NULL; its_p = pop_run_from_tuple_runs(input_param))
	{
		if(!mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), 0, &(inputs->record_def->size_def), inputs->minimum_run_size))
			delete_interim_tuple_store(its_p);
		else
		{
			// comes here only if mmap succeeds, will happen always, unless the run is empty

			// embed_ptrs[0] is used to store the materialized keys
			its_p->embed_ptrs[0] = &(keys[(total_input_runs_count++) * (inputs->key_element_count)]);

			// revise materialized keys before pushing it to mergeable_open_runs
			revise_materialized_keys_in_interim_tuple_store(o, its_p);

			push_to_pheap(&mergeable_open_runs, its_p);
		}
	}

	int produce_failed = 0;

	// merge all one by one from the top into output_its_p or produce them if possible
	while(!is_empty_pheap(&mergeable_open_runs) && can_decrement_tuples_down_counter(&result_counter))
	{
		interim_tuple_store* its_p = (interim_tuple_store*) get_top_of_pheap(&mergeable_open_runs);

		decrement_tuples_down_counter(&result_counter);
		if(!produce_tuple_from_operator(o, its_p->embed_regions[0].tuple))
		{
			produce_failed = 1;
			break;
		}

		// go next on its_p, and insert it back into mergeable_open_runs
		{
			uint64_t next_tuple_offset = next_tuple_offset_for_interim_tuple_region(&(its_p->embed_regions[0]));
			if(!mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), next_tuple_offset, &(inputs->record_def->size_def), inputs->minimum_run_size))
			{
				// implies there are no more tuples

				total_input_runs_count--;
				remove_from_pheap(&mergeable_open_runs, its_p);

				unmap_all_embed_regions_in_interim_tuple_store(its_p);
				delete_interim_tuple_store(its_p);
			}
			else
			{
				// revise materialized keys before heapify-ing it in mergeable_open_runs
				revise_materialized_keys_in_interim_tuple_store(o, its_p);

				heapify_for_in_pheap(&mergeable_open_runs, its_p);
			}
		}
	}

	// destroy if any is still remaining
	if(!is_empty_pheap(&mergeable_open_runs))
		remove_all_from_pheap(&mergeable_open_runs, DELETE_ON_NOTIFY_FOR_INTERIM_TUPLE_STORE);

	// free the keys
	free(keys);

	// add input_param back to free list
	pthread_mutex_lock(&(inputs->runs_lock));
	remove_from_linkedlist(&(inputs->job_param_list), input_param);
	insert_tail_in_linkedlist(&(inputs->job_param_free_list), input_param);
	inputs->total_concurrent_jobs_count--;
	pthread_mutex_unlock(&(inputs->runs_lock));

	if(produce_failed)
		kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
	else
		kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));

	// this would always be the final job, so no need to request for any more jobs
}

static void merge_job(operator* o, void* param)
{
	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->runs_lock));
	int can_directly_produce = (inputs->total_concurrent_jobs_count == 1) && (inputs->flag_no_new_un_sorted_runs) && (inputs->un_sorted_runs.runs_count == 0) && (inputs->total_sorted_runs_count == 0);
	pthread_mutex_unlock(&(inputs->runs_lock));

	if(can_directly_produce)
		merge_into_produce_job(o, param);
	else
		merge_into_run_job(o, param);
}

static void produce_job(operator* o, void* param)
{
	input_values* inputs = o->inputs;
	tuples_down_counter result_counter = inputs->result_counter;

	tuple_runs* input_param = param;

	int produce_failed = 0;

	// produce tuple in all the runs
	for(interim_tuple_store* its_p = pop_run_from_tuple_runs(input_param); its_p != NULL && can_decrement_tuples_down_counter(&result_counter); its_p = pop_run_from_tuple_runs(input_param))
	{
		{
			FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(tuple, tuple_index, tuple_offset, &(inputs->record_def->size_def), its_p, inputs->minimum_run_size, {
				if(!decrement_tuples_down_counter(&result_counter)) // try and decrement, if it fails nothing to be produced any further
					break;
				if(!produce_tuple_from_operator(o, tuple))
				{
					produce_failed = 1;
					break;
				}
			})
		}
		delete_interim_tuple_store(its_p);
		if(produce_failed)
			break;
	}

	delete_all_runs_in_tuple_runs(input_param);

	// mark job completed
	// also add input_param back to free list
	pthread_mutex_lock(&(inputs->runs_lock));
	remove_from_linkedlist(&(inputs->job_param_list), input_param);
	insert_tail_in_linkedlist(&(inputs->job_param_free_list), input_param);
	inputs->total_concurrent_jobs_count--;
	pthread_mutex_unlock(&(inputs->runs_lock));

	if(!produce_failed)
		kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
	else
		kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));

	// this would always be the final job, so no need to request for any more jobs
}

static void request_to_process_some_jobs(operator* o)
{
	if(can_not_proceed_for_execution_operator(o))
		return;

	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->runs_lock));

	if(inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count)
	{
		for(int t = 0; t < inputs->un_sorted_runs.runs_count
			&& (inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count); t++)
		{
			tuple_runs* input_param = (tuple_runs*) get_head_of_linkedlist(&(inputs->job_param_free_list));
			remove_from_linkedlist(&(inputs->job_param_free_list), input_param);
			insert_tail_in_linkedlist(&(inputs->job_param_list), input_param);

			interim_tuple_store* its_p = pop_run_from_tuple_runs(&(inputs->un_sorted_runs));
			push_run_in_tuple_runs(input_param, its_p);
			if(!run_concurrent_job_for_operator(o, input_param, sort_job) && can_not_proceed_for_execution_operator(o))
				goto EXIT;
			inputs->total_concurrent_jobs_count++;
		}
	}

	if(inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count)
	{
		for(int i = 0; i < MAX_LEVELS && (inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count); i++)
		{
			for(int t = 0; t < UINT_ALIGN_DOWN(inputs->sorted_runs[i].runs_count, inputs->N_way_sort) / inputs->N_way_sort
				&& (inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count); t++)
			{
				tuple_runs* input_param = (tuple_runs*) get_head_of_linkedlist(&(inputs->job_param_free_list));
				remove_from_linkedlist(&(inputs->job_param_free_list), input_param);
				insert_tail_in_linkedlist(&(inputs->job_param_list), input_param);

				input_param->level_for_merged_run = i+1;
				for(int r = 0; r < inputs->N_way_sort; r++)
				{
					interim_tuple_store* its_p = pop_run_from_tuple_runs(&(inputs->sorted_runs[i]));
					inputs->total_sorted_runs_count--;
					push_run_in_tuple_runs(input_param, its_p);
				}
				if(!run_concurrent_job_for_operator(o, input_param, merge_job) && can_not_proceed_for_execution_operator(o))
					goto EXIT;
				inputs->total_concurrent_jobs_count++;
			}
		}
	}

	if(inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count)
	{
		if(inputs->flag_no_new_un_sorted_runs && inputs->total_sorted_runs_count > 1)
		{
			// current level
			int i = 0;
			tuple_runs* input_param = NULL;
			while(inputs->total_concurrent_jobs_count < inputs->max_concurrent_jobs_count && i < MAX_LEVELS)
			{
				if(inputs->sorted_runs[i].runs_count == 0)
				{
					i++;
					continue;
				}
				if(input_param == NULL)
				{
					input_param = (tuple_runs*) get_head_of_linkedlist(&(inputs->job_param_free_list));
					remove_from_linkedlist(&(inputs->job_param_free_list), input_param);
					insert_tail_in_linkedlist(&(inputs->job_param_list), input_param);
				}
				if(input_param->runs_count == inputs->N_way_sort)
				{
					if(!run_concurrent_job_for_operator(o, input_param, merge_job) && can_not_proceed_for_execution_operator(o))
						goto EXIT;
					inputs->total_concurrent_jobs_count++;
					input_param = NULL;
				}
				else
				{
					interim_tuple_store* its_p = pop_run_from_tuple_runs(&(inputs->sorted_runs[i]));
					inputs->total_sorted_runs_count--;
					push_run_in_tuple_runs(input_param, its_p);
					input_param->level_for_merged_run = i+1;
				}
			}

			if(input_param != NULL)
			{
				if(!run_concurrent_job_for_operator(o, input_param, merge_job) && can_not_proceed_for_execution_operator(o))
					goto EXIT;
				inputs->total_concurrent_jobs_count++;
				input_param = NULL;
			}
		}
	}

	if(inputs->flag_no_new_un_sorted_runs && inputs->total_concurrent_jobs_count == 0 && inputs->total_sorted_runs_count <= 1)
	{
		tuple_runs* input_param = (tuple_runs*) get_head_of_linkedlist(&(inputs->job_param_free_list));
		remove_from_linkedlist(&(inputs->job_param_free_list), input_param);
		insert_tail_in_linkedlist(&(inputs->job_param_list), input_param);

		for(int i = 0; i < MAX_LEVELS; i++)
		{
			inputs->total_sorted_runs_count -=  inputs->sorted_runs[i].runs_count;
			push_all_runs_in_tuple_runs(input_param, &(inputs->sorted_runs[i]));
		}

		if(!run_concurrent_job_for_operator(o, input_param, produce_job) && can_not_proceed_for_execution_operator(o))
			goto EXIT;
		inputs->total_concurrent_jobs_count++;
		input_param = NULL;
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
			// destroy input_iterator, this is success case
			{
				destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;
			}

			// its ownership for inputs->input_un_sorted_run, is changing, so unmap it's embed_regions
			if(inputs->input_un_sorted_run != NULL)
				unmap_all_embed_regions_in_interim_tuple_store(inputs->input_un_sorted_run);

			pthread_mutex_lock(&(inputs->runs_lock));
			if(inputs->input_un_sorted_run != NULL)
				push_run_in_tuple_runs(&(inputs->un_sorted_runs), inputs->input_un_sorted_run);
			inputs->flag_no_new_un_sorted_runs = 1;
			pthread_mutex_unlock(&(inputs->runs_lock));
			inputs->input_un_sorted_run = NULL;

			// only after creation of a new unsorted run, is we will need to request for more jobs
			request_to_process_some_jobs(o);
			return;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			destroy_consumption_iterator(inputs->input_iterator); inputs->input_iterator = NULL;

			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		if(tuple != NULL)
		{
			if(inputs->input_un_sorted_run == NULL)
				inputs->input_un_sorted_run = get_new_interim_tuple_store(inputs->minimum_run_size);

			append_tuple_to_interim_tuple_store2(inputs->input_un_sorted_run, &(inputs->input_un_sorted_run->embed_regions[0]), (void*)tuple, &(inputs->record_def->size_def), inputs->minimum_run_size);

			if(get_total_bytes_in_interim_tuple_store(inputs->input_un_sorted_run) >= inputs->minimum_run_size)
			{
				// its ownership for inputs->input_un_sorted_run, is changing, so unmap it's embed_regions
				unmap_all_embed_regions_in_interim_tuple_store(inputs->input_un_sorted_run);

				pthread_mutex_lock(&(inputs->runs_lock));
				push_run_in_tuple_runs(&(inputs->un_sorted_runs), inputs->input_un_sorted_run);
				pthread_mutex_unlock(&(inputs->runs_lock));
				inputs->input_un_sorted_run = NULL;

				// only after creation of a new unsorted run, is we will need to request for more jobs
				request_to_process_some_jobs(o);
			}

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

	for(interim_tuple_store* its_p = pop_run_from_tuple_runs(&(inputs->un_sorted_runs)); its_p != NULL; its_p = pop_run_from_tuple_runs(&(inputs->un_sorted_runs)))
		delete_interim_tuple_store(its_p);

	for(int i = 0; i < MAX_LEVELS; i++)
		for(interim_tuple_store* its_p = pop_run_from_tuple_runs(&(inputs->sorted_runs[i])); its_p != NULL; its_p = pop_run_from_tuple_runs(&(inputs->sorted_runs[i])))
			delete_interim_tuple_store(its_p);
		
	while(!is_empty_linkedlist(&(inputs->job_param_list)))
	{
		tuple_runs* truns_p = (tuple_runs*) get_head_of_linkedlist(&(inputs->job_param_list));
		remove_from_linkedlist(&(inputs->job_param_list), truns_p);
		delete_all_runs_in_tuple_runs(truns_p);
		free(truns_p);
	}

	while(!is_empty_linkedlist(&(inputs->job_param_free_list)))
	{
		tuple_runs* truns_p = (tuple_runs*) get_head_of_linkedlist(&(inputs->job_param_free_list));
		remove_from_linkedlist(&(inputs->job_param_free_list), truns_p);
		free(truns_p);
	}

	pthread_mutex_destroy(&(inputs->runs_lock));

	free(inputs->key_dtis);
	free(inputs);
}

#include<rhendb/tuple_transformers.h>

void setup_external_sort_operator(operator* o, tuples_down_counter result_counter, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, const compare_direction* key_compare_direction, uint64_t minimum_run_size, uint32_t N_way_sort, uint32_t max_concurrent_jobs_count)
{
	if(is_zero_tuples_down_counter(&result_counter))
	{
		printf("result_counter can not be zero for external sort operator\n");
		exit(-1);
	}

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = free_resources;

	// it is an identity operator, produces the same thing as it consumes
	init_tuple_transformers(&(o->output_tuple_transformers), get_tuple_def_for_tuples_to_be_consumed_from(input_operator));

	o->inputs = malloc(sizeof(input_values));
	input_values* inputs = o->inputs;
	*inputs = (input_values){
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.input_un_sorted_run = NULL,
		.record_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator),
		.key_element_count = key_element_count,
		.key_element_ids = key_element_ids,
		.key_compare_direction = key_compare_direction,
		.key_dtis = malloc(sizeof(data_type_info*) * key_element_count),
		.minimum_run_size = minimum_run_size,
		.N_way_sort = N_way_sort,
		.max_concurrent_jobs_count = max_concurrent_jobs_count,
		.result_counter = result_counter,
		.runs_lock = PTHREAD_MUTEX_INITIALIZER,
		.flag_no_new_un_sorted_runs = 0,
		.total_sorted_runs_count = 0,
		.total_concurrent_jobs_count = 0,
	};

	for(uint32_t j = 0; j < key_element_count; j++)
		inputs->key_dtis[j] = get_type_info_for_element_from_tuple_def(inputs->record_def, key_element_ids[j]);

	initialize_tuple_runs(&(inputs->un_sorted_runs));
	for(int i = 0; i < MAX_LEVELS; i++)
		initialize_tuple_runs(&(inputs->sorted_runs[i]));
	initialize_linkedlist(&(inputs->job_param_list), offsetof(tuple_runs, embed_node));
	initialize_linkedlist(&(inputs->job_param_free_list), offsetof(tuple_runs, embed_node));

	for(uint32_t i = 0; i < max_concurrent_jobs_count; i++)
	{
		tuple_runs* truns_p = malloc(sizeof(tuple_runs));
		initialize_tuple_runs(truns_p);
		insert_tail_in_linkedlist(&(inputs->job_param_free_list), truns_p);
	}
}