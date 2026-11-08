#include<rhendb/query_plan.h>

#include<rhendb/function_compare.h>

#include<rhendb/interim_tuple_store_sort.h>

#include<rhendb/tuples_down_counter.h>

#define MAX_LEVELS 70

typedef struct op_state op_state;
struct op_state
{
	operator* input_operator;

	// a run that is still accumulating tuples, when it reaches or exceeds min_run_size, it is sorted and pushed into sorted_runs at log(unsorted_run->next_tuple_offset)/log(2) position
	interim_tuple_store* unsorted_run;

	// MAX_LEVELS number of linkedlist-s of sorted interim_tuple_storel-s
	linkedlist sorted_runs[MAX_LEVELS];
	uint64_t sorted_runs_count[MAX_LEVELS];

	uint64_t total_sorted_runs_count;

	interim_tuple_store* output_run; // NULL until all the tuples are produced

	// derieved information
	const data_type_info** key_dtis;
};

// returns the level it was put in
static uint32_t sort_unsorted_run_and_produce_sorted_run(operator* o)
{
	op_state* this_state = o->state;

	tuples_down_counter result_counter = o->desc->params.sort.result_counter;

	interim_tuple_store* ots_p = sort_interim_tuples(this_state->unsorted_run, result_counter, o->desc->output_tuple_def, o->desc->params.sort.key_element_ids, o->desc->params.sort.key_compare_direction, o->desc->params.sort.key_element_count, o->self_query_plan->desc->curr_tx);

	delete_interim_tuple_store(this_state->unsorted_run);
	this_state->unsorted_run = NULL;

	uint32_t level = log2((double)get_total_bytes_in_interim_tuple_store(ots_p));

	insert_tail_in_linkedlist(&(this_state->sorted_runs[level]), ots_p);
	this_state->sorted_runs_count[level]++;
	this_state->total_sorted_runs_count++;

	return level;
}

// we materialize, the keys in its_p->embed_ptrs[0], using it as datum[] having key_element_count elements long
static void revise_materialized_keys_in_interim_tuple_store(operator* o, interim_tuple_store* its_p)
{
	for(uint32_t j = 0; j < o->desc->params.sort.key_element_count; j++)
	{
		const data_type_info* dti_temp;
		if(!get_value_from_element_from_tuple(&(((datum*)(its_p->embed_ptrs[0]))[j]), &dti_temp, o->desc->output_tuple_def, o->desc->params.sort.key_element_ids[j], its_p->embed_regions[0].tuple))
			((datum*)(its_p->embed_ptrs[0]))[j] = (*NULL_DATUM);
	}
}

// comparator to compare materialized tuples at its_p->embed_ptrs[0]
static int compare_interim_tuple_stores_for_pheap_runs(const void* o_vp, const void* its1_vp, const void* its2_vp)
{
	const operator* o = o_vp;
	const op_state* this_state = o->state;

	const interim_tuple_store* its1_p = its1_vp;
	const interim_tuple_store* its2_p = its2_vp;

	return compare_datums3_rhendb(its1_p->embed_ptrs[0], its2_p->embed_ptrs[0], this_state->key_dtis, o->desc->params.sort.key_compare_direction, o->desc->params.sort.key_element_count, o->self_query_plan->desc->curr_tx);
}

// returns the level output was put in
// on_success makes input_runs an empty linkedlist
static uint64_t merge_runs_into_run_job(operator* o, linkedlist* input_runs)
{
	op_state* this_state = o->state;

	tuples_down_counter result_counter = o->desc->params.sort.result_counter;

	// malloc keys a datum[], to be used by all the runs existing in pheap until they are all merged and destroyed
	datum* keys = malloc(o->desc->params.sort.N_way_sort * o->desc->params.sort.key_element_count * sizeof(datum));

	// open runs
	pheap mergeable_open_runs;
	initialize_pheap(&mergeable_open_runs, MIN_HEAP, LEFTIST, &contexted_comparator(o, compare_interim_tuple_stores_for_pheap_runs), offsetof(interim_tuple_store, embed_node_php));

	uint64_t total_output_size_in_bytes = 0;
	uint64_t total_input_runs_count = 0;

	// populate runs in mergeable_open_runs
	while(!is_empty_linkedlist(input_runs))
	{
		interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_linkedlist(input_runs);
		remove_head_from_linkedlist(input_runs);

		if(!mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), 0, &(o->desc->output_tuple_def->size_def), o->desc->params.sort.min_run_size))
			delete_interim_tuple_store(its_p);
		else
		{
			// comes here only if mmap succeeds, will happen always, unless the run is empty

			// embed_ptrs[0] is used to store the materialized keys
			its_p->embed_ptrs[0] = &(keys[(total_input_runs_count++) * (o->desc->params.sort.key_element_count)]);

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
		interim_tuple_store* its_p = (interim_tuple_store*) get_top_of_pheap(&mergeable_open_runs);

		// copy the top tuple of its_p into (append it to) output_its_p
		decrement_tuples_down_counter(&result_counter);
		append_tuple_to_interim_tuple_store2(output_its_p, &(output_its_p->embed_regions[0]), its_p->embed_regions[0].tuple, &(o->desc->output_tuple_def->size_def), o->desc->params.sort.min_run_size);

		// go next on its_p, and insert it back into mergeable_open_runs
		{
			uint64_t next_tuple_offset = next_tuple_offset_for_interim_tuple_region(&(its_p->embed_regions[0]));
			if(!mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), next_tuple_offset, &(o->desc->output_tuple_def->size_def), o->desc->params.sort.min_run_size))
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

	uint32_t level = log2(get_total_bytes_in_interim_tuple_store(output_its_p));

	insert_tail_in_linkedlist(&(this_state->sorted_runs[level]), output_its_p);
	this_state->sorted_runs_count[level]++;
	this_state->total_sorted_runs_count++;

	return level;
}

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	if(this_state->output_run == NULL)
	{
		while(1)
		{
			// get next tuple until we get tuples from input_operator
			// if there is no more tuple break out of the loop
			// on error, destroy state and exit

			// insert it in unsorted_run

			// if the unsorted run exceeds make it sorted and record it level

			// not run a merging loop and keep updating the level, until the level in context has more than or equal to the N_way_sort number of runs
		}

		// if no tuple was ever produced (this_state->total_sorted_runs_count == 0 and unsorted_run == NULL), destroy and exit

		while(this_state->total_sorted_runs_count > 1)
		{
			// now run a loop on all levels, and put then in linkedlist and ask to be merged
		}

		// make sure you noted down the final run, and move it into the output_run

		// store 0 into the output_run->embed_uints = 0, using it as next offset to produce the tuple
	}

	// copy the return the next tuple in output_run and advance the offset
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		if(this_state->unsorted_run != NULL)
			delete_interim_tuple_store(this_state->unsorted_run);

		for(int i = 0; i < MAX_LEVELS; i++)
			while(!is_empty_linkedlist(&(this_state->sorted_runs[i])))
			{
				interim_tuple_store* its_p = (interim_tuple_store*) get_head_of_linkedlist(&(this_state->sorted_runs[i]));
				remove_head_from_linkedlist(&(this_state->sorted_runs[i]));
				delete_interim_tuple_store(its_p);
			}

		if(this_state->output_run != NULL)
			delete_interim_tuple_store(this_state->output_run);

		free(this_state->key_dtis);
	}
}

void open_sort_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.sort.input_operator_index);

	this_state->unsorted_run = NULL;

	for(uint32_t i = 0; i < MAX_LEVELS; i++)
	{
		initialize_linkedlist(&(this_state->sorted_runs[i]), offsetof(interim_tuple_store, embed_node_ll));
		this_state->sorted_runs_count[i] = 0;
	}

	this_state->total_sorted_runs_count = 0;

	this_state->output_run = NULL;

	for(uint32_t j = 0; j < o->desc->params.sort.key_element_count; j++)
		this_state->key_dtis[j] = get_type_info_for_element_from_tuple_def(o->desc->output_tuple_def, o->desc->params.sort.key_element_ids[j]);

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}