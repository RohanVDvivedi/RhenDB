#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<rhendb/mvcc_header.h>

#include<rhendb/table_operator_output_type.h>
#include<rhendb/fetched_table.h>

#include<tupleindexer/heap_page/heap_page.h>
#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/utils/heap_table_accumulative_notifier.h>

#include<pthread.h>
#include<stdlib.h>
#include<string.h>

typedef struct input_values input_values;
struct input_values
{
	// the table being scanned, all of its partitions are scanned, from the first to the last one
	const fetched_table* ftabl;

	// number of jobs that may scan the partitions of this table concurrently, one job is assigned
	// to atmost one partition at any point of time
	uint32_t max_concurrent_jobs_count;

	// partition_pos is the position of the next partition that is yet to be picked up by a scan job,
	// it starts at 0 and is incremented until it reaches the partitions_count of the ftabl,
	pthread_mutex_t partition_pos_lock; // also protects active_scan_job_count
	uint64_t partition_pos;
	uint32_t active_scan_job_count;

	int scan_jobs_started; // this flag will be set if the scan jobs were started by the execute function

	int output_flags;

	const tuple_def* output_tuple_def;

	int additional_flags; // same flags as given in transaction.h, toggles the additional book keeping this operator performs

	transaction* tx;
};

// produces the output tuple for a single visible tuple of the partition at partition_index_in_info,
// returns 0, only if the tuple could not be produced, in which case the caller must kill itself
static int produce_output_for_scanned_tuple(operator* o, uint64_t partition_index_in_info, tuple_pointer tptr, const void* heap_record)
{
	input_values* inputs = o->inputs;

	rage_engine* engine = &(inputs->tx->rdb->persistent_acid_rage_engine);

	if(inputs->output_flags == 0)
		return 1;

	uint32_t output_tuple_size = get_minimum_tuple_size(inputs->output_tuple_def);
	uint64_t output_tuple_capacity = output_tuple_size;
	void* output_tuple = malloc(output_tuple_capacity);
	init_tuple(inputs->output_tuple_def, output_tuple);

	uint32_t attr_index = 0;

	if(MUST_OUTPUT_TABLE_ID(inputs->output_flags))
	{
		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = inputs->ftabl->table_info.id}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

		attr_index++;
	}

	if(MUST_OUTPUT_PARTITION_ID(inputs->output_flags))
	{
		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = inputs->ftabl->table_partitions_info[partition_index_in_info].partition_id}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

		attr_index++;
	}

	if(MUST_OUTPUT_TUPLE_POINTER_ID(inputs->output_flags))
	{
		char tptr_tpl[sizeof(tuple_pointer)];
		set_tuple_pointer(tptr_tpl, tptr, &(engine->pam_p->pas));

		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.tuple_value = tptr_tpl}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);

		attr_index++;
	}

	if(MUST_OUTPUT_HEAP_TUPLE(inputs->output_flags))
	{
		void* final_readers_heap_record = project_to_final_readers_tuple_def(inputs->ftabl, heap_record, partition_index_in_info);

		// ensure there are enough bytes in the output_tuple, as we try to insert this datum
		while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.tuple_value = final_readers_heap_record}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(inputs->output_tuple_def, output_tuple);
		free(final_readers_heap_record);

		attr_index++;
	}

	int produced = produce_tuple_from_operator(o, output_tuple);
	free(output_tuple);

	return produced;
}

// scans every tuple of the partition at partition_index_in_info, producing only the ones that are visible to the snapshot of this transaction,
// returns 0, only if this partition could not be scanned completely, in which case the caller must kill itself
static int scan_partition(operator* o, uint64_t partition_index_in_info)
{
	input_values* inputs = o->inputs;

	rage_engine* engine = &(inputs->tx->rdb->persistent_acid_rage_engine);

	const rhendb_table_partition* table_partition = &(inputs->ftabl->table_partitions_info[partition_index_in_info]);
	const tuple_def* partition_tuple_def = &(inputs->ftabl->table_partition_tuple_defs[partition_index_in_info]);

	// the mvcc_header is always the first element of the record of any partition
	tuple_def mvcc_def;
	initialize_tuple_def(&mvcc_def, (data_type_info*)(partition_tuple_def->type_info->containees[0].al.type_info));

	// only the tuples of the last partition may have been inserted by this very query,
	// the older partitions can not be inserted into, hence they never need this check
	int must_skip_self_inserted_tuples = (partition_index_in_info == (inputs->ftabl->partitions_count - 1)) && IS_RESCAN_PROTECTION_ENABLED(inputs->additional_flags);

	// the heap_table_tuple_defs of this very partition, every job builds its own for the partition it scans
	heap_table_tuple_defs httd;
	init_heap_table_tuple_definitions(&httd, &(engine->pam_p->pas), partition_tuple_def);

	int abort_error = 0;

	heap_table_iterator* hti_p = get_new_heap_table_iterator(table_partition->heap_root_page_id, 0, 0, &httd, engine->pam_p, NULL, &abort_error);
	if(abort_error)
		goto ABORT_ERROR;

	while(1)
	{
		uint32_t unused_space = 0;
		int entry_needs_fixing = 0;
		persistent_page ppage = lock_and_get_curr_heap_page_heap_table_iterator(hti_p, 0 /* READ_LOCK */, &unused_space, &entry_needs_fixing, NULL, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		// no more heap pages left in this partition
		if(is_persistent_page_NULL(&ppage, engine->pam_p))
			break;

		uint32_t tuple_count = get_tuple_count_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(partition_tuple_def->size_def));

		for(uint32_t tuple_index = 0; tuple_index < tuple_count; tuple_index++)
		{
			// a tomb stone, there is no tuple here to be scanned
			if(!exists_tuple_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(partition_tuple_def->size_def), tuple_index))
				continue;

			const void* heap_record = get_nth_tuple_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(partition_tuple_def->size_def), tuple_index);
			if(heap_record == NULL)
				continue;

			// every tuple must pass the mvcc visibility check, before it is even projected for the output
			{
				datum mvcc_hdr_datum;
				get_value_from_element_from_tuple(&mvcc_hdr_datum, partition_tuple_def, STATIC_POSITION(0), heap_record);

				mvcc_header mvcchdr;
				read_mvcc_header(&mvcchdr, mvcc_hdr_datum.tuple_value, &mvcc_def);

				int were_hints_updated = 0;
				if(!is_tuple_visible_to_mvcc_snapshot(inputs->tx->snapshot, &mvcchdr, &(inputs->tx->rdb->tsg), &were_hints_updated))
					continue;
			}

			tuple_pointer tptr = (tuple_pointer){.page_id = ppage.page_id, .tuple_index = tuple_index};

			// a tuple that this very query inserted, must not be rescanned
			if(must_skip_self_inserted_tuples && was_registered_as_inserted_tuple_pointer(inputs->tx, tptr))
				continue;

			if(!produce_output_for_scanned_tuple(o, partition_index_in_info, tptr, heap_record))
			{
				kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
				release_lock_on_persistent_page(engine->pam_p, NULL, &ppage, NONE_OPTION, &abort_error);
				delete_heap_table_iterator(hti_p, NULL, &abort_error);
				return 0;
			}
		}

		release_lock_on_persistent_page(engine->pam_p, NULL, &ppage, NONE_OPTION, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		int went_next = next_heap_table_iterator(hti_p, NULL, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;
		if(!went_next)
			break;
	}

	delete_heap_table_iterator(hti_p, NULL, &abort_error);
	if(abort_error)
		goto ABORT_ERROR;

	deinit_heap_table_tuple_definitions(&httd);

	return 1;

	ABORT_ERROR:;

	kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("scanner_read_only_mini_tx_aborted"));

	if(hti_p != NULL)
	{
		delete_heap_table_iterator(hti_p, NULL, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;
	}

	deinit_heap_table_tuple_definitions(&httd);

	return 0;
}

// a single job, it keeps picking up the next partition that is yet to be scanned, until there are none left
static void scan_partitions_job(operator* o, void* param)
{
	input_values* inputs = o->inputs;

	while(1)
	{
		if(can_not_proceed_for_execution_operator(o))
			break;

		// pick up the next partition that no other job has picked up yet
		pthread_mutex_lock(&(inputs->partition_pos_lock));
		uint64_t partition_index_in_info = inputs->partition_pos;
		if(partition_index_in_info < inputs->ftabl->partitions_count)
			inputs->partition_pos++;
		pthread_mutex_unlock(&(inputs->partition_pos_lock));

		// all the partitions have been picked up already
		if(partition_index_in_info >= inputs->ftabl->partitions_count)
			break;

		if(!scan_partition(o, partition_index_in_info))
			break;
	}

	// this job is done scanning, if it was the last one, the operator must be woken up to kill itself
	pthread_mutex_lock(&(inputs->partition_pos_lock));
	inputs->active_scan_job_count--;
	int wake_up_operator = (inputs->active_scan_job_count == 0);
	pthread_mutex_unlock(&(inputs->partition_pos_lock));

	if(wake_up_operator)
		trigger_execution_on_operator(o);
}

static void start_scan_jobs(operator* o)
{
	input_values* inputs = o->inputs;

	pthread_mutex_lock(&(inputs->partition_pos_lock));
	if(!(inputs->scan_jobs_started))
	{
		inputs->scan_jobs_started = 1;

		// one job per partition, but never more than max_concurrent_jobs_count of them
		uint32_t new_scan_jobs = min(inputs->max_concurrent_jobs_count, inputs->ftabl->partitions_count);
		while(new_scan_jobs > 0)
		{
			if(!run_concurrent_job_for_operator(o, NULL, scan_partitions_job))
				break;
			inputs->active_scan_job_count++;
			new_scan_jobs--;
		}
	}
	pthread_mutex_unlock(&(inputs->partition_pos_lock));
}

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	// on the very first execution, spawn the jobs that will do the scanning
	if(!(inputs->scan_jobs_started))
		start_scan_jobs(o);
	else
	{
		// the operator is woken up by every job that finishes, it may only kill itself once all of them are done
		pthread_mutex_lock(&(inputs->partition_pos_lock));
		uint32_t active_scan_job_count = inputs->active_scan_job_count;
		pthread_mutex_unlock(&(inputs->partition_pos_lock));

		if(active_scan_job_count == 0)
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
	}

	return ;
}

static void clean_up_resources(operator* o)
{
	input_values* inputs = o->inputs;

	pthread_mutex_destroy(&(inputs->partition_pos_lock));
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->output_tuple_def != NULL)
	{
		// the heap tuple of the output is the final_readers_tuple_def of the ftabl, and it is not owned by us
		free((void*)(inputs->output_tuple_def->type_info));
		free((void*)(inputs->output_tuple_def));
		inputs->output_tuple_def = NULL;
	}

	free(inputs);
}

operator_resource_counter setup_scan_operator(operator* o, query_plan* qp, const fetched_table* ftabl, uint32_t max_concurrent_jobs_count, int output_flags, int additional_flags)
{
	transaction* tx = qp->curr_tx;

	if(tx->snapshot == NULL)
	{
		printf("must have a snapshot for scan_operator\n");
		exit(-1);
	}

	if(max_concurrent_jobs_count == 0)
	{
		printf("max_concurrent_jobs_count can not be 0 for scan_operator\n");
		exit(-1);
	}

	// we can not scan more than partition number of tables at once
	max_concurrent_jobs_count = min(max_concurrent_jobs_count, ftabl->partitions_count);

	// there is latch crabbing at every point hence 2 buffers per worker are needed
	operator_resource_counter result = {.buffer_counter = 2 * max_concurrent_jobs_count, .job_counter = max_concurrent_jobs_count, .thread_counter = max_concurrent_jobs_count};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = free_resources;

	tuple_def* output_tuple_def = NULL;
	if(output_flags != 0)
	{
		output_tuple_def = malloc(sizeof(tuple_def));
		data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(10)); // over allocation for being safe from future changes

		uint32_t output_dti_element_count = 0;
		uint32_t output_tuple_max_size = 8;

		if(MUST_OUTPUT_TABLE_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "table_id", 64);
			output_dti->containees[output_dti_element_count].al.type_info = UINT_NON_NULLABLE[8];

			output_tuple_max_size += 8;
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_PARTITION_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "partition_id", 64);
			output_dti->containees[output_dti_element_count].al.type_info = UINT_NON_NULLABLE[8];

			output_tuple_max_size += 8;
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_TUPLE_POINTER_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "tuple_pointer", 64);
			output_dti->containees[output_dti_element_count].al.type_info = &(tx->rdb->persistent_acid_rage_engine.pam_p->pas.tuple_pointer_type_info);

			output_tuple_max_size += sizeof(tuple_pointer);
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_HEAP_TUPLE(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, ftabl->table_info.name, 64);
			output_dti->containees[output_dti_element_count].al.type_info = ftabl->final_readers_tuple_def.type_info;

			output_tuple_max_size += 8 + get_maximum_tuple_size(&(ftabl->final_readers_tuple_def));
			output_dti_element_count++;
		}

		initialize_tuple_data_type_info(output_dti, "scanned_tuple_context", 0, output_tuple_max_size, output_dti_element_count);

		initialize_tuple_def(output_tuple_def, output_dti);
	}

	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.ftabl = ftabl,
		.max_concurrent_jobs_count = max_concurrent_jobs_count,
		.partition_pos = 0,
		.active_scan_job_count = 0,
		.scan_jobs_started = 0,
		.output_flags = output_flags,
		.output_tuple_def = output_tuple_def,
		.additional_flags = additional_flags,
		.tx = tx,
	};

	input_values* inputs = o->inputs;
	pthread_mutex_init(&(inputs->partition_pos_lock), NULL);

	return result;
}