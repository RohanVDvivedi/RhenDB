#include<rhendb/query_plan.h>

#include<rhendb/transaction.h>

#include<rhendb/mvcc_header.h>

#include<rhendb/table_operator_output_type.h>
#include<rhendb/fetched_table.h>

#include<tupleindexer/heap_page/heap_page.h>
#include<tupleindexer/heap_table/heap_table.h>

#include<rhendb/operator_error_codes.h>

typedef struct op_state op_state;
struct op_state
{
	// starts with 0, 
	uint64_t partition_index;

	// if NULL init with the current partition_index
	// increment partition_index if it does not exists
	heap_table_iterator* hti_p;

	// inited on every partition changes along with hti_p creation
	heap_table_tuple_defs httd;
	tuple_def mvcc_def;

	const tuple_def* partition_tuple_def; // borrowed, upon init of hti_p

	// kept NULL_PAGE until unised, and is always read locked
	persistent_page ppage;

	uint32_t tuple_index; // starts with 0, page is changed when reached tuple_count
	uint32_t tuple_count; // read with locking the ppage
};

// builds the flag-driven output context tuple for a single visible tuple; trimmed to its exact size
static void* build_output_tuple(operator* o, rage_engine* engine, tuple_pointer tptr, const void* heap_record)
{
	uint32_t output_tuple_size = get_minimum_tuple_size(st->output_tuple_def);
	uint64_t output_tuple_capacity = output_tuple_size;
	void* output_tuple = malloc(output_tuple_capacity);
	init_tuple(st->output_tuple_def, output_tuple);

	uint32_t attr_index = 0;

	if(MUST_OUTPUT_TABLE_ID(st->output_flags))
	{
		while(!set_element_in_tuple(st->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = st->ftabl->table_info.id}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(st->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(st->output_tuple_def, output_tuple);
		attr_index++;
	}

	if(MUST_OUTPUT_PARTITION_ID(st->output_flags))
	{
		while(!set_element_in_tuple(st->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = st->ftabl->table_partitions_info[st->partition_index].partition_id}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(st->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(st->output_tuple_def, output_tuple);
		attr_index++;
	}

	if(MUST_OUTPUT_TUPLE_POINTER_ID(st->output_flags))
	{
		char tptr_tpl[sizeof(tuple_pointer)];
		set_tuple_pointer(tptr_tpl, tptr, &(engine->pam_p->pas));

		while(!set_element_in_tuple(st->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.tuple_value = tptr_tpl}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(st->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(st->output_tuple_def, output_tuple);
		attr_index++;
	}

	if(MUST_OUTPUT_HEAP_TUPLE(st->output_flags))
	{
		void* final_readers_heap_record = project_to_final_readers_tuple_def(st->ftabl, heap_record, st->partition_index);

		while(!set_element_in_tuple(st->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.tuple_value = final_readers_heap_record}), output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(st->output_tuple_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(st->output_tuple_def, output_tuple);
		free(final_readers_heap_record);
		attr_index++;
	}

	// trim the (possibly doubled) buffer down to the record's actual size, as every other operator does
	output_tuple = realloc(output_tuple, get_tuple_size(st->output_tuple_def, output_tuple));
	return output_tuple;
}

// closes the current partition's iterator and free-space defs, and advances to the next partition
static void close_current_partition(op_state* st, rage_engine* engine)
{
	int abort_error = 0;
	if(st->hti_p != NULL)
	{
		delete_heap_table_iterator(st->hti_p, NULL, &abort_error);
		st->hti_p = NULL;
	}
	deinit_heap_table_tuple_definitions(&(st->httd));
	st->partition_open = 0;
	st->partition_index++;
}

static void* next_tuple(operator* o, int* error_code)
{
	if(o->state == NULL)
		return NULL;

	op_state* st = o->state;
	rage_engine* engine = &(st->tx->rdb->persistent_acid_rage_engine);

	// a scan with no output columns (output_flags == 0) has nothing to emit in the pull model
	if(st->output_tuple_def == NULL)
	{
		o->destroy_state(o);
		return NULL;
	}

	while(1)
	{
		// all partitions scanned
		if(st->partition_index >= st->ftabl->partitions_count)
		{
			o->destroy_state(o);
			return NULL;
		}

		// open the current partition, if not already open
		if(!st->partition_open)
		{
			st->partition_tuple_def = &(st->ftabl->table_partition_tuple_defs[st->partition_index]);
			initialize_tuple_def(&(st->mvcc_def), (data_type_info*)(st->partition_tuple_def->type_info->containees[0].al.type_info));
			init_heap_table_tuple_definitions(&(st->httd), &(engine->pam_p->pas), st->partition_tuple_def);

			int abort_error = 0;
			st->hti_p = get_new_heap_table_iterator(st->ftabl->table_partitions_info[st->partition_index].heap_root_page_id, 0, 0, &(st->httd), engine->pam_p, NULL, &abort_error);
			if(abort_error)
			{
				deinit_heap_table_tuple_definitions(&(st->httd));
				(*error_code) = SCAN_READ_MINI_TX_ABORTED;
				o->destroy_state(o);
				return NULL;
			}

			st->partition_open = 1;
			st->page_locked = 0;
			st->at_first_page = 1;
		}

		// acquire the next heap_page if one is not currently locked
		if(!st->page_locked)
		{
			int abort_error = 0;

			if(!st->at_first_page)
			{
				int went_next = next_heap_table_iterator(st->hti_p, NULL, &abort_error);
				if(abort_error)
				{
					(*error_code) = SCAN_READ_MINI_TX_ABORTED;
					o->destroy_state(o);
					return NULL;
				}
				if(!went_next)
				{
					close_current_partition(st, engine);
					continue;
				}
			}
			st->at_first_page = 0;

			uint32_t unused_space = 0;
			int entry_needs_fixing = 0;
			st->ppage = lock_and_get_curr_heap_page_heap_table_iterator(st->hti_p, 0 /* READ_LOCK */, &unused_space, &entry_needs_fixing, NULL, &abort_error);
			if(abort_error)
			{
				(*error_code) = SCAN_READ_MINI_TX_ABORTED;
				o->destroy_state(o);
				return NULL;
			}
			if(is_persistent_page_NULL(&(st->ppage), engine->pam_p))
			{
				close_current_partition(st, engine);
				continue;
			}

			st->page_locked = 1;
			st->tuple_count = get_tuple_count_on_persistent_page(&(st->ppage), engine->pam_p->pas.page_size, &(st->partition_tuple_def->size_def));
			st->tuple_index = 0;
		}

		// scan the tuples on the currently locked page, resuming from tuple_index
		while(st->tuple_index < st->tuple_count)
		{
			uint32_t ti = st->tuple_index;
			st->tuple_index++; // advance first, so the next call resumes after this tuple

			// tomb stone, no tuple here
			if(!exists_tuple_on_persistent_page(&(st->ppage), engine->pam_p->pas.page_size, &(st->partition_tuple_def->size_def), ti))
				continue;

			const void* heap_record = get_nth_tuple_on_persistent_page(&(st->ppage), engine->pam_p->pas.page_size, &(st->partition_tuple_def->size_def), ti);
			if(heap_record == NULL)
				continue;

			// MVCC visibility is the only filter
			{
				const data_type_info* dti_temp;
				datum mvcc_hdr_datum;
				get_value_from_element_from_tuple(&mvcc_hdr_datum, &dti_temp, st->partition_tuple_def, STATIC_POSITION(0), heap_record);

				mvcc_header mvcchdr;
				read_mvcc_header(&mvcchdr, mvcc_hdr_datum.tuple_value, &(st->mvcc_def));

				int were_hints_updated = 0;
				if(!is_tuple_visible_to_mvcc_snapshot(st->tx->snapshot, &mvcchdr, &(st->tx->rdb->tsg), &were_hints_updated))
					continue;
			}

			// NO Halloween (was_registered_as_inserted_tuple_pointer) check here.

			tuple_pointer tptr = (tuple_pointer){.page_id = st->ppage.page_id, .tuple_index = ti};

			// keep the page locked and return; the next call resumes on this same page at tuple_index
			return build_output_tuple(st, engine, tptr, heap_record);
		}

		// this page is exhausted: release its read lock and move to the next page
		{
			int abort_error = 0;
			release_lock_on_persistent_page(engine->pam_p, NULL, &(st->ppage), NONE_OPTION, &abort_error);
			st->page_locked = 0;
		}
	}
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		if(this_state->page_locked)
			release_lock_on_persistent_page(o->self_query_plan->desc->curr_tx->rdb->persistent_acid_rage_engine->pam_p->pam_p, NULL, &(this_state->ppage), NONE_OPTION, &abort_error);

		if(this_state->hti_p != NULL)
		{
			delete_heap_table_iterator(this_state->hti_p, NULL, &abort_error);
			deinit_heap_table_tuple_definitions(&(this_state->httd));
		}

		free(this_state);
	}
	o->state = NULL;
}

void open_scan_operator(operator* o)
{
	op_state* this_state = malloc(sizeof(op_state));

	this_state->partition_index = 0; // increment until there is a partition that exists

	this_state->hti_p = NULL;

	this_state->partition_tuple_def = NULL;

	this_state->ppage = get_NULL_persistent_page(o->self_query_plan->desc->curr_tx->rdb->persistent_acid_rage_engine->pam_p);

	this_state->tuple_index = 0;
	this_state->tuple_count = 0;

	o->state = st;
	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}