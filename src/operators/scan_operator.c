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
	// index of the partition being scanned; starts at 0 and is incremented as each partition is exhausted
	uint64_t partition_index;

	// NULL => the current partition is not open yet; opening it creates this iterator (at the first heap page)
	// and (re)builds httd, mvcc_def and partition_tuple_def for partition_index
	heap_table_iterator* hti_p;

	// valid only while hti_p is non-NULL (i.e. for the current partition)
	heap_table_tuple_defs httd;
	tuple_def mvcc_def;
	const tuple_def* partition_tuple_def; // borrowed from ftabl for the current partition

	// NULL_PAGE => no heap page is held; a non-NULL ppage is always READ locked
	persistent_page ppage;

	// cursor within the currently held ppage; the page is done once tuple_index reaches tuple_count
	uint32_t tuple_index;
	uint32_t tuple_count;
};

static void* build_output_tuple(operator* o, rage_engine* engine, tuple_pointer tptr, const void* heap_record)
{
	op_state* this_state = o->state;

	const fetched_table* ftabl = o->desc->params.scan.ftabl;

	void* output_tuple = malloc(get_maximum_tuple_size(o->desc->output_tuple_def));
	init_tuple(o->desc->output_tuple_def, output_tuple);

	uint32_t attr_index = 0;

	if(MUST_OUTPUT_TABLE_ID(o->desc->params.scan.output_flags))
	{
		set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = ftabl->table_info.id}), UINT32_MAX);
		attr_index++;
	}

	if(MUST_OUTPUT_PARTITION_ID(o->desc->params.scan.output_flags))
	{
		set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = ftabl->table_partitions_info[this_state->partition_index].partition_id}), UINT32_MAX);
		attr_index++;
	}

	if(MUST_OUTPUT_TUPLE_POINTER_ID(o->desc->params.scan.output_flags))
	{
		char tptr_tpl[sizeof(tuple_pointer)];
		set_tuple_pointer(tptr_tpl, tptr, &(engine->pam_p->pas));
		set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.tuple_value = tptr_tpl}), UINT32_MAX);
		attr_index++;
	}

	if(MUST_OUTPUT_HEAP_TUPLE(o->desc->params.scan.output_flags))
	{
		void* final_readers_heap_record = project_to_final_readers_tuple_def(ftabl, heap_record, this_state->partition_index);
		set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.tuple_value = final_readers_heap_record}), UINT32_MAX);
		free(final_readers_heap_record);
		attr_index++;
	}

	output_tuple = realloc(output_tuple, get_tuple_size(o->desc->output_tuple_def, output_tuple));
	return output_tuple;
}

static void* next_tuple(operator* o, int* error_code)
{
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	transaction* tx = o->self_query_plan->desc->curr_tx;
	rage_engine* engine = &(tx->rdb->persistent_acid_rage_engine);
	const fetched_table* ftabl = o->desc->params.scan.ftabl;

	while(1)
	{
		int abort_error = 0;

		// every partition scanned => done
		if(this_state->partition_index >= ftabl->partitions_count)
		{
			o->destroy_state(o);
			return NULL;
		}

		// hti_p == NULL => open the current partition: (re)build its defs and create the iterator at its first page
		if(this_state->hti_p == NULL)
		{
			this_state->partition_tuple_def = &(ftabl->table_partition_tuple_defs[this_state->partition_index]);
			initialize_tuple_def(&(this_state->mvcc_def), (data_type_info*)(this_state->partition_tuple_def->type_info->containees[0].al.type_info));
			init_heap_table_tuple_definitions(&(this_state->httd), &(engine->pam_p->pas), this_state->partition_tuple_def);

			this_state->hti_p = get_new_heap_table_iterator(ftabl->table_partitions_info[this_state->partition_index].heap_root_page_id, 0, 0, &(this_state->httd), engine->pam_p, NULL, &abort_error);
			if(abort_error)
			{
				deinit_heap_table_tuple_definitions(&(this_state->httd));
				this_state->hti_p = NULL;
				o->destroy_state(o);
				(*error_code) = SCAN_READ_MINI_TX_ABORTED;
				return NULL;
			}
		}

		// ppage is NULL => lock the iterator's current heap page (the first page right after opening, or the one
		// the iterator was advanced to). A NULL page here means this partition is exhausted.
		if(is_persistent_page_NULL(&(this_state->ppage), engine->pam_p))
		{
			uint32_t unused_space = 0;
			int entry_needs_fixing = 0;
			this_state->ppage = lock_and_get_curr_heap_page_heap_table_iterator(this_state->hti_p, 0 /* READ_LOCK */, &unused_space, &entry_needs_fixing, NULL, &abort_error);
			if(abort_error)
			{
				o->destroy_state(o);
				(*error_code) = SCAN_READ_MINI_TX_ABORTED;
				return NULL;
			}

			// end of this partition: drop its iterator and defs, then advance to the next partition
			if(is_persistent_page_NULL(&(this_state->ppage), engine->pam_p))
			{
				delete_heap_table_iterator(this_state->hti_p, NULL, &abort_error);
				this_state->hti_p = NULL;
				deinit_heap_table_tuple_definitions(&(this_state->httd));
				this_state->partition_index++;
				continue;
			}

			this_state->tuple_count = get_tuple_count_on_persistent_page(&(this_state->ppage), engine->pam_p->pas.page_size, &(this_state->partition_tuple_def->size_def));
			this_state->tuple_index = 0;
		}

		// scan the held page from tuple_index; return the next MVCC-visible tuple, leaving the page locked
		while(this_state->tuple_index < this_state->tuple_count)
		{
			const void* heap_record = get_nth_tuple_on_persistent_page(&(this_state->ppage), engine->pam_p->pas.page_size, &(this_state->partition_tuple_def->size_def), this_state->tuple_index);
			if(heap_record == NULL)
			{
				this_state->tuple_index++;
				continue;
			}

			// MVCC check
			const data_type_info* dti_temp;
			datum mvcc_hdr_datum;
			get_value_from_element_from_tuple(&mvcc_hdr_datum, &dti_temp, this_state->partition_tuple_def, STATIC_POSITION(0), heap_record);

			mvcc_header mvcchdr;
			read_mvcc_header(&mvcchdr, mvcc_hdr_datum.tuple_value, &(this_state->mvcc_def));

			int were_hints_updated = 0;
			if(!is_tuple_visible_to_mvcc_snapshot(tx->snapshot, &mvcchdr, &(tx->rdb->tsg), &were_hints_updated))
			{
				this_state->tuple_index++;
				continue;
			}

			tuple_pointer tptr = (tuple_pointer){.page_id = this_state->ppage.page_id, .tuple_index = this_state->tuple_index};
			void* output_tuple = build_output_tuple(o, engine, tptr, heap_record);
			this_state->tuple_index++;
			return output_tuple;
		}

		// page fully scanned: release its read lock (ppage back to NULL) and advance the iterator.
		// advancing past the last page lands the iterator at its end, so the lock_and_get above then returns
		// a NULL page and the partition is closed there; no end-of-scan flag is needed.
		release_lock_on_persistent_page(engine->pam_p, NULL, &(this_state->ppage), NONE_OPTION, &abort_error);
		if(abort_error)
		{
			o->destroy_state(o);
			(*error_code) = SCAN_READ_MINI_TX_ABORTED;
			return NULL;
		}
		this_state->ppage = get_NULL_persistent_page(engine->pam_p);

		next_heap_table_iterator(this_state->hti_p, NULL, &abort_error);
		if(abort_error)
		{
			o->destroy_state(o);
			(*error_code) = SCAN_READ_MINI_TX_ABORTED;
			return NULL;
		}
	}
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		rage_engine* engine = &(o->self_query_plan->desc->curr_tx->rdb->persistent_acid_rage_engine);
		int abort_error = 0;

		if(!is_persistent_page_NULL(&(this_state->ppage), engine->pam_p))
			release_lock_on_persistent_page(engine->pam_p, NULL, &(this_state->ppage), NONE_OPTION, &abort_error);

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
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	rage_engine* engine = &(o->self_query_plan->desc->curr_tx->rdb->persistent_acid_rage_engine);

	this_state->partition_index = 0; // scan partitions from the first, incrementing as each is exhausted

	this_state->hti_p = NULL; // the current partition is opened lazily on the first next_tuple()

	this_state->partition_tuple_def = NULL;

	this_state->ppage = get_NULL_persistent_page(engine->pam_p); // no heap page held yet

	this_state->tuple_index = 0;
	this_state->tuple_count = 0;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}