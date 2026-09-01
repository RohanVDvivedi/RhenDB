#include<rhendb/aggregate_functions.h>

#include<rhendb/transaction.h>
#include<rhendb/util_materialization.h>
#include<rhendb/util_transaction_ext_storer.h>

#include<cutlery/dstring.h>

#include<stdlib.h>

typedef struct concat_context concat_context;
struct concat_context
{
	transaction* tx;

	dstring prefix;

	dstring delimeter;

	dstring suffix;

	tuple_def output_tuple_def;
};

typedef struct concat_state concat_state;
struct concat_state
{
	uint64_t concats_completed; // 0 -> prefix then data, > 0 -> delimeter than data and finally suffix

	uint64_t bytes_appended; // no more than 2^32 bytes allowed for any state, this includes prefix suffix and delimeter

	void* result; // this will be an extended text or blob in volatile page store

	tuple_pointer tail_pointer;

	// temp ext_store it belongs to, NULL if not yet known
	temporary_extension_store* temp_ext_store;
};

static concat_state* create_concat_state(const aggregate_function* af_p)
{
	const concat_context* cc = af_p->context_p;

	concat_state* state = malloc(sizeof(concat_state));
	state->concats_completed = 0;
	state->bytes_appended = 0;
	state->result = malloc(af_p->output_type_info->max_size);
	init_tuple(&(cc->output_tuple_def), state->result);
	state->tail_pointer = get_NULL_tuple_pointer(&(cc->tx->rdb->volatile_rage_engine.pam_p->pas));
	state->temp_ext_store = NULL;

	return state;
}

static int append_bytes(const aggregate_function* af_p, concat_state* state, const void* data, uint64_t data_size)
{
	// fail if the bytes count can not be held in 32 bit unsigned integer
	if(data_size > UINT32_MAX)
		return 0;
	if(state->bytes_appended + data_size > UINT32_MAX)
		return 0;

	const concat_context* cc = af_p->context_p;

	rage_engine* ex_engine = &(cc->tx->rdb->volatile_rage_engine);

	binary_write_iterator* wr = get_new_binary_write_iterator(state->result, &(cc->output_tuple_def), SELF, (state->temp_ext_store == NULL) ? 0 /*dummy root*/ : state->temp_ext_store->blob_store_root_page_id, state->tail_pointer, ex_engine->max_prefix_size_in_bytes, &(ex_engine->bstd), ex_engine->pam_p, ex_engine->pmm_p);

	if(state->temp_ext_store != NULL)
		write_lock(&(state->temp_ext_store->blob_store_lock), BLOCKING);

	int abort_error_dummy = 0;

	{
		uint32_t bytes_written = 0;
		do
		{
			if(state->temp_ext_store == NULL)
			{
				// write just the prefix
				while(bytes_written < data_size && wr->bytes_written_to_prefix < wr->bytes_to_be_written_to_prefix)
				{
					uint32_t bytes_to_write_this_iteration = min(wr->bytes_to_be_written_to_prefix - wr->bytes_written_to_prefix, data_size - bytes_written);
					uint32_t bytes_written_this_iteration = append_to_binary_write_iterator(wr, data + bytes_written, bytes_to_write_this_iteration, NULL, NULL, &abort_error_dummy);
					if(bytes_written_this_iteration == 0)
						break;
					bytes_written += bytes_written_this_iteration;
					state->bytes_appended += bytes_written_this_iteration;
				}

				if(state->bytes_appended == ex_engine->max_prefix_size_in_bytes)
				{
					state->temp_ext_store = &(cc->tx->temp_ext_stores[hash_element_within_tuple(state->result, &(cc->output_tuple_def), EXTENDED_PREFIX_POS_ACC, FNV_64_TUPLE_HASHER) % TEMPORARY_EXTENSION_STORE_COUNT]);
					write_lock(&(state->temp_ext_store->blob_store_lock), BLOCKING);
					wr->blob_store_root_page_id = state->temp_ext_store->blob_store_root_page_id;
				}
			}
			else
			{
				const heap_table_notifier* htan_p = &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(state->temp_ext_store->htan));

				while(bytes_written < data_size)
				{
					uint32_t bytes_written_this_iteration = append_to_binary_write_iterator(wr, data + bytes_written, data_size - bytes_written, htan_p, NULL, &abort_error_dummy);
					if(bytes_written_this_iteration == 0)
						break;
					bytes_written += bytes_written_this_iteration;
					state->bytes_appended += bytes_written_this_iteration;

					fix_unused_space_entries_in_store(cc->tx, state->temp_ext_store);
				}
			}
		}
		while(bytes_written < data_size);
	}

	state->tail_pointer = wr->extension_tail;
	delete_binary_write_iterator(wr, NULL, &abort_error_dummy);

	if(state->temp_ext_store != NULL)
		write_unlock(&(state->temp_ext_store->blob_store_lock));

	return 1;
}

static int process_input(const aggregate_function* af_p, void** state_p, const datum inputs[])
{
	const concat_context* cc = af_p->context_p;

	if(!is_datum_NULL(&(inputs[0])))
	{
		// always create an empty state if one does not exist yet
		if((*state_p) == NULL)
			(*state_p) = create_concat_state(af_p);

		concat_state* state = (*state_p);

		if(state->concats_completed == 0)
		{
			if(!append_bytes(af_p, state, get_byte_array_dstring(&(cc->prefix)), get_char_count_dstring(&(cc->prefix))))
				return 0;
		}
		else
		{
			if(!append_bytes(af_p, state, get_byte_array_dstring(&(cc->delimeter)), get_char_count_dstring(&(cc->delimeter))))
				return 0;
		}

		uint32_t length = 0;
		uint32_t capacity = 0;
		int error_code = 0;
		char* data = materialize_tb(inputs[0], af_p->input_type_infos[0], cc->tx, &length, &capacity, &error_code);
		if(error_code)
			return 0;

		if(!append_bytes(af_p, state, data, length))
		{
			if(capacity > 0)
				free(data);
			return 0;
		}
		if(capacity > 0)
			free(data);

		state->concats_completed++;

		return 1;
	}

	return 1;
}

static int produce_output(const aggregate_function* af_p, datum* output, void** state_p)
{
	// just return NULL_DATUM, no tuple/row was seen
	if((*state_p) == NULL)
	{
		(*output) = (*NULL_DATUM);
		return 1;
	}

	const concat_context* cc = af_p->context_p;
	concat_state* state = (*state_p);

	if(!append_bytes(af_p, state, get_byte_array_dstring(&(cc->suffix)), get_char_count_dstring(&(cc->suffix))))
	{
		(*output) = (*NULL_DATUM);
		return 0;
	}

	(*output) = (datum){.tuple_value = state->result};
	return 1;
}

static void destroy_state(const aggregate_function* af_p, void** state_p)
{
	// NOP if the state_p is already NULL
	if((*state_p) == NULL)
		return;

	concat_state* state = (*state_p);
	free(state->result);
	free(state);
	(*state_p) = NULL;
}

static void destroy_aggregate_function(aggregate_function* af_p)
{
	free((void*)(af_p->context_p));
	free(af_p);
}

aggregate_function* get_concat_aggregate_function(transaction* tx, const data_type_info* input_type_info, const dstring prefix, const dstring delimeter, const dstring suffix)
{
	aggregate_function* af_p = malloc(size_of_aggregate_function(1));

	if(!is_text_type_info(input_type_info) && !is_blob_type_info(input_type_info))
	{
		printf("incompatible input_type_info for concat_aggregate_function\n");
		exit(-1);
	}

	// context stores persistent_rage_engine here, for this aggregate function
	af_p->context_p = malloc(sizeof(concat_context));
	((concat_context*)(af_p->context_p))->tx = tx;
	((concat_context*)(af_p->context_p))->prefix = get_dstring_pointing_to_dstring(&prefix);
	((concat_context*)(af_p->context_p))->delimeter = get_dstring_pointing_to_dstring(&delimeter);
	((concat_context*)(af_p->context_p))->suffix = get_dstring_pointing_to_dstring(&suffix);

	af_p->process_input = process_input;

	af_p->produce_output = produce_output;

	af_p->destroy_state = destroy_state;

	af_p->destroy_aggregate_function = destroy_aggregate_function;

	af_p->output_type_info = is_text_type_info(input_type_info) ? tx->rdb->volatile_rage_engine.text_extended_type_info : tx->rdb->volatile_rage_engine.blob_extended_type_info;

	initialize_tuple_def(&(((concat_context*)(af_p->context_p))->output_tuple_def), (data_type_info*)af_p->output_type_info);

	af_p->buffers_resource_count = has_extended_type_info(input_type_info, PERSISTENT_EXT_SUB_TYPE); // only the case with extended numeric to be added

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}