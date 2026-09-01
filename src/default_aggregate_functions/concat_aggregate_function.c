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
};

typedef struct concat_state concat_state;
struct concat_state
{
	uint64_t concats_completed; // 0 -> prefix then data, > 0 -> delimeter than data and finally suffix

	uint64_t bytes_appended; // no more than 2^32 bytes allowed for any state, this includes prefix suffix and delimeter

	void* result; // this will be an extended text or blob in volatile page store

	tuple_pointer tail_pointer;
};

static concat_state* create_concat_state(const aggregate_function* af_p)
{
	const concat_context* cc = af_p->context_p;

	concat_state* state = malloc(sizeof(concat_state));
	state->concats_completed = 0;
	state->bytes_appended = 0;
	state->result = malloc(af_p->output_type_info->max_size);
	state->tail_pointer = get_NULL_tuple_pointer(&(cc->tx->rdb->volatile_rage_engine.pam_p->pas));

	return state;
}

static int append_bytes(const aggregate_function* af_p, concat_state* state, const void* data, uint64_t data_size)
{
	const concat_context* cc = af_p->context_p;
	return 0;
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
	state_p = NULL;
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

	af_p->buffers_resource_count = has_extended_type_info(input_type_info, PERSISTENT_EXT_SUB_TYPE); // only the case with extended numeric to be added

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}