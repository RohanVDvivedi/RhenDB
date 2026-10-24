#include<rhendb/aggregate_functions.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<rhendb/transaction.h>
#include<rhendb/util_materialization.h>
#include<rhendb/util_transaction_ext_storer.h>

#include<rhendb/util_numeric_conversions.h>

#include<cutlery/dstring.h>

#include<stdlib.h>

typedef jsonb_node* (*make_jsonb_callback)(const datum input, const data_type_info* input_type_info, transaction* tx);

jsonb_node* make_jsonb_native_number(const datum input, const data_type_info* input_type_info, transaction* tx)
{
	if(is_datum_NULL(&input))
		return &jsonb_null;

	int error_code = 0;
	mpd_t n = numeric_from_primitive_numeral(input_type_info, &input, &error_code);
	if(error_code)
		return NULL;

	int exponent_too_big = 0;
	materialized_numeric mn = decimal_to_materialized_numeric(&n, &exponent_too_big);
	mpd_del(&n);
	if(exponent_too_big)
		return NULL;

	return new_jsonb_numeric_node2(mn);
}

jsonb_node* make_jsonb_extended_numeric(const datum input, const data_type_info* input_type_info, transaction* tx)
{
	if(is_datum_NULL(&input))
		return &jsonb_null;

	int error_code = 0;
	materialized_numeric mn = materialize_numeric1(input, input_type_info, tx, 0, &error_code);
	if(error_code)
		return NULL;

	return new_jsonb_numeric_node2(mn);
}

jsonb_node* make_jsonb_string(const datum input, const data_type_info* input_type_info, transaction* tx)
{
	if(is_datum_NULL(&input))
		return &jsonb_null;

	uint32_t length = 0, capacity = 0;
	int error_code = 0;
	char* data = materialize_tbj(input, input_type_info, tx, &length, &capacity, 0, &error_code);
	if(error_code)
	{
		if(capacity > 0)
			free(data);
		return NULL;
	}

	jsonb_node* node = new_jsonb_string_node(&(get_dstring_pointing_to(data, length)));
	if(capacity > 0)
		free(data);

	return node;
}

jsonb_node* make_jsonb_from_jsonb_tuple(const datum input, const data_type_info* input_type_info, transaction* tx)
{
	if(is_datum_NULL(&input))
		return &jsonb_null;

	int error_code = 0;
	jsonb_node* j = materialize_jsonb(input, input_type_info, tx, &error_code);
	if(error_code)
	{
		if(j != NULL)
			delete_jsonb_node(j);
		return NULL;
	}

	return j;
}

typedef struct jsonb_state jsonb_state;
struct jsonb_state
{
	jsonb_node* jsonb_array_root;

	void* jsonb_array_tuple;
};

static make_jsonb_callback get_dedicated_jsonb_maker_function(const data_type_info* input_type_info)
{
	switch(input_type_info->type)
	{
		case BIT_FIELD :
		case UINT :
		case LARGE_UINT:
		case INT :
		case LARGE_INT:
		case FLOAT :
			return make_jsonb_native_number;

		case STRING :
			return make_jsonb_string;

		case TUPLE :
		{
			if(is_numeric_type_info(input_type_info))
				return make_jsonb_extended_numeric;
			if(is_text_type_info(input_type_info))
				return make_jsonb_string;
			if(is_jsonb_type_info(input_type_info))
				return make_jsonb_from_jsonb_tuple;
			return NULL;
		}

		default :
			return NULL;
	}
}

typedef struct jsonb_context jsonb_context;
struct jsonb_context
{
	transaction* tx;

	make_jsonb_callback make_jsonb_callback;
};

static int process_input(const aggregate_function* af_p, void** state_p, const datum inputs[])
{
	if(!is_datum_NULL(&(inputs[0])))
	{
		// always create an empty state if one does not exist yet
		if((*state_p) == NULL)
		{
			(*state_p) = malloc(sizeof(jsonb_state));
			((jsonb_state*)(*state_p))->jsonb_array_root = new_jsonb_array_node(1);
			((jsonb_state*)(*state_p))->jsonb_array_tuple = NULL;
		}

		jsonb_context* jc = (jsonb_context*)(af_p->context_p);
		jsonb_node* value = (jc->make_jsonb_callback)(inputs[0], af_p->input_type_infos[0], jc->tx);
		if(value == NULL)
			return 0;

		jsonb_state* agg_state = (*state_p);
		if(!push_in_jsonb_array_node(agg_state->jsonb_array_root, value)) // it already has a check for UINT32_MAX element_count
		{
			delete_jsonb_node(value);
			return 1;
		}
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

	jsonb_state* agg_state = (*state_p);

	uint32_t total_size = 0;
	if(!finalize_jsonb(agg_state->jsonb_array_root, &total_size))
	{
		(*output) = (*NULL_DATUM);
		return 0;
	}

	agg_state->jsonb_array_tuple = tx_temp_store_jsonb(agg_state->jsonb_array_root, af_p->output_type_info, ((jsonb_context*)(af_p->context_p))->tx);
	if(agg_state->jsonb_array_tuple == NULL)
	{
		(*output) = (*NULL_DATUM);
		return 0;
	}

	(*output) = (datum){.tuple_value = agg_state->jsonb_array_tuple};
	return 1;
}

static void destroy_state(const aggregate_function* af_p, void** state_p)
{
	// NOP if the state_p is already NULL
	if((*state_p) == NULL)
		return;

	jsonb_state* agg_state = (*state_p);
	if(agg_state->jsonb_array_root != NULL)
	{
		delete_jsonb_node(agg_state->jsonb_array_root);
		agg_state->jsonb_array_root = NULL;
	}
	if(agg_state->jsonb_array_tuple != NULL)
	{
		free(agg_state->jsonb_array_tuple);
		agg_state->jsonb_array_tuple = NULL;
	}
	free(agg_state);
	(*state_p) = NULL;
}

static void destroy_aggregate_function(aggregate_function* af_p)
{
	free((void*)af_p->context_p);
	free(af_p);
}

aggregate_function* get_jsonb_aggregate_function(transaction* tx, const data_type_info* input_type_info)
{
	aggregate_function* af_p = malloc(size_of_aggregate_function(1));

	if(get_dedicated_jsonb_maker_function(input_type_info) == NULL)
	{
		printf("incompatible input_type_info for jsonb_aggregate_function\n");
		exit(-1);
	}

	af_p->context_p = malloc(sizeof(jsonb_context));
	((jsonb_context*)(af_p->context_p))->tx = tx;
	((jsonb_context*)(af_p->context_p))->make_jsonb_callback = get_dedicated_jsonb_maker_function(input_type_info);

	af_p->process_input = process_input;

	af_p->produce_output = produce_output;

	af_p->destroy_state = destroy_state;

	af_p->destroy_aggregate_function = destroy_aggregate_function;

	af_p->output_type_info = tx->rdb->volatile_rage_engine.jsonb_extended_type_info;

	af_p->buffers_resource_count = has_extended_type_info(input_type_info, PERSISTENT_EXT_SUB_TYPE);

	af_p->input_type_infos_count = 1;
	af_p->input_type_infos[0] = input_type_info;

	return af_p;
}