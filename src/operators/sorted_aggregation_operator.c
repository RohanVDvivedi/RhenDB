#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<rhendb/aggregate_functions.h>

#include<rhendb/function_compare.h>

#include<rhendb/nullable_type_info_maker.h>

#include<stdlib.h>

typedef struct input_values input_values;
struct input_values
{
	// input_tuple_def for the aggregate functions
	const tuple_def* input_tuple_def;

	consumption_iterator* input_iterator;

	// keys for the aggregation
	uint32_t key_element_count;
	const positional_accessor* key_element_ids;

	uint32_t aggregate_functions_count;
	aggregate_function** aggregate_functions;

	// array of void pointers, of size aggregate_functions_count, one for each aggregate functions
	void** states;

	// array of datums of size = max(aggregate_functions->input_type_infos_count)
	datum* input_datums;

	// 2D array of positonal_accessors
	// aggregate_input_element_ids[i][j]; -> gives the position of the j-th input_param of the i-th aggregate functions
	// its size is [aggregate_functions_count] and [i][aggregate_functions[i]->input_type_infos_count]
	const positional_accessor** aggregate_input_element_ids;

	// consists of all the output_type_infos of all aggregate_functions
	const tuple_def* output_tuple_def;

	// partially prepared output_tuple
	void* output_tuple;
	uint32_t output_tuple_size;
	uint64_t output_tuple_capacity;
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data) // if no_more_data, produce the final output
		{
			// produce output tuple and return it, if one exists
			if(inputs->output_tuple != NULL)
			{
				for(uint32_t i = 0, j = inputs->key_element_count; i < inputs->aggregate_functions_count; i++, j++)
				{
					// produce the output_uval the output of the i-th aggregate function
					datum output_uval;
					if(!inputs->aggregate_functions[i]->produce_output(inputs->aggregate_functions[i], &output_uval, &(inputs->states[i])))
					{
						kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("produce_output_of_udaf_failed"));
						return ;
					}

					// ensure there are enough bytes in the output_tuple, as we try to insert this datum
					while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(j), inputs->output_tuple, &output_uval, inputs->output_tuple_capacity - inputs->output_tuple_size))
					{
						inputs->output_tuple_capacity = min(inputs->output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
						inputs->output_tuple = realloc(inputs->output_tuple, inputs->output_tuple_capacity);
					}

					// recompute tuple_size
					inputs->output_tuple_size = get_tuple_size(inputs->output_tuple_def, inputs->output_tuple);
				}

				// produce output_tuple
				int produced = produce_tuple_from_operator(o, inputs->output_tuple);
				if(!produced)
				{
					kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
					return ;
				}
			}

			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("completed_and_killed"));
			return ;
		}
		if(can_not_proceed_for_execution_operator(o))
		{
			kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_consume"));
			return ;
		}

		if(tuple != NULL)
		{
			// if a prepared output_tuple exists then we may need to match it's key with tuple to ensure that this tuple falls into same group or not
			if(inputs->output_tuple != NULL)
			{
				int same_group = (0 == compare_tuples_rhendb(inputs->output_tuple, inputs->output_tuple_def, NULL, tuple, inputs->input_tuple_def, inputs->key_element_ids, NULL, inputs->key_element_count, o->self_query_plan->curr_tx));

				// if tuple does not belong to the same group, then produce output tuple
				if(!same_group)
				{
					for(uint32_t i = 0, j = inputs->key_element_count; i < inputs->aggregate_functions_count; i++, j++)
					{
						// produce the output_uval the output of the i-th aggregate function
						datum output_uval;
						if(!inputs->aggregate_functions[i]->produce_output(inputs->aggregate_functions[i], &output_uval, &(inputs->states[i])))
						{
							kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("produce_output_of_udaf_failed"));
							return ;
						}

						// ensure there are enough bytes in the output_tuple, as we try to insert this datum
						while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(j), inputs->output_tuple, &output_uval, inputs->output_tuple_capacity - inputs->output_tuple_size))
						{
							inputs->output_tuple_capacity = min(inputs->output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
							inputs->output_tuple = realloc(inputs->output_tuple, inputs->output_tuple_capacity);
						}

						// recompute tuple_size
						inputs->output_tuple_size = get_tuple_size(inputs->output_tuple_def, inputs->output_tuple);
					}

					// produce output_tuple
					int produced = produce_tuple_from_operator(o, inputs->output_tuple);
					if(!produced)
					{
						kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
						return ;
					}

					// clear output_tuple
					free(inputs->output_tuple);
					inputs->output_tuple = NULL;
					inputs->output_tuple_size = 0;
					inputs->output_tuple_capacity = 0;

					// destroy aggregate states, right before we go forward to make/start a new group
					{
						for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
							inputs->aggregate_functions[i]->destroy_state(inputs->aggregate_functions[i], &(inputs->states[i]));
					}
				}
			}

			// if an output_tuple does not exist now create one
			if(inputs->output_tuple == NULL)
			{
				// generate the smallest possible tuple
				inputs->output_tuple_size = get_minimum_tuple_size(inputs->output_tuple_def);
				inputs->output_tuple_capacity = inputs->output_tuple_size;
				inputs->output_tuple = malloc(inputs->output_tuple_capacity);
				init_tuple(inputs->output_tuple_def, inputs->output_tuple);

				for(uint32_t i = 0; i < inputs->key_element_count; i++)
				{
					datum key_val;
					if(get_value_from_element_from_tuple(&key_val, inputs->input_tuple_def, inputs->key_element_ids[i], tuple))
					{
						// ensure there are enough bytes in the output_tuple, as we try to insert this datum
						while(!set_element_in_tuple(inputs->output_tuple_def, STATIC_POSITION(i), inputs->output_tuple, &key_val, inputs->output_tuple_capacity - inputs->output_tuple_size))
						{
							inputs->output_tuple_capacity = min(inputs->output_tuple_capacity * 2, get_maximum_tuple_size(inputs->output_tuple_def));
							inputs->output_tuple = realloc(inputs->output_tuple, inputs->output_tuple_capacity);
						}

						// recompute tuple_size
						inputs->output_tuple_size = get_tuple_size(inputs->output_tuple_def, inputs->output_tuple);
					}
				}
			}

			// iterate for process_input of each udaf
			for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
			{
				// generate input params to the i-th udaf
				for(uint32_t j = 0; j < inputs->aggregate_functions[i]->input_type_infos_count; j++)
				{
					if(!get_value_from_element_from_tuple(&(inputs->input_datums[j]), inputs->input_tuple_def, inputs->aggregate_input_element_ids[i][j], tuple))
						inputs->input_datums[j] = (*NULL_DATUM);
				}

				// process_input for the udaf, if it fails kill the operator
				if(!inputs->aggregate_functions[i]->process_input(inputs->aggregate_functions[i], &(inputs->states[i]), inputs->input_datums))
				{
					kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("process_input_of_udaf_failed"));
					return ;
				}
			}
		}
		else
			break;
	}

	return ;
}

static void clean_up_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->output_tuple != NULL)
	{
		free(inputs->output_tuple);
		inputs->output_tuple = NULL;
		inputs->output_tuple_size = 0;
		inputs->output_tuple_capacity = 0;
	}

	if(inputs->input_iterator != NULL)
	{
		destroy_consumption_iterator(inputs->input_iterator);
		inputs->input_iterator = NULL;
	}

	// destroy aggregate states, right before we quit
	for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
		inputs->aggregate_functions[i]->destroy_state(inputs->aggregate_functions[i], &(inputs->states[i]));

	free(inputs->states);

	free(inputs->input_datums);

	free(inputs->aggregate_input_element_ids);
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	for(uint32_t i = 0; i < inputs->aggregate_functions_count; i++)
		inputs->aggregate_functions[i]->destroy_aggregate_function(inputs->aggregate_functions[i]);

	free(inputs->aggregate_functions);

	// all key_dtis were made into nullable types, so free them first
	for(uint32_t i = 0; i < inputs->key_element_count; i++)
		free((data_type_info*)(inputs->output_tuple_def->type_info->containees[i].al.type_info));
	free((data_type_info*)(inputs->output_tuple_def->type_info));
	free((tuple_def*)(inputs->output_tuple_def));

	free(inputs);
}

operator_resource_counter setup_sorted_aggregation_operator(operator* o, operator* input_operator, uint32_t key_element_count, const positional_accessor* key_element_ids, uint32_t aggregate_functions_count, aggregate_function* const * aggregate_functions, const positional_accessor** aggregate_input_element_ids)
{
	if(key_element_count == 0)
	{
		printf("key_element_count is zero in sorted_aggregation_operator\n");
		exit(-1);
	}

	const tuple_def* input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator);

	operator_resource_counter result = {.buffer_counter = max(2 * has_extended_type_info3(input_tuple_def, key_element_count, key_element_ids, PERSISTENT_EXT_SUB_TYPE), get_max_buffers_count_for_all_aggregate_functions(aggregate_functions_count, (aggregate_function const * const *) aggregate_functions)), .job_counter = 1};
	if(o == NULL)
		return result;

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->clean_up_resources = clean_up_resources;
	o->free_resources = free_resources;

	uint32_t input_datums_count = 0;
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(key_element_count + aggregate_functions_count));
	uint64_t max_output_tuple_size = 8;

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		data_type_info* key_dti = (data_type_info*) get_type_info_for_element_from_tuple_def(input_tuple_def, key_element_ids[i]);

		if(key_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += key_dti->is_variable_sized ? (8 + key_dti->max_size) : (1 + key_dti->size);

		sprintf(output_dti->containees[i].field_name, "key_%u", i);
		output_dti->containees[i].al.type_info = shallow_clone_into_nullable_type(key_dti); // some nested key could be out-of-bounds so making it into a nullable type info is required
	}

	for(uint32_t i = 0, j = key_element_count; i < aggregate_functions_count; i++, j++)
	{
		input_datums_count = max(input_datums_count, aggregate_functions[i]->input_type_infos_count);

		if(aggregate_functions[i]->output_type_info->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += aggregate_functions[i]->output_type_info->is_variable_sized ? (8 + aggregate_functions[i]->output_type_info->max_size) : (1 + aggregate_functions[i]->output_type_info->size);

		sprintf(output_dti->containees[j].field_name, "agg_%u", i);
		output_dti->containees[j].al.type_info = (data_type_info*) aggregate_functions[i]->output_type_info;
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for sorted_aggregation_operator\n");
		exit(-1);
	}

	initialize_tuple_data_type_info(output_dti, "keyed_aggregates", 0, max_output_tuple_size, key_element_count + aggregate_functions_count);

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(output_tuple_def, output_dti);

	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	input_values* inputs = o->inputs;
	*inputs = (input_values){
		.input_tuple_def = input_tuple_def,
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.key_element_count = key_element_count,
		.key_element_ids = key_element_ids,
		.aggregate_functions_count = aggregate_functions_count,
		.aggregate_functions = malloc(sizeof(aggregate_function*) * aggregate_functions_count),
		.states = calloc(sizeof(void*), aggregate_functions_count),
		.input_datums = malloc(sizeof(datum) * input_datums_count),
		.aggregate_input_element_ids = malloc(sizeof(positional_accessor*) * aggregate_functions_count),
		.output_tuple_def = output_tuple_def,
		.output_tuple = NULL,
		.output_tuple_size = 0,
		.output_tuple_capacity = 0,
	};

	memory_move(inputs->aggregate_functions, aggregate_functions, sizeof(aggregate_function*) * aggregate_functions_count);

	memory_move(inputs->aggregate_input_element_ids, aggregate_input_element_ids, sizeof(positional_accessor*) * aggregate_functions_count);

	return result;
}