#include<rhendb/query_plan_interface.h>

#include<rhendb/transaction.h>

#include<rhendb/function_compare.h>

#include<stdlib.h>

/*
	TEMPLATE FOR INTERMEDIATE OPERATORS (sorting(ordering), joins(hash_joins), aggregations(groupby->aggregates))
*/

typedef struct input_values input_values;
struct input_values
{
	const tuple_def* input_defs[2];

	consumption_iterator* input_iterators[2];

	const void* tuples[2];

	int no_more_data[2];

	uint32_t element_count;
	positional_accessor elements[];
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	dstring kill_reason = get_dstring_pointing_to_literal_cstring("completed_and_killed");

	while(1)
	{
		// get all next tuples
		for(int i = 0; i < 2; i++)
		{ 
			if(inputs->tuples[i] == NULL && !(inputs->no_more_data[i]))
			{
				inputs->tuples[i] = consume_for_consumption_iterator(inputs->input_iterators[i], &(inputs->no_more_data[i]));
				if(inputs->no_more_data[i])
					continue;
				if(inputs->tuples[i] == NULL)
					return;
			}
		}

		// we reach here only if they are all non-NULL or have no more data
		if(inputs->no_more_data[0] != inputs->no_more_data[1])
		{
			if(inputs->no_more_data[0])
			{
				kill_reason = get_dstring_pointing_to_literal_cstring("resuts_not_match_more_data_in_input_0");
				kill_signal_for_self_operator(o, kill_reason); return ;
			}
			else
			{
				kill_reason = get_dstring_pointing_to_literal_cstring("resuts_not_match_more_data_in_input_1");
				kill_signal_for_self_operator(o, kill_reason); return ;
			}
		}

		if(inputs->no_more_data[0] == 1)
		{
			kill_reason = get_dstring_pointing_to_literal_cstring("resuts_match_successfully");
			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		int abort_error = 0;
		int compare = compare_tuples_rhendb(inputs->tuples[0], inputs->input_defs[0], inputs->elements, inputs->tuples[1], inputs->input_defs[1], inputs->elements, NULL, inputs->element_count, &(o->self_query_plan->curr_tx->db->persistent_acid_rage_engine), NULL, &abort_error);
		if(compare == 0)
		{
			printf("result_match_operator says outputs do not match:\n");
			print_tuple(inputs->tuples[0], inputs->input_defs[0]);
			print_tuple(inputs->tuples[1], inputs->input_defs[1]);
			printf("\n\n");
			kill_reason = get_dstring_pointing_to_literal_cstring("resuts_match_FAILED");
			kill_signal_for_self_operator(o, kill_reason); return ;
		}

		for(int i = 0; i < 2; i++)
			inputs->tuples[i] = NULL;
	}

	return ;
}

void setup_result_match_operator(operator* o, operator* input_operators[2])
{
	const tuple_def* input_defs[2] = {
		get_tuple_def_for_tuples_to_be_consumed_from(input_operators[0]),
		get_tuple_def_for_tuples_to_be_consumed_from(input_operators[1]),
	};

	for(int i = 0; i < 2; i++)
	{
		if(input_defs[i]->type_info->type != TUPLE)
		{
			printf("match operator, %d-th input is not a TUPLE\n", i);
			exit(-1);
		}
	}

	if(input_defs[0]->type_info->element_count != input_defs[1]->type_info->element_count)
	{
		printf("match operator, input tuple def's element counts do not match\n");
		exit(-1);
	}

	uint32_t element_count = input_defs[0]->type_info->element_count;

	for(uint32_t i = 0; i < input_defs[0]->type_info->element_count; i++)
	{
		if(!can_compare_datum_rhendb(get_data_type_info_for_containee_of_container_without_data(input_defs[0]->type_info, i),
			get_data_type_info_for_containee_of_container_without_data(input_defs[1]->type_info, i)))
		{
			printf("match operator, input tuple def's element types are not comparable\n");
			exit(-1);
		}
	}

	o->execute = execute;
	o->operator_release_latches_and_store_context = OPERATOR_RELEASE_LATCH_NO_OP_FUNCTION;
	o->free_resources = OPERATOR_FREE_RESOURCE_NO_OP_FUNCTION;

	o->inputs = malloc(sizeof(input_values) + (element_count * sizeof(positional_accessor)));
	*((input_values*)(o->inputs)) = (input_values){
		.input_defs = {input_defs[0], input_defs[1]},
		.input_iterators = {
			create_consumption_iterator(input_operators[0], o, NULL),
			create_consumption_iterator(input_operators[1], o, NULL),
		},
		.tuples = {NULL, NULL},
		.no_more_data = {0, 0},
		.element_count = element_count,
	};

	for(uint32_t i = 0; i < element_count; i++)
		((input_values*)(o->inputs))->elements[i] = STATIC_POSITION(i);
}