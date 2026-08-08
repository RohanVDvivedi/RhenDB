#include<rhendb/query_plan.h>

#include<rhendb/operator_resource_counter.h>

#include<rhendb/transaction.h>

#include<rhendb/mvcc_header.h>

#include<rhendb/table_operator_output_type.h>

#include<rhendb/nullable_type_info_maker.h>

#include<stdlib.h>

/*
	heap table insertion operator
*/

typedef struct input_values input_values;
struct input_values
{
	const tuple_def* input_tuple_def;

	consumption_iterator* input_iterator;

	rhendb_table_partition insertion_table_partition;

	positional_accessor* insertion_from_source_positional_accessors;

	const tuple_def* partition_tuple_def;

	int output_flags; // same flags as table_operator_output_type.h
	const tuple_def* output_tuple_def; // will remain set to NULL, if output_flags is 0

	// cached here for snapshot and transaction_id
	transaction* tx;
};

static void execute(operator* o)
{
	input_values* inputs = o->inputs;

	while(1)
	{
		int no_more_data = 0;
		const void* tuple = consume_for_consumption_iterator(inputs->input_iterator, &no_more_data);
		if(no_more_data)
		{
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
			// open a mini transaction
			// need to do simple basic projections, let this be done by me OR expect the text is always text, and numeric is always numeric, and so on and projections are not needed
			// compute how many prefix bytes will we need for each one of the extended attributes of partition_tuple_def, this computation will be done by me OR just make a wild guess and let prefix bytes be persistent_engine page size * 0.3 and subtract the basic min size and divide to have limited number of bytes in each minus the 20 bytes for the meta data of each field and divide by 5 five for the numerics
			// construct all prefix containing heap table tuple, containing atmost 1 additonal page for each one of the extended attribute in it's blob store
			// do the real projection of simple primitive types add attributes, skip the nulls, and set the prefix of the rest extended ones, do materialize the extended ones though and keep the pointers handy
			// insert this heap table tuple, with it's fixed heap blob tuple pointer
			// append all the remaining extended bytes from the extended attributes such that the head tuple pointers are left as is
			// keep the heap table pointer handy we might need it to produce output
			// now register the register_inserted_tuple_pointer to the transaction struct, right before commit
			// commit the mini transaction
			// in for loop now, append all the remaining extended bytes from the extended attributes such that the head tuple pointers are left as is
			// each of this loop does complete insert in each single mini transaction and commits at the end
			if(inputs->output_flags != 0)
			{
				void* output_tuple = NULL;

				// project the heap table tuple and the tuple pointer and table_id and partition_id into the output_tuple
				// null objects in the heap table tuple should be skipped

				int produced = produce_tuple_from_operator(o, output_tuple);
				free(output_tuple);
				if(!produced)
				{
					kill_signal_for_self_operator(o, get_dstring_pointing_to_literal_cstring("could_not_produce"));
					return ;
				}
			}

			// finally free the heap table tuple that we kept handy
		}
		else
			break;
	}

	return ;
}

static void clean_up_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->input_iterator != NULL)
	{
		destroy_consumption_iterator(inputs->input_iterator);
		inputs->input_iterator = NULL;
	}
}

static void free_resources(operator* o)
{
	input_values* inputs = o->inputs;

	if(inputs->output_tuple_def != NULL)
	{
		// all other possible outputs are static types
		if(MUST_OUTPUT_HEAP_TUPLE(inputs->output_flags))
		{
			uint32_t heap_tuple_position = inputs->output_tuple_def->type_info->element_count - 1;
			uint32_t heap_tuple_element_count = inputs->partition_tuple_def->type_info->element_count;
			for(uint32_t i = 1; i < heap_tuple_element_count; i++) // destroy all shallow copied outputs, except for the mvcc header at position 0
				free(inputs->output_tuple_def->type_info->containees[heap_tuple_position].al.type_info->containees[i].al.type_info);
			free((void*)(inputs->output_tuple_def->type_info->containees[heap_tuple_position].al.type_info));
		}
		free((void*)(inputs->output_tuple_def->type_info));
		free((void*)(inputs->output_tuple_def));
		inputs->output_tuple_def = NULL;
	}

	if(inputs->partition_tuple_def != NULL)
	{
		destroy_type_info_recursively(inputs->partition_tuple_def->type_info, NULL);
		free((void*)(inputs->partition_tuple_def));
		inputs->partition_tuple_def = NULL;
	}

	free(inputs);
}

operator_resource_counter setup_insertion_operator(operator* o, operator* input_operator, positional_accessor* insertion_from_source_positional_accessors, rhendb_table_partition* table_partition, int output_flags)
{
	transaction* tx = input_operator->self_query_plan->curr_tx;

	if(tx->snapshot == NULL)
	{
		printf("must have writable snapshot for insertion_operator\n");
		exit(-1);
	}

	if(!tx->snapshot->has_self_transaction_id)
	{
		printf("must have self_transaction_id for insertion_operator\n");
		exit(-1);
	}

	const tuple_def* input_tuple_def = get_tuple_def_for_tuples_to_be_consumed_from(input_operator);

	data_type_info* partition_type_info = get_data_type_info_for_rhendb_table_partition_from_catalog(&(tx->rdb->cat_mgr), tx->snapshot, table_partition);
	if(!are_identical_type_info(partition_type_info->containees[0].al.type_info, tx->rdb->mvcc_hdr_type_info))
	{
		printf("must have mvcc_header for insertion_operator in the tuples of table_partition\n");
		exit(-1);
	}
	tuple_def* partition_tuple_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(partition_tuple_def, partition_type_info);

	operator_resource_counter result = {.job_counter = 1};
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
			// make each element instead of the first one a shallow copied nullable type info
			data_type_info* intermediate_table_data_type_info = malloc(sizeof_tuple_data_type_info(partition_type_info->element_count));
			{
				uint32_t intermediate_table_data_type_info_max_size = 8 + sizeof(mvcc_header);

				// we already made sure that the first element is the mvcc_header

				strncpy(intermediate_table_data_type_info->containees[0].field_name, partition_type_info->containees[0].field_name, sizeof(intermediate_table_data_type_info->containees[0].field_name));
				intermediate_table_data_type_info->containees[0].al.type_info = partition_type_info->containees[0].al.type_info;

				// clone the rest while making them nullable ahallowly with same field and type names
				for(uint32_t i = 1; i < partition_type_info->element_count; i++)
				{
					if(partition_type_info->containees[i].al.type_info->type == BIT_FIELD)
						intermediate_table_data_type_info_max_size += 9;
					else
						intermediate_table_data_type_info_max_size += partition_type_info->containees[i].al.type_info->is_variable_sized ? (8 + partition_type_info->containees[i].al.type_info->max_size) : (1 + partition_type_info->containees[i].al.type_info->size);

					strncpy(intermediate_table_data_type_info->containees[i].field_name, partition_type_info->containees[i].field_name, sizeof(intermediate_table_data_type_info->containees[i].field_name));
					intermediate_table_data_type_info->containees[i].al.type_info = shallow_clone_into_nullable_type(partition_type_info->containees[i].al.type_info);
				}

				initialize_tuple_data_type_info(intermediate_table_data_type_info, partition_type_info->type_name, 0, intermediate_table_data_type_info_max_size, partition_type_info->element_count);
				finalize_type_info(intermediate_table_data_type_info);
			}

			// insert this new type_info that has each of it's elements nullable as the last attribute
			strncpy(output_dti->containees[output_dti_element_count].field_name, partition_type_info->type_name, 64);
			output_dti->containees[output_dti_element_count].al.type_info = intermediate_table_data_type_info;

			output_tuple_max_size += intermediate_table_data_type_info->max_size + 8;
			output_dti_element_count++;
		}

		initialize_tuple_data_type_info(output_dti, "inserted_tuple_context", 0, output_tuple_max_size, output_dti_element_count);

		initialize_tuple_def(output_tuple_def, output_dti);
	}

	init_tuple_transformers(&(o->output_tuple_transformers), output_tuple_def);

	o->inputs = malloc(sizeof(input_values));
	*((input_values*)(o->inputs)) = (input_values){
		.input_tuple_def = input_tuple_def,
		.input_iterator = create_consumption_iterator(input_operator, o, NULL, NULL),
		.insertion_table_partition = *table_partition,
		.insertion_from_source_positional_accessors = insertion_from_source_positional_accessors,
		.partition_tuple_def = partition_tuple_def,
		.output_flags = output_flags,
		.output_tuple_def = output_tuple_def,
		.tx = tx,
	};

	return result;
}