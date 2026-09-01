#include<rhendb/tuple_transformer_interface.h>

#include<rhendb/transaction.h>

/*
	This tuple transformer is meants to be placed for the inputs of the union_operator and merge_sorted_inputs_operator
	This is only required if the text, blob, numeric and jsonb extended types are not identical extended types, i.e. when some come from persistent engine and some from volatile engine
	This is done outside the operator to retain the wide range of functionality and tuple types supported by these operators
*/

static void* process(tuple_transformer* tt_p, void* tuple)
{
	uint32_t output_tuple_size = get_minimum_tuple_size(tt_p->output_def);
	uint64_t output_tuple_capacity = output_tuple_size;
	void* output_tuple = malloc(output_tuple_size);
	init_tuple(tt_p->output_def, output_tuple);

	for(uint32_t i = 0; i < tt_p->output_def->type_info->element_count; i++)
	{
		datum input_uval;
		if(!get_value_from_element_from_tuple(&input_uval, tt_p->input_def, STATIC_POSITION(i), tuple) || is_datum_NULL(&input_uval))
			continue;

		const data_type_info* input_dti = get_type_info_for_element_from_tuple_def(tt_p->input_def, STATIC_POSITION(i));
		const data_type_info* output_dti = get_type_info_for_element_from_tuple_def(tt_p->output_def, STATIC_POSITION(i));

		positional_accessor output_pos;
		if(input_dti == output_dti) // fast path
			output_pos = STATIC_POSITION(i);
		else
		{
			// output_dti must be a union type_info and input being some extended type
			while(!set_element_in_tuple(tt_p->output_def, STATIC_POSITION(i), output_tuple, EMPTY_DATUM, output_tuple_capacity - output_tuple_size))
			{
				output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(tt_p->output_def));
				output_tuple = realloc(output_tuple, output_tuple_capacity);
			}
			output_tuple_size = get_tuple_size(tt_p->output_def, output_tuple);

			if(has_extended_type_info(input_dti, VOLATILE_EXT_SUB_TYPE))
				output_pos = STATIC_POSITION(i, 0);
			else if(has_extended_type_info(input_dti, PERSISTENT_EXT_SUB_TYPE))
				output_pos = STATIC_POSITION(i, 1);
		}


		while(!set_element_in_tuple(tt_p->output_def, output_pos, output_tuple, &input_uval, output_tuple_capacity - output_tuple_size))
		{
			output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(tt_p->output_def));
			output_tuple = realloc(output_tuple, output_tuple_capacity);
		}
		output_tuple_size = get_tuple_size(tt_p->output_def, output_tuple);
	}

	return output_tuple;
}

static void destroy(tuple_transformer* tt_p)
{
	free(tt_p->output_def->type_info);
	free((void*)(tt_p->output_def));

	free(tt_p->context);
}

tuple_transformer* get_new_unified_typing_transformer(const tuple_def* input_def, transaction* tx, uint32_t* column_ids_to_transform, uint32_t column_to_transform_count)
{
	tuple_def* output_def = malloc(sizeof(tuple_def));
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(input_def->type_info->element_count));

	uint32_t max_output_tuple_size = 8;

	for(uint32_t i = 0, j = 0; i < input_def->type_info->element_count; i++)
	{
		strncpy(output_dti->containees[i].field_name, input_def->type_info->containees[i].field_name, 64);
		output_dti->containees[i].al.type_info = (data_type_info*) get_type_info_for_element_from_tuple_def(input_def, STATIC_POSITION(i));

		if(j < column_to_transform_count && column_ids_to_transform[j] == i)
		{
			if(is_extended_type_info(output_dti->containees[i].al.type_info) || is_unified_type_info(output_dti->containees[i].al.type_info))
			{
				if(is_text_type_info(output_dti->containees[i].al.type_info))
					output_dti->containees[i].al.type_info = tx->rdb->union_text_type_info;
				else if(is_blob_type_info(output_dti->containees[i].al.type_info))
					output_dti->containees[i].al.type_info = tx->rdb->union_blob_type_info;
				else if(is_numeric_type_info(output_dti->containees[i].al.type_info))
					output_dti->containees[i].al.type_info = tx->rdb->union_numeric_type_info;
				else if(is_jsonb_type_info(output_dti->containees[i].al.type_info))
					output_dti->containees[i].al.type_info = tx->rdb->union_jsonb_type_info;
			}
			j++;
		}

		if(output_dti->containees[i].al.type_info->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += output_dti->containees[i].al.type_info->is_variable_sized ? (8 + output_dti->containees[i].al.type_info->max_size) : (1 + output_dti->containees[i].al.type_info->size);
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for simple_projection_transformer\n");
		exit(-1);
	}

	initialize_tuple_data_type_info(output_dti, input_def->type_info->type_name, 1, max_output_tuple_size, input_def->type_info->element_count);
	initialize_tuple_def(output_def, output_dti);

	return get_new_tuple_transformer(NULL, input_def, output_def, process, destroy);
}