#include<rhendb/tuple_transformer_interface.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<stdlib.h>

#include<rhendb/max_intermediate_tuple_size.h>

// any allocation made here will be freed by the caller
static void* process(tuple_transformer* tt_p, void* tuple)
{
	uint32_t output_tuple_size = get_minimum_tuple_size(tt_p->output_def);
	uint64_t output_tuple_capacity = output_tuple_size;
	void* output_tuple = malloc(output_tuple_size);
	init_tuple(tt_p->output_def, output_tuple);

	positional_accessor** projections = tt_p->context;

	uint32_t element_count = get_element_count_for_element_from_tuple(tt_p->output_def, SELF, output_tuple);

	for(uint32_t i = 0; i < element_count; i++)
	{
		while(!set_element_in_tuple_from_tuple(tt_p->output_def, STATIC_POSITION(i), output_tuple, tt_p->input_def, *(projections[i]), tuple, output_tuple_capacity - output_tuple_size))
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

tuple_transformer* get_new_simple_projection_transformer(const char* output_table_name, const tuple_def* input_def, uint32_t projections_count, positional_accessor** projections, const char** field_names)
{
	tuple_def* output_def = malloc(sizeof(tuple_def));
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(projections_count));

	positional_accessor** projections_context = malloc(sizeof(positional_accessor*) * projections_count);

	uint32_t max_output_tuple_size = 8;

	for(uint32_t i = 0; i < projections_count; i++)
	{
		data_type_info* input_dti = (data_type_info*) get_type_info_for_element_from_tuple_def(input_def, *projections[i]);

		strcpy(output_dti->containees[i].field_name, field_names[i]);
		output_dti->containees[i].al.type_info = input_dti;

		if(input_dti->type == BIT_FIELD)
			max_output_tuple_size += 9;
		else
			max_output_tuple_size += input_dti->is_variable_sized ? (8 + input_dti->max_size) : (1 + input_dti->size);

		projections_context[i] = projections[i];
	}

	if(max_output_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big output tuple for simple_projection_transformer\n");
		exit(-1);
	}

	initialize_tuple_data_type_info(output_dti, output_table_name, 1, max_output_tuple_size, projections_count);
	initialize_tuple_def(output_def, output_dti);

	return get_new_tuple_transformer(projections_context, input_def, output_def, process, destroy);
}