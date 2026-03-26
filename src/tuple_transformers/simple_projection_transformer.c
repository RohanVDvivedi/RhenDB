#include<rhendb/tuple_transformer_interface.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<stdlib.h>

/*
	it is expected that mvcc_header is the first attribute in the tuple for input_def
*/

#define MVCC_HEADER_POSITION STATIC_POSITION(0)

static void* process(tuple_transformer* tt_p, void* tuple)
{
	void* output_tuple = malloc(get_maximum_tuple_size(tt_p->output_def));
	init_tuple(tt_p->output_def, output_tuple);

	positional_accessor** projections = tt_p->context;

	uint32_t element_count = get_element_count_for_element_from_tuple(tt_p->output_def, SELF, output_tuple);

	for(uint32_t i = 0; i < element_count; i++)
		set_element_in_tuple_from_tuple(tt_p->output_def, STATIC_POSITION(i), output_tuple, tt_p->input_def, *(projections[i]), tuple, UINT32_MAX);

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

	uint32_t output_max_size = get_maximum_tuple_size(input_def);

	for(uint32_t i = 0; i < projections_count; i++)
	{
		strcpy(output_dti->containees[i].field_name, field_names[i]);
		output_dti->containees[i].al.type_info = (data_type_info*) get_type_info_for_element_from_tuple_def(input_def, *projections[i]);

		projections_context[i] = projections[i];
	}

	initialize_tuple_data_type_info(output_dti, output_table_name, 1, output_max_size, projections_count);
	initialize_tuple_def(output_def, output_dti);

	return get_new_tuple_transformer(projections_context, input_def, output_def, process, destroy);
}