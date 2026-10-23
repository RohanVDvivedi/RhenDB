#include<rhendb/tuple_transformer_interface.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<stdlib.h>

#include<rhendb/max_intermediate_tuple_size.h>

#include<rhendb/nullable_type_info_maker.h>

// any allocation made here will be freed by the caller
static void* process(tuple_transformer* tt_p, void* tuple)
{
	uint32_t output_tuple_size = get_minimum_tuple_size(tt_p->output_def);
	uint64_t output_tuple_capacity = output_tuple_size;
	void* output_tuple = malloc(output_tuple_size);
	init_tuple(tt_p->output_def, output_tuple);

	positional_accessor* projection = tt_p->context;

	{
		const data_type_info* dti_temp;
		datum output_uval;
		if(get_value_from_element_from_tuple(&output_uval, &dti_temp, tt_p->input_def, *(projection), tuple))
		{
			while(!set_element_in_tuple(tt_p->output_def, SELF, output_tuple, &output_uval, output_tuple_capacity - output_tuple_size))
			{
				output_tuple_capacity = min(output_tuple_capacity * 2, get_maximum_tuple_size(tt_p->output_def));
				output_tuple = realloc(output_tuple, output_tuple_capacity);
			}
			output_tuple_size = get_tuple_size(tt_p->output_def, output_tuple);
		}
	}

	return output_tuple;
}

static void destroy(tuple_transformer* tt_p)
{
	if(tt_p->output_def->type_info != UINT_NULLABLE[8])
		free(tt_p->output_def->type_info);
	free((void*)(tt_p->output_def));

	free(tt_p->context);
}

tuple_transformer* get_new_scalar_projection_transformer(const tuple_def* input_def, positional_accessor* projection)
{
	tuple_def* output_def = malloc(sizeof(tuple_def));

	data_type_info* output_dti = NULL;

	data_type_info* input_dti = (data_type_info*) get_type_info_for_element_from_tuple_def(input_def, *projection);
	if(input_dti->type == BIT_FIELD)
		output_dti = UINT_NULLABLE[8]; // a integer big enough to hold it, a bit_field always lives in some container
	else
		output_dti = shallow_clone_into_nullable_type(input_dti);

	initialize_tuple_def(output_def, output_dti);

	return get_new_tuple_transformer(projection, input_def, output_def, process, destroy);
}