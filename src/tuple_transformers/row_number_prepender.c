#include<rhendb/tuple_transformer_interface.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<pthread.h>

#include<stdlib.h>

typedef struct row_number_prepender_context row_number_prepender_context;
struct row_number_prepender_context
{
	pthread_mutex_t lock;
	uint64_t next_row_number;
};

// any allocation made here will be freed by the caller
static void* process(tuple_transformer* tt_p, void* tuple)
{
	void* output_tuple = malloc(get_tuple_size(tt_p->input_def, tuple) + 16);
	init_tuple(tt_p->output_def, output_tuple);

	row_number_prepender_context* context_p = tt_p->context;

	pthread_mutex_lock(&(context_p->lock));
	set_element_in_tuple(tt_p->output_def, STATIC_POSITION(0), output_tuple, &((const datum){.uint_value = (context_p->next_row_number++)}), UINT32_MAX);
	pthread_mutex_unlock(&(context_p->lock));

	set_element_in_tuple_from_tuple(tt_p->output_def, STATIC_POSITION(1), output_tuple, tt_p->input_def, SELF, tuple, UINT32_MAX);

	return output_tuple;
}

static void destroy(tuple_transformer* tt_p)
{
	free(tt_p->output_def->type_info);
	free((void*)(tt_p->output_def));
	free(tt_p->context);
}

tuple_transformer* get_new_simple_projection_transformer(const tuple_def* input_def, uint64_t next_row_number)
{
	tuple_def* output_def = malloc(sizeof(tuple_def));
	data_type_info* output_dti = malloc(sizeof_tuple_data_type_info(2));

	row_number_prepender_context* context_p = malloc(sizeof(row_number_prepender_context));
	(*context_p) = (row_number_prepender_context){
		.lock = PTHREAD_MUTEX_INITIALIZER,
		.next_row_number = next_row_number,
	};

	uint32_t output_max_size = get_maximum_tuple_size(input_def) + 16;

	strcpy(output_dti->containees[0].field_name, "row_number");
	output_dti->containees[0].al.type_info = UINT_NON_NULLABLE[8];

	strcpy(output_dti->containees[1].field_name, "row");
	output_dti->containees[1].al.type_info = input_def->type_info;

	initialize_tuple_data_type_info(output_dti, "row_numbered", 1, output_max_size, 2);
	initialize_tuple_def(output_def, output_dti);

	return get_new_tuple_transformer(context_p, input_def, output_def, process, destroy);
}