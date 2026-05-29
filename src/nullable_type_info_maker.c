#include<rhendb/nullable_type_info_maker.h>

data_type_info* shallow_clone_into_nullable_type(const data_type_info* dti_p)
{
	// figure out the number of bytes to shallow copy input_type_info
	size_t bytes_to_shallow_copy = get_shallow_copy_struct_size_for_data_type_info(dti_p);

	// make shallow copy, mark it nullable, and finalize this type
	data_type_info* output_dti_p = malloc(bytes_to_shallow_copy);
	memory_move(output_dti_p, dti_p, bytes_to_shallow_copy);
	output_dti_p->is_nullable = 1;
	finalize_type_info(output_dti_p);

	// return it
	return output_dti_p;
}