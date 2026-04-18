#include<rhendb/function_hash.h>

#include<tuplelargetypes/text_extended.h>
#include<tuplelargetypes/blob_extended.h>
#include<tuplelargetypes/numeric_extended.h>

uint64_t hash_datum_rhendb(const datum* uval, const data_type_info* dti, tuple_hasher* th, rage_engine* ex_engine, const void* transaction_id, int* abort_error)
{
	if(is_datum_NULL(uval))
		return th->hash;
	else if(!is_container_type_info(dti)) // non container types, primitive numbers: bit_field, uint, int, large_uint, large_int, float
		return hash_datum(uval, dti, th);
	else if(is_text_type_info(dti) || is_blob_type_info(dti) || is_numeric_type_info(dti)) // all string/blob and numeric types
	{
		uint64_t hash_value = hash_tbn(uval, dti, th, &(ex_engine->wtd), ex_engine->pam_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
		return hash_value;
	}
	else if(is_extended_type_info(dti)) // then it is a non hash-able extended type like jsonb or tuple_list, so skip it
		return th->hash;
	else // else it is an inline tuple or inline array, hash it nestedly
	{
		for(uint32_t i = 0; i < get_element_count_for_datum(uval, dti); i++)
		{
			const data_type_info* child_dti;
			datum child_value;
			if(!get_containee_from_datum(&child_value, &child_dti, uval, dti, i))
				continue;
			hash_datum_rhendb(&child_value, child_dti, th, ex_engine, transaction_id, abort_error); // nestedly hash it using the very same function
			if(*abort_error)
				return 0;
		}
		return th->hash;
	}
}

uint64_t hash_tuple_rhendb(const void* tup, const tuple_def* tpl_d, const positional_accessor* element_ids, tuple_hasher* th, uint32_t element_count, rage_engine* ex_engine, const void* transaction_id, int* abort_error)
{
	for(uint32_t i = 0; i < element_count; i++)
	{
		const data_type_info* dti = get_type_info_for_element_from_tuple_def(tpl_d, (element_ids != NULL) ? element_ids[i] : STATIC_POSITION(i));
		datum uval;
		if(!get_value_from_element_from_tuple(&uval, tpl_d, (element_ids != NULL) ? element_ids[i] : STATIC_POSITION(i), tup))
			uval = (*NULL_DATUM);

		hash_datum_rhendb(&uval, dti, th, ex_engine, transaction_id, abort_error);
		if(*abort_error)
			return 0;
	}

	return th->hash;
}