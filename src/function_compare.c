#include<rhendb/function_compare.h>

#include<tuplelargetypes/text_extended.h>
#include<tuplelargetypes/blob_extended.h>
#include<tuplelargetypes/numeric_extended.h>

int can_compare_datum_rhendb(const data_type_info* dti1, const data_type_info* dti2)
{
	if(are_identical_type_info(dti1, dti2))
		return 1;
	else if(!is_container_type_info(dti1) && !is_container_type_info(dti2)) // non container types, primitive numbers: bit_field, uint, int, large_uint, large_int, float
		return can_compare_datum(dti1, dti2);
	else if((is_text_type_info(dti1) || is_blob_type_info(dti1)) && (is_text_type_info(dti2) || is_blob_type_info(dti2))) // both are text or blob
		return 1;
	else if(is_numeric_type_info(dti1) && is_numeric_type_info(dti2)) // both are numeric
		return 1;
	else if(is_extended_type_info(dti1) || is_extended_type_info(dti2)) // one of them is some not comparable extended types, like jsonb or tuple_list
		return 0;
	else if((dti1->type == STRING || dti1->type == BINARY || dti1->type == ARRAY) && (dti2->type == STRING || dti2->type == BINARY || dti2->type == ARRAY)) // STRING, BINARY and ARRAY are internally comparable, if their containee types are comparable
		return can_compare_datum_rhendb(dti1->containee, dti2->containee); // recursive call, so 2 inline arrays of extended-text types are comparable
	else // else it is not identical inline tuple, and they can not be compared directly
		return 0;

	return 0;
}

int compare_datum_rhendb(const datum* uval1, const data_type_info* dti1, const datum* uval2, const data_type_info* dti2, rage_engine* ex_engine, const void* transaction_id, int* abort_error)
{
	if(!is_container_type_info(dti1) && !is_container_type_info(dti2)) // non container types, primitive numbers: bit_field, uint, int, large_uint, large_int, float
		return compare_datum(uval1, dti1, uval2, dti2);
	else if((is_text_type_info(dti1) || is_blob_type_info(dti1)) && (is_text_type_info(dti2) || is_blob_type_info(dti2))) // both are text or blob
	{
		binary_read_iterator* bri1_p = get_new_binary_read_iterator(uval1, dti1, &(ex_engine->wtd), ex_engine->pam_p);
		binary_read_iterator* bri2_p = get_new_binary_read_iterator(uval2, dti2, &(ex_engine->wtd), ex_engine->pam_p);
		int is_prefix = 0;

		int cmp = compare_tb(bri1_p, bri2_p, &is_prefix, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR_TB;

		ABORT_ERROR_TB:;
		delete_binary_read_iterator(bri1_p, transaction_id, abort_error);
		delete_binary_read_iterator(bri2_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
		return cmp;
	}
	else if(is_numeric_type_info(dti1) && is_numeric_type_info(dti2)) // both are numeric
	{
		numeric_reader_interface nri1 = init_intuple_numeric_reader_interface((*uval1), dti1, &(ex_engine->wtd), ex_engine->pam_p, transaction_id, abort_error);
		numeric_reader_interface nri2 = init_intuple_numeric_reader_interface((*uval2), dti2, &(ex_engine->wtd), ex_engine->pam_p, transaction_id, abort_error);
		int is_prefix = 0;
		int error = 0;

		int cmp = compare_numeric(&nri1, &nri2, &is_prefix, &error);
		if(*abort_error)
			goto ABORT_ERROR_NUMERIC;

		ABORT_ERROR_NUMERIC:;
		nri1.close_digits_stream(&nri1);
		nri2.close_digits_stream(&nri2);
		if(*abort_error)
			return 0;
		return cmp;
	}
	else if(is_extended_type_info(dti1) || is_extended_type_info(dti2)) // not comparable extended types, like jsonb or tuple_list
	{
		return -2;
	}
	else // else it is inline tuple or inline array, compared using the very same function nestedly
	{
		int cmp = 0;
		uint32_t element_count1 = get_element_count_for_datum(uval1, dti1);
		uint32_t element_count2 = get_element_count_for_datum(uval2, dti2);
		uint32_t element_count = min(element_count1, element_count2);
		for(uint32_t i = 0; i < element_count && cmp == 0; i++) // perform comparison over the minimum element count of both the containers
		{
			const data_type_info* child_dti1;
			datum child_value1;
			if(!get_containee_from_datum(&child_value1, &child_dti1, uval1, dti1, i))
				return -2;

			const data_type_info* child_dti2;
			datum child_value2;
			if(!get_containee_from_datum(&child_value2, &child_dti2, uval2, dti2, i))
				return -2;

			cmp = compare_datum_rhendb(&child_value1, child_dti1, &child_value2, child_dti2, ex_engine, transaction_id, abort_error);
		}
		if(cmp == 0 && (element_count1 != element_count2))
		{
			if(element_count1 > element_count2)
				cmp = 1;
			else
				cmp = -1;
		}
		return cmp;
	}
}

int compare_datum2_rhendb(const datum* uval1, const datum* uval2, const data_type_info* dti, rage_engine* ex_engine, const void* transaction_id, int* abort_error)
{
	if(!is_container_type_info(dti)) // non container types, primitive numbers: bit_field, uint, int, large_uint, large_int, float
		return compare_datum2(uval1, uval2, dti);
	else if(is_text_type_info(dti) || is_blob_type_info(dti))
	{
		binary_read_iterator* bri1_p = get_new_binary_read_iterator(uval1, dti, &(ex_engine->wtd), ex_engine->pam_p);
		binary_read_iterator* bri2_p = get_new_binary_read_iterator(uval2, dti, &(ex_engine->wtd), ex_engine->pam_p);
		int is_prefix = 0;

		int cmp = compare_tb(bri1_p, bri2_p, &is_prefix, transaction_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR_TB;

		ABORT_ERROR_TB:;
		delete_binary_read_iterator(bri1_p, transaction_id, abort_error);
		delete_binary_read_iterator(bri2_p, transaction_id, abort_error);
		if(*abort_error)
			return 0;
		return cmp;
	}
	else if(is_numeric_type_info(dti))
	{
		numeric_reader_interface nri1 = init_intuple_numeric_reader_interface((*uval1), dti, &(ex_engine->wtd), ex_engine->pam_p, transaction_id, abort_error);
		numeric_reader_interface nri2 = init_intuple_numeric_reader_interface((*uval2), dti, &(ex_engine->wtd), ex_engine->pam_p, transaction_id, abort_error);
		int is_prefix = 0;
		int error = 0;

		int cmp = compare_numeric(&nri1, &nri2, &is_prefix, &error);
		if(*abort_error)
			goto ABORT_ERROR_NUMERIC;

		ABORT_ERROR_NUMERIC:;
		nri1.close_digits_stream(&nri1);
		nri2.close_digits_stream(&nri2);
		if(*abort_error)
			return 0;
		return cmp;
	}
	else if(is_extended_type_info(dti)) // not comparable extended types, like jsonb or tuple_list
	{
		return 0;
	}
	else // else it is inline tuple or inline array, compared using the very same function nestedly
	{
		int cmp = 0;
		uint32_t element_count1 = get_element_count_for_datum(uval1, dti);
		uint32_t element_count2 = get_element_count_for_datum(uval2, dti);
		uint32_t element_count = min(element_count1, element_count2);
		for(uint32_t i = 0; i < element_count && cmp == 0; i++) // perform comparison over the minimum element count of both the containers
		{
			const data_type_info* child_dti = get_data_type_info_for_containee_of_container_without_data(dti, i);

			const data_type_info* temp;

			datum child_value1;
			if(!get_containee_from_datum(&child_value1, &temp, uval1, dti, i))
				return -2;

			datum child_value2;
			if(!get_containee_from_datum(&child_value2, &temp, uval2, dti, i))
				return -2;

			cmp = compare_datum2_rhendb(&child_value1, &child_value2, child_dti, ex_engine, transaction_id, abort_error);
			if(*abort_error)
				return 0;
		}
		if(cmp == 0 && (element_count1 != element_count2))
		{
			if(element_count1 > element_count2)
				cmp = 1;
			else
				cmp = -1;
		}
		return cmp;
	}
}