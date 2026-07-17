#include<rhendb/function_compare.h>

#include<tuplelargetypes/text_extended.h>
#include<tuplelargetypes/blob_extended.h>
#include<tuplelargetypes/numeric_extended.h>

#include<stdlib.h>

int can_compare_datum_rhendb(const data_type_info* dti1, const data_type_info* dti2)
{
	if(dti1 == dti2)
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
	else if(dti1->type == TUPLE && dti2->type == TUPLE)
	{
		if(dti1->element_count != dti2->element_count)
			return 0;
		for(uint32_t i = 0; i < dti1->element_count; i++)
			if(!can_compare_datum_rhendb(get_data_type_info_for_containee_of_container_without_data(dti1, i), get_data_type_info_for_containee_of_container_without_data(dti2, i)))
				return 0;
		return 1;
	}
	else // else fail
		return 0;

	return 0;
}

int compare_datum_rhendb(const datum* uval1, const data_type_info* dti1, const datum* uval2, const data_type_info* dti2, transaction* tx)
{
	if(is_datum_NULL(uval1) && is_datum_NULL(uval2))
		return 0;
	else if(is_datum_NULL(uval1) && !is_datum_NULL(uval2))
		return -1;
	else if(!is_datum_NULL(uval1) && is_datum_NULL(uval2))
		return 1;

	if(!is_container_type_info(dti1) && !is_container_type_info(dti2)) // non container types, primitive numbers: bit_field, uint, int, large_uint, large_int, float
		return compare_datum(uval1, dti1, uval2, dti2);
	else if((is_text_type_info(dti1) || is_blob_type_info(dti1)) && (is_text_type_info(dti2) || is_blob_type_info(dti2))) // both are text or blob
	{
		// for implicit read only transactions for accessing extended types in blob_store
		const void* transaction_id = NULL;
		int abort_error = 0;

		extension_reader_iterator_callback temp1;
		rage_engine* ex_engine1;
		extension_reader_iterator_callback* callbacks1 = get_callback_and_engine_for_extended_type(tx, dti1, &ex_engine1, &temp1);

		extension_reader_iterator_callback temp2;
		rage_engine* ex_engine2;
		extension_reader_iterator_callback* callbacks2 = get_callback_and_engine_for_extended_type(tx, dti2, &ex_engine2, &temp2);

		binary_read_iterator* bri1_p = get_new_binary_read_iterator(uval1, dti1, ex_engine1 ? &(ex_engine1->bstd) : NULL, ex_engine1 ? ex_engine1->pam_p : NULL, callbacks1);
		binary_read_iterator* bri2_p = get_new_binary_read_iterator(uval2, dti2, ex_engine2 ? &(ex_engine2->bstd) : NULL, ex_engine2 ? ex_engine2->pam_p : NULL, callbacks2);
		int is_prefix = 0;

		int cmp = compare_tb(bri1_p, bri2_p, &is_prefix, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while comparing extended text/blob types\n");
			exit(-1);
		}

		delete_binary_read_iterator(bri1_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended text/blob types\n");
			exit(-1);
		}

		delete_binary_read_iterator(bri2_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended text/blob types\n");
			exit(-1);
		}

		return cmp;
	}
	else if(is_numeric_type_info(dti1) && is_numeric_type_info(dti2)) // both are numeric
	{
		// for implicit read only transactions for accessing extended types in blob_store
		const void* transaction_id = NULL;
		int abort_error = 0;

		extension_reader_iterator_callback temp1;
		rage_engine* ex_engine1;
		extension_reader_iterator_callback* callbacks1 = get_callback_and_engine_for_extended_type(tx, dti1, &ex_engine1, &temp1);

		extension_reader_iterator_callback temp2;
		rage_engine* ex_engine2;
		extension_reader_iterator_callback* callbacks2 = get_callback_and_engine_for_extended_type(tx, dti2, &ex_engine2, &temp2);

		numeric_reader_interface nri1 = init_intuple_numeric_reader_interface((*uval1), dti1, ex_engine1 ? &(ex_engine1->bstd) : NULL, ex_engine1 ? ex_engine1->pam_p : NULL, callbacks1, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended numeric type\n");
			exit(-1);
		}

		numeric_reader_interface nri2 = init_intuple_numeric_reader_interface((*uval2), dti2, ex_engine2 ? &(ex_engine2->bstd) : NULL, ex_engine2 ? ex_engine2->pam_p : NULL, callbacks2, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended numeric type\n");
			exit(-1);
		}

		int is_prefix = 0;
		int error = 0;
		int cmp = compare_numeric(&nri1, &nri2, &is_prefix, &error);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended numeric type\n");
			exit(-1);
		}

		nri1.close_digits_stream(&nri1);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended numeric type\n");
			exit(-1);
		}

		nri2.close_digits_stream(&nri2);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended numeric type\n");
			exit(-1);
		}

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
				child_value1 = (*NULL_DATUM);

			const data_type_info* child_dti2;
			datum child_value2;
			if(!get_containee_from_datum(&child_value2, &child_dti2, uval2, dti2, i))
				child_value2 = (*NULL_DATUM);

			cmp = compare_datum_rhendb(&child_value1, child_dti1, &child_value2, child_dti2, tx);
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

int compare_datum2_rhendb(const datum* uval1, const datum* uval2, const data_type_info* dti, transaction* tx)
{
	if(is_datum_NULL(uval1) && is_datum_NULL(uval2))
		return 0;
	else if(is_datum_NULL(uval1) && !is_datum_NULL(uval2))
		return -1;
	else if(!is_datum_NULL(uval1) && is_datum_NULL(uval2))
		return 1;

	if(!is_container_type_info(dti)) // non container types, primitive numbers: bit_field, uint, int, large_uint, large_int, float
		return compare_datum2(uval1, uval2, dti);
	else if(is_text_type_info(dti) || is_blob_type_info(dti))
	{
		// for implicit read only transactions for accessing extended types in blob_store
		const void* transaction_id = NULL;
		int abort_error = 0;

		extension_reader_iterator_callback temp1;
		rage_engine* ex_engine1;
		extension_reader_iterator_callback* callbacks1 = get_callback_and_engine_for_extended_type(tx, dti, &ex_engine1, &temp1);

		extension_reader_iterator_callback temp2;
		rage_engine* ex_engine2;
		extension_reader_iterator_callback* callbacks2 = get_callback_and_engine_for_extended_type(tx, dti, &ex_engine2, &temp2);

		binary_read_iterator* bri1_p = get_new_binary_read_iterator(uval1, dti, ex_engine1 ? &(ex_engine1->bstd) : NULL, ex_engine1 ? ex_engine1->pam_p : NULL, callbacks1);
		binary_read_iterator* bri2_p = get_new_binary_read_iterator(uval2, dti, ex_engine2 ? &(ex_engine2->bstd) : NULL, ex_engine2 ? ex_engine2->pam_p : NULL, callbacks2);
		int is_prefix = 0;

		int cmp = compare_tb(bri1_p, bri2_p, &is_prefix, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while comparing extended text/blob types\n");
			exit(-1);
		}

		delete_binary_read_iterator(bri1_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while comparing extended text/blob types\n");
			exit(-1);
		}

		delete_binary_read_iterator(bri2_p, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while comparing extended text/blob types\n");
			exit(-1);
		}

		return cmp;
	}
	else if(is_numeric_type_info(dti))
	{
		// for implicit read only transactions for accessing extended types in blob_store
		const void* transaction_id = NULL;
		int abort_error = 0;

		extension_reader_iterator_callback temp1;
		rage_engine* ex_engine1;
		extension_reader_iterator_callback* callbacks1 = get_callback_and_engine_for_extended_type(tx, dti, &ex_engine1, &temp1);

		extension_reader_iterator_callback temp2;
		rage_engine* ex_engine2;
		extension_reader_iterator_callback* callbacks2 = get_callback_and_engine_for_extended_type(tx, dti, &ex_engine2, &temp2);

		numeric_reader_interface nri1 = init_intuple_numeric_reader_interface((*uval1), dti, ex_engine1 ? &(ex_engine1->bstd) : NULL, ex_engine1 ? ex_engine1->pam_p : NULL, callbacks1, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended numeric type\n");
			exit(-1);
		}

		numeric_reader_interface nri2 = init_intuple_numeric_reader_interface((*uval2), dti, ex_engine2 ? &(ex_engine2->bstd) : NULL, ex_engine2 ? ex_engine2->pam_p : NULL, callbacks2, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended numeric type\n");
			exit(-1);
		}

		int is_prefix = 0;
		int error = 0;

		int cmp = compare_numeric(&nri1, &nri2, &is_prefix, &error);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended numeric type\n");
			exit(-1);
		}

		nri1.close_digits_stream(&nri1);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended numeric type\n");
			exit(-1);
		}

		nri2.close_digits_stream(&nri2);
		if(abort_error)
		{
			printf("experienced abort_error while hashing extended numeric type\n");
			exit(-1);
		}

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
				child_value1 = (*NULL_DATUM);

			datum child_value2;
			if(!get_containee_from_datum(&child_value2, &temp, uval2, dti, i))
				child_value2 = (*NULL_DATUM);

			cmp = compare_datum2_rhendb(&child_value1, &child_value2, child_dti, tx);
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

int compare_tuples_rhendb(const void* tup1, const tuple_def* tpl_d1, const positional_accessor* element_ids1, const void* tup2, const tuple_def* tpl_d2, const positional_accessor* element_ids2, const compare_direction* cmp_dir, uint32_t element_count, transaction* tx)
{
	int compare = 0;

	for(uint32_t i = 0; ((i < element_count) && (compare == 0)); i++)
	{
		const data_type_info* dti1 = get_type_info_for_element_from_tuple_def(tpl_d1, (element_ids1 != NULL) ? element_ids1[i] : STATIC_POSITION(i));
		datum uval1;
		if(!get_value_from_element_from_tuple(&uval1, tpl_d1, (element_ids1 != NULL) ? element_ids1[i] : STATIC_POSITION(i), tup1))
			uval1 = (*NULL_DATUM);

		const data_type_info* dti2 = get_type_info_for_element_from_tuple_def(tpl_d2, (element_ids2 != NULL) ? element_ids2[i] : STATIC_POSITION(i));
		datum uval2;
		if(!get_value_from_element_from_tuple(&uval2, tpl_d2, (element_ids2 != NULL) ? element_ids2[i] : STATIC_POSITION(i), tup2))
			uval2 = (*NULL_DATUM);

		compare = compare_datum_rhendb(&uval1, dti1, &uval2, dti2, tx);

		if(cmp_dir)
			compare = compare * cmp_dir[i];
	}

	return compare;
}

int compare_tuples2_rhendb(const void* tup1, const void* tup2, const tuple_def* tpl_d, const positional_accessor* element_ids, const compare_direction* cmp_dir, uint32_t element_count, transaction* tx)
{
	int compare = 0;

	for(uint32_t i = 0; ((i < element_count) && (compare == 0)); i++)
	{
		const data_type_info* dti = get_type_info_for_element_from_tuple_def(tpl_d, (element_ids != NULL) ? element_ids[i] : STATIC_POSITION(i));

		datum uval1;
		if(!get_value_from_element_from_tuple(&uval1, tpl_d, (element_ids != NULL) ? element_ids[i] : STATIC_POSITION(i), tup1))
			uval1 = (*NULL_DATUM);

		datum uval2;
		if(!get_value_from_element_from_tuple(&uval2, tpl_d, (element_ids != NULL) ? element_ids[i] : STATIC_POSITION(i), tup2))
			uval2 = (*NULL_DATUM);

		compare = compare_datum2_rhendb(&uval1, &uval2, dti, tx);

		if(cmp_dir)
			compare = compare * cmp_dir[i];
	}

	return compare;
}

int compare_datums3_rhendb(const datum* uvals1, const datum* uvals2, data_type_info const * const * dtis, const compare_direction* cmp_dir, uint32_t element_count, transaction* tx)
{
	int compare = 0;

	for(uint32_t i = 0; ((i < element_count) && (compare == 0)); i++)
	{
		compare = compare_datum2_rhendb(uvals1 + i, uvals2 + i, dtis[i], tx);

		if(cmp_dir)
			compare = compare * cmp_dir[i];
	}

	return compare;
}