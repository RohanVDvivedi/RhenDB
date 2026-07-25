#include<rhendb/util_materialization.h>

#include<stdlib.h>

char* materialize_tb(const datum uval, const data_type_info* dti, transaction* tx, uint32_t* length, uint32_t* capacity, int* error_code)
{
	(*error_code) = MATERIALIZED_SUCCESSFULLY;

	(*length) = 0;
	(*capacity) = 0;

	if((dti != NULL && !is_text_type_info(dti) && !is_blob_type_info(dti)) || is_datum_NULL(&uval))
	{
		if(is_datum_NULL(&uval))
			(*error_code) = MATERIALIZING_NULL_DATUM;
		if(dti != NULL && !is_text_type_info(dti) && !is_blob_type_info(dti))
			(*error_code) = MATERIALIZATION_TYPE_INVALID;

		return NULL;
	}

	char* buffer = NULL;

	if(dti == NULL)
	{
		(*length) = uval.string_or_binary_size;
		(*capacity) = (*length);
		buffer = malloc(*capacity);
		memory_move(buffer, uval.string_or_binary_value, (*length));
		return buffer;
	}

	{
		const void* transaction_id = NULL;
		int abort_error = 0;

		extension_reader_iterator_callback temp;
		rage_engine* ex_engine;
		extension_reader_iterator_callback* callbacks = get_callback_and_engine_for_extended_type(tx, dti, &ex_engine, &temp);

		binary_read_iterator* bri = get_new_binary_read_iterator(&uval, dti, ex_engine ? &(ex_engine->bstd) : NULL, ex_engine ? ex_engine->pam_p : NULL, callbacks);

		(*capacity) = 64;
		buffer = malloc((*capacity));

		while(1)
		{
			if((*length) == (*capacity))
			{
				if((*capacity) == UINT32_MAX)
				{
					uint32_t has_more_bytes = 0;
					peek_in_binary_read_iterator(bri, &has_more_bytes, transaction_id, &abort_error);
					if(abort_error)
					{
						printf("experienced abort_error while materializing text/blob type\n");
						exit(-1);
					}
					if(has_more_bytes)
					{
						delete_binary_read_iterator(bri, transaction_id, &abort_error);
						(*error_code) = MATERIALIZED_RESULT_TOO_BIG;
						free(buffer);
						return NULL;
					}
				}
				if(will_unsigned_mul_overflow(uint32_t, (*capacity), 2))
					(*capacity) = UINT32_MAX;
				else
					(*capacity) = 2 * (*capacity);
				buffer = realloc(buffer, (*capacity));
			}
			uint32_t bytes_read = read_from_binary_read_iterator(bri, buffer + (*length), (*capacity) - (*length), transaction_id, &abort_error);
			if(abort_error)
			{
				printf("experienced abort_error while materializing text/blob type\n");
				exit(-1);
			}
			if(bytes_read == 0)
				break;
			(*length) += bytes_read;
		}

		delete_binary_read_iterator(bri, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while materializing text/blob type\n");
			exit(-1);
		}
	}

	return buffer;
}

mpd_t materialize_numeric(const datum uval, const data_type_info* dti, transaction* tx, int* error_code)
{
	(*error_code) = MATERIALIZED_SUCCESSFULLY;

	if(!is_numeric_type_info(dti) || is_datum_NULL(&uval))
	{
		if(is_datum_NULL(&uval))
			(*error_code) = MATERIALIZING_NULL_DATUM;
		if(!is_numeric_type_info(dti))
			(*error_code) = MATERIALIZATION_TYPE_INVALID;

		mpd_t number;
		number.flags = MPD_NAN | MPD_STATIC | MPD_CONST_DATA;
		number.exp = 0;
		number.digits = 0;
		number.len = 0;
		number.alloc = 0;
		number.data = NULL;
		return number;
	}

	mpd_t number;

	{
		const void* transaction_id = NULL;
		int abort_error = 0;

		extension_reader_iterator_callback temp;
		rage_engine* ex_engine;
		extension_reader_iterator_callback* callbacks = get_callback_and_engine_for_extended_type(tx, dti, &ex_engine, &temp);

		numeric_reader_interface nri = init_intuple_numeric_reader_interface(uval, dti, ex_engine ? &(ex_engine->bstd) : NULL, ex_engine ? ex_engine->pam_p : NULL, callbacks, transaction_id, &abort_error);
		if(abort_error)
		{
			printf("experienced abort_error while materializing numeric type\n");
			exit(-1);
		}

		numeric_sign_bits sb; int16_t exp;
		nri.extract_sign_bits_and_exponent(&nri, &sb, &exp);

		materialized_numeric mn;
		if(!initialize_materialized_numeric(&mn, 8))
			exit(-1);
		set_sign_bits_and_exponent_for_materialized_numeric(&mn, sb, exp);

		if(sb == POSITIVE_NUMERIC || sb == NEGATIVE_NUMERIC)   /* only finite non-zero values carry digits */
		{
			uint64_t buf[256];
			while(1)
			{
				int err = 0;
				uint32_t digits_read = nri.read_digits_as_stream(&nri, buf, 256, &err);
				if(err)
				{
					printf("experienced abort_error while materializing numeric type\n");
					exit(-1);
				}
				if(digits_read == 0)
					break;
				/* digits stream MSD-first; push_lsd appends, keeping the MSD at the front */
				for(uint32_t i = 0; i < digits_read; i++)
				{
					if(!push_lsd_in_materialized_numeric(&mn, buf[i]))
					{
						(*error_code) = MATERIALIZED_RESULT_TOO_BIG;
						deinitialize_materialized_numeric(&mn);

						number.flags = MPD_NAN | MPD_STATIC | MPD_CONST_DATA;
						number.exp = 0;
						number.digits = 0;
						number.len = 0;
						number.alloc = 0;
						number.data = NULL;
						return number;
					}
				}
			}
		}
		nri.close_digits_stream(&nri);

		number = decimal_from_materialized_numeric(&mn);
		deinitialize_materialized_numeric(&mn);
	}

	return number;
}