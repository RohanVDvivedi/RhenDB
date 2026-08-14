#include<rhendb/util_transaction_ext_storer.h>

#include<stdlib.h>

void* tx_temp_store_tb(const char* data, uint32_t data_size, const data_type_info* ext_type_info, transaction* tx)
{
	tuple_def output_tuple_def;
	initialize_tuple_def(&output_tuple_def, (data_type_info*)ext_type_info);

	void* output_buffer = malloc(ext_type_info->max_size);
	init_tuple(&output_tuple_def, output_buffer);

	{
		int abort_error_dummy = 0;
		rage_engine* ex_engine = &(tx->rdb->volatile_rage_engine);

		binary_write_iterator* wr = get_new_binary_write_iterator(output_buffer, &output_tuple_def, SELF, 0 /*dummy root*/, get_NULL_tuple_pointer(&(ex_engine->pam_p->pas)), ex_engine->max_prefix_size_in_bytes, &(ex_engine->bstd), ex_engine->pam_p, ex_engine->pmm_p);

		temporary_extension_store* temp_ext_store = NULL;
		uint32_t bytes_written = 0;

		// write just the prefix
		while(bytes_written < data_size && wr->bytes_written_to_prefix < wr->bytes_to_be_written_to_prefix)
		{
			uint32_t bytes_to_write_this_iteration = min(wr->bytes_to_be_written_to_prefix - wr->bytes_written_to_prefix, data_size - bytes_written);
			uint32_t bytes_written_this_iteration = append_to_binary_write_iterator(wr, data + bytes_written, bytes_to_write_this_iteration, NULL, NULL, &abort_error_dummy);
			if(bytes_written_this_iteration == 0)
				break;
			bytes_written += bytes_written_this_iteration;
		}

		// write the extension
		if(bytes_written < data_size)
		{
			temp_ext_store = &(tx->temp_ext_stores[hash_element_within_tuple(output_buffer, &output_tuple_def, EXTENDED_PREFIX_POS_ACC, FNV_64_TUPLE_HASHER) % TEMPORARY_EXTENSION_STORE_COUNT]);

			// set the write iterator with new root_page_id and take the lock before any further access
			write_lock(&(temp_ext_store->blob_store_lock), BLOCKING);
			wr->blob_store_root_page_id = temp_ext_store->blob_store_root_page_id;

			const heap_table_notifier* htan_p = &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(temp_ext_store->htan));

			while(bytes_written < data_size)
			{
				uint32_t bytes_written_this_iteration = append_to_binary_write_iterator(wr, data + bytes_written, data_size - bytes_written, htan_p, NULL, &abort_error_dummy);
				if(bytes_written_this_iteration == 0)
					break;
				bytes_written += bytes_written_this_iteration;

				fix_unused_space_entries_in_store(tx, temp_ext_store);
			}
		}

		delete_binary_write_iterator(wr, NULL, &abort_error_dummy);
		if(temp_ext_store != NULL)
		{
			fix_unused_space_entries_in_store(tx, temp_ext_store);
			write_unlock(&(temp_ext_store->blob_store_lock));
		}
	}

	return output_buffer;
}

void* tx_temp_store_numeric(mpd_t* number, const data_type_info* ext_type_info, transaction* tx)
{
	tuple_def output_tuple_def;
	initialize_tuple_def(&output_tuple_def, (data_type_info*)ext_type_info);

	void* output_buffer = malloc(ext_type_info->max_size);
	init_tuple(&output_tuple_def, output_buffer);

	int exponent_too_big = 0;
	materialized_numeric mn = decimal_to_materialized_numeric(number, &exponent_too_big);

	set_sign_bits_and_exponent_for_numeric(mn.sign_bits, mn.exponent, output_buffer, &output_tuple_def, SELF);

	if(mn.sign_bits == POSITIVE_NUMERIC || mn.sign_bits == NEGATIVE_NUMERIC)
	{
		uint32_t digits_count = get_digits_count_for_materialized_numeric(&mn);

		{
			int abort_error_dummy = 0;
			rage_engine* ex_engine = &(tx->rdb->volatile_rage_engine);

			digit_write_iterator* wr = get_new_digit_write_iterator(output_buffer, &output_tuple_def, SELF, 0 /*dummy root*/, get_NULL_tuple_pointer(&(ex_engine->pam_p->pas)), ex_engine->max_prefix_size_in_bytes / BYTES_PER_NUMERIC_DIGIT, &(ex_engine->bstd), ex_engine->pam_p, ex_engine->pmm_p);

			temporary_extension_store* temp_ext_store = NULL;
			uint32_t digits_written = 0;

			// write just the prefix
			while(digits_written < digits_count && wr->digits_written_to_prefix < wr->digits_to_be_written_to_prefix)
			{
				uint32_t next_digits_count = 0;
				const uint64_t* next_digits = peek_all_contiguous_digits_from_materialized_numeric(&mn, digits_written, &next_digits_count);

				uint32_t digits_to_write_this_iteration = min(min(wr->digits_to_be_written_to_prefix - wr->digits_written_to_prefix, digits_count - digits_written), next_digits_count);
				uint32_t digits_written_this_iteration = append_to_digit_write_iterator(wr, (uint64_t*)next_digits, digits_to_write_this_iteration, NULL, NULL, &abort_error_dummy);
				if(digits_written_this_iteration == 0)
					break;
				digits_written += digits_written_this_iteration;
			}

			// write the extension
			if(digits_written < digits_count)
			{
				temp_ext_store = &(tx->temp_ext_stores[hash_element_within_tuple(output_buffer, &output_tuple_def, EXTENDED_PREFIX_POS_ACC, FNV_64_TUPLE_HASHER) % TEMPORARY_EXTENSION_STORE_COUNT]);

				// set the write iterator with new root_page_id and take the lock before any further access
				write_lock(&(temp_ext_store->blob_store_lock), BLOCKING);
				wr->blob_store_root_page_id = temp_ext_store->blob_store_root_page_id;

				const heap_table_notifier* htan_p = &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(temp_ext_store->htan));

				while(digits_written < digits_count)
				{
					uint32_t next_digits_count = 0;
					const uint64_t* next_digits = peek_all_contiguous_digits_from_materialized_numeric(&mn, digits_written, &next_digits_count);

					uint32_t digits_written_this_iteration = append_to_digit_write_iterator(wr, (uint64_t*)next_digits, min(next_digits_count, digits_count - digits_written), htan_p, NULL, &abort_error_dummy);
					if(digits_written_this_iteration == 0)
						break;
					digits_written += digits_written_this_iteration;

					fix_unused_space_entries_in_store(tx, temp_ext_store);
				}
			}

			delete_digit_write_iterator(wr, NULL, &abort_error_dummy);
			if(temp_ext_store != NULL)
			{
				fix_unused_space_entries_in_store(tx, temp_ext_store);
				write_unlock(&(temp_ext_store->blob_store_lock));
			}
		}

		// the materialized_numeric was streamed from directly, release it now
		deinitialize_materialized_numeric(&mn);
	}
	else // only positive/negative numbers have digits
	{
		deinitialize_materialized_numeric(&mn);
	}

	return output_buffer;
}