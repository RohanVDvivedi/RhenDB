#include<rhendb/rash_table.h>

#include<rhendb/function_compare.h>
#include<rhendb/function_hash.h>

positional_accessor actual_key_positions[] = {STATIC_POSITION(0)};

void initialize_hash_table_tuple_defs_for_using_rash_table(rhendb* rdb)
{
	data_type_info* record_type_info = malloc(sizeof_tuple_data_type_info(3));

	initialize_tuple_data_type_info(record_type_info, "rash_record", 1, RASH_RECORD_MAX_SIZE, 3);

	strcpy(record_type_info->containees[0].field_name, "hash");
	record_type_info->containees[0].al.type_info = UINT_NON_NULLABLE[8];

	strcpy(record_type_info->containees[1].field_name, "key");
	record_type_info->containees[1].al.type_info = get_tuple_list_extended_type_info(EXTENDED_TYPE_MAX_SIZE_FOR_KEY, PREFIX_BYTES_FOR_KEY + 4, &(rdb->volatile_rage_engine.pam_p->pas));

	strcpy(record_type_info->containees[2].field_name, "value");
	record_type_info->containees[2].al.type_info = get_blob_extended_type_info(EXTENDED_TYPE_MAX_SIZE_FOR_VALUE, get_blob_inline_type_info(PREFIX_BYTES_FOR_VALUE + 4), &(rdb->volatile_rage_engine.pam_p->pas));

	tuple_def* record_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(record_def, record_type_info);

	init_hash_table_tuple_definitions(&(rdb->rash_httd), &(rdb->volatile_rage_engine.pam_p->pas), record_def, actual_key_positions, sizeof(actual_key_positions)/sizeof(actual_key_positions[0]), FNV_64_TUPLE_HASHER);
}

#define MIN_BUCKET_COUNT 2

static int must_expand_rash_table(const rash_table_handle* rth_p)
{
	if(rth_p->bucket_count < MIN_BUCKET_COUNT)
		return 0;

	if(rth_p->total_inline_size <= rth_p->rdb->volatile_rage_engine.pam_p->pas.page_size * MIN_BUCKET_COUNT)
		return 0;

	return (rth_p->total_inline_size / (rth_p->bucket_count * rth_p->rdb->volatile_rage_engine.pam_p->pas.page_size)) > MAX_LOAD_FACTOR_IN_BYTES;
}

static int must_shrink_rash_table(const rash_table_handle* rth_p)
{
	if(rth_p->bucket_count < MIN_BUCKET_COUNT)
		return 0;

	if(rth_p->total_inline_size <= rth_p->rdb->volatile_rage_engine.pam_p->pas.page_size * MIN_BUCKET_COUNT)
		return 0;

	return (rth_p->total_inline_size / (rth_p->bucket_count * rth_p->rdb->volatile_rage_engine.pam_p->pas.page_size)) < MIN_LOAD_FACTOR_IN_BYTES;
}

rash_table_handle get_new_rash_table(const tuple_def* key_def, const positional_accessor* key_element_ids, uint32_t key_element_count, rage_engine* ex_engine, rhendb* rdb)
{
	int abort_error_dummy = 0;

	rash_table_handle rth = {
		.root_page_id = get_new_hash_table(MIN_BUCKET_COUNT, &(rdb->rash_httd), rdb->volatile_rage_engine.pam_p, rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy),

		.element_count = 0,
		.bucket_count = 0,

		.total_inline_size = 0,

		.key_tuple_defs = malloc(sizeof(tuple_def) * key_element_count),
		.key_element_count = key_element_count,

		.ex_engine = ex_engine,

		.rdb = rdb,
	};

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		data_type_info* key_dti_p = (data_type_info*) get_type_info_for_element_from_tuple_def(key_def, key_element_ids[i]);
		initialize_tuple_def(&(rth.key_tuple_defs[i]), key_dti_p);
	}

	return rth;
}

void destroy_rash_table(rash_table_handle* rth_p)
{
	int abort_error_dummy = 0;

	rash_table_iterator rti = find_all_in_rash_table(rth_p, 1);

	while(1)
	{
		const void* record_tuple = get_tuple_hash_table_iterator(rti.hti_p);
		if(record_tuple != NULL)
		{
			datum uval;
			get_value_from_element_from_tuple(&uval, rth_p->rdb->rash_httd.lpltd.record_def, SELF, record_tuple);
			delete_all_extension_worms(&uval, rth_p->rdb->rash_httd.lpltd.record_def->type_info, &(rth_p->rdb->volatile_rage_engine.wtd), rth_p->rdb->volatile_rage_engine.pam_p, rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);
		}

		if(!next_in_rash_table_iterator(&rti))
			break;
	}

	delete_rash_table_iterator(&rti);

	destroy_hash_table(rth_p->root_page_id, &(rth_p->rdb->rash_httd), rth_p->rdb->volatile_rage_engine.pam_p, NULL, &abort_error_dummy);
}

int can_initialize_rash_table_key(const rash_table_handle* rth_p, const tuple_def* record_def, const positional_accessor* key_element_ids, uint32_t key_element_count, rage_engine* ex_engine)
{
	if(rth_p->key_element_count != key_element_count)
		return 0;

	if(rth_p->ex_engine != ex_engine)
		return 0;

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		if(!are_identical_type_info(rth_p->key_tuple_defs[i].type_info, get_type_info_for_element_from_tuple_def(record_def, key_element_ids[i])))
			return 0;
	}

	return 1;
}

rash_table_key get_new_rash_table_key(const void* record, const tuple_def* record_def, const positional_accessor* key_element_ids, uint32_t key_element_count, rage_engine* ex_engine, const void* transaction_id, int* abort_error)
{
	rash_table_key rkey = {
		.record = record,
		.record_def = record_def,
		.key_element_ids = key_element_ids,
		.key_element_count = key_element_count,

		.ex_engine = ex_engine,

		.hash_value = {},
	};

	uint64_t hash_value = hash_tuple_rhendb(record, record_def, key_element_ids, FNV_64_TUPLE_HASHER, key_element_count, ex_engine, transaction_id, abort_error);

	serialize_uint64(rkey.hash_value, 8, hash_value);

	return rkey;
}

uint64_t get_hash_value_for_rash_table_key(const rash_table_key* rkey_p)
{
	return deserialize_uint64(rkey_p->hash_value, 8);
}

rash_table_iterator find_all_in_rash_table(rash_table_handle* rth_p, int is_read_only)
{
	int abort_error_dummy = 0;

	rash_table_iterator rti = {.rth_p = rth_p, .hti_p = NULL, .is_read_only = is_read_only, .rkey_p = NULL};

	rti.hti_p = get_new_hash_table_iterator(rth_p->root_page_id, WHOLE_BUCKET_RANGE, NULL, &(rth_p->rdb->rash_httd), rth_p->rdb->volatile_rage_engine.pam_p, is_read_only ? NULL : rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);

	return rti;
}

rash_table_iterator find_equals_in_rash_table(rash_table_handle* rth_p, const rash_table_key* rkey_p, int is_read_only, const void* transaction_id, int* abort_error)
{
	int abort_error_dummy = 0;

	rash_table_iterator rti = {.rth_p = rth_p, .hti_p = NULL, .is_read_only = is_read_only, .rkey_p = rkey_p};

	rti.hti_p = get_new_hash_table_iterator(rth_p->root_page_id, (bucket_range){}, rkey_p->hash_value, &(rth_p->rdb->rash_httd), rth_p->rdb->volatile_rage_engine.pam_p, is_read_only ? NULL : rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);

	while(1)
	{
		// if exists, i.e. key compares equal is found, then break out
		int exists = exists_in_rash_table_iterator(&rti, transaction_id, abort_error);
		if(*abort_error)
			break;
		if(exists)
			break;

		// if we could not go next then also break
		// going next only in the same bucket
		if(!next_hash_table_iterator(rti.hti_p, GO_NEXT_TUPLE_IN_SAME_BUCKET, NULL, &abort_error_dummy))
			break;
	}

	return rti;
}

binary_read_iterator* read_key_in_rash_table_iterator(const rash_table_iterator* rti_p)
{
	const void* record_tuple = get_tuple_hash_table_iterator(rti_p->hti_p);
	if(record_tuple == NULL)
		return NULL;

	const data_type_info* dti = get_type_info_for_element_from_tuple_def(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, STATIC_POSITION(1));
	datum uval;
	get_value_from_element_from_tuple(&uval, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, STATIC_POSITION(1), record_tuple);

	return get_new_binary_read_iterator(&uval, dti, &(rti_p->rth_p->rdb->volatile_rage_engine.wtd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p);
}

int exists_in_rash_table_iterator(const rash_table_iterator* rti_p, const void* transaction_id, int* abort_error)
{
	int abort_error_dummy = 0;

	const void* record_tuple = get_tuple_hash_table_iterator(rti_p->hti_p);

	// if the hash_table_iterator itself, says that the keys ould not match based on hash_value, then fail
	if(record_tuple == NULL)
		return 0;

	// we can compare only if the rkey_p exists, else succeed indifferently
	if(rti_p->rkey_p == NULL)
		return 1;

	int result = 1;

	// key exists so compare them
	binary_read_iterator* key_bri_p = read_key_in_rash_table_iterator(rti_p);

	for(uint32_t i = 0; i < rti_p->rkey_p->key_element_count && result == 1; i++)
	{
		const data_type_info* dti1 = get_type_info_for_element_from_tuple_def(rti_p->rkey_p->record_def, rti_p->rkey_p->key_element_ids[i]);
		datum uval1;
		get_value_from_element_from_tuple(&uval1, rti_p->rkey_p->record_def, rti_p->rkey_p->key_element_ids[i], rti_p->rkey_p->record);

		{
			char is_valid_byte = 0;
			if(!read_from_binary_read_iterator(key_bri_p, &is_valid_byte, 1, NULL, &abort_error_dummy)) // if a byte could not be read fail, e have already rached the end
			{
				result = 0;
				break;
			}
			if(!is_valid_byte) // we can not read any more bytes for this tuple in the key_bri_p
			{
				if(is_datum_NULL(&uval1))
				{
					result = 1;
					continue;
				}
				else
				{
					result = 0;
					break;
				}
			}
		}

		consume_tuple_from_tuple_list(tuple, &(rti_p->rth_p->key_tuple_defs[i]), key_bri_p, NULL, &abort_error_dummy, {
			const data_type_info* dti2 = rti_p->rth_p->key_tuple_defs[i].type_info;
			datum uval2 = {.tuple_value = tuple};

			result = (0 == compare_datum_rhendb(&uval1, dti1, &uval2, dti2, rti_p->rth_p->ex_engine, transaction_id, abort_error));
		});

		if(*abort_error)
			break;
	}

	// if ther has not been an abort_error and the result = 1
	// then result is still 1, only if key_bri_p is at its end, (ensured by checking that no more bytes can be peeked)
	if(!(*abort_error) && result == 1)
	{
		uint32_t bytes_peeked;
		peek_in_binary_read_iterator(key_bri_p, &bytes_peeked, NULL, &abort_error_dummy);
		if((*abort_error) || bytes_peeked > 0)
			result = 0;
	}

	delete_binary_read_iterator(key_bri_p, NULL, &abort_error_dummy);

	return result;
}

int remove_from_rash_table_iterator(rash_table_iterator* rti_p)
{
	if(rti_p->is_read_only)
		return 0;

	int abort_error_dummy = 0;

	const void* record_tuple = get_tuple_hash_table_iterator(rti_p->hti_p);
	if(record_tuple == NULL)
		return 0;
	else
	{
		datum uval;
		get_value_from_element_from_tuple(&uval, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, SELF, record_tuple);
		delete_all_extension_worms(&uval, rti_p->rth_p->rdb->rash_httd.lpltd.record_def->type_info, &(rti_p->rth_p->rdb->volatile_rage_engine.wtd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p, rti_p->rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);
	}

	rti_p->rth_p->element_count--;
	rti_p->rth_p->total_inline_size -= get_tuple_size(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, record_tuple);

	remove_from_hash_table_iterator(rti_p->hti_p, NULL, &abort_error_dummy);
	return 1;
}

binary_read_iterator* read_value_in_rash_table_iterator(const rash_table_iterator* rti_p)
{
	const void* record_tuple = get_tuple_hash_table_iterator(rti_p->hti_p);
	if(record_tuple == NULL)
		return NULL;

	const data_type_info* dti = get_type_info_for_element_from_tuple_def(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, STATIC_POSITION(2));
	datum uval;
	get_value_from_element_from_tuple(&uval, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, STATIC_POSITION(2), record_tuple);

	return get_new_binary_read_iterator(&uval, dti, &(rti_p->rth_p->rdb->volatile_rage_engine.wtd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p);
}

binary_write_iterator* open_for_writing_value_in_rash_table_iterator(rash_table_iterator* rti_p, const void* transaction_id, int* abort_error)
{
	if(rti_p->is_read_only)
		return NULL;

	if(rti_p->rkey_p == NULL)
		return NULL;

	int abort_error_dummy = 0;

	int exists = exists_in_rash_table_iterator(rti_p, transaction_id, abort_error);
	if(*abort_error)
		return NULL;

	if(exists) // update call
	{
		rti_p->perform_insert = 0;

		const void* record_tuple = get_tuple_hash_table_iterator(rti_p->hti_p);
		uint32_t record_tuple_size = get_tuple_size(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, record_tuple);

		void* record_tuple_copy = malloc(RASH_RECORD_MAX_SIZE);
		memory_move(record_tuple_copy, record_tuple, record_tuple_size);

		return get_new_binary_write_iterator(record_tuple_copy, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, STATIC_POSITION(2), PREFIX_BYTES_FOR_VALUE, &(rti_p->rth_p->rdb->volatile_rage_engine.wtd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p, rti_p->rth_p->rdb->volatile_rage_engine.pmm_p);
	}
	else // insert call
	{
		rti_p->perform_insert = 1;

		void* record_tuple = malloc(RASH_RECORD_MAX_SIZE);

		init_tuple(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, record_tuple);

		// insert hash_value
		set_element_in_tuple(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, STATIC_POSITION(0), record_tuple, &((const datum){.uint_value = get_hash_value_for_rash_table_key(rti_p->rkey_p)}), 0);

		// insert key
		{
			binary_write_iterator* bwi_p = get_new_binary_write_iterator(record_tuple, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, STATIC_POSITION(1), PREFIX_BYTES_FOR_KEY, &(rti_p->rth_p->rdb->volatile_rage_engine.wtd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p, rti_p->rth_p->rdb->volatile_rage_engine.pmm_p);
			for(uint32_t i = 0; i < rti_p->rkey_p->key_element_count; i++)
			{
				datum uval;
				get_value_from_element_from_tuple(&uval, rti_p->rkey_p->record_def, rti_p->rkey_p->key_element_ids[i], rti_p->rkey_p->record);

				if(is_datum_NULL(&uval))
				{
					char is_valid_byte = 0;
					append_to_binary_write_iterator(bwi_p, &is_valid_byte, 1, NULL, &abort_error_dummy);
				}
				else
				{
					char is_valid_byte = 1;
					append_to_binary_write_iterator(bwi_p, &is_valid_byte, 1, NULL, &abort_error_dummy);

					{
						void* key_element = malloc(get_maximum_tuple_size(&(rti_p->rth_p->key_tuple_defs[i])));

						init_tuple(&(rti_p->rth_p->key_tuple_defs[i]), key_element);
						set_element_in_tuple(&(rti_p->rth_p->key_tuple_defs[i]), SELF, key_element, &uval, UINT32_MAX);

						append_to_binary_write_iterator(bwi_p, key_element, get_tuple_size(&(rti_p->rth_p->key_tuple_defs[i]), key_element), NULL, &abort_error_dummy);

						free(key_element);
					}
				}
			}
			delete_binary_write_iterator(bwi_p, NULL, &abort_error_dummy);
		}

		return get_new_binary_write_iterator(record_tuple, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, STATIC_POSITION(2), PREFIX_BYTES_FOR_VALUE, &(rti_p->rth_p->rdb->volatile_rage_engine.wtd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p, rti_p->rth_p->rdb->volatile_rage_engine.pmm_p);
	}
}

void close_and_write_value_in_hash_table_iterator(rash_table_iterator* rti_p, binary_write_iterator* bwi_p)
{
	int abort_error_dummy = 0;

	// copy the tuple to be inserted or updated
	const void* tuple_to_insert = bwi_p->tupl;

	delete_binary_write_iterator(bwi_p, NULL, &abort_error_dummy);

	if(rti_p->perform_insert) // insert
	{
		rti_p->rth_p->element_count++;
		rti_p->rth_p->total_inline_size += get_tuple_size(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, tuple_to_insert);

		insert_in_hash_table_iterator(rti_p->hti_p, tuple_to_insert, NULL, &abort_error_dummy);
	}
	else // update
	{
		rti_p->rth_p->total_inline_size -= get_tuple_size(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, get_tuple_hash_table_iterator(rti_p->hti_p));
		rti_p->rth_p->total_inline_size += get_tuple_size(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, tuple_to_insert);

		update_at_hash_table_iterator(rti_p->hti_p, tuple_to_insert, NULL, &abort_error_dummy);
	}
}

int next_in_rash_table_iterator(rash_table_iterator* rti_p)
{
	int abort_error_dummy = 0;

	int went_next = 1;
	while(1)
	{
		went_next = next_hash_table_iterator(rti_p->hti_p, GO_NEXT_TUPLE_IN_MAY_BE_NEXT_EXISTING_BUCKET, NULL, &abort_error_dummy);
		if(!went_next) // if going next fails break out
			break;

		if(get_tuple_hash_table_iterator(rti_p->hti_p) != NULL) // if tuple there exists, then break out of the loop
			break;

		// continue only, if we went next but the tuple is NULL
	}

	return went_next;
}

void delete_rash_table_iterator(rash_table_iterator* rti_p)
{
	int abort_error_dummy = 0;

	hash_table_vaccum_params htvp;

	delete_hash_table_iterator(rti_p->hti_p, &htvp, NULL, &abort_error_dummy);
	rti_p->hti_p = NULL;

	perform_vaccum_hash_table(rti_p->rth_p->root_page_id, &htvp, 1, &(rti_p->rth_p->rdb->rash_httd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p, rti_p->rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);

	if(must_expand_rash_table(rti_p->rth_p))
		rti_p->rth_p->bucket_count += expand_hash_table(rti_p->rth_p->root_page_id, &(rti_p->rth_p->rdb->rash_httd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p, rti_p->rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);
	else if(must_shrink_rash_table(rti_p->rth_p))
		rti_p->rth_p->bucket_count -= shrink_hash_table(rti_p->rth_p->root_page_id, &(rti_p->rth_p->rdb->rash_httd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p, rti_p->rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);
}