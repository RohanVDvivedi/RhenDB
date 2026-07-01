#include<rhendb/rash_table.h>

#include<rhendb/function_compare.h>
#include<rhendb/function_hash.h>

#include<rhendb/nullable_type_info_maker.h>

static const positional_accessor hash_position = STATIC_POSITION(0);
static const positional_accessor key_position = STATIC_POSITION(1);
static const positional_accessor value_position = STATIC_POSITION(2);
static const positional_accessor tail_of_value_position = STATIC_POSITION(3);
static const positional_accessor state_position = STATIC_POSITION(4);

static const positional_accessor actual_key_positions[] = {hash_position};

void initialize_hash_table_tuple_defs_for_using_rash_table(rhendb* rdb)
{
	data_type_info* record_type_info = malloc(sizeof_tuple_data_type_info(5));

	initialize_tuple_data_type_info(record_type_info, "rash_record", 1, RASH_RECORD_MAX_SIZE, 5);

	strcpy(record_type_info->containees[0].field_name, "hash");
	record_type_info->containees[0].al.type_info = UINT_NON_NULLABLE[8];

	strcpy(record_type_info->containees[1].field_name, "key");
	record_type_info->containees[1].al.type_info = get_tuple_list_extended_type_info(EXTENDED_TYPE_MAX_SIZE_FOR_KEY, PREFIX_BYTES_FOR_KEY + 4, &(rdb->volatile_rage_engine.pam_p->pas));

	strcpy(record_type_info->containees[2].field_name, "value");
	record_type_info->containees[2].al.type_info = get_blob_extended_type_info(EXTENDED_TYPE_MAX_SIZE_FOR_VALUE, get_blob_inline_type_info(PREFIX_BYTES_FOR_VALUE + 4), &(rdb->volatile_rage_engine.pam_p->pas));

	strcpy(record_type_info->containees[3].field_name, "tail_of_value");
	record_type_info->containees[3].al.type_info = &(rdb->volatile_rage_engine.pam_p->pas.tuple_pointer_type_info);

	strcpy(record_type_info->containees[4].field_name, "state");
	record_type_info->containees[4].al.type_info = BIT_FIELD_NON_NULLABLE[STATE_BITS];

	tuple_def* record_def = malloc(sizeof(tuple_def));
	initialize_tuple_def(record_def, record_type_info);

	init_hash_table_tuple_definitions(&(rdb->rash_httd), &(rdb->volatile_rage_engine.pam_p->pas), record_def, actual_key_positions, sizeof(actual_key_positions)/sizeof(actual_key_positions[0]), FNV_64_TUPLE_HASHER);
}

#define MAX_BLOB_STORE_INVALID_ENTRIES 56
#define THRESHOLD_BLOB_STORE_INVALID_ENTRIES 14

#define MAX_INTERMEDIATE_TUPLE_SIZE 3.9e9

rash_table_handle get_new_rash_table(uint64_t initial_bucket_count, const tuple_def* key_def, const positional_accessor* key_element_ids, uint32_t key_element_count, rage_engine* ex_engine, rhendb* rdb)
{
	int abort_error_dummy = 0;

	rash_table_handle rth = {
		.hth = get_new_hash_table(initial_bucket_count, &(rdb->rash_httd), rdb->volatile_rage_engine.pam_p, rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy),

		.element_count = 0,
		.bucket_count = initial_bucket_count,

		.total_inline_size = 0,

		.key_element_count = key_element_count,

		.blob_store_root_page_id = get_new_blob_store(&(rdb->volatile_rage_engine.bstd), rdb->volatile_rage_engine.pam_p, rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy),

		.ex_engine = ex_engine,

		.rdb = rdb,
	};

	data_type_info* key_dti_p = malloc(sizeof_tuple_data_type_info(key_element_count));
	uint64_t max_key_tuple_size = 8;

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		data_type_info* key_dti = (data_type_info*) get_type_info_for_element_from_tuple_def(key_def, key_element_ids[i]);

		if(key_dti->type == BIT_FIELD)
			max_key_tuple_size += 9;
		else
			max_key_tuple_size += key_dti->is_variable_sized ? (8 + key_dti->max_size) : (1 + key_dti->size);

		sprintf(key_dti_p->containees[i].field_name, "key_%u", i);
		key_dti_p->containees[i].al.type_info = shallow_clone_into_nullable_type(key_dti);
	}

	if(max_key_tuple_size > MAX_INTERMEDIATE_TUPLE_SIZE)
	{
		printf("too big key tuple for rash_table\n");
		exit(-1);
	}

	initialize_tuple_data_type_info(key_dti_p, "rash_table_key", 0, max_key_tuple_size, key_element_count);

	initialize_tuple_def(&(rth.key_tuple_def), key_dti_p);

	initialize_heap_table_accumulative_notifier(&(rth.htan), MAX_BLOB_STORE_INVALID_ENTRIES);

	return rth;
}

int expand_rash_table(rash_table_handle* rth_p)
{
	int abort_error_dummy = 0;
	int expanded = expand_hash_table(&(rth_p->hth), &(rth_p->rdb->rash_httd), rth_p->rdb->volatile_rage_engine.pam_p, rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);
	rth_p->bucket_count += expanded;
	return expanded;
}

int shrink_rash_table(rash_table_handle* rth_p)
{
	int abort_error_dummy = 0;
	int shrunk = shrink_hash_table(&(rth_p->hth), &(rth_p->rdb->rash_httd), rth_p->rdb->volatile_rage_engine.pam_p, rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);
	rth_p->bucket_count -= shrunk;
	return shrunk;
}

void fix_all_incorrect_unused_space_entries_in_blob_store_of_rash_table(rash_table_handle* rth_p, int force_fix_invalid_entries)
{
	int abort_error_dummy = 0;

	if(get_notification_count_for_heap_table_accumulative_notifier(&(rth_p->htan)) == 0)
		return;

	// if not-forced-to-fix and threshold-not-crossed, then no need to fix
	if((!force_fix_invalid_entries) && (get_notification_count_for_heap_table_accumulative_notifier(&(rth_p->htan)) < THRESHOLD_BLOB_STORE_INVALID_ENTRIES))
		return;

	uint64_t root_page_id;
	uint32_t unused_space;
	uint64_t page_id;
	while(pop_from_heap_table_accumulative_notifier(&(rth_p->htan), &root_page_id, &unused_space, &page_id))
	{
		fix_unused_space_in_heap_table(root_page_id, unused_space, page_id, &(rth_p->rdb->volatile_rage_engine.bstd.httd), rth_p->rdb->volatile_rage_engine.pam_p, rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);
	}
}

void destroy_rash_table(rash_table_handle* rth_p)
{
	int abort_error_dummy = 0;

	deinitialize_heap_table_accumulative_notifier(&(rth_p->htan));

	destroy_hash_table(&(rth_p->hth), &(rth_p->rdb->rash_httd), rth_p->rdb->volatile_rage_engine.pam_p, NULL, &abort_error_dummy);

	destroy_blob_store(rth_p->blob_store_root_page_id, &(rth_p->rdb->volatile_rage_engine.bstd), rth_p->rdb->volatile_rage_engine.pam_p, NULL, &abort_error_dummy);

	for(uint32_t i = 0; i < rth_p->key_element_count; i++)
		free(rth_p->key_tuple_def.type_info->containees[i].al.type_info);
	free(rth_p->key_tuple_def.type_info);
}

void print_rash_table(rash_table_handle* rth_p, void (*print_value)(binary_read_iterator* value_bri_p))
{
	int abort_error_dummy = 0;

	printf("RASH_TABLE\n\n");
	printf("element_count = %"PRIu64"\n", rth_p->element_count);
	printf("bucket_count = %"PRIu64"\n", rth_p->bucket_count);
	printf("total_inline_size = %"PRIu64"\n\n", rth_p->total_inline_size);

	uint64_t bucket_count = get_bucket_count_hash_table(&(rth_p->hth), &(rth_p->rdb->rash_httd), rth_p->rdb->volatile_rage_engine.pam_p, NULL, &abort_error_dummy);

	printf("actual_bucket_count = %"PRIu64"\n\n", bucket_count);

	for(uint64_t i = 0; i < bucket_count; i++)
	{
		hash_table_iterator* hti_p = get_new_hash_table_iterator(&(rth_p->hth), (bucket_range){i, i}, NULL, &(rth_p->rdb->rash_httd), rth_p->rdb->volatile_rage_engine.pam_p, NULL, NULL, &abort_error_dummy);

		printf("BUCKET : %"PRIu64"\n\n", i);
		while(1)
		{
			const void* entry = get_tuple_hash_table_iterator(hti_p);
			if(entry == NULL)
				break;

			{
				datum uval;
				get_value_from_element_from_tuple(&uval, rth_p->rdb->rash_httd.lpltd.record_def, hash_position, entry);
				printf("\tHASH(%"PRIu64")\n", uval.uint_value);
			}

			{
				datum uval;
				get_value_from_element_from_tuple(&uval, rth_p->rdb->rash_httd.lpltd.record_def, state_position, entry);
				printf("\tSTATE(%"PRIu64")\n", uval.bit_field_value);
			}

			printf("\t\tKEY(\n");
			{
				const data_type_info* dti = get_type_info_for_element_from_tuple_def(rth_p->rdb->rash_httd.lpltd.record_def, key_position);
				datum uval;
				get_value_from_element_from_tuple(&uval, rth_p->rdb->rash_httd.lpltd.record_def, key_position, entry);

				binary_read_iterator* key_bri_p = get_new_binary_read_iterator(&uval, dti, &(rth_p->rdb->volatile_rage_engine.bstd), rth_p->rdb->volatile_rage_engine.pam_p);
				{
					consume_tuple_from_tuple_list(tuple, &(rth_p->key_tuple_def), key_bri_p, NULL, &abort_error_dummy, {
						printf("\t\t\t");print_tuple(tuple, &(rth_p->key_tuple_def));
					});
				}
				delete_binary_read_iterator(key_bri_p, NULL, &abort_error_dummy);
			}
			printf("\t\t)\n");

			printf("\t\tVALUE(\n");
			{
				const data_type_info* dti = get_type_info_for_element_from_tuple_def(rth_p->rdb->rash_httd.lpltd.record_def, value_position);
				datum uval;
				get_value_from_element_from_tuple(&uval, rth_p->rdb->rash_httd.lpltd.record_def, value_position, entry);

				binary_read_iterator* value_bri_p = get_new_binary_read_iterator(&uval, dti, &(rth_p->rdb->volatile_rage_engine.bstd), rth_p->rdb->volatile_rage_engine.pam_p);
				
				print_value(value_bri_p);

				delete_binary_read_iterator(value_bri_p, NULL, &abort_error_dummy);
			}
			printf("\t\t)\n");

			printf("\n");

			if(!next_hash_table_iterator(hti_p, GO_NEXT_TUPLE_IN_SAME_BUCKET, NULL, &abort_error_dummy))
				break;
		}

		printf("\n\n");

		hash_table_vaccum_params htvp;
		delete_hash_table_iterator(hti_p, &htvp, NULL, &abort_error_dummy);
	}
}

int can_initialize_rash_table_key(const rash_table_handle* rth_p, const tuple_def* record_def, const positional_accessor* key_element_ids, uint32_t key_element_count, rage_engine* ex_engine)
{
	if(rth_p->key_element_count != key_element_count)
		return 0;

	if(rth_p->ex_engine != ex_engine)
		return 0;

	for(uint32_t i = 0; i < key_element_count; i++)
	{
		if(!are_identical_type_info(rth_p->key_tuple_def.type_info->containees[i].al.type_info, get_type_info_for_element_from_tuple_def(record_def, key_element_ids[i])))
			return 0;
	}

	return 1;
}

rash_table_key get_new_rash_table_key(const void* record, const tuple_def* record_def, const positional_accessor* key_element_ids, uint32_t key_element_count, rage_engine* ex_engine)
{
	rash_table_key rkey = {
		.record = record,
		.record_def = record_def,
		.key_element_ids = key_element_ids,
		.key_element_count = key_element_count,

		.ex_engine = ex_engine,

		.hash_value = {},
	};

	uint64_t hash_value = hash_tuple_rhendb(record, record_def, key_element_ids, FNV_64_TUPLE_HASHER, key_element_count, ex_engine);

	serialize_uint64(rkey.hash_value, 8, hash_value);

	return rkey;
}

uint64_t get_hash_value_for_rash_table_key(const rash_table_key* rkey_p)
{
	return deserialize_uint64(rkey_p->hash_value, 8);
}

void destroy_rash_table_key(rash_table_key* rkey_p)
{
}

rash_table_iterator find_all_in_rash_table(rash_table_handle* rth_p, int is_read_only)
{
	int abort_error_dummy = 0;

	rash_table_iterator rti = {.rth_p = rth_p, .hti_p = NULL, .is_read_only = is_read_only, .rkey_p = NULL, .pointing_to_rkey = 0};

	rti.hti_p = get_new_hash_table_iterator(&(rth_p->hth), WHOLE_BUCKET_RANGE, NULL, &(rth_p->rdb->rash_httd), rth_p->rdb->volatile_rage_engine.pam_p, is_read_only ? NULL : rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);

	return rti;
}

rash_table_iterator find_equals_in_rash_table(rash_table_handle* rth_p, const rash_table_key* rkey_p, int is_read_only)
{
	int abort_error_dummy = 0;

	rash_table_iterator rti = {.rth_p = rth_p, .hti_p = NULL, .is_read_only = is_read_only, .rkey_p = rkey_p, .pointing_to_rkey = 0};

	rti.hti_p = get_new_hash_table_iterator(&(rth_p->hth), (bucket_range){}, rkey_p->hash_value, &(rth_p->rdb->rash_httd), rth_p->rdb->volatile_rage_engine.pam_p, is_read_only ? NULL : rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);

	while(1)
	{
		// if exists, i.e. key compares equal is found, then break out
		int exists = exists_in_rash_table_iterator(&rti);
		if(exists)
		{
			rti.pointing_to_rkey = 1;
			break;
		}

		// if we could not go next then also break
		// going next only in the same bucket
		if(!next_hash_table_iterator(rti.hti_p, GO_NEXT_TUPLE_IN_SAME_BUCKET, NULL, &abort_error_dummy))
			break;
	}

	return rti;
}

void* read_key_in_rash_table_iterator(const rash_table_iterator* rti_p)
{
	int abort_error_dummy = 0;

	const void* record_tuple = get_tuple_hash_table_iterator(rti_p->hti_p);
	if(record_tuple == NULL)
		return NULL;

	const data_type_info* dti = get_type_info_for_element_from_tuple_def(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, key_position);
	datum uval;
	get_value_from_element_from_tuple(&uval, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, key_position, record_tuple);

	binary_read_iterator* key_bri_p = get_new_binary_read_iterator(&uval, dti, &(rti_p->rth_p->rdb->volatile_rage_engine.bstd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p);

	void* key_tuple = read_tuple_from_binary_read_iterator(key_bri_p, &(rti_p->rth_p->key_tuple_def), NULL, &abort_error_dummy);

	delete_binary_read_iterator(key_bri_p, NULL, &abort_error_dummy);

	return key_tuple;
}

uint64_t read_state_in_rash_table_iterator(const rash_table_iterator* rti_p)
{
	const void* record_tuple = get_tuple_hash_table_iterator(rti_p->hti_p);
	if(record_tuple == NULL)
		return 0;

	datum uval;
	get_value_from_element_from_tuple(&uval, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, state_position, record_tuple);

	return uval.bit_field_value;
}

int write_state_in_rash_table_iterator(rash_table_iterator* rti_p, uint64_t state)
{
	if(rti_p->is_read_only)
		return 0;

	int abort_error_dummy = 0;

	if(NULL == get_tuple_hash_table_iterator(rti_p->hti_p))
		return 0;

	return update_non_key_element_in_place_at_hash_table_iterator(rti_p->hti_p, state_position, &((const datum){.bit_field_value = state}), NULL, &abort_error_dummy);
}

int exists_in_rash_table_iterator(const rash_table_iterator* rti_p)
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
	const data_type_info* dti = get_type_info_for_element_from_tuple_def(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, key_position);
	datum uval;
	get_value_from_element_from_tuple(&uval, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, key_position, record_tuple);

	binary_read_iterator* key_bri_p = get_new_binary_read_iterator(&uval, dti, &(rti_p->rth_p->rdb->volatile_rage_engine.bstd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p);

	consume_tuple_from_tuple_list(tuple, &(rti_p->rth_p->key_tuple_def), key_bri_p, NULL, &abort_error_dummy, {
		result = (0 == compare_tuples_rhendb(tuple, &(rti_p->rth_p->key_tuple_def), NULL, rti_p->rkey_p->record, rti_p->rkey_p->record_def, rti_p->rkey_p->key_element_ids, NULL, rti_p->rth_p->key_element_count, rti_p->rth_p->ex_engine));
	});

	delete_binary_read_iterator(key_bri_p, NULL, &abort_error_dummy);

	return result;
}

static void delete_all_chunks_in_blobs_of_blob_stores(rash_table_handle* rth_p, tuple_pointer blob_head)
{
	int abort_error_dummy = 0;

	// if blob_head is NULL, return immediately
	if(is_tuple_pointer_NULL(blob_head, &(rth_p->rdb->volatile_rage_engine.pam_p->pas)))
		return;

	blob_store_write_iterator* bswi_p = get_new_blob_store_write_iterator(rth_p->blob_store_root_page_id, blob_head, get_NULL_tuple_pointer(&(rth_p->rdb->volatile_rage_engine.pam_p->pas)), &(rth_p->rdb->volatile_rage_engine.bstd), rth_p->rdb->volatile_rage_engine.pam_p, rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);

	while(0 < discard_from_head_in_blob(bswi_p, 32 * 1024 * 1024, &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(rth_p->htan)), NULL, &abort_error_dummy));

	delete_blob_store_write_iterator(bswi_p, NULL, &abort_error_dummy);
}

int remove_from_rash_table_iterator(rash_table_iterator* rti_p)
{
	if(rti_p->is_read_only)
		return 0;

	int abort_error_dummy = 0;

	const void* record_tuple = get_tuple_hash_table_iterator(rti_p->hti_p);
	if(record_tuple == NULL)
		return 0;

	{
		{
			datum uval;
			get_value_from_element_from_tuple(&uval, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, STATIC_POSITION(1, 1), record_tuple);
			tuple_pointer blob_head = get_tuple_pointer(uval.tuple_value, &(rti_p->rth_p->rdb->volatile_rage_engine.pam_p->pas));
			delete_all_chunks_in_blobs_of_blob_stores(rti_p->rth_p, blob_head);
		}

		{
			datum uval;
			get_value_from_element_from_tuple(&uval, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, STATIC_POSITION(2, 1), record_tuple);
			tuple_pointer blob_head = get_tuple_pointer(uval.tuple_value, &(rti_p->rth_p->rdb->volatile_rage_engine.pam_p->pas));
			delete_all_chunks_in_blobs_of_blob_stores(rti_p->rth_p, blob_head);
		}

		// it might have invalidated entries in heap table of the blob_store so fix them next
		fix_all_incorrect_unused_space_entries_in_blob_store_of_rash_table(rti_p->rth_p, 0);
	}

	rti_p->rth_p->element_count--;
	rti_p->rth_p->total_inline_size -= get_tuple_size(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, record_tuple);
	rti_p->pointing_to_rkey = 0;

	remove_from_hash_table_iterator(rti_p->hti_p, NULL, &abort_error_dummy);
	return 1;
}

binary_read_iterator* read_value_in_rash_table_iterator(const rash_table_iterator* rti_p)
{
	const void* record_tuple = get_tuple_hash_table_iterator(rti_p->hti_p);
	if(record_tuple == NULL)
		return NULL;

	const data_type_info* dti = get_type_info_for_element_from_tuple_def(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, value_position);
	datum uval;
	get_value_from_element_from_tuple(&uval, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, value_position, record_tuple);

	return get_new_binary_read_iterator(&uval, dti, &(rti_p->rth_p->rdb->volatile_rage_engine.bstd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p);
}

binary_write_iterator* open_for_writing_value_in_rash_table_iterator(rash_table_iterator* rti_p)
{
	if(rti_p->is_read_only)
		return NULL;

	if(rti_p->rkey_p == NULL)
		return NULL;

	int abort_error_dummy = 0;

	if(rti_p->pointing_to_rkey) // update call
	{
		rti_p->perform_insert = 0;

		const void* record_tuple = get_tuple_hash_table_iterator(rti_p->hti_p);
		uint32_t record_tuple_size = get_tuple_size(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, record_tuple);

		void* record_tuple_copy = malloc(RASH_RECORD_MAX_SIZE);
		memory_move(record_tuple_copy, record_tuple, record_tuple_size);

		tuple_pointer tail_of_value;
		{
			datum uval;
			get_value_from_element_from_tuple(&uval, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, tail_of_value_position, record_tuple_copy);
			tail_of_value = get_tuple_pointer(uval.tuple_value, &(rti_p->rth_p->rdb->volatile_rage_engine.pam_p->pas));
		}

		return get_new_binary_write_iterator(record_tuple_copy, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, value_position, rti_p->rth_p->blob_store_root_page_id, tail_of_value, PREFIX_BYTES_FOR_VALUE, &(rti_p->rth_p->rdb->volatile_rage_engine.bstd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p, rti_p->rth_p->rdb->volatile_rage_engine.pmm_p);
	}
	else // insert call
	{
		rti_p->perform_insert = 1;

		void* record_tuple = malloc(RASH_RECORD_MAX_SIZE);

		init_tuple(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, record_tuple);

		// insert hash_value
		set_element_in_tuple(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, hash_position, record_tuple, &((const datum){.uint_value = get_hash_value_for_rash_table_key(rti_p->rkey_p)}), 0);

		// insert key
		set_element_in_tuple(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, key_position, record_tuple, EMPTY_DATUM, UINT32_MAX);
		{
			binary_write_iterator* bwi_p = get_new_binary_write_iterator(record_tuple, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, key_position, rti_p->rth_p->blob_store_root_page_id, get_NULL_tuple_pointer(&(rti_p->rth_p->rdb->volatile_rage_engine.pam_p->pas)), PREFIX_BYTES_FOR_KEY, &(rti_p->rth_p->rdb->volatile_rage_engine.bstd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p, rti_p->rth_p->rdb->volatile_rage_engine.pmm_p);

			{
				uint32_t key_tuple_size = get_minimum_tuple_size(&(rti_p->rth_p->key_tuple_def));
				uint32_t key_tuple_capacity = key_tuple_size;
				void* key_tuple = malloc(key_tuple_capacity);
				init_tuple(&(rti_p->rth_p->key_tuple_def), key_tuple);

				for(uint32_t i = 0; i < rti_p->rth_p->key_element_count; i++)
				{
					datum uval;
					get_value_from_element_from_tuple(&uval, rti_p->rkey_p->record_def, rti_p->rkey_p->key_element_ids[i], rti_p->rkey_p->record);

					while(!set_element_in_tuple(&(rti_p->rth_p->key_tuple_def), STATIC_POSITION(i), key_tuple, &uval, key_tuple_capacity - key_tuple_size))
					{
						key_tuple_capacity = min(key_tuple_capacity * 2, get_maximum_tuple_size(&(rti_p->rth_p->key_tuple_def)));
						key_tuple = realloc(key_tuple, key_tuple_capacity);
					}

					// recompute tuple_size
					key_tuple_size = get_tuple_size(&(rti_p->rth_p->key_tuple_def), key_tuple);
				}

				append_to_binary_write_iterator(bwi_p, key_tuple, get_tuple_size(&(rti_p->rth_p->key_tuple_def), key_tuple), &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(rti_p->rth_p->htan)), NULL, &abort_error_dummy);

				free(key_tuple);
			}

			delete_binary_write_iterator(bwi_p, NULL, &abort_error_dummy);

			// it might have invalidated entries in heap table of the blob_store so fix them next
			fix_all_incorrect_unused_space_entries_in_blob_store_of_rash_table(rti_p->rth_p, 0);
		}

		set_element_in_tuple(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, value_position, record_tuple, EMPTY_DATUM, UINT32_MAX);
		return get_new_binary_write_iterator(record_tuple, rti_p->rth_p->rdb->rash_httd.lpltd.record_def, value_position, rti_p->rth_p->blob_store_root_page_id, get_NULL_tuple_pointer(&(rti_p->rth_p->rdb->volatile_rage_engine.pam_p->pas)), PREFIX_BYTES_FOR_VALUE, &(rti_p->rth_p->rdb->volatile_rage_engine.bstd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p, rti_p->rth_p->rdb->volatile_rage_engine.pmm_p);
	}
}

void close_and_write_value_in_hash_table_iterator(rash_table_iterator* rti_p, binary_write_iterator* bwi_p)
{
	int abort_error_dummy = 0;

	// it might have invalidated entries in heap table of the blob_store so fix them next
	fix_all_incorrect_unused_space_entries_in_blob_store_of_rash_table(rti_p->rth_p, 0);

	// copy the tuple to be inserted or updated
	void* tuple_to_insert = bwi_p->tupl;
	int was_inline_OR_extended_head_modified = bwi_p->was_inline_OR_extended_head_modified;

	// set the tail of the bwi_p in the tuple to be inserted, this allows us to append more valuees to it
	char tail_of_value_tuple[sizeof(tuple_pointer)];
	{
		set_tuple_pointer(tail_of_value_tuple, bwi_p->extension_tail, &(rti_p->rth_p->rdb->volatile_rage_engine.pam_p->pas));
		set_element_in_tuple(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, tail_of_value_position, tuple_to_insert, &((const datum){.tuple_value = tail_of_value_tuple}), 0);
	}

	delete_binary_write_iterator(bwi_p, NULL, &abort_error_dummy);

	if(rti_p->perform_insert) // insert
	{
		rti_p->rth_p->element_count++;
		rti_p->rth_p->total_inline_size += get_tuple_size(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, tuple_to_insert);

		insert_in_hash_table_iterator(rti_p->hti_p, tuple_to_insert, NULL, &abort_error_dummy);

		// by default the hash_table_iterator inserts prior to the tuple being pointed to
		// so we go previous, so that we still point to the presently inserted tuple

		prev_hash_table_iterator(rti_p->hti_p, GO_NEXT_TUPLE_IN_SAME_BUCKET, NULL, &abort_error_dummy);
	}
	else // update
	{
		rti_p->rth_p->total_inline_size -= get_tuple_size(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, get_tuple_hash_table_iterator(rti_p->hti_p));
		rti_p->rth_p->total_inline_size += get_tuple_size(rti_p->rth_p->rdb->rash_httd.lpltd.record_def, tuple_to_insert);

		if(was_inline_OR_extended_head_modified) // update the whole tuple in place
			update_at_hash_table_iterator(rti_p->hti_p, tuple_to_insert, NULL, &abort_error_dummy);
		else
			update_non_key_element_in_place_at_hash_table_iterator(rti_p->hti_p, tail_of_value_position, &((const datum){.tuple_value = tail_of_value_tuple}), NULL, &abort_error_dummy);
	}

	free(tuple_to_insert);
}

int next_in_rash_table_iterator(rash_table_iterator* rti_p)
{
	if(rti_p->rkey_p != NULL)
		return 0;

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

	perform_vaccum_hash_table(&(rti_p->rth_p->hth), &htvp, 1, &(rti_p->rth_p->rdb->rash_httd), rti_p->rth_p->rdb->volatile_rage_engine.pam_p, rti_p->rth_p->rdb->volatile_rage_engine.pmm_p, NULL, &abort_error_dummy);
}