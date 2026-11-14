#include<rhendb/cash_table.h>

#include<rhendb/function_hash.h>
#include<rhendb/function_compare.h>

static int compare_cash_table_entry(const void* context_p, const void* v1_vp, const void* v2_vp)
{
	const cash_table* ct_p = context_p;
	const cash_table_entry* v1 = v1_vp;
	const cash_table_entry* v2 = v2_vp;

	// since we will be using linkedlist as collision resolution policy, we only care if the values are equal or not, never the output of comparison result
	if(v1->hash_value != v2->hash_value)
		return 1;

	if(v1->is_key == 0 && v2->is_key == 0)
		return compare_datums3_rhendb(v1->as_value.keys, v2->as_value.keys, ct_p->key_dtis, NULL, ct_p->key_element_count, ct_p->tx);

	const datum* keys1 = v1->is_key ? v1->as_key.keys : v1->as_value.keys;
	const data_type_info** key_dtis1 = v1->is_key ? v1->as_key.key_dtis : ct_p->key_dtis;

	const datum* keys2 = v2->is_key ? v2->as_key.keys : v2->as_value.keys;
	const data_type_info** key_dtis2 = v2->is_key ? v2->as_key.key_dtis : ct_p->key_dtis;

	return compare_datums4_rhendb(keys1, key_dtis1, keys2, key_dtis2, NULL, ct_p->key_element_count, ct_p->tx);
}

static cy_uint hash_cash_table_entry(const void* v_vp)
{
	const cash_table_entry* v = v_vp;
	return v->hash_value;
}

cash_table* get_new_cash_table(uint64_t bucket_count, const tuple_def* value_def, const positional_accessor* key_element_ids, uint32_t key_element_count, transaction* tx)
{
	cash_table* ct_p = malloc(sizeof(cash_table));

	(*ct_p) = (cash_table){
		.memory_utilization = 0,
		.value_def = value_def,
		.key_element_ids = key_element_ids,
		.key_element_count = key_element_count,
		.key_dtis = malloc(sizeof(data_type_info*) * key_element_count),
		.tx = tx,
	};

	for(uint32_t j = 0; j < key_element_count; j++)
		ct_p->key_dtis[j] = get_type_info_for_element_from_tuple_def(value_def, key_element_ids[j]);

	if(!initialize_hashmap(&(ct_p->value_set), ELEMENTS_AS_LINKEDLIST_INSERT_AT_HEAD, bucket_count, &simple_hasher(hash_cash_table_entry), &contexted_comparator(ct_p, compare_cash_table_entry), offsetof(cash_table_entry, as_value.embed_node)))
		exit(-1);

	return ct_p;
}

cash_table_entry* get_new_cash_table_value(const cash_table* ct_p, void* value)
{
	cash_table_entry* v = malloc(sizeof(cash_table_entry));

	v->hash_value = 0;
	v->is_key = 0;

	v->as_value.state = 0;
	v->as_value.value = value;
	v->as_value.keys = malloc(sizeof(datum) * ct_p->key_element_count);
	initialize_llnode(&(v->as_value.embed_node));

	for(uint32_t j = 0; j < ct_p->key_element_count; j++)
	{
		const data_type_info* dti_temp;
		if(!get_value_from_element_from_tuple(&(v->as_value.keys[j]), &dti_temp, ct_p->value_def, ct_p->key_element_ids[j], value))
			v->as_value.keys[j] = (*NULL_DATUM);
	}

	tuple_hasher* th = FNV_64_TUPLE_HASHER; v->hash_value = th->hash;
	for(uint32_t j = 0; j < ct_p->key_element_count; j++)
		v->hash_value = hash_datum_rhendb(&(v->as_value.keys[j]), ct_p->key_dtis[j], th, ct_p->tx);

	return v;
}

int insert_in_cash_table(cash_table* ct_p, cash_table_entry* v)
{
	if(v->is_key)
	{
		printf("attempting to insert a key type entry in cash_table\n");
		exit(-1);
	}

	uint64_t memory_additonal_usage = sizeof(cash_table_entry) + get_tuple_size(ct_p->value_def, v->as_value.value) + (sizeof(datum) * ct_p->key_element_count);

	if(!insert_in_hashmap(&(ct_p->value_set), v))
		return 0;

	if(get_bucket_count_hashmap(&(ct_p->value_set)) * 3 < get_element_count_hashmap(&(ct_p->value_set)))
		expand_hashmap(&(ct_p->value_set), 1.5);

	ct_p->memory_utilization += memory_additonal_usage;

	return 1;
}

int remove_from_cash_table(cash_table* ct_p, cash_table_entry* v)
{
	if(v->is_key)
	{
		printf("attempting to remove a key type entry in cash_table\n");
		exit(-1);
	}

	uint64_t memory_additonal_usage = sizeof(cash_table_entry) + get_tuple_size(ct_p->value_def, v->as_value.value) + (sizeof(datum) * ct_p->key_element_count);

	if(!remove_from_hashmap(&(ct_p->value_set), v))
		return 0;

	ct_p->memory_utilization -= memory_additonal_usage;

	return 1;
}

cash_table_entry get_new_cash_table_key(const cash_table* ct_p, void* key_tuple, const tuple_def* key_def, const positional_accessor* key_element_ids)
{
	cash_table_entry k = (cash_table_entry){
		.hash_value = 0,
		.is_key = 1,
		.as_key = {
			.keys = malloc(sizeof(datum) * ct_p->key_element_count),
			.key_dtis = malloc(sizeof(data_type_info*) * ct_p->key_element_count),
		}
	};

	for(uint32_t j = 0; j < ct_p->key_element_count; j++)
		if(!get_value_from_element_from_tuple(&(k.as_key.keys[j]), &(k.as_key.key_dtis[j]), key_def, key_element_ids[j], key_tuple))
			k.as_key.keys[j] = (*NULL_DATUM);

	tuple_hasher* th = FNV_64_TUPLE_HASHER; k.hash_value = th->hash;
	for(uint32_t j = 0; j < ct_p->key_element_count; j++)
		k.hash_value = hash_datum_rhendb(&(k.as_key.keys[j]), k.as_key.key_dtis[j], th, ct_p->tx);

	return k;
}

void destroy_cash_table_entry(cash_table_entry* e)
{
	if(e->is_key)
	{
		free(e->as_key.keys);
		free(e->as_key.key_dtis);
	}
	else
	{
		free(e->as_value.keys);
		free(e->as_value.value);
		free(e);
	}
}

static void destroy_cash_table_entry_internal(void* resource_p, const void* v_vp)
{
	cash_table_entry* v = (cash_table_entry*) v_vp;
	destroy_cash_table_entry(v);
}

void destroy_cash_table(cash_table* ct_p)
{
	remove_all_from_hashmap(&(ct_p->value_set), &((const notifier_interface){.notify = destroy_cash_table_entry_internal}));
	deinitialize_hashmap(&(ct_p->value_set));
	free(ct_p->key_dtis);
	free(ct_p);
}