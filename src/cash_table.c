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
		return compare_tuples2_rhendb(v1->value, v2->value, ct_p->value_def, ct_p->key_element_ids, NULL, ct_p->key_element_count, ct_p->tx);


	const void* tup1 = (v1->is_key == 0) ? v1->key_tuple : v1->value;
	const tuple_def* tpl_d1 = (v1->is_key == 0) ? v1->key_tuple_def : ct_p->value_def;
	const positional_accessor* element_ids1 = (v1->is_key == 0) ? v1->key_element_ids : ct_p->key_element_ids;

	const void* tup2 = (v2->is_key == 0) ? v2->key_tuple : v2->value;
	const tuple_def* tpl_d2 = (v2->is_key == 0) ? v2->key_tuple_def : ct_p->value_def;
	const positional_accessor* element_ids2 = (v2->is_key == 0) ? v2->key_element_ids : ct_p->key_element_ids;

	return compare_tuples_rhendb(tup1, tpl_d1, element_ids1, tup2, tpl_d2, element_ids2, NULL, ct_p->key_element_count, ct_p->tx);
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
		.tx = tx,
	};

	if(!initialize_hashmap(&(ct_p->value_set), ELEMENTS_AS_LINKEDLIST_INSERT_AT_HEAD, bucket_count, &simple_hasher(hash_cash_table_entry), &contexted_comparator(ct_p, compare_cash_table_entry), offsetof(cash_table_entry, embed_node)))
		exit(-1);

	return ct_p;
}

cash_table_entry get_new_cash_table_value(const cash_table* ct_p, void* value);

cash_table_entry* insert_in_cash_table(cash_table* ct_p, const cash_table_entry* v)
{
	if(v->is_key)
		exit(-1);

	uint64_t memory_additonal_usage = sizeof(cash_table_entry) + get_tuple_size(ct_p->value_def, v->value);

	cash_table_entry* c = malloc(sizeof(cash_table_entry));
	c->hash_value = v->hash_value;
	c->is_key = 0;
	c->state = 0;
	c->value = v->value;
	initialize_bstnode(&(c->embed_node));

	insert_in_hashmap(&(ct_p->value_set), v);

	if(get_bucket_count_hashmap(&(ct_p->value_set)) * 3 < get_element_count_hashmap(&(ct_p->value_set)))
		expand_hashmap(&(ct_p->value_set), 1.5);

	ct_p->memory_utilization += memory_additonal_usage;

	return c;
}

cash_table_entry get_new_cash_table_key(const cash_table* ct_p, void* key_tuple, tuple_def* key_tuple_def, positional_accessor* key_element_ids);

static void destroy_cash_table_entry(void* resource_p, const void* v_vp)
{
	cash_table_entry* v = (cash_table_entry*) v_vp;
	free(v->value);
	free(v);
}

void destroy_cash_table(cash_table* ct_p)
{
	remove_all_from_hashmap(&(ct_p->value_set), &((const notifier_interface){.notify = destroy_cash_table_entry}));
	deinitialize_hashmap(&(ct_p->value_set));
}