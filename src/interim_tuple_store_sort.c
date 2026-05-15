#include<rhendb/interim_tuple_store_sort.h>

#include<cutlery/value_arraylist.h>

#include<stdlib.h>
#include<setjmp.h>

#include<rhendb/function_compare.h>

typedef struct sorting_context sorting_context;
struct sorting_context
{
	uint32_t element_count;

	const data_type_info** key_dtis;

	const compare_direction* key_cmp_dirs;

	rage_engine* ex_engine;
};

typedef struct sortable_tuple_reference sortable_tuple_reference;
struct sortable_tuple_reference
{
	void* tuple;

	// keys for the tuple at this offset
	datum* keys;
};

static int compare_tuples_for_interim_tuple_store_sort(const void* sc_vp, const void* ref1_vp, const void* ref2_vp)
{
	const sorting_context* sc_p = sc_vp;
	const sortable_tuple_reference* ref1 = ref1_vp;
	const sortable_tuple_reference* ref2 = ref2_vp;

	return compare_datums3_rhendb(ref1->keys, ref2->keys, sc_p->key_dtis, sc_p->key_cmp_dirs, sc_p->element_count, sc_p->ex_engine);
}

data_definitions_value_arraylist(sortable_tuple_references, sortable_tuple_reference)
declarations_value_arraylist(sortable_tuple_references, sortable_tuple_reference, static inline)
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(sortable_tuple_references, sortable_tuple_reference, static inline)

interim_tuple_store* sort_interim_tuples(interim_tuple_store* its_p, tuples_down_counter result_counter, const tuple_def* tpl_d, const positional_accessor* element_ids, const compare_direction* cmp_dir, uint32_t element_count, rage_engine* ex_engine)
{
	// if the its_p is empty, OR if no result is expected, then return immediately
	if(its_p->tuples_count == 0 || is_zero_tuples_down_counter(&result_counter))
		return get_new_interim_tuple_store(0);

	// create a list_of_sortable_tuple_references
	sortable_tuple_references list_of_sortable_tuple_references;
	if(!initialize_sortable_tuple_references(&list_of_sortable_tuple_references, its_p->tuples_count))
		exit(-1);

	// mmap the complete interm tuple store from it's offset 0
	mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), 0, &(tpl_d->size_def), get_total_bytes_in_interim_tuple_store(its_p));

	// allocate all datums for materilizing keys all at once
	datum* keyss = malloc(sizeof(datum) * its_p->tuples_count * element_count);
	if(keyss == NULL)
		exit(-1);

	// gather all the offsets
	{void* tuple = its_p->embed_regions[0].tuple;
	for(uint64_t i = 0; i < its_p->tuples_count; i++)
	{
		for(uint32_t j = 0; j < element_count; j++)
			if(!get_value_from_element_from_tuple(&(keyss[i * element_count + j]), tpl_d, element_ids[j], tuple))
				keyss[i * element_count + j] = (*NULL_DATUM);
		if(!push_back_to_sortable_tuple_references(&list_of_sortable_tuple_references, &(sortable_tuple_reference){tuple, &(keyss[i * element_count])}))
			exit(-1);
		tuple += get_tuple_size(tpl_d, tuple);
	}}

	// build sorting context
	sorting_context sc = {
		element_count,
		NULL,
		cmp_dir,
		ex_engine,
	};
	sc.key_dtis = malloc(sizeof(data_type_info) * element_count);
	if(sc.key_dtis == NULL)
		exit(-1);
	for(uint32_t j = 0; j < element_count; j++)
		sc.key_dtis[j] = get_type_info_for_element_from_tuple_def(tpl_d, element_ids[j]);

	// sort its_p using sc and iai
	merge_sort_sortable_tuple_references(&list_of_sortable_tuple_references, 0, get_element_count_sortable_tuple_references(&list_of_sortable_tuple_references)-1, &contexted_comparator(&sc, compare_tuples_for_interim_tuple_store_sort), STD_C_mem_allocator);

	// create output interim_tuple_store
	interim_tuple_store* ots_p = get_new_interim_tuple_store(get_total_bytes_in_interim_tuple_store(its_p));

	for(uint32_t i = 0; i < get_element_count_sortable_tuple_references(&list_of_sortable_tuple_references) && can_decrement_tuples_down_counter(&result_counter); i++, decrement_tuples_down_counter(&result_counter))
	{
		// fetch the tuple to be copied
		const sortable_tuple_reference* ref = get_from_front_of_sortable_tuple_references(&list_of_sortable_tuple_references, i);

		// apped it to output
		append_tuple_to_interim_tuple_store2(ots_p, &(ots_p->embed_regions[0]), ref->tuple, &(tpl_d->size_def), get_total_bytes_in_interim_tuple_store(its_p));
	}

	// destroy all regions we might have useds
	unmap_all_embed_regions_in_interim_tuple_store(its_p);
	unmap_all_embed_regions_in_interim_tuple_store(ots_p);

	// destroy the sortable_tuple_references
	deinitialize_sortable_tuple_references(&list_of_sortable_tuple_references);
	free(sc.key_dtis);
	free(keyss);
	return ots_p;
}