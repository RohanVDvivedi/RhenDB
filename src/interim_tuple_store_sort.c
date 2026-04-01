#include<rhendb/interim_tuple_store_sort.h>

#include<cutlery/value_arraylist.h>

#include<stdlib.h>

#include<rhendb/function_compare.h>

typedef struct sorting_context sorting_context;
struct sorting_context
{
	interim_tuple_store* its_p;
	uint32_t min_bytes_to_mmap;

	const tuple_def* tpl_d;
	const positional_accessor* element_ids;
	const compare_direction* cmp_dir;
	uint32_t element_count;

	rage_engine* ex_engine;
	const void* transaction_id;
	int* abort_error;
};

int compare_tuples_for_interim_tuple_store_offets(const void* sc_vp, const void* off1_vp, const void* off2_vp)
{
	const sorting_context* sc_p = sc_vp;
	uint64_t off1 = *((const uint64_t*)off1_vp);
	uint64_t off2 = *((const uint64_t*)off2_vp);

	mmap_for_reading_tuple(sc_p->its_p, &(sc_p->its_p->embed_regions[0]), off1, &(sc_p->tpl_d->size_def), sc_p->min_bytes_to_mmap);
	mmap_for_reading_tuple(sc_p->its_p, &(sc_p->its_p->embed_regions[1]), off2, &(sc_p->tpl_d->size_def), sc_p->min_bytes_to_mmap);

	return comare_tuples2_rhendb(sc_p->its_p->embed_regions[0].tuple, sc_p->its_p->embed_regions[1].tuple, sc_p->tpl_d, sc_p->element_ids, sc_p->cmp_dir, sc_p->element_count, sc_p->ex_engine, sc_p->transaction_id, sc_p->abort_error);
}

data_definitions_value_arraylist(offset_list, uint64_t)
declarations_value_arraylist(offset_list, uint64_t, static inline)
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(offset_list, uint64_t, static inline)

interim_tuple_store* sort_interim_tuples(interim_tuple_store* its_p, uint32_t min_bytes_to_mmap, const tuple_def* tpl_d, const positional_accessor* element_ids, const compare_direction* cmp_dir, uint32_t element_count, rage_engine* ex_engine, const void* transaction_id, int* abort_error)
{
	// create a list_of_offsets
	offset_list list_of_offsets;
	if(!initialize_offset_list(&list_of_offsets, its_p->tuples_count))
		exit(-1);

	// gather all the offsets
	FOR_EACH_TUPLE_IN_INTERIM_TUPLE_STORE(tuple, tuple_index, tuple_offset, &(tpl_d->size_def), its_p, min_bytes_to_mmap, {
		if(!push_back_to_offset_list(&list_of_offsets, &tuple_offset))
			exit(-1);
	});

	// build sorting context
	sorting_context sc = {
		its_p,
		min_bytes_to_mmap,

		tpl_d,
		element_ids,
		cmp_dir,
		element_count,

		ex_engine,
		transaction_id,
		abort_error,
	};

	// buils index accessed interface to sort it
	index_accessed_interface iai = get_index_accessed_interface_for_front_of_offset_list(&list_of_offsets);

	// sort its_p using sc and iai
	if(!is_empty_offset_list(&list_of_offsets))
		quick_sort_iai(&(iai), 0, get_element_count_offset_list(&list_of_offsets)-1, &contexted_comparator(&sc, compare_tuples_for_interim_tuple_store_offets));

	if(*abort_error)
	{
		deinitialize_offset_list(&list_of_offsets);
		return NULL;
	}

	// destroy all regions we might have used
	unmap_all_embed_regions_in_interim_tuple_store(its_p);

	// create output interim_tuple_store
	interim_tuple_store* ots_p = get_new_interim_tuple_store(".");
	extend_interim_tuple_store(ots_p, get_total_bytes_in_interim_tuple_store(its_p));

	for(uint32_t i = 0; i < get_element_count_offset_list(&list_of_offsets); i++)
	{
		uint64_t offset = *get_from_front_of_offset_list(&list_of_offsets, i);
		mmap_for_reading_tuple(its_p, &(its_p->embed_regions[0]), offset, &(tpl_d->size_def), min_bytes_to_mmap);
		uint32_t tuple_size = get_tuple_size_using_tuple_size_def(&(tpl_d->size_def), its_p->embed_regions[0].tuple);
		mmap_for_writing_tuple(ots_p, &(ots_p->embed_regions[0]), &(tpl_d->size_def), tuple_size, min_bytes_to_mmap);
		memory_move(ots_p->embed_regions[0].tuple, its_p->embed_regions[0].tuple, tuple_size);
		finalize_written_tuple(ots_p, &(ots_p->embed_regions[0]));
	}

	// destroy all regions we might have useds
	unmap_all_embed_regions_in_interim_tuple_store(its_p);
	unmap_all_embed_regions_in_interim_tuple_store(ots_p);

	// destroy the offset list
	deinitialize_offset_list(&list_of_offsets);

	return ots_p;
}