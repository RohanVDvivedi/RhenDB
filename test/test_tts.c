#include<rhendb/temp_tuple_store.h>

#include<unistd.h>
#include<stdio.h>
#include<stdlib.h>

#include<tuplestore/tuple.h>

tuple_def tpl_d;
data_type_info dti;

void print_all_tuples(temp_tuple_store* tts_p)
{
	printf("\n\nprinting temp_tuple_store with %"PRIu64" tuples, and filled upto %"PRIu64"/%"PRIu64"\n\n", tts_p->tuple_count, tts_p->next_tuple_offset, tts_p->total_size);
	uint64_t index = 0;
	uint64_t offset = 0;
	tuple_region tr = INIT_TUPLE_REGION;
	while(mmap_for_reading_tuple(tts_p, &tr, offset, &(tpl_d.size_def)))
	{
		printf("tuple_index = %"PRIu64", tuple_offset = %"PRIu64", tuple_size = %"PRIu32"\n", index, offset, curr_tuple_size_for_tuple_region(&tr));
		print_tuple(tr.tuple, &tpl_d);
		printf("\n\n");
		offset = next_tuple_offset_for_tuple_region(&tr);
		index++;
	}
	unmap_for_tuple_region(&tr);
	printf("\n\n");
}

void append_all_tuples(temp_tuple_store* tts_p, uint32_t chunk_size, char** strings_to_insert)
{
	tuple_region tr = INIT_TUPLE_REGION;
	for(char** t = strings_to_insert; (*t) != NULL; t++)
	{
		uint32_t len = strlen((*t));

		uint32_t required_size = 20;
		mmap_for_writing_tuple(tts_p, &tr, &(tpl_d.size_def), required_size);

		init_tuple(&tpl_d, tr.tuple);

		for(uint32_t len_added = 0; len_added < len; )
		{
			uint32_t len_to_add = min(chunk_size, len - len_added);

			mmap_for_writing_tuple(tts_p, &tr, &(tpl_d.size_def), len_added + len_to_add);

			init_tuple(&tpl_d, tr.tuple);
			set_element_in_tuple(&tpl_d, SELF, tr.tuple, &((user_value){.string_value = (*t), .string_size = len_added + len_to_add}), len_added + len_to_add);

			len_added += len_to_add;
		}

		finalize_written_tuple(tts_p, &tr);
	}
	unmap_for_tuple_region(&tr);
}

int main()
{
	printf("page_size = %"PRIu64"\n\n", sysconf(_SC_PAGE_SIZE));

	dti = get_variable_length_string_type("var_string_8192", sysconf(_SC_PAGE_SIZE) * 5);
	initialize_tuple_def(&tpl_d, &dti);

	print_tuple_def(&tpl_d);

	temp_tuple_store* tts_p = get_new_temp_tuple_store(".");

	// init complete

	print_all_tuples(tts_p);

	// deinit start

	delete_temp_tuple_store(tts_p);
	return 0;
}