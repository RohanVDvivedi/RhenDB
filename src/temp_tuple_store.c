#include<rhendb/temp_tuple_store.h>

temp_tuple_store* get_new_temp_tuple_store(uint64_t spill_over_size);

void delete_temp_tuple_store(temp_tuple_store* tts_p);

int mmap_for_reading_tuple(temp_tuple_store* tts_p, tuple_region* tr_p, uint64_t offset, tuple_size_def* tpl_sz_d);

int mmap_for_writing_tuple(temp_tuple_store* tts_p, tuple_region* tr_p, tuple_size_def* tpl_sz_d, uint32_t required_size);

int finalize_written_tuple(temp_tuple_store* tts_p, tuple_region* tr_p);

void unmap_for_tuple_region(temp_tuple_store* tts_p, tuple_region* tr_p);

uint64_t curr_tuple_offset_for(tuple_region* tr_p)
{
	return tr_p->region_offset + (tr_p->tuple - tr_p->region_memory);
}

uint32_t curr_tuple_size_for(tuple_region* tr_p)
{
	return get_tuple_size_using_tuple_size_def(tr_p->tpl_sz_d, tr_p->tuple);
}

uint64_t next_tuple_offset_for(tuple_region* tr_p)
{
	return curr_tuple_offset_for(tr_p) + curr_tuple_size_for(tr_p);
}