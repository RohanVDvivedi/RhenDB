#include<rhendb/temp_tuple_store.h>

#include<sys/mman.h>

#include<stdlib.h>

#include<fcntl.h>
#include<unistd.h>

temp_tuple_store* get_new_temp_tuple_store(const char* directory)
{
	temp_tuple_store* tts_p = malloc(sizeof(temp_tuple_store));
	if(tts_p == NULL)
		return NULL;

	tts_p->total_size = 0;
	tts_p->next_tuple_offset = 0;

	tts_p->fd = open64(directory, O_TMPFILE | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);

	if(tts_p->fd == -1)
	{
		free(tts_p);
		return NULL;
	}

	return tts_p;
}

void delete_temp_tuple_store(temp_tuple_store* tts_p)
{
	close(tts_p->fd);
	free(tts_p);
}

int mmap_for_reading_tuple(temp_tuple_store* tts_p, tuple_region* tr_p, uint64_t offset, tuple_size_def* tpl_sz_d);

int mmap_for_writing_tuple(temp_tuple_store* tts_p, tuple_region* tr_p, tuple_size_def* tpl_sz_d, uint32_t required_size);

int finalize_written_tuple(temp_tuple_store* tts_p, tuple_region* tr_p)
{
	// if the tuple that suggest to be finalized must at the next_tuple_offset
	if(tts_p->next_tuple_offset != curr_tuple_offset_for_tuple_region(tr_p))
		return 0;

	tts_p->next_tuple_offset = next_tuple_offset_for_tuple_region(tr_p);
	return 1;
}

int unmap_for_tuple_region(tuple_region* tr_p)
{
	if(0 == munmap(tr_p->region_memory, tr_p->region_size))
	{
		(*tr_p) = INIT_TUPLE_REGION;
		return 1;
	}
	return 0;
}

uint64_t curr_tuple_offset_for_tuple_region(const tuple_region* tr_p)
{
	return tr_p->region_offset + (tr_p->tuple - tr_p->region_memory);
}

uint32_t curr_tuple_size_for_tuple_region(const tuple_region* tr_p)
{
	return get_tuple_size_using_tuple_size_def(tr_p->tpl_sz_d, tr_p->tuple);
}

uint64_t next_tuple_offset_for_tuple_region(const tuple_region* tr_p)
{
	return curr_tuple_offset_for_tuple_region(tr_p) + curr_tuple_size_for_tuple_region(tr_p);
}

int contains_for_tuple_region(const tuple_region* tr_p, uint64_t offset_start, uint64_t offset_end);