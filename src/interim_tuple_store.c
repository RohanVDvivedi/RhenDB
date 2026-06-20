// below 2 macros are required for using 64-bit version of copy_file_range() function
#define _GNU_SOURCE
#define _FILE_OFFSET_BITS 64

#include<rhendb/interim_tuple_store.h>
#include<rhendb/temp_file_dir_path.h>

#include<sys/mman.h>

#include<stdlib.h>

#include<fcntl.h>
#include<unistd.h>
#include<sys/types.h>

interim_tuple_store* get_new_interim_tuple_store(uint64_t initial_total_size)
{
	interim_tuple_store* its_p = malloc(sizeof(interim_tuple_store));
	if(its_p == NULL)
	{
		printf("FAILED to allocate memory for interim_tuple_store\n");
		exit(-1);
	}

	its_p->total_size = 0;
	its_p->next_tuple_offset = 0;

	its_p->tuples_count = 0;

	its_p->fd = open64(INTERIM_TUPLE_STORE_DIR_PATH, O_TMPFILE | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	if(its_p->fd == -1)
	{
		printf("FAILED to open a file for interim_tuple_store\n");
		exit(-1);
	}

	// cutlery always assigns all embedded nodes that are free to be 0 initialized, so the below calls are just an formality
	initialize_llnode(&(its_p->embed_node_ll));
	initialize_slnode(&(its_p->embed_node_sl));
	initialize_bstnode(&(its_p->embed_node_bst));
	initialize_rbhnode(&(its_p->embed_node_rbh));
	initialize_hpnode(&(its_p->embed_node_hp));
	initialize_phpnode(&(its_p->embed_node_php));

	for(int i = 0; i < sizeof(its_p->embed_uints)/sizeof(its_p->embed_uints[0]); i++)
		its_p->embed_uints[i] = 0;

	for(int i = 0; i < sizeof(its_p->embed_ptrs)/sizeof(its_p->embed_ptrs[0]); i++)
		its_p->embed_ptrs[i] = NULL;

	for(int i = 0; i < sizeof(its_p->embed_regions)/sizeof(its_p->embed_regions[0]); i++)
		its_p->embed_regions[i] = INIT_INTERIM_TUPLE_REGION;

	if(initial_total_size > 0)
		extend_interim_tuple_store(its_p, initial_total_size);

	return its_p;
}

void reinitialize_interim_tuple_store(interim_tuple_store* its_p, uint64_t initial_total_size)
{
	unmap_all_embed_regions_in_interim_tuple_store(its_p);

	its_p->next_tuple_offset = 0;
	its_p->tuples_count = 0;

	if(initial_total_size != UINT64_MAX)
	{
		its_p->total_size = UINT_ALIGN_UP(initial_total_size, sysconf(_SC_PAGE_SIZE));
		if(-1 == ftruncate64(its_p->fd, its_p->total_size))
		{
			printf("FAILED to extend the file for interim_tuple_store\n");
			exit(-1);
			return;
		}
	}
}

void unmap_all_embed_regions_in_interim_tuple_store(interim_tuple_store* its_p)
{
	for(int i = 0; i < sizeof(its_p->embed_regions)/sizeof(its_p->embed_regions[0]); i++)
		unmap_for_interim_tuple_region(&(its_p->embed_regions[i]));
}

void delete_interim_tuple_store(interim_tuple_store* its_p)
{
	unmap_all_embed_regions_in_interim_tuple_store(its_p);
	close(its_p->fd);
	free(its_p);
}

void extend_interim_tuple_store(interim_tuple_store* its_p, uint64_t additional_total_size)
{
	if(will_unsigned_sum_overflow(uint64_t, its_p->total_size, additional_total_size))
	{
		printf("FAILED to increment the size of the interim_tuple_store, total_size overflowed\n");
		exit(-1);
		return;
	}
	its_p->total_size += additional_total_size;

	if(will_UINT_ALIGN_UP_overflow(uint64_t, its_p->total_size, sysconf(_SC_PAGE_SIZE)))
	{
		printf("FAILED to increment the size of the interim_tuple_store, total_size overflowed on aligning up\n");
		exit(-1);
		return;
	}
	its_p->total_size = UINT_ALIGN_UP(its_p->total_size, sysconf(_SC_PAGE_SIZE));

	if(-1 == ftruncate64(its_p->fd, its_p->total_size))
	{
		printf("FAILED to extend the file for interim_tuple_store\n");
		exit(-1);
		return;
	}
}

uint64_t get_total_bytes_in_interim_tuple_store(const interim_tuple_store* its_p)
{
	return its_p->next_tuple_offset;
}

typedef struct tuple_size_getter_context tuple_size_getter_context;
struct tuple_size_getter_context
{
	int fd;
	uint64_t offset;

	// helper_itr_p may be NULL
	const interim_tuple_region* helper_itr_p;
};

static uint32_t read_tuple_prefix_from_file(void* context_p, void* data, uint32_t data_size)
{
	tuple_size_getter_context* temp = context_p;

	if(temp->helper_itr_p != NULL)
	{
		if(contains_for_interim_tuple_region(temp->helper_itr_p, temp->offset, temp->offset + data_size))
		{
			memory_move(data, temp->helper_itr_p->region_memory + (temp->offset - temp->helper_itr_p->region_offset), data_size);
			return data_size;
		}
	}

	ssize_t bytes_read = pread64(temp->fd, data, data_size, temp->offset);
	if(bytes_read == -1)
	{
		printf("FAILED to fetch the size of the next tuple for interim_tuple_store\n");
		exit(-1);
	}
	return bytes_read;
}

uint32_t get_tuple_size_for_interim_tuple_store(const interim_tuple_store* its_p, const interim_tuple_region* helper_itr_p, uint64_t tuple_offset, const tuple_size_def* tpl_sz_d)
{
	return get_tuple_size_using_tuple_size_def2(tpl_sz_d, &((tuple_size_getter_context){its_p->fd, tuple_offset, helper_itr_p}), read_tuple_prefix_from_file);
}

int mmap_for_reading_tuple(interim_tuple_store* its_p, interim_tuple_region* itr_p, uint64_t offset, const tuple_size_def* tpl_sz_d, uint32_t min_bytes_to_mmap)
{
	// offset must be withing readable file region to begin with, with enough bytes for the smallest sized tuple
	if(offset + get_minimum_tuple_size_using_tuple_size_def(tpl_sz_d) > its_p->next_tuple_offset)
		return 0;

	uint64_t tuple_offset_start = offset;
	uint32_t tuple_size = get_tuple_size_for_interim_tuple_store(its_p, itr_p, tuple_offset_start, tpl_sz_d);
	uint64_t tuple_offset_end = tuple_offset_start + tuple_size;

	// tuple_offset_end <= its_p->next_tuple_offset, is a must
	if(tuple_offset_end > its_p->next_tuple_offset)
		return 0;

	// if the memory region is already contained then return true directly, pointing the interim_tuple_region to the right tuple
	if(contains_for_interim_tuple_region(itr_p, tuple_offset_start, tuple_offset_end))
	{
		itr_p->tuple = itr_p->region_memory + (tuple_offset_start - itr_p->region_offset);
		itr_p->tpl_sz_d = tpl_sz_d;
		return 1;
	}

	// now we free up the old region
	if(!is_empty_interim_tuple_region(itr_p))
		unmap_for_interim_tuple_region(itr_p);

	// build up region offsets
	uint64_t region_offset_start = UINT_ALIGN_DOWN(tuple_offset_start, sysconf(_SC_PAGE_SIZE));
	// place region_offset_end right after the max of the position of tuple_offset_end OR min_bytes_to_mmap after the tuple (and ofcourse not overflowing the next_tuple_offset)
	uint64_t region_offset_end = UINT_ALIGN_UP(min(max(tuple_offset_end, tuple_offset_start + min_bytes_to_mmap), its_p->next_tuple_offset), sysconf(_SC_PAGE_SIZE));
	uint32_t region_size = region_offset_end - region_offset_start;

	// map the new interim_tuple_region and return it
	void* region_memory = mmap64(NULL, region_size, PROT_READ | PROT_WRITE, MAP_SHARED, its_p->fd, region_offset_start);
	if(region_memory == MAP_FAILED)
	{
		printf("FAILED to create a interim_tuple_region for interim_tuple_store\n");
		exit(-1);
		return 0;
	}

	itr_p->region_memory = region_memory;
	itr_p->region_offset = region_offset_start;
	itr_p->region_size = region_size;
	itr_p->tuple = region_memory + (tuple_offset_start - region_offset_start);
	itr_p->tpl_sz_d = tpl_sz_d;
	return 1;
}

int mmap_for_writing_tuple(interim_tuple_store* its_p, interim_tuple_region* itr_p, const tuple_size_def* tpl_sz_d, uint32_t required_size, uint32_t min_bytes_to_mmap)
{
	uint64_t tuple_offset_start = its_p->next_tuple_offset;
	uint32_t tuple_size = required_size;
	uint64_t tuple_offset_end = tuple_offset_start + tuple_size;

	// if the memory region is already contained then return true directly, pointing the interim_tuple_region to the right tuple
	if(contains_for_interim_tuple_region(itr_p, tuple_offset_start, tuple_offset_end))
	{
		itr_p->tuple = itr_p->region_memory + (tuple_offset_start - itr_p->region_offset);
		itr_p->tpl_sz_d = tpl_sz_d;
		return 1;
	}

	// now we free up the old region
	if(!is_empty_interim_tuple_region(itr_p))
		unmap_for_interim_tuple_region(itr_p);

	// build up region offsets
	uint64_t region_offset_start = UINT_ALIGN_DOWN(tuple_offset_start, sysconf(_SC_PAGE_SIZE));
	// place region_offset_end right after the max of the position of tuple_offset_end OR min_bytes_to_mmap after the tuple
	uint64_t region_offset_end = UINT_ALIGN_UP(max(tuple_offset_end, tuple_offset_start + min_bytes_to_mmap), sysconf(_SC_PAGE_SIZE));
	uint32_t region_size = region_offset_end - region_offset_start;

	// ftruncate to extend the file, if the region_offset_end is greater than total_size
	// we do not need to extend if region_offset_end <= total_size
	if(region_offset_end > its_p->total_size)
	{
		if(-1 == ftruncate64(its_p->fd, region_offset_end))
		{
			printf("FAILED to extend the file for interim_tuple_store\n");
			exit(-1);
			return 0;
		}
		else
			its_p->total_size = region_offset_end;
	}

	// map the new interim_tuple_region and return it
	void* region_memory = mmap64(NULL, region_size, PROT_READ | PROT_WRITE, MAP_SHARED, its_p->fd, region_offset_start);
	if(region_memory == MAP_FAILED)
	{
		printf("FAILED to create a interim_tuple_region for interim_tuple_store\n");
		exit(-1);
		return 0;
	}

	itr_p->region_memory = region_memory;
	itr_p->region_offset = region_offset_start;
	itr_p->region_size = region_size;
	itr_p->tuple = region_memory + (tuple_offset_start - region_offset_start);
	itr_p->tpl_sz_d = tpl_sz_d;
	return 1;
}

int finalize_written_tuple(interim_tuple_store* its_p, interim_tuple_region* itr_p)
{
	// if the tuple that suggest to be finalized must at the next_tuple_offset
	if(its_p->next_tuple_offset != curr_tuple_offset_for_interim_tuple_region(itr_p))
		return 0;

	its_p->next_tuple_offset = next_tuple_offset_for_interim_tuple_region(itr_p);
	its_p->tuples_count++;
	return 1;
}

int unmap_for_interim_tuple_region(interim_tuple_region* itr_p)
{
	if(is_empty_interim_tuple_region(itr_p))
		return 0;

	if(0 == munmap(itr_p->region_memory, itr_p->region_size))
	{
		(*itr_p) = INIT_INTERIM_TUPLE_REGION;
		return 1;
	}

	printf("FAILED to unmap a interim_tuple_region for interim_tuple_store\n");
	exit(-1);
	return 0;
}

uint64_t append_tuple_to_interim_tuple_store(interim_tuple_store* its_p, const void* tupl, const tuple_size_def* tpl_sz_d)
{
	// compute the offset to be returned
	// this will be the offset for the first tuple that will be copied in from the other_its_p
	uint64_t offset = its_p->next_tuple_offset;

	// update tuples count
	its_p->tuples_count++;

	// get the size of the tuple
	uint32_t tuple_size = get_tuple_size_using_tuple_size_def(tpl_sz_d, tupl);

	// compute next_tuple_offset, it will be pointing to the end now, after this appended tuple
	its_p->next_tuple_offset += tuple_size;

	// assign the new_size and align it to the next multiple of page_size
	uint64_t new_total_size = UINT_ALIGN_UP(its_p->next_tuple_offset, sysconf(_SC_PAGE_SIZE));
	if(its_p->total_size < new_total_size)
	{
		its_p->total_size = new_total_size;
		// and extend the file to the new size
		if(-1 == ftruncate64(its_p->fd, its_p->total_size))
		{
			printf("FAILED to extend the file for interim_tuple_store\n");
			exit(-1);
			return 0;
		}
	}

	// directly do a pwrite instead
	if(tuple_size != pwrite64(its_p->fd, tupl, tuple_size, offset))
	{
		printf("FAILED to append a tuple to interim_tuple_store\n");
		exit(-1);
		return 0;
	}

	return offset;
}

uint64_t append_tuple_to_interim_tuple_store2(interim_tuple_store* its_p, interim_tuple_region* itr_p, const void* tupl, const tuple_size_def* tpl_sz_d, uint32_t min_bytes_to_mmap)
{
	// get size of the tuple
	uint32_t tuple_size = get_tuple_size_using_tuple_size_def(tpl_sz_d, tupl);

	// mmap the large enough region
	mmap_for_writing_tuple(its_p, itr_p, tpl_sz_d, tuple_size, min_bytes_to_mmap);

	// copy the tuple into the region
	memory_move(itr_p->tuple, tupl, tuple_size);

	// finalize the tuple appended
	finalize_written_tuple(its_p, itr_p);

	// return the offset that this tuple got inserted at
	return curr_tuple_offset_for_interim_tuple_region(itr_p);
}

uint64_t append_all_from_another_interim_tuple_store(interim_tuple_store* its_p, const interim_tuple_store* other_its_p)
{
	return append_all_from_another_interim_tuple_store2(its_p, other_its_p, 0, 0);
}

uint64_t append_all_from_another_interim_tuple_store2(interim_tuple_store* its_p, const interim_tuple_store* other_its_p, uint64_t from_tuple_offset, uint64_t from_tuple_index)
{
	if((from_tuple_offset >= other_its_p->next_tuple_offset) || (from_tuple_index >= other_its_p->tuples_count))
	{
		printf("FAILED to append all to interim_tuple_store, because either from_tuple_offset or from_tuple_index are outof bounds\n");
		return 0;
	}

	// compute the offset to be returned
	// this will be the offset for the first tuple that will be copied in from the other_its_p
	uint64_t offset = its_p->next_tuple_offset;

	// update tuples_count
	its_p->tuples_count += (other_its_p->tuples_count - from_tuple_index);

	// compute next_tuple_offset, it will be sum of both the next_tuple_offsets
	its_p->next_tuple_offset += (get_total_bytes_in_interim_tuple_store(other_its_p) - from_tuple_offset);

	// assign the new_size and align it to the next multiple of page_size
	uint64_t new_total_size = UINT_ALIGN_UP(its_p->next_tuple_offset, sysconf(_SC_PAGE_SIZE));
	if(its_p->total_size < new_total_size)
	{
		its_p->total_size = new_total_size;
		// and extend the file to the new size
		if(-1 == ftruncate64(its_p->fd, its_p->total_size))
		{
			printf("FAILED to extend the file for interim_tuple_store\n");
			exit(-1);
			return 0;
		}
	}

	// now perform file descriptor copy directly in the kernel
	// this is linux specific
	{
		off64_t off_in = from_tuple_offset;
		off64_t off_out = offset;
		size_t remaining_bytes = (get_total_bytes_in_interim_tuple_store(other_its_p) - from_tuple_offset); // these many bytes are needed to be transferred
		while(remaining_bytes > 0)
		{
			ssize_t bytes_copied = copy_file_range(other_its_p->fd, &off_in, its_p->fd, &off_out, remaining_bytes, 0);
			if(bytes_copied == -1)
			{
				printf("FAILED to copy_file_range for interim_tuple_store\n");
				exit(-1);
				return 0;
			}
			if(bytes_copied == 0)
			{
				printf("FAILED because copy_file_range for interim_tuple_store returned an unexpected 0 bytes transferred\n");
				exit(-1);
				return 0;
			}
			remaining_bytes -= bytes_copied;
		}
	}

	return offset;
}

int is_empty_interim_tuple_region(const interim_tuple_region* itr_p)
{
	return itr_p->region_memory == NULL;
}

uint64_t start_offset_for_interim_tuple_region(const interim_tuple_region* itr_p)
{
	if(is_empty_interim_tuple_region(itr_p))
		return 0;

	return itr_p->region_offset;
}

uint64_t end_offset_for_interim_tuple_region(const interim_tuple_region* itr_p)
{
	if(is_empty_interim_tuple_region(itr_p))
		return 0;

	return itr_p->region_offset + itr_p->region_size;
}

uint64_t curr_tuple_offset_for_interim_tuple_region(const interim_tuple_region* itr_p)
{
	if(is_empty_interim_tuple_region(itr_p))
		return 0;

	return itr_p->region_offset + (itr_p->tuple - itr_p->region_memory);
}

uint32_t curr_tuple_size_for_interim_tuple_region(const interim_tuple_region* itr_p)
{
	if(is_empty_interim_tuple_region(itr_p))
		return 0;

	return get_tuple_size_using_tuple_size_def(itr_p->tpl_sz_d, itr_p->tuple);
}

uint64_t next_tuple_offset_for_interim_tuple_region(const interim_tuple_region* itr_p)
{
	if(is_empty_interim_tuple_region(itr_p))
		return 0;

	return curr_tuple_offset_for_interim_tuple_region(itr_p) + curr_tuple_size_for_interim_tuple_region(itr_p);
}

int contains_for_interim_tuple_region(const interim_tuple_region* itr_p, uint64_t offset_start, uint64_t offset_end)
{
	if(is_empty_interim_tuple_region(itr_p))
		return 0;

	// offset_start <= offset_end, is a must
	if(offset_start > offset_end)
		return 0;

	uint64_t region_start = itr_p->region_offset;
	uint64_t region_end = region_start + itr_p->region_size;

	return (region_start <= offset_start) && (offset_end <= region_end);
}

int contains_tuple_at_offset_in_interim_tuple_store(const interim_tuple_store* its_p, const interim_tuple_region* itr_p, uint64_t tuple_offset, const tuple_size_def* tpl_sz_d)
{
	if(is_empty_interim_tuple_region(itr_p))
		return 0;

	uint64_t region_start = itr_p->region_offset;
	uint64_t region_end = region_start + itr_p->region_size;

	// fail immediately, if the tuple_offset if completely outside the tuple region
	if(tuple_offset < region_start || region_end <= tuple_offset)
		return 0;

	// if the tuple_offset is more than the highest value then fail immediately
	if(its_p->next_tuple_offset <= tuple_offset)
		return 0;

	// calculate tuple_offset start and end
	uint64_t tuple_offset_start = tuple_offset;
	uint64_t tuple_offset_end = tuple_offset_start + get_tuple_size_for_interim_tuple_store(its_p, itr_p, tuple_offset_start, tpl_sz_d);

	if((region_start <= tuple_offset_start) && (tuple_offset_end <= region_end))
		return 1;

	return 0;
}

void delete_on_notify_for_interim_tuple_store(void* resource_p, const void* data_p)
{
	delete_interim_tuple_store((interim_tuple_store*)data_p);
}