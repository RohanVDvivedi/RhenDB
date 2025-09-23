#ifndef TEMP_TUPLE_STORE_H
#define TEMP_TUPLE_STORE_H

// for using additional linux specific flags
#define _GNU_SOURCE

#include<inttypes.h>
#include<pthread.h>

#include<tuplestore/tuple_def.h>

/*
	temp_tuple_store is basically a sequentially inserted, non-page aligned collection of heterogeneous tuples (i.e. having different tuple_defs)
	it will primarily be used for working with passing tuples between operator or for internal processing and collection of the tuples

	yes this is look-alike of the volatile_page_store used as the volatie engine, and that too can be used by the relational operators that you design
	but temp_tuple_store is more suited to giant_tuple_defs bcause there is no restriction for the size of the tuple to be stored here
	as the tuple is laid out sequentially, one after the pther, we may not need to chase pointers to recontruct the tuple in the memory, it will be paged in and out by the mmap calls
	virtual memory comming to the rescue and saving us the hazzle, when we could be dealing with 32-bit systems
	giant_tuples generated from tuple_defs are still going to have to fit 2GB so that remains the constraint even here
*/

/*
	please be sure to avoid overflowing the uint64_t for tuple_offsets and sizes passed and provided, because there are no overflow checks in this module
*/

// this module has been designed to be used by a single thread only, it's exposed functions are not thread safe

typedef struct temp_tuple_store temp_tuple_store;
struct temp_tuple_store
{
	uint64_t total_size; // total memory region to be used by the file or the (in-) memory region below

	uint64_t next_tuple_offset; // next tuple gets appended here, it starts with 0 and increments by writable tuple_regions upon calling finalize_written_tuple() operation

	int fd; // file_descriptor to be accessed for mapping the memory
};

typedef struct tuple_region tuple_region;
struct tuple_region
{
	void* region_memory;
	uint64_t region_size; // possibly multiple of page size, covering the whole of the tupl
	uint64_t region_offset; // possibly the offset of the region_memory in the temp_tuple_store

	void* tuple; // a pointer to a complete tuple in the region_memory
	tuple_size_def* tpl_sz_d; // this allows the temp_tuple_store get knowledge about the memory in use by this tuple_region
};

// please be sure that page_size will be rounded to the next page_size available
temp_tuple_store* get_new_temp_tuple_store(const char* directory);

void delete_temp_tuple_store(temp_tuple_store* tts_p);

// remaps the tr_p to a new offset, if it is valid else creates a new mapping
// it may use the same mapping, if the tuple fits in this region
// you can have multiple reading tuple_regions open
int mmap_for_reading_tuple(temp_tuple_store* tts_p, tuple_region* tr_p, uint64_t offset, tuple_size_def* tpl_sz_d);

// remaps the tr_p to the current next_tuple_offset, holding as much memory as required_size
// it may use the same mapping, if the tuple fits in this region
// you must have atmost 1 writing tuple_regions open
// this function may extend the temp_tuple_store if it's total_size is lesser than the next_tuple_offset + required_size
// you may also recall this function if you have a better estimate of the size of the next tuple, before ofcourse you hit finalize
int mmap_for_writing_tuple(temp_tuple_store* tts_p, tuple_region* tr_p, tuple_size_def* tpl_sz_d, uint32_t required_size);

/*
	both the mmap functions may pass their tuple_regions to each other, it will just force the mmap_* function to remap the regions
*/

// fails if you pass a is_only_readable flagged tuple
// else it forwards the next_tuple_offset
// this function may be called as many times as you like, intermediary, to finalize the same tuple
int finalize_written_tuple(temp_tuple_store* tts_p, tuple_region* tr_p);

// unmaps the tuple_region provided and sets all its attributes to 0s
int unmap_for_tuple_region(tuple_region* tr_p);

// utility function for the tuple_region below

uint64_t curr_tuple_offset_for_tuple_region(const tuple_region* tr_p);
uint32_t curr_tuple_size_for_tuple_region(const tuple_region* tr_p);
uint64_t next_tuple_offset_for_tuple_region(const tuple_region* tr_p);

// offset_start is inclusive and offset_end is exclusive
int contains_for_tuple_region(const tuple_region* tr_p, uint64_t offset_start, uint64_t offset_end);

#define INIT_TUPLE_REGION ((tuple_region){})

#endif