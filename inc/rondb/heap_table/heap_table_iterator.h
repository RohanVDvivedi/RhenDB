#ifndef HEAP_TABLE_ITERATOR_H
#define HEAP_TABLE_ITERATOR_H

#include<rondb/heap_table/heap_table.h>

typedef struct heap_table_iterator heap_table_iterator;
struct heap_table_iterator
{
	// actual heap_table that this iterator is iterating
	heap_table* htbl;

	// the bplus_tree_iterator over the heap_pages_tree of the heap_table
	// this iterator is always a READ-only leaf-only iterator, you will not in any condition be allowed to modify the heap_pages_tree
	bplus_tree_iterator* heap_pages_tree_iterator;

	// current heap_page, (may be read or write locked unlike the heap_pages_tree_iterator)
	persistent_page heap_page;

	// below bit is set to true, if the heap_page attribute above is write locked
	int is_heap_page_write_locked;

	// current tuple_index
	uint32_t tuple_index;
};

heap_table_iterator* get_new_heap_table_iterator(heap_table_iterator* htbl, uint64_t page_id, uint32_t tuple_index, void* transaction_id, int* abort_error);

uint64_t get_curr_heap_page_id(const heap_table_iterator* htbli_p);

persistent_page* get_curr_heap_page(const heap_table_iterator* htbli_p);

uint32_t get_curr_heap_tuple_index(const heap_table_iterator* htbli_p);

const void* get_curr_heap_tuple(const heap_table_iterator* htbli_p);

int next_heap_page_heap_table_iterator(heap_table_iterator* htbli_p, void* transaction_id, int* abort_error);

int next_heap_tuple_heap_table_iterator(heap_table_iterator* htbli_p, void* transaction_id, int* abort_error);

heap_table_iterator* clone_heap_table_iterator(heap_table_iterator* htbli_p, void* transaction_id, int* abort_error);

void delete_heap_table_iterator(heap_table_iterator* htbli_p, void* transaction_id, int* abort_error);

#endif