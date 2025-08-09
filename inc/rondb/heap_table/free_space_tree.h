#ifndef FREE_SPACE_TREE_H
#define FREE_SPACE_TREE_H

#include<rondb/heap_table/heap_table.h>

persistent_page get_heap_page_with_enough_free_space_from_heap_table(heap_table* htbl, uint64_t free_space, void* transaction_id, int* abort_error);

void async_insert_in_free_space_tree_for_heap_table(heap_table* htbl, uint32_t free_space, uint64_t page_id);

void async_remove_from_free_space_tree_for_heap_table(heap_table* htbl, uint32_t free_space, uint64_t page_id);

#endif