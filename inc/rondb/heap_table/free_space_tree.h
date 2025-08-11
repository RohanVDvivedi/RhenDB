#ifndef FREE_SPACE_TREE_H
#define FREE_SPACE_TREE_H

#include<rondb/heap_table/heap_table.h>

typedef enum free_space_tree_operation free_space_tree_operation;
enum free_space_tree_operation
{
	REMOVE_FROM_FREE_SPACE_TREE,
	INSERT_INTO_FREE_SPACE_TREE,
};


// push this struct into the tail of the free_space_tree_modifying_params_list, of the heap_table to perform asynchronous insert/remove from the free_space_tree of the heap_table
typedef struct free_space_tree_modifier_params free_space_tree_modifier_params;
struct free_space_tree_modifier_params
{
	// entry params to be inserted to removed
	uint32_t free_space;
	uint64_t page_id;

	// operation to perform
	free_space_tree_operation op;

	// embed_node for the free_space_tree_modifying_params_list
	slnode embed_node;
};

persistent_page get_heap_page_with_enough_free_space_from_heap_table(heap_table* htbl, uint64_t free_space, void* transaction_id, int* abort_error);

// this function can be called from anywhere
void async_insert_in_free_space_tree_for_heap_table(heap_table* htbl, uint32_t free_space, uint64_t page_id);

// this function can be called from anywhere
void async_remove_from_free_space_tree_for_heap_table(heap_table* htbl, uint32_t free_space, uint64_t page_id);

#endif