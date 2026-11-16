#include<rhendb/grash_table.h>

grash_table* get_new_grash_table(const tuple_def* value_def, const positional_accessor* key_element_ids, uint32_t key_element_count);

void delete_grash_table(grash_table* gt_p);

void insert_to_grash_table(grash_table* gt_p, void* value, uint64_t optional_pre_computed_hash_value);

int partition_grash_table_node(grash_table* gt_p, grash_table_node* gtn_p);

cash_table* index_grash_table_node(grash_table* gt_p, grash_table_node* gtn_p);

void unmap_all_partition_mmaped_regions(grash_table_node* gtn_p);

void close_all_partition_files(grash_table_node* gtn_p);
