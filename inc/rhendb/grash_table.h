#ifndef GRASH_TABLE_H
#define GRASH_TABLE_H

#include<rhendb/grash_table.h>

#include<rhendb/interim_tuple_store.h>

#define BITS_PER_COLLECTION 3 // must be greater than 0

#define PARTITIONS_PER_COLLECTION (1ULL << BITS_PER_COLLECTION)

typedef enum grash_table_node_type grash_table_node_type;
enum grash_table_node_type
{
	PARTITION,
	COLLECTION,
};

typedef struct grash_table_node grash_table_node;
struct grash_table_node
{
	grash_table_node_type type;

	uint64_t level; // starts with 0 at the root, and increments by BITS_PER_COLLECTION

	union
	{
		interim_tuple_store* partition;

		grash_table_node* partitions[PARTITIONS_PER_COLLECTION];
	};
};

typedef struct grash_table grash_table;
struct grash_table
{
	const tuple_def* value_def;

	const positional_accessor* key_element_ids;

	uint32_t key_element_count;

	tuple_def hash_def;

	grash_table_node* root;

	uint32_t min_bytes_to_mmap;

	transaction* tx;
};

grash_table* get_new_grash_table(const tuple_def* value_def, const positional_accessor* key_element_ids, uint32_t key_element_count, uint32_t min_bytes_to_mmap, transaction* tx);
grash_table* get_new_grash_table_partitioned(const tuple_def* value_def, const positional_accessor* key_element_ids, uint32_t key_element_count, uint32_t min_bytes_to_mmap, transaction* tx);

void delete_grash_table(grash_table* gt_p);

void insert_to_grash_table(grash_table* gt_p, void* value, uint64_t optional_pre_computed_hash_value);

// partition works only if type == COLLECTION
int partition_grash_table_node(grash_table* gt_p, grash_table_node* gtn_p);

// makes index only for type == PARTITION
// if distinct_keys_only is set, only distinct keys will be inserted
cash_table* index_grash_table_node(grash_table* gt_p, grash_table_node* gtn_p, int distinct_keys_only);

// recursive functions
void unmap_all_partition_mmaped_regions(grash_table_node* gtn_p);
void close_all_partition_files(grash_table_node* gtn_p);

#endif