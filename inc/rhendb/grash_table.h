#ifndef GRASH_TABLE_H
#define GRASH_TABLE_H

#include<rhendb/grash_table.h>

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

	grash_table_node* root;
};

grash_table* get_new_grash_table(const tuple_def* value_def, const positional_accessor* key_element_ids, uint32_t key_element_count);

void delete_grash_table(grash_table* gt_p);

void insert_to_grash_table(grash_table* gt_p, void* value, uint64_t optional_pre_computed_hash_value);

// partition works only if type == COLLECTION
int partition_grash_table_node(grash_table* gt_p, grash_table_node* gtn_p);

// makes index only for type == PARTITION
cash_table* index_grash_table_node(grash_table* gt_p, grash_table_node* gtn_p);

// recursive functions
void unmap_all_partition_mmaped_regions(grash_table_node* gtn_p);
void close_all_partition_files(grash_table_node* gtn_p);

#endif