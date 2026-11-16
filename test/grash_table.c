#include<rhendb/grash_table.h>

#include<rhendb/function_hash.h>

grash_table* get_new_grash_table(const tuple_def* value_def, const positional_accessor* key_element_ids, uint32_t key_element_count, uint32_t min_bytes_to_mmap, transaction* tx)
{
	grash_table* gt_p = malloc(sizeof(grash_table));

	gt_p->value_def = value_def;
	gt_p->key_element_ids = key_element_ids;
	gt_p->key_element_count = key_element_count;

	initialize_tuple_def(&(gt_p->hash_def), UINT_NON_NULLABLE[8]);

	gt_p->root = malloc(sizeof(grash_table_node));

	gt_p->root->type = PARTITION;
	gt_p->root->level = 0;
	gt_p->root->partition = get_new_interim_tuple_store(min_bytes_to_mmap);

	gt_p->min_bytes_to_mmap = min_bytes_to_mmap;

	gt_p->tx = tx;

	return gt_p;
}

static void delete_grash_table_node(grash_table_node* gtn_p)
{
	if(gtn_p == NULL)
		return;

	switch(gtn_p->type)
	{
		case PARTITION :
		{
			delete_interim_tuple_store(gtn_p->partition);
			free(gtn_p);
			break;
		}
		case COLLECTION :
		{
			for(uint32_t i = 0; i < PARTITIONS_PER_COLLECTION; i++)
				delete_grash_table_node(gtn_p->partitions[i])
			free(gtn_p);
			break;
		}
	}
}

void delete_grash_table(grash_table* gt_p)
{
	delete_grash_table_node(gt_p->root)
	free(gt_p);
}

#define child_partition_in_collection(hash_value, level) ((hash_value >> level) % PARTITIONS_PER_COLLECTION)

static void build_hash_tuple(grash_table* gt_p, void* buffer, uint64_t hash_value)
{
	init_tuple(&(gt_p->hash_def), buffer);
	set_element_in_tuple(&(gt_p->hash_def), SELF, buffer, &((const datum){.uint_value = hash_value}), UINT32_MAX);
}

void insert_to_grash_table(grash_table* gt_p, void* value, uint64_t optional_pre_computed_hash_value)
{
	uint64_t hash_value = optional_pre_computed_hash_value;
	if(hash_value == 0)
		hash_tuple_rhendb(value, gt_p->value_def, gt_p->key_element_ids, FNV_64_TUPLE_HASHER, gt_p->key_element_count, gt_p->tx);
	char hash_tuple_buffer[20];
	build_hash_tuple(gt_p, hash_tuple_buffer, hash_value);

	grash_table_node* gtn_p = gt_p->root;
	while(1)
	{
		switch(gtn_p->type)
		{
			case PARTITION :
			{
				append_tuple_to_interim_tuple_store2(gtn_p->partition, &(gtn_p->partition->embed_regions[0]), hash_tuple_buffer, &(gt_p->hash_def.size_def), gt_p->min_bytes_to_mmap); // first append the value value, then the tuple itself
				append_tuple_to_interim_tuple_store2(gtn_p->partition, &(gtn_p->partition->embed_regions[0]), value, &(gt_p->value_def->size_def), gt_p->min_bytes_to_mmap)
				return;
			}
			case COLLECTION :
			{
				gtn_p = gtn_p->partitions[child_partition_in_collection(hash_value, gtn_p->level)];
				break;
			}
		}
	}
}

int partition_grash_table_node(grash_table* gt_p, grash_table_node* gtn_p);

cash_table* index_grash_table_node(grash_table* gt_p, grash_table_node* gtn_p);

void unmap_all_partition_mmaped_regions(grash_table_node* gtn_p)
{
	if(gtn_p == NULL)
		return;

	switch(gtn_p->type)
	{
		case PARTITION :
		{
			unmap_all_embed_regions_in_interim_tuple_store(gtn_p->partition);
			break;
		}
		case COLLECTION :
		{
			for(uint32_t i = 0; i < PARTITIONS_PER_COLLECTION; i++)
				unmap_all_partition_mmaped_regions(gtn_p->partitions[i])
			break;
		}
	}
}

void close_all_partition_files(grash_table_node* gtn_p)
{
	if(gtn_p == NULL)
		return;

	switch(gtn_p->type)
	{
		case PARTITION :
		{
			close_file_for_interim_tuple_store(gtn_p->partition);
			break;
		}
		case COLLECTION :
		{
			for(uint32_t i = 0; i < PARTITIONS_PER_COLLECTION; i++)
				close_all_partition_files(gtn_p->partitions[i])
			break;
		}
	}
}
