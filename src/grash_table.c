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

	gt_p->root->type = COLLECTION;
	gt_p->root->level = 0;
	for(uint32_t i = 0; i < PARTITIONS_PER_COLLECTION; i++)
		gt_p->root->partitions[i] = NULL;
	gt_p->root->parent = NULL;
	gt_p->root->index_in_parent = 0;

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
			open_file_for_interim_tuple_store(gtn_p->partition);
			delete_interim_tuple_store(gtn_p->partition);
			free(gtn_p);
			break;
		}
		case COLLECTION :
		{
			for(uint32_t i = 0; i < PARTITIONS_PER_COLLECTION; i++)
				delete_grash_table_node(gtn_p->partitions[i]);
			free(gtn_p);
			break;
		}
	}
}

void delete_grash_table(grash_table* gt_p)
{
	delete_grash_table_node(gt_p->root);
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
		hash_value = hash_tuple_rhendb(value, gt_p->value_def, gt_p->key_element_ids, FNV_64_TUPLE_HASHER, gt_p->key_element_count, gt_p->tx);
	char hash_tuple_buffer[20];
	build_hash_tuple(gt_p, hash_tuple_buffer, hash_value);

	grash_table_node* gtn_p = gt_p->root;
	while(1)
	{
		switch(gtn_p->type)
		{
			case PARTITION :
			{
				open_file_for_interim_tuple_store(gtn_p->partition);
				append_tuple_to_interim_tuple_store2(gtn_p->partition, &(gtn_p->partition->embed_regions[0]), hash_tuple_buffer, &(gt_p->hash_def.size_def), gt_p->min_bytes_to_mmap); // first append the value value, then the tuple itself
				append_tuple_to_interim_tuple_store2(gtn_p->partition, &(gtn_p->partition->embed_regions[0]), value, &(gt_p->value_def->size_def), gt_p->min_bytes_to_mmap);
				return;
			}
			case COLLECTION :
			{
				grash_table_node** next_gtn_p = &(gtn_p->partitions[child_partition_in_collection(hash_value, gtn_p->level)]);
				if((*next_gtn_p) == NULL) // if the next_gtn_p is NULL, create one for this new tuple
				{
					(*next_gtn_p) = malloc(sizeof(grash_table_node));

					(*next_gtn_p)->type = PARTITION;
					(*next_gtn_p)->level = gtn_p->level + BITS_PER_COLLECTION;
					(*next_gtn_p)->partition = get_new_interim_tuple_store(gt_p->min_bytes_to_mmap);
					(*next_gtn_p)->parent = gtn_p;
					(*next_gtn_p)->index_in_parent = child_partition_in_collection(hash_value, gtn_p->level);
				}
				gtn_p = (*next_gtn_p);
				break;
			}
		}
	}
}

int partition_grash_table_node(grash_table* gt_p, grash_table_node* gtn_p)
{
	if(gtn_p->type == COLLECTION)
		return 0;

	if(gtn_p->level >= 64)
		return 0;

	// tuples count in the partition must be atleast 2 (1 hash value + 1 value tuple), else fail partitioning
	if((gtn_p->partition->tuples_count / 2) < 2)
		return 0;

	interim_tuple_store* partitions[PARTITIONS_PER_COLLECTION] = {0};

	open_file_for_interim_tuple_store(gtn_p->partition);

	uint64_t offset = 0;
	while(offset < get_total_bytes_in_interim_tuple_store(gtn_p->partition))
	{
		// get hash value
		mmap_for_reading_tuple(gtn_p->partition, &(gtn_p->partition->embed_regions[0]), offset, &(gt_p->hash_def.size_def), gt_p->min_bytes_to_mmap);
		char hash_tuple_buffer[20];
		memory_move(hash_tuple_buffer, gtn_p->partition->embed_regions[0].tuple, get_tuple_size(&(gt_p->hash_def), gtn_p->partition->embed_regions[0].tuple));
		uint64_t hash_value = 0;
		{
			const data_type_info* dti_temp = NULL;
			datum uval;
			get_value_from_element_from_tuple(&uval, &dti_temp, &(gt_p->hash_def), SELF, hash_tuple_buffer);
			hash_value = uval.uint_value;
		}
		offset = next_tuple_offset_for_interim_tuple_region(&(gtn_p->partition->embed_regions[0]));

		// get value tuple
		mmap_for_reading_tuple(gtn_p->partition, &(gtn_p->partition->embed_regions[0]), offset, &(gt_p->value_def->size_def), gt_p->min_bytes_to_mmap);
		void* value = gtn_p->partition->embed_regions[0].tuple;

		interim_tuple_store** part = &(partitions[child_partition_in_collection(hash_value, gtn_p->level)]);

		if((*part) == NULL)
			(*part) = get_new_interim_tuple_store(gt_p->min_bytes_to_mmap);

		// append both the hash value and value tuple to this partition

		append_tuple_to_interim_tuple_store2((*part), &((*part)->embed_regions[0]), hash_tuple_buffer, &(gt_p->hash_def.size_def), gt_p->min_bytes_to_mmap); // first append the value value, then the tuple itself
		append_tuple_to_interim_tuple_store2((*part), &((*part)->embed_regions[0]), value, &(gt_p->value_def->size_def), gt_p->min_bytes_to_mmap);

		offset = next_tuple_offset_for_interim_tuple_region(&(gtn_p->partition->embed_regions[0]));
	}

	uint32_t total_valid_partitions = 0;
	for(uint32_t i = 0; i < PARTITIONS_PER_COLLECTION; i++)
		total_valid_partitions += (partitions[i] != NULL);
	if(total_valid_partitions <= 1) // if we failed partitioning because of skew, do not modify the gtn_p, discard the partitions
	{
		for(uint32_t i = 0; i < PARTITIONS_PER_COLLECTION; i++)
			if(partitions[i] != NULL)
				delete_interim_tuple_store(partitions[i]);
		unmap_all_embed_regions_in_interim_tuple_store(gtn_p->partition);
		close_file_for_interim_tuple_store(gtn_p->partition);
		return 0;
	}

	delete_interim_tuple_store(gtn_p->partition);

	gtn_p->type = COLLECTION;

	for(uint32_t i = 0; i < PARTITIONS_PER_COLLECTION; i++)
	{
		if(partitions[i] == NULL)
		{
			gtn_p->partitions[i] = NULL;
			continue;
		}

		gtn_p->partitions[i] = malloc(sizeof(grash_table_node));

		gtn_p->partitions[i]->type = PARTITION;
		gtn_p->partitions[i]->level = gtn_p->level + BITS_PER_COLLECTION;
		gtn_p->partitions[i]->partition = partitions[i];
		gtn_p->partitions[i]->parent = gtn_p;
		gtn_p->partitions[i]->index_in_parent = i;
	}

	return 1;
}

cash_table* index_grash_table_node(grash_table* gt_p, grash_table_node* gtn_p, int distinct_keys_only)
{
	if(gtn_p->type == COLLECTION)
		return NULL;

	cash_table* ct_p = get_new_cash_table(10 + ((gtn_p->partition->tuples_count / 2) * 0.8), gt_p->value_def, gt_p->key_element_ids, gt_p->key_element_count, gt_p->tx);

	open_file_for_interim_tuple_store(gtn_p->partition);

	mmap_for_reading_tuple(gtn_p->partition, &(gtn_p->partition->embed_regions[0]), 0, &(gt_p->hash_def.size_def), get_total_bytes_in_interim_tuple_store(gtn_p->partition));

	int is_valid_k = 0;
	cash_table_entry k;

	uint64_t offset = 0;
	while(offset < get_total_bytes_in_interim_tuple_store(gtn_p->partition))
	{
		// get hash value
		mmap_for_reading_tuple(gtn_p->partition, &(gtn_p->partition->embed_regions[0]), offset, &(gt_p->hash_def.size_def), gt_p->min_bytes_to_mmap);
		uint64_t hash_value = 0;
		{
			const data_type_info* dti_temp = NULL;
			datum uval;
			get_value_from_element_from_tuple(&uval, &dti_temp, &(gt_p->hash_def), SELF, gtn_p->partition->embed_regions[0].tuple);
			hash_value = uval.uint_value;
		}
		offset = next_tuple_offset_for_interim_tuple_region(&(gtn_p->partition->embed_regions[0]));

		// get value tuple
		mmap_for_reading_tuple(gtn_p->partition, &(gtn_p->partition->embed_regions[0]), offset, &(gt_p->value_def->size_def), gt_p->min_bytes_to_mmap);
		void* value = gtn_p->partition->embed_regions[0].tuple;

		// add then both to the index
		if(distinct_keys_only)
		{
			if(is_valid_k)
				reset_cash_table_key(&k, ct_p, value, gt_p->value_def, gt_p->key_element_ids, hash_value);
			else
			{
				k = get_new_cash_table_key(ct_p, value, gt_p->value_def, gt_p->key_element_ids, hash_value);
				is_valid_k = 1;
			}

			if(find_equals_in_cash_table(ct_p, &k) == NULL) // only insert if not found
			{
				cash_table_entry* e = get_new_cash_table_value_borrow(ct_p, value, hash_value);
				insert_in_cash_table(ct_p, e);
			}
		}
		else
		{
			cash_table_entry* e = get_new_cash_table_value_borrow(ct_p, value, hash_value);
			insert_in_cash_table(ct_p, e);
		}

		offset = next_tuple_offset_for_interim_tuple_region(&(gtn_p->partition->embed_regions[0]));
	}

	if(is_valid_k)
	{
		delete_cash_table_entry(&k);
		is_valid_k = 0;
	}

	return ct_p;
}

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
				unmap_all_partition_mmaped_regions(gtn_p->partitions[i]);
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
				close_all_partition_files(gtn_p->partitions[i]);
			break;
		}
	}
}
