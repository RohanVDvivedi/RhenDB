#include<rhendb/fetched_table.h>

#include<rhendb/mvcc_header.h>

#include<rhendb/nullable_type_info_maker.h>

#include<tuplestore/tuple.h>

#include<stdlib.h>

#include<string.h>

// builds the final_readers_tuple_def out of the type_info of the last (latest) visible partition,
// it is an almost identical version of that partition's type_info, where every attribute, except for
// the mvcc_header at position 0, is a shallow cloned nullable type
// this is the same construction that the insertion_operator does for its heap tuple output
static void initialize_final_readers_tuple_def(fetched_table* ftabl, const data_type_info* last_partition_type_info)
{
	data_type_info* final_readers_data_type_info = malloc(sizeof_tuple_data_type_info(last_partition_type_info->element_count));
	if(final_readers_data_type_info == NULL)
		exit(-1);

	uint32_t final_readers_data_type_info_max_size = 8 + sizeof(mvcc_header);

	// the first element is always the mvcc_header, and it is never made nullable, so it is taken as is
	strncpy(final_readers_data_type_info->containees[0].field_name, last_partition_type_info->containees[0].field_name, sizeof(final_readers_data_type_info->containees[0].field_name));
	final_readers_data_type_info->containees[0].al.type_info = last_partition_type_info->containees[0].al.type_info;

	// clone the rest while making them nullable shallowly with same field and type names
	for(uint32_t i = 1; i < last_partition_type_info->element_count; i++)
	{
		if(last_partition_type_info->containees[i].al.type_info->type == BIT_FIELD)
			final_readers_data_type_info_max_size += 9;
		else
			final_readers_data_type_info_max_size += last_partition_type_info->containees[i].al.type_info->is_variable_sized ? (8 + last_partition_type_info->containees[i].al.type_info->max_size) : (1 + last_partition_type_info->containees[i].al.type_info->size);

		strncpy(final_readers_data_type_info->containees[i].field_name, last_partition_type_info->containees[i].field_name, sizeof(final_readers_data_type_info->containees[i].field_name));
		final_readers_data_type_info->containees[i].al.type_info = shallow_clone_into_nullable_type(last_partition_type_info->containees[i].al.type_info);
	}

	initialize_tuple_data_type_info(final_readers_data_type_info, ftabl->table_info.name, 0, final_readers_data_type_info_max_size, last_partition_type_info->element_count);
	finalize_type_info(final_readers_data_type_info);

	initialize_tuple_def(&(ftabl->final_readers_tuple_def), final_readers_data_type_info);
}

fetched_table* fetch_table_from_catalog_manager(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, char* table_name, uint64_t table_id)
{
	// fetch the table itself, by name if we were given one, else by its id
	rhendb_table* table_info = NULL;
	if(table_name != NULL)
		table_info = get_catalog_object_by_name_from_catalog(catmgr_p, ss_p, RHENDB_TABLE, table_name);
	else
		table_info = get_catalog_object_by_id_from_catalog(catmgr_p, ss_p, RHENDB_TABLE, table_id);

	// no such table is visible to this snapshot
	if(table_info == NULL)
		return NULL;

	fetched_table* ftabl = malloc(sizeof(fetched_table));
	if(ftabl == NULL)
		exit(-1);

	ftabl->table_info = (*table_info);
	free(table_info);

	// fetch all the partitions of this table, there is always atleast one of them
	ftabl->partitions_count = 0;
	ftabl->table_partitons_info = get_partitions_for_table_from_catalog(catmgr_p, ss_p, ftabl->table_info.id, &(ftabl->partitions_count));
	if(ftabl->table_partitons_info == NULL || ftabl->partitions_count == 0)
	{
		free(ftabl);
		return NULL;
	}

	ftabl->table_partitions_attributes_info = malloc(sizeof(rhendb_attribute*) * ftabl->partitions_count);
	if(ftabl->table_partitions_attributes_info == NULL)
		exit(-1);

	ftabl->attributes_count_per_partition = malloc(sizeof(uint32_t) * ftabl->partitions_count);
	if(ftabl->attributes_count_per_partition == NULL)
		exit(-1);

	ftabl->table_partition_tuple_defs = malloc(sizeof(tuple_def) * ftabl->partitions_count);
	if(ftabl->table_partition_tuple_defs == NULL)
		exit(-1);

	// for each of the partitions, fetch its attributes and build a tuple_def to read its rows with
	for(uint64_t i = 0; i < ftabl->partitions_count; i++)
	{
		uint64_t temp;
		ftabl->table_partitions_attributes_info[i] = get_attributes_for_catalog_object_from_catalog(catmgr_p, ss_p, ftabl->table_info.id, ftabl->table_partitons_info[i].partition_id, &temp);
		if(temp > UINT32_MAX)
		{
			printf("ISSUE (in fetched_table) more than UINT32_MAX attributes in a table for some partition\n");
			exit(-1);
		}
		ftabl->attributes_count_per_partition[i] = temp;

		data_type_info* partition_type_info = get_data_type_info_for_rhendb_table_partition_from_catalog(catmgr_p, ss_p, &(ftabl->table_partitons_info[i]));
		initialize_tuple_def(&(ftabl->table_partition_tuple_defs[i]), partition_type_info);
	}

	// the final_readers_tuple_def is completely based off the last visible partition to this query
	initialize_final_readers_tuple_def(ftabl, ftabl->table_partition_tuple_defs[ftabl->partitions_count - 1].type_info);

	return ftabl;
}

void* project_to_final_readers_tuple_def(const fetched_table* ftabl, const void* partition_tuple, uint64_t partition_index_in_info)
{
	const tuple_def* partition_tuple_def = &(ftabl->table_partition_tuple_defs[partition_index_in_info]);
	const tuple_def* final_readers_tuple_def = &(ftabl->final_readers_tuple_def);

	const rhendb_attribute* partition_attrs = ftabl->table_partitions_attributes_info[partition_index_in_info];
	const rhendb_attribute* final_attrs = ftabl->table_partitions_attributes_info[ftabl->partitions_count - 1];

	uint32_t partition_element_count = ftabl->attributes_count_per_partition[partition_index_in_info];
	uint32_t final_element_count = ftabl->attributes_count_per_partition[ftabl->partitions_count - 1];

	uint32_t projected_tuple_size = get_minimum_tuple_size(final_readers_tuple_def);
	uint64_t projected_tuple_capacity = projected_tuple_size;
	void* projected_tuple = malloc(projected_tuple_capacity);
	if(projected_tuple == NULL)
		exit(-1);
	init_tuple(final_readers_tuple_def, projected_tuple);

	// the mvcc_header at position 0 is common to every partition, so it is always copied over
	while(!set_element_in_tuple_from_tuple(final_readers_tuple_def, STATIC_POSITION(0), projected_tuple, partition_tuple_def, STATIC_POSITION(0), partition_tuple, projected_tuple_capacity - projected_tuple_size))
	{
		projected_tuple_capacity = projected_tuple_capacity * 2;
		projected_tuple = realloc(projected_tuple, projected_tuple_capacity);
	}
	projected_tuple_size = get_tuple_size(final_readers_tuple_def, projected_tuple);

	// both the attributes arrays are ordered by their rel_pos_in_owner, so they are walked over
	// together in a merge like O(N) fashion, matching the attributes that have identical rel_pos_in_owner
	// p indexes the attributes of this partition, and f indexes the attributes of the last partition
	uint32_t p = 1;
	uint32_t f = 1;

	while(p < partition_element_count && f < final_element_count)
	{
		// this attribute was dropped before the last partition, so it has no place to be projected to
		if(partition_attrs[p].rel_pos_in_owner < final_attrs[f].rel_pos_in_owner)
		{
			p++;
			continue;
		}

		// skipping is equal to setting it to NULL
		if(final_attrs[f].rel_pos_in_owner < partition_attrs[p].rel_pos_in_owner)
		{
			f++;
			continue;
		}

		// identical rel_pos_in_owner, so this is the very same attribute, insert it
		while(!set_element_in_tuple_from_tuple(final_readers_tuple_def, STATIC_POSITION(f), projected_tuple, partition_tuple_def, STATIC_POSITION(p), partition_tuple, projected_tuple_capacity - projected_tuple_size))
		{
			projected_tuple_capacity = projected_tuple_capacity * 2;
			projected_tuple = realloc(projected_tuple, projected_tuple_capacity);
		}
		projected_tuple_size = get_tuple_size(final_readers_tuple_def, projected_tuple);

		p++;
		f++;
	}

	// skip the rest i.e. let then be NULL

	return projected_tuple;
}

void destroy_fetched_table(fetched_table* ftabl)
{
	if(ftabl == NULL)
		return ;

	// the first element of the final_readers_tuple_def is not a shallow clone, it is the mvcc_header
	// of the last partition itself, hence it is not to be destroyed here
	for(uint32_t i = 1; i < ftabl->final_readers_tuple_def.type_info->element_count; i++)
		free((void*)(ftabl->final_readers_tuple_def.type_info->containees[i].al.type_info));
	free((void*)(ftabl->final_readers_tuple_def.type_info));

	for(uint64_t i = 0; i < ftabl->partitions_count; i++)
	{
		destroy_type_info_recursively(ftabl->table_partition_tuple_defs[i].type_info, NULL);

		for(uint32_t j = 0; j < ftabl->attributes_count_per_partition[i]; j++)
		{
			if(ftabl->table_partitions_attributes_info[i][j].derived_from_expr != NULL)
				free(ftabl->table_partitions_attributes_info[i][j].derived_from_expr);
		}
		free(ftabl->table_partitions_attributes_info[i]);
	}

	free(ftabl->table_partition_tuple_defs);
	free(ftabl->attributes_count_per_partition);
	free(ftabl->table_partitions_attributes_info);
	free(ftabl->table_partitons_info);

	free(ftabl);
}