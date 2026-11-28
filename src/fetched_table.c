#include<rhendb/fetched_table.h>

#include<rhendb/mvcc_header.h>

#include<rhendb/nullable_type_info_maker.h>

#include<tuplestore/tuple.h>

#include<stdlib.h>

#include<string.h>

fetched_table* fetch_table_from_catalog_manager(rhendb* rdb, const mvcc_snapshot* ss_p, char* table_name, uint64_t table_id)
{
	// fetch the table itself, by name if we were given one, else by its id
	rhendb_table* table_info = NULL;
	if(table_name != NULL)
		table_info = get_catalog_object_by_name_from_catalog(&(rdb->cat_mgr), ss_p, RHENDB_TABLE, table_name);
	else
		table_info = get_catalog_object_by_id_from_catalog(&(rdb->cat_mgr), ss_p, RHENDB_TABLE, table_id);

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
	ftabl->table_partitions_info = get_partitions_for_table_from_catalog(&(rdb->cat_mgr), ss_p, ftabl->table_info.id, &(ftabl->partitions_count));
	if(ftabl->table_partitions_info == NULL || ftabl->partitions_count == 0)
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
		ftabl->table_partitions_attributes_info[i] = get_attributes_for_catalog_object_from_catalog(&(rdb->cat_mgr), ss_p, ftabl->table_info.id, ftabl->table_partitions_info[i].partition_id, &temp);
		if(temp > UINT32_MAX)
		{
			printf("ISSUE (in fetched_table) more than UINT32_MAX attributes in a table for some partition\n");
			exit(-1);
		}
		ftabl->attributes_count_per_partition[i] = temp;

		data_type_info* partition_type_info = get_data_type_info_for_rhendb_table_partition_from_catalog(&(rdb->cat_mgr), ss_p, &(ftabl->table_partitions_info[i]));
		initialize_tuple_def(&(ftabl->table_partition_tuple_defs[i]), partition_type_info);
	}

	// reuse the last partition's type info if it is equal to any of the partitions before it
	for(uint64_t i = 0; i < ftabl->partitions_count-1; i++)
	{
		// if i-th partition_id's type_info is identical to the last partition, then reuse it's type_info
		if(are_identical_type_info(ftabl->table_partition_tuple_defs[i].type_info, ftabl->table_partition_tuple_defs[ftabl->partitions_count-1].type_info))
		{
			destroy_type_info_recursively(ftabl->table_partition_tuple_defs[i].type_info, NULL);
			ftabl->table_partition_tuple_defs[i].type_info = ftabl->table_partition_tuple_defs[ftabl->partitions_count-1].type_info;
		}
	}

	return ftabl;
}

void destroy_fetched_table(fetched_table* ftabl)
{
	if(ftabl == NULL)
		return ;

	for(uint64_t i = 0; i < ftabl->partitions_count; i++)
	{
		// must destroy the ith partition)id's type_info, only if it is last partitoon or is not the same as the last partition
		if((i == (ftabl->partitions_count-1)) || (ftabl->table_partition_tuple_defs[i].type_info != ftabl->table_partition_tuple_defs[ftabl->partitions_count-1].type_info))
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
	free(ftabl->table_partitions_info);

	free(ftabl);
}

tuple_def* get_scan_dml_output_tuple_def(rhendb* rdb, const fetched_table* ftabl, int output_flags)
{
	if(output_flags == 0)
		return NULL;

	tuple_def* output_tuple_def = malloc(sizeof(tuple_def));
	data_type_info* output_dti = NULL;

	if(MUST_OUTPUT_HEAP_TUPLE_ONLY(output_flags))
	{
		output_dti = ftabl->table_partition_tuple_defs[ftabl->partitions_count-1].type_info->containees[1].al.type_info;
	}
	else
	{
		output_dti = malloc(sizeof_tuple_data_type_info(5)); // may be over allocation

		uint32_t output_dti_element_count = 0;
		uint64_t output_tuple_max_size = 8;

		if(MUST_OUTPUT_TABLE_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "table_id", 64);
			output_dti->containees[output_dti_element_count].al.type_info = UINT_NON_NULLABLE[8];

			output_tuple_max_size += 8;
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_PARTITION_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "partition_id", 64);
			output_dti->containees[output_dti_element_count].al.type_info = UINT_NON_NULLABLE[8];

			output_tuple_max_size += 8;
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_TUPLE_POINTER_ID(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "tuple_pointer", 64);
			output_dti->containees[output_dti_element_count].al.type_info = &(rdb->persistent_acid_rage_engine.pam_p->pas.tuple_pointer_type_info);

			output_tuple_max_size += sizeof(tuple_pointer);
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_MVCC_HEADER(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, "mvcc_header", 64);
			output_dti->containees[output_dti_element_count].al.type_info = rdb->mvcc_hdr_type_info;

			output_tuple_max_size += output_dti->containees[output_dti_element_count].al.type_info->is_variable_sized ? (8 + output_dti->containees[output_dti_element_count].al.type_info->max_size) : (1 + output_dti->containees[output_dti_element_count].al.type_info->size);
			output_dti_element_count++;
		}

		if(MUST_OUTPUT_HEAP_TUPLE(output_flags))
		{
			strncpy(output_dti->containees[output_dti_element_count].field_name, ftabl->table_info.name, 64);
			output_dti->containees[output_dti_element_count].al.type_info = ftabl->table_partition_tuple_defs[ftabl->partitions_count-1].type_info->containees[1].al.type_info;

			output_tuple_max_size += output_dti->containees[output_dti_element_count].al.type_info->is_variable_sized ? (8 + output_dti->containees[output_dti_element_count].al.type_info->max_size) : (1 + output_dti->containees[output_dti_element_count].al.type_info->size);
			output_dti_element_count++;
		}

		initialize_tuple_data_type_info(output_dti, ftabl->table_info.name, 0, output_tuple_max_size, output_dti_element_count);
	}

	initialize_tuple_def(output_tuple_def, output_dti);

	return output_tuple_def;
}

void* project_to_scan_dml_output_tuple_def(rhendb* rdb, const tuple_def* output_tuple_def, const fetched_table* ftabl, uint64_t partition_index_in_info, tuple_pointer tptr, const void* partition_tuple, int output_flags)
{
	const tuple_def* partition_tuple_def = &(ftabl->table_partition_tuple_defs[partition_index_in_info]);

	if(MUST_OUTPUT_HEAP_TUPLE_ONLY(output_flags))
	{
		if((partition_index_in_info == (ftabl->partitions_count-1)) || (ftabl->table_partition_tuple_defs[partition_index_in_info].type_info == ftabl->table_partition_tuple_defs[ftabl->partitions_count-1].type_info)) // same partition output tuple as is
		{
			const data_type_info* dti_temp;
			datum data_inside_partition_tuple;
			get_value_from_element_from_tuple(&data_inside_partition_tuple, &dti_temp, partition_tuple_def, STATIC_POSITION(1), partition_tuple);

			uint32_t output_tuple_size = get_tuple_size(output_tuple_def, data_inside_partition_tuple.tuple_value);

			void* output_tuple = malloc(output_tuple_size);

			memmove(output_tuple, data_inside_partition_tuple.tuple_value, output_tuple_size);

			return output_tuple;
		}
		else
		{
			void* output_tuple = malloc(get_maximum_tuple_size(output_tuple_def));
			init_tuple(output_tuple_def, output_tuple);

			const rhendb_attribute* partition_attrs = ftabl->table_partitions_attributes_info[partition_index_in_info];
			const rhendb_attribute* final_attrs = ftabl->table_partitions_attributes_info[ftabl->partitions_count - 1];

			uint32_t partition_element_count = ftabl->attributes_count_per_partition[partition_index_in_info];
			uint32_t final_element_count = ftabl->attributes_count_per_partition[ftabl->partitions_count - 1];

			// both the attributes arrays are ordered by their rel_pos_in_owner, so they are walked over
			// together in a merge like O(N) fashion, matching the attributes that have identical rel_pos_in_owner
			// p indexes the attributes of this partition, and f indexes the attributes of the last partition
			uint32_t p = 0;
			uint32_t f = 0;

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

				set_element_in_tuple_from_tuple(output_tuple_def, STATIC_POSITION(f), output_tuple, partition_tuple_def, STATIC_POSITION(1, p), partition_tuple, UINT32_MAX);

				p++;
				f++;
			}

			output_tuple = realloc(output_tuple, get_tuple_size(output_tuple_def, output_tuple));
			return output_tuple;
		}
	}

	void* output_tuple = malloc(get_maximum_tuple_size(output_tuple_def));
	init_tuple(output_tuple_def, output_tuple);

	uint32_t attr_index = 0;

	if(MUST_OUTPUT_TABLE_ID(output_flags))
	{
		set_element_in_tuple(output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = ftabl->table_info.id}), UINT32_MAX);
		attr_index++;
	}

	if(MUST_OUTPUT_PARTITION_ID(output_flags))
	{
		set_element_in_tuple(output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.uint_value = ftabl->table_partitions_info[partition_index_in_info].partition_id}), UINT32_MAX);
		attr_index++;
	}

	if(MUST_OUTPUT_TUPLE_POINTER_ID(output_flags))
	{
		char tptr_tpl[sizeof(tuple_pointer)];
		set_tuple_pointer(tptr_tpl, tptr, &(rdb->persistent_acid_rage_engine.pam_p->pas));
		set_element_in_tuple(output_tuple_def, STATIC_POSITION(attr_index), output_tuple, &((datum){.tuple_value = tptr_tpl}), UINT32_MAX);
		attr_index++;
	}

	if(MUST_OUTPUT_MVCC_HEADER(output_flags))
	{
		set_element_in_tuple_from_tuple(output_tuple_def, STATIC_POSITION(attr_index), output_tuple, partition_tuple_def, STATIC_POSITION(0), partition_tuple, UINT32_MAX);
		attr_index++;
	}

	if(MUST_OUTPUT_HEAP_TUPLE(output_flags))
	{
		if((partition_index_in_info == (ftabl->partitions_count-1)) || (ftabl->table_partition_tuple_defs[partition_index_in_info].type_info == ftabl->table_partition_tuple_defs[ftabl->partitions_count-1].type_info)) // same partition output tuple as is
		{
			set_element_in_tuple_from_tuple(output_tuple_def, STATIC_POSITION(attr_index), output_tuple, partition_tuple_def, STATIC_POSITION(1), partition_tuple, UINT32_MAX);
		}
		else
		{
			set_element_in_tuple(output_tuple_def, STATIC_POSITION(attr_index), output_tuple, EMPTY_DATUM, UINT32_MAX);

			const rhendb_attribute* partition_attrs = ftabl->table_partitions_attributes_info[partition_index_in_info];
			const rhendb_attribute* final_attrs = ftabl->table_partitions_attributes_info[ftabl->partitions_count-1];

			uint32_t partition_element_count = ftabl->attributes_count_per_partition[partition_index_in_info];
			uint32_t final_element_count = ftabl->attributes_count_per_partition[ftabl->partitions_count-1];

			// both the attributes arrays are ordered by their rel_pos_in_owner, so they are walked over
			// together in a merge like O(N) fashion, matching the attributes that have identical rel_pos_in_owner
			// p indexes the attributes of this partition, and f indexes the attributes of the last partition
			uint32_t p = 0;
			uint32_t f = 0;

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

				set_element_in_tuple_from_tuple(output_tuple_def, STATIC_POSITION(attr_index, f), output_tuple, partition_tuple_def, STATIC_POSITION(1, p), partition_tuple, UINT32_MAX);

				p++;
				f++;
			}
		}

		attr_index++;
	}

	output_tuple = realloc(output_tuple, get_tuple_size(output_tuple_def, output_tuple));
	return output_tuple;
}

void destroy_scan_dml_output_tuple_def(tuple_def* output_tuple_def, int output_flags)
{
	if(output_tuple_def == NULL || MUST_OUTPUT_HEAP_TUPLE_ONLY(output_flags))
		return ;

	free(output_tuple_def->type_info);
	free(output_tuple_def);
}