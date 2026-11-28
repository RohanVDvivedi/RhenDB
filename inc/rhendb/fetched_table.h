#ifndef TABLE_SCHEMA_H
#define TABLE_SCHEMA_H

#include<rhendb/rhendb.h>

// must be refetched after every DDL that modifies the table
typedef struct fetched_table fetched_table;
struct fetched_table
{
	rhendb_table table_info;

	// will always be non zero
	uint64_t partitions_count;

	rhendb_table_partition* table_partitions_info;

	uint32_t* attributes_count_per_partition;

	rhendb_attribute** table_partitions_attributes_info;
	// table_partitions_attributes_info[partition_index in range [0, partitions_count)][attribute_index in range [0, attributes_count_per_partition[partition_index])]

	// tuple_defs to reach each of the rows in corresponding tuple_partitions
	tuple_def* table_partition_tuple_defs;
};

// table_name if NULL, then we will use table_id
fetched_table* fetch_table_from_catalog_manager(rhendb* rdb, const mvcc_snapshot* ss_p, char* table_name, uint64_t table_id);

void destroy_fetched_table(fetched_table* ftabl);

#include<rhendb/scan_dml_output_type.h>

// these 3 functions take care of the output of the scan and dml function
tuple_def* get_scan_dml_output_tuple_def(rhendb* rdb, const fetched_table* ftabl, int output_flags);
void* project_to_scan_dml_output_tuple_def(rhendb* rdb, const tuple_def* output_tuple_def, const fetched_table* ftabl, uint64_t partition_index_in_info, tuple_pointer tptr, const void* heap_record, int output_flags);
void destroy_scan_dml_output_tuple_def(tuple_def* output_tuple_def, int output_flags);

#endif