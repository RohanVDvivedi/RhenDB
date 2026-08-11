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

	rhendb_table_partition* table_partitons_info;

	uint32_t* attributes_count_per_partition;

	rhendb_attribute** table_partitions_attributes_info;
	// table_partitions_attributes_info[partition_index in range [0, partitions_count)][attribute_index in range [0, attributes_count_per_partition[partition_index])]

	// tuple_defs to reach each of the rows in corresponding tuple_partitions
	tuple_def* table_partition_tuple_defs;

	// the tuple_def that should be passed between operators
	// every partition's tuple when read must be projected to this ans passed between operators or sent to user upon select all
	// it is completely based off the last visible partition to this query
	tuple_def final_readers_tuple_def;
};

// table_name if NULL, then we will use table_id
fetched_table* fetch_table_from_catalog_manager(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, char* table_name, uint64_t table_id);

// partition_index_in_info is not partition_id, this is index of the corresponding partition in the table_partitons_info array
void* project_to_final_readers_tuple_def(const fetched_table* ftabl, const void* partition_tuple, uint64_t partition_index_in_info);

void destroy_fetched_table(fetched_table* ftabl);

#endif