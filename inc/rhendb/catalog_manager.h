#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include<rhendb/rage_engine.h>

#include<tupleindexer/page_table/page_table.h>
#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/heap_page/heap_page.h>
#include<tupleindexer/bplus_tree/bplus_tree.h>

typedef enum catalog_object_type catalog_object_type;
enum catalog_object_type
{
	RHENDB_TYPE = 0,
	RHENDB_ATTRIBUTE = 1,
	RHENDB_RELATION = 2,
	RHENDB_INDEX = 3,
	RHENDB_FUNCTION = 4,
};

typedef struct catalog_manager catalog_manager;
struct catalog_manager
{
	// this page_table stores the 
	page_table_tuple_defs catalog_root_tuple_defs;
	uint64_t catalog_root_page_id;

	// ---------------- TABLES FOR SCHEMA

	heap_table_tuple_defs db_types_tuple_defs;
	uint64_t db_types_root_page_id;

	heap_table_tuple_defs db_attributes_tuple_defs;
	uint64_t db_attributes_root_page_id;

	heap_table_tuple_defs db_relations_tuple_defs;
	uint64_t db_relations_root_page_id;

	heap_table_tuple_defs db_indices_tuple_defs;
	uint64_t db_indices_root_page_id;

	heap_table_tuple_defs db_functions_tuple_defs;
	uint64_t db_functions_root_page_id;

	// ---------------- INDICES ON SCHEMA TABLE

	// bplus_tree index object_type, id, part_id -> row_id
	bplus_tree_tuple_defs idx_id_part_tuple_defs;
	uint64_t idx_id_part_root_page_id;

	// bplus_tree index object_type, name -> row_id
	bplus_tree_tuple_defs idx_name_tuple_defs;
	uint64_t idx_name_root_page_id;

	// rage_engine to be used with catalog manager
	rage_engine* catmgr_engine;
};

void initialize_catalog_manager(catalog_manager* catmgr_p, uint64_t* root_page_id, data_type_info* mvcc_hdr_dti_p, rage_engine* catmgr_engine);

#endif