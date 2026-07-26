#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include<rhendb/rage_engine.h>

#include<rhendb/mvcc_header.h>
#include<rhendb/mvcc_snapshot.h>

#include<rhendb/rhendb_types.h>

#include<tupleindexer/page_table/page_table.h>
#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/heap_page/heap_page.h>
#include<tupleindexer/bplus_tree/bplus_tree.h>

#define FIRST_SCHEMA_UNIQUE_ID 7ULL

typedef enum catalog_object_type catalog_object_type;
enum catalog_object_type
{
	RHENDB_TYPE = 0,
	RHENDB_TABLE = 1,
};

typedef struct catalog_manager catalog_manager;
struct catalog_manager
{
	// this page_table stores the all the root_page_ids of the static schema-ed catalog tables
	page_table_tuple_defs catalog_root_tuple_defs;
	uint64_t catalog_root_page_id;

	// ---------------- TABLES FOR SCHEMA

	bplus_tree_tuple_defs db_attributes_tuple_defs;
	uint64_t db_attributes_root_page_id;
	// key(owner_id, table_part_id, rel_pos_in_owner) -> attribute_name, base_type (rhendb_base_type), attribute_type_id (valid for base_type == RHENDB_TUPLE), count (0->variable length, 1->direct-element, N->fixed length array of N elements), is_auto_increment, is_nullable, derived_from_expression(null if not derived column and not index attribute)
	// table_part_id is 0, if it is not a table

	heap_table_tuple_defs db_types_tuple_defs;
	uint64_t db_types_root_page_id;
	// mvcc_hdr, id, name
	// only user defined types are here, not the primitive ones

	bplus_tree_tuple_defs db_indices_tuple_defs;
	uint64_t db_indices_root_page_id;
	// key(table_id, id, table_part_id) -> mvcc_hdr, name, access_type(btree or hash), root_page_id, predicate

	heap_table_tuple_defs db_tables_tuple_defs;
	uint64_t db_tables_root_page_id;
	// mvcc_hdr, id, part_id, name, heap_root_page_id, blobs_root_page_id

	// heap_table_tuple_defs db_functions_tuple_defs;
	// uint64_t db_functions_root_page_id;

	// ---------------- INDICES ON SCHEMA TABLE

	// indices carry entries only for catalog_object_type(RHENDB_TYPE = 0 and RHENDB_TABLE = 1)

	// bplus_tree index
	// key(object_type, id, table_part_id, row_id)
	bplus_tree_tuple_defs idx_id_part_tuple_defs;
	uint64_t idx_id_part_root_page_id;

	// bplus_tree index
	// key(object_type, name, row_id)
	bplus_tree_tuple_defs idx_name_tuple_defs;
	uint64_t idx_name_root_page_id;

	// all extension get stored here
	uint64_t ext_store_root_page_id;

	// rage_engine to be used with catalog manager
	rage_engine* catmgr_engine;

	// this where new ids come from, each id in the schema is unique across all entities, unless it has partitions
	pthread_mutex_t global_unique_schema_id_lock;
	uint64_t global_unique_schema_id; // starts with FIRST_SCHEMA_UNIQUE_ID
};

typedef struct rhendb_attribute rhendb_attribute;
struct rhendb_attribute
{
	uint64_t owner_id;

	uint64_t table_part_id; // valid only for table else it possibly is 0

	uint64_t rel_pos_in_owner;

	char attribute_name[64];

	rhendb_base_type base_type;

	uint64_t attribute_type_id; // valid only for base_type = RHENDB_TYPE

	uint32_t count; // (0->variable length, 1->direct-element, N->fixed length array of N elements)

	unsigned int is_auto_increment:1;

	unsigned int is_nullable:1;

	char* derived_from_expr; // valid if the attribute is derived from expression, or for an index
	// derived_from_expr is to be freed if not NULL, and needs to be deep copied
};

typedef struct rhendb_type rhendb_type;
struct rhendb_type
{
	uint64_t id;

	rhendb_base_type base_type;

	char name[64];
};

typedef enum rhendb_access_type rhendb_access_type;
enum rhendb_access_type
{
	RHENDB_BTREE,
	RHENDB_HASH,
};

typedef struct rhendb_index rhendb_index;
struct rhendb_index
{
	uint64_t id;

	uint64_t table_id;

	uint64_t table_part_id; // there is a unique table_part_id for id, table_id pair

	char name[64];

	rhendb_access_type access_type; // (btree or hash)

	uint64_t root_page_id;

	char* predicate_expr; // predicate selectivity for the index
};

typedef struct rhendb_table rhendb_table;
struct rhendb_table
{
	uint64_t id;

	uint64_t part_id;

	char name[64];

	uint64_t heap_root_page_id;

	uint64_t blobs_root_page_id;
};

// here the root_page_id is an in-out parameter, pass it as NULL_PAGE_ID to create a new transaction table, or an existing one to open that particular transaction_table
void initialize_catalog_manager(catalog_manager* catmgr_p, uint64_t* root_page_id, data_type_info* mvcc_hdr_dti_p, rage_engine* catmgr_engine);

// returns id of created table, it will always start with no indices and a single part_id of 1, by the provided name
uint64_t create_table(catalog_manager* catmgr_p, char* name, const rhendb_attribute* attrs, uint32_t attrs_count);

// returns new part_id, and also creates new partitions of existing indices on the table for this partition, with new root_page_id-s
// returns 0 if this call fails
uint64_t alter_table_add_column(catalog_manager* catmgr_p, uint64_t table_id, const rhendb_attribute* attr);

// returns new part_id, and also creates new partitions of existing indices on the table for this partition, with new root_page_id-s
// and drops whole indices that reference the dropped column
// returns 0 if this call fails
uint64_t alter_table_drop_column(catalog_manager* catmgr_p, uint64_t table_id, uint64_t rel_pos_in_owner_to_drop);

// drops all it's partitions, and all indices for this table
void drop_table(catalog_manager* catmgr_p, uint64_t table_id);

// returns id of created index, and creates the same index for all the partitions of this table
// all part_id's of this index point to the same attributes list
uint64_t create_index(catalog_manager* catmgr_p, rhendb_index* index_like, const rhendb_attribute* attrs, uint32_t attrs_count);

// drops index with this id and all it's partitions, and the corresponding list of attributes
void drop_index(catalog_manager* catmgr_p, uint64_t table_id, uint64_t index_id);

// returns id of created type
uint64_t create_type(catalog_manager* catmgr_p, char* name, const rhendb_attribute* attrs, uint32_t attrs_count);

// drops all the attributes also
void drop_type(catalog_manager* catmgr_p, uint64_t type_id);

// returns id of created function
// uint64_t create_function(catalog_manager* catmgr_p, char* name);
// void drop_function(catalog_manager* catmgr_p, uint64_t id);

#endif