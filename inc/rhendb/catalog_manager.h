#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H

#include<rhendb/rage_engine.h>

#include<rhendb/mvcc_header.h>
#include<rhendb/mvcc_snapshot.h>

#include<rhendb/rhendb_types.h>

#include<tupleindexer/heap_table/heap_table.h>
#include<tupleindexer/bplus_tree/bplus_tree.h>

#define FIRST_SCHEMA_UNIQUE_ID 7ULL

typedef enum catalog_object_type catalog_object_type;
enum catalog_object_type
{
	RHENDB_TYPE = 0,
	RHENDB_TABLE = 1,
	RHENDB_INDEX = 2,
};

typedef struct catalog_heap_table catalog_heap_table;
struct catalog_heap_table
{
	tuple_def record_def;

	heap_table_tuple_defs heap_table_defs;

	uint64_t root_page_id;

	heap_table_accumulative_notifier htan;
};

typedef struct catalog_clust_table catalog_clust_table;
struct catalog_clust_table
{
	tuple_def record_def;

	bplus_tree_tuple_defs clust_table_defs;

	uint64_t root_page_id;
};

typedef struct catalog_btree_index catalog_btree_index;
struct catalog_btree_index
{
	tuple_def record_def;

	bplus_tree_tuple_defs index_defs;

	uint64_t root_page_id;
};

typedef struct catalog_manager catalog_manager;
struct catalog_manager
{
	// this page_table stores the all the root_page_ids of the static schema-ed catalog tables
	uint64_t catalog_root_page_id;

	// ---------------- TABLES FOR SCHEMA

	catalog_heap_table attributes_table;
	// mvcc_hdr, owner_id, rel_pos_in_owner, table_part_id_from, table_part_id_to, attribute_name, base_type (rhendb_base_type), size (for non composite base type), attribute_type_id (valid for base_type == RHENDB_TUPLE), count (0->variable length, 1->direct-element, N->fixed length array of N elements), is_auto_increment, is_nullable, derived_from_expression(null if not derived column and not index attribute)
	// has rows for attributes of table, type and index only
	// table_part_id_* will remain 0 if owner_id is not a table

	catalog_heap_table types_table;
	// mvcc_hdr, id, name and it has only user defined types, not the primitive ones

	catalog_clust_table index_fragments_table;
	// only supports insert/delete no updates to any attribute, fully delete the data structures here at the root_page_ids on garbage ciollection
	// key(table_id, index_id, partition_id) -> mvcc_hdr, root_page_id

	catalog_heap_table indices_table;
	// delete predicate_expr only when no other id for the same index is alive
	// mvcc_hdr, id, name, table_id, access_methos(btree or hash), predicate_expr

	catalog_clust_table table_partitions_table;
	// only supports insert/delete no updates to any attribute, fully delete the data structures here at the root_page_ids on garbage ciollection
	// key(table_id, partition_id) -> mvcc_hdr, heap_root_page_id, blobs_root_page_id
	// partition_id starts with 1 and is not globally unique, but is per table unique

	catalog_heap_table tables_table;
	// mvcc_hdr, id, name

	// catalog_heap_table functions_table;

	// ---------------- INDICES ON SCHEMA TABLE

	// for catalog_object_types(RHENDB_TYPE, RHENDB_TABLE, and RHENDB_INDEX)
	// key(object_type, name, object.tuple_pointer)
	catalog_btree_index name_idx;

	// for catalog_object_types(RHENDB_TYPE, RHENDB_TABLE, and RHENDB_INDEX)
	// key(object_type, id, object.tuple_pointer)
	catalog_btree_index id_idx;

	// for table_to_indices key(table_id, index.tuple_pointer)
	catalog_btree_index table_to_indices_idx;

	// for owner_to_attribute key(owner_id, rel_pos_in_owner, attribute.tuple_pointer)
	catalog_btree_index owner_to_attributes_idx;

	// ---------------- EXTENSION FOR ALL THE BLOBS IN THE SYSTEM FOR ALL EXPRESSIONS ARE STORED HERE

	// all extension get stored here
	uint64_t ext_store_root_page_id;

	// htan for the blob_store above
	heap_table_accumulative_notifier htan;

	// ---------------- FOR FIXING THE UNUSED SPACE ENTRIES

	// rage_engine to be used with catalog manager
	rage_engine* catmgr_engine;

	// tuple_def for the mvcc_header, its data_type_info is the containee at position 0 of every catalog table record
	tuple_def mvcc_header_tuple_def;

	// interface to fetch transaction statuses, needed for mvcc visibility checks
	transaction_status_getter* tsg_p;

	// this where new ids come from, each id in the schema is unique across all entities, unless it has partitions
	pthread_mutex_t global_unique_schema_id_lock;
	uint64_t global_unique_schema_id; // starts with FIRST_SCHEMA_UNIQUE_ID
};

typedef enum rhendb_base_type rhendb_base_type;
enum rhendb_base_type
{
	// primitive numbers of fixed length
	RHENDB_BIT_FIELD = 0,    // size 1-64 in bits
	RHENDB_UINT = 1,         // size 1-32 in bytes
	RHENDB_INT = 2,          // size 1-32 in bytes
	RHENDB_FLOAT = 3,        // either sizeof(float) or sizeof(double)

	// default composite types
	RHENDB_TUPLE_POINTER = 4,
	RHENDB_MVCC_HEADER = 5,

	// inline basic variable length types
	RHENDB_STRING = 6,
	RHENDB_BINARY = 7,
	RHENDB_NUMBER = 8,

	// extended versions of those types
	RHENDB_TEXT = 9,
	RHENDB_BLOB = 10,
	RHENDB_NUMERIC = 11,
	RHENDB_JSONB = 12,

	RHENDB_COMPOSITE_TYPE = 13, // check attribute_type_id, and read it
};

typedef struct rhendb_attribute rhendb_attribute;
struct rhendb_attribute
{
	uint64_t owner_id;

	uint64_t table_part_id_from; // valid only for table else it is 0
	uint64_t table_part_id_to; // valid only for table else it is 0

	uint64_t rel_pos_in_owner;

	char attribute_name[64];

	rhendb_base_type base_type;

	uint32_t size; // for base type

	uint64_t attribute_type_id; // valid only for base_type = RHENDB_COMPOSITE_TYPE

	uint32_t* count; // (0->variable length, N->fixed length array of N elements, NULL means direct element)
	uint32_t _count; // if valid count points here

	unsigned int is_auto_increment:1;

	unsigned int is_nullable:1;

	char* derived_from_expr; // valid if the attribute is derived from expression, or for an index
	uint32_t derived_from_expr_size;
	// derived_from_expr is to be freed if not NULL, and needs to be deep copied
};

typedef struct rhendb_type rhendb_type;
struct rhendb_type
{
	uint64_t id;

	char name[64];
};

typedef enum rhendb_index_access_type rhendb_index_access_type;
enum rhendb_index_access_type
{
	RHENDB_BTREE,
	RHENDB_HASH,
};

typedef struct rhendb_index_fragment rhendb_index_fragment;
struct rhendb_index_fragment
{
	uint64_t table_id;

	uint64_t index_id;

	uint64_t partition_id;

	uint64_t root_page_id;
};

typedef struct rhendb_index rhendb_index;
struct rhendb_index
{
	uint64_t id;

	char name[64];

	uint64_t table_id;

	rhendb_index_access_type access_methos; // (btree or hash)

	char* predicate_expr; // predicate selectivity for the index
	uint32_t predicate_expr_size;
};

typedef struct rhendb_table_partition rhendb_table_partition;
struct rhendb_table_partition
{
	uint64_t table_id;

	uint64_t partition_id;

	uint64_t heap_root_page_id;

	uint64_t blobs_root_page_id;
};

typedef struct rhendb_table rhendb_table;
struct rhendb_table
{
	uint64_t id;

	char name[64];
};

// here the root_page_id is an in-out parameter, pass it as NULL_PAGE_ID to create a new transaction table, or an existing one to open that particular transaction_table
void initialize_catalog_manager(catalog_manager* catmgr_p, uint64_t* root_page_id, data_type_info* mvcc_hdr_dti_p, rage_engine* catmgr_engine, transaction_status_getter* tsg_p);

// note:: must lock table by it's name before calling this function, and keep it locked until the transaction ends, for the below functions

// returns id of created table, it will always start with no indices and a single partition_id of 1, by the provided name
// on failure returns 0
uint64_t create_table(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, char* name, const rhendb_attribute* attrs, uint32_t attrs_count);

// returns new part_id, and also creates new partitions of existing indices on the table for this partition, with new root_page_id-s
// returns 0 if this call fails
uint64_t alter_table_add_column(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t table_id, const rhendb_attribute* attr);

// returns new part_id, and also creates new partitions of existing indices on the table for this partition, with new root_page_id-s
// returns 0 if this call fails
uint64_t alter_table_drop_column(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t table_id, uint64_t rel_pos_in_owner_to_drop);

// drops all it's partitions, and all indices for this table
void drop_table(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t table_id);

// returns id of created index, and creates the same index for all the partitions of this table
// all part_id's of this index point to the same attributes list
uint64_t create_index(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, rhendb_index* index_like, const rhendb_attribute* attrs, uint32_t attrs_count);

// drops index with this id and all it's partitions, and the corresponding list of attributes
void drop_index(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t table_id, uint64_t index_id);

// note:: must lock type by it's name before calling this function, and keep it locked until the transaction ends, for the below functions

// returns id of created type
uint64_t create_type(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, char* name, const rhendb_attribute* attrs, uint32_t attrs_count);

// drops all the attributes also
void drop_type(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t type_id);

// returns id of created function
// uint64_t create_function(catalog_manager* catmgr_p, char* name);
// void drop_function(catalog_manager* catmgr_p, uint64_t id);

#endif