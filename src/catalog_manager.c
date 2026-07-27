#include<rhendb/catalog_manager.h>

#include<tupleindexer/page_table/page_table.h>
#include<tupleindexer/heap_page/heap_page.h>

#include<tupleindexer/interface/page_access_methods.h>

// index utility struct

typedef struct rhendb_name_idx_entry rhendb_name_idx_entry;
struct rhendb_name_idx_entry
{
	catalog_object_type object_type;

	char name[64];

	tuple_pointer object_tuple_pointer;
};

typedef struct rhendb_id_idx_entry rhendb_id_idx_entry;
struct rhendb_id_idx_entry
{
	catalog_object_type object_type;

	uint64_t id;

	tuple_pointer object_tuple_pointer;
};

typedef struct rhendb_table_to_indices_entry rhendb_table_to_indices_entry;
struct rhendb_table_to_indices_entry
{
	uint64_t table_id;

	tuple_pointer indices_tuple_pointer;
};

typedef struct rhendb_owner_to_attributes_idx_entry rhendb_owner_to_attributes_idx_entry;
struct rhendb_owner_to_attributes_idx_entry
{
	uint64_t owner_id;

	uint64_t rel_pos_in_owner;

	tuple_pointer attributes_tuple_pointer;
};

// serialization (struct -> (void*)tuple) functions
// if should_blob is true, then serialize expression in the blob_store, in the current mini transaction passed
// it is expected that on should_blob = 1, this function calls may abort with a abort_error, and will return NULL for sure

void* serialize_rhendb_attribute(catalog_manager* catmgr_p, rhendb_attribute* attr, int should_blob, const void* min_tx_engine, int* abort_error);

void* serialize_rhendb_type(catalog_manager* catmgr_p, rhendb_type* typ);

void* serialize_rhendb_index_fragment(catalog_manager* catmgr_p, rhendb_index_fragment* ifrag);

void* serialize_rhendb_index(catalog_manager* catmgr_p, rhendb_index* idx, int should_blob, const void* min_tx_engine, int* abort_error);

void* serialize_rhendb_table_partition(catalog_manager* catmgr_p, rhendb_table_partition* tpart);

void* serialize_rhendb_table(catalog_manager* catmgr_p, rhendb_table* tbl);

void* serialize_rhendb_name_idx_entry(catalog_manager* catmgr_p, rhendb_name_idx_entry* nidx);

void* serialize_rhendb_id_idx_entry(catalog_manager* catmgr_p, rhendb_id_idx_entry* ididx);

void* serialize_rhendb_table_to_indices_entry(catalog_manager* catmgr_p, rhendb_table_to_indices_entry* t2iidx);

void* serialize_rhendb_owner_to_attributes_idx_entry(catalog_manager* catmgr_p, rhendb_owner_to_attributes_idx_entry* o2aidx);

// deserialization ((void*)tuple -> struct) functions
// if should_blob is true, then deserialize expression in the blob_store, in a separate NULL read only transaction
// else leave the blob of the expression as NULL

rhendb_attribute* deserialize_rhendb_attribute(catalog_manager* catmgr_p, void* tuple, int should_blob);

rhendb_type* deserialize_rhendb_type(catalog_manager* catmgr_p, void* tuple);

rhendb_index_fragment* deserialize_rhendb_index_fragment(catalog_manager* catmgr_p, void* tuple);

rhendb_index* deserialize_rhendb_index(catalog_manager* catmgr_p, void* tuple, int should_blob);

rhendb_table_partition* deserialize_rhendb_table_partition(catalog_manager* catmgr_p, void* tuple);

rhendb_table* deserialize_rhendb_table(catalog_manager* catmgr_p, void* tuple);

rhendb_name_idx_entry* deserialize_rhendb_name_idx_entry(catalog_manager* catmgr_p, void* tuple);

rhendb_id_idx_entry* deserialize_rhendb_id_idx_entry(catalog_manager* catmgr_p, void* tuple);

rhendb_table_to_indices_entry* deserialize_rhendb_table_to_indices_entry(catalog_manager* catmgr_p, void* tuple);

rhendb_owner_to_attributes_idx_entry* deserialize_rhendb_owner_to_attributes_idx_entry(catalog_manager* catmgr_p, void* tuple);

// ------

#define ID_n_REL_POS_BYTES 8
#define NAME_BYTES 64

#define HTAN_ENTRIES_MAX    56
#define HTAN_ENTRIES_THRES  35

static const compare_direction cmp_dirs_all_asc[] = {ASC, ASC, ASC, ASC, ASC, ASC};

const positional_accessor key_element_ids0[] = {STATIC_POSITION(0), STATIC_POSITION(1), STATIC_POSITION(2), STATIC_POSITION(3), STATIC_POSITION(4), STATIC_POSITION(5)};

const positional_accessor key_element_ids1[] = {STATIC_POSITION(1), STATIC_POSITION(2), STATIC_POSITION(3), STATIC_POSITION(4), STATIC_POSITION(5), STATIC_POSITION(6)};

void initialize_catalog_manager(catalog_manager* catmgr_p, uint64_t* root_page_id, data_type_info* mvcc_hdr_dti_p, rage_engine* catmgr_engine)
{
	data_type_info* obj_type_dti_p = UINT_NON_NULLABLE[2];

	data_type_info* id_dti_p = UINT_NON_NULLABLE[ID_n_REL_POS_BYTES];

	data_type_info* name_dti_p = malloc(sizeof(data_type_info));
	(*name_dti_p) = get_variable_length_string_type("name", NAME_BYTES + 8);

	data_type_info* rel_pos_in_owner_dti_p = UINT_NON_NULLABLE[ID_n_REL_POS_BYTES];

	data_type_info* root_page_id_dti_p = &(catmgr_engine->pam_p->pas.page_id_type_info);

	data_type_info* base_dti_p = UINT_NON_NULLABLE[2];

	{
		data_type_info* attributes_type_info = malloc(sizeof_tuple_data_type_info(12));

		strcpy(attributes_type_info->containees[0].field_name, "mvcc_hdr");
		attributes_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(attributes_type_info->containees[1].field_name, "owner_id");
		attributes_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(attributes_type_info->containees[2].field_name, "rel_pos_in_owner");
		attributes_type_info->containees[2].al.type_info = rel_pos_in_owner_dti_p;

		strcpy(attributes_type_info->containees[3].field_name, "table_part_id_from");
		attributes_type_info->containees[3].al.type_info = id_dti_p;

		strcpy(attributes_type_info->containees[4].field_name, "table_part_id_to");
		attributes_type_info->containees[4].al.type_info = id_dti_p;

		strcpy(attributes_type_info->containees[5].field_name, "attribute_name");
		attributes_type_info->containees[5].al.type_info = name_dti_p;

		strcpy(attributes_type_info->containees[6].field_name, "base_type");
		attributes_type_info->containees[6].al.type_info = base_dti_p;

		strcpy(attributes_type_info->containees[7].field_name, "attribute_type_id");
		attributes_type_info->containees[7].al.type_info = id_dti_p;

		strcpy(attributes_type_info->containees[8].field_name, "count");
		attributes_type_info->containees[8].al.type_info = UINT_NULLABLE[4];

		strcpy(attributes_type_info->containees[9].field_name, "is_auto_increment");
		attributes_type_info->containees[9].al.type_info = BIT_FIELD_NON_NULLABLE[1];

		strcpy(attributes_type_info->containees[10].field_name, "is_nullable");
		attributes_type_info->containees[10].al.type_info = BIT_FIELD_NON_NULLABLE[1];

		strcpy(attributes_type_info->containees[11].field_name, "derived_from_expr");
		attributes_type_info->containees[11].al.type_info = catmgr_engine->text_extended_type_info;

		initialize_tuple_data_type_info(attributes_type_info, "rhendb_attribute", 0, 900, 12);

		initialize_tuple_def(&(catmgr_p->attributes_table.record_def), attributes_type_info);

		init_heap_table_tuple_definitions(&(catmgr_p->attributes_table.heap_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->attributes_table.record_def));
	}

	{
		data_type_info* types_type_info = malloc(sizeof_tuple_data_type_info(3));

		strcpy(types_type_info->containees[0].field_name, "mvcc_hdr");
		types_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(types_type_info->containees[1].field_name, "id");
		types_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(types_type_info->containees[2].field_name, "name");
		types_type_info->containees[2].al.type_info = name_dti_p;

		initialize_tuple_data_type_info(types_type_info, "rhendb_type", 0, 900, 3);

		initialize_tuple_def(&(catmgr_p->types_table.record_def), types_type_info);

		init_heap_table_tuple_definitions(&(catmgr_p->types_table.heap_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->types_table.record_def));
	}

	{
		data_type_info* index_fragments_type_info = malloc(sizeof_tuple_data_type_info(5));

		strcpy(index_fragments_type_info->containees[0].field_name, "mvcc_hdr");
		index_fragments_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(index_fragments_type_info->containees[1].field_name, "table_id");
		index_fragments_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(index_fragments_type_info->containees[2].field_name, "index_id");
		index_fragments_type_info->containees[2].al.type_info = id_dti_p;

		strcpy(index_fragments_type_info->containees[3].field_name, "partition_id");
		index_fragments_type_info->containees[3].al.type_info = id_dti_p;

		strcpy(index_fragments_type_info->containees[4].field_name, "root_page_id");
		index_fragments_type_info->containees[4].al.type_info = root_page_id_dti_p;

		initialize_tuple_data_type_info(index_fragments_type_info, "rhendb_index", 0, 900, 5);

		initialize_tuple_def(&(catmgr_p->index_fragments_table.record_def), index_fragments_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->index_fragments_table.clust_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->index_fragments_table.record_def), key_element_ids1, cmp_dirs_all_asc, 3);
	}

	{
		data_type_info* indices_type_info = malloc(sizeof_tuple_data_type_info(6));

		strcpy(indices_type_info->containees[0].field_name, "mvcc_hdr");
		indices_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(indices_type_info->containees[1].field_name, "id");
		indices_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(indices_type_info->containees[2].field_name, "name");
		indices_type_info->containees[2].al.type_info = name_dti_p;

		strcpy(indices_type_info->containees[3].field_name, "table_id");
		indices_type_info->containees[3].al.type_info = id_dti_p;

		strcpy(indices_type_info->containees[4].field_name, "access_method");
		indices_type_info->containees[4].al.type_info = UINT_NON_NULLABLE[2];

		strcpy(indices_type_info->containees[5].field_name, "predicate_expr");
		indices_type_info->containees[5].al.type_info = catmgr_engine->text_extended_type_info;

		initialize_tuple_data_type_info(indices_type_info, "rhendb_index", 0, 900, 6);

		initialize_tuple_def(&(catmgr_p->indices_table.record_def), indices_type_info);

		init_heap_table_tuple_definitions(&(catmgr_p->indices_table.heap_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->indices_table.record_def));
	}

	{
		data_type_info* table_partitions_type_info = malloc(sizeof_tuple_data_type_info(5));

		strcpy(table_partitions_type_info->containees[0].field_name, "mvcc_hdr");
		table_partitions_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(table_partitions_type_info->containees[1].field_name, "table_id");
		table_partitions_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(table_partitions_type_info->containees[2].field_name, "partition_id");
		table_partitions_type_info->containees[2].al.type_info = id_dti_p;

		strcpy(table_partitions_type_info->containees[3].field_name, "heap_root_page_id");
		table_partitions_type_info->containees[3].al.type_info = root_page_id_dti_p;

		strcpy(table_partitions_type_info->containees[4].field_name, "blobs_root_page_id");
		table_partitions_type_info->containees[4].al.type_info = root_page_id_dti_p;

		initialize_tuple_data_type_info(table_partitions_type_info, "rhendb_table_partition", 0, 900, 5);

		initialize_tuple_def(&(catmgr_p->table_partitions_table.record_def), table_partitions_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->table_partitions_table.clust_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->table_partitions_table.record_def), key_element_ids1, cmp_dirs_all_asc, 2);
	}

	{
		data_type_info* tables_type_info = malloc(sizeof_tuple_data_type_info(3));

		strcpy(tables_type_info->containees[0].field_name, "mvcc_hdr");
		tables_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(tables_type_info->containees[1].field_name, "id");
		tables_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(tables_type_info->containees[2].field_name, "name");
		tables_type_info->containees[2].al.type_info = name_dti_p;

		initialize_tuple_data_type_info(tables_type_info, "rhendb_table", 0, 900, 3);

		initialize_tuple_def(&(catmgr_p->tables_table.record_def), tables_type_info);

		init_heap_table_tuple_definitions(&(catmgr_p->tables_table.heap_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->tables_table.record_def));
	}

	{
		data_type_info* name_idx_type_info = malloc(sizeof_tuple_data_type_info(3));

		strcpy(name_idx_type_info->containees[0].field_name, "object_type");
		name_idx_type_info->containees[0].al.type_info = obj_type_dti_p;

		strcpy(name_idx_type_info->containees[1].field_name, "name");
		name_idx_type_info->containees[1].al.type_info = name_dti_p;

		strcpy(name_idx_type_info->containees[2].field_name, "object.tuple_pointer");
		name_idx_type_info->containees[2].al.type_info = &(catmgr_engine->pam_p->pas.tuple_pointer_type_info);

		initialize_tuple_data_type_info(name_idx_type_info, "rhendb_name_idx_entry", 0, 900, 3);

		initialize_tuple_def(&(catmgr_p->name_idx.record_def), name_idx_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->name_idx.index_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->name_idx.record_def), key_element_ids0, cmp_dirs_all_asc, 3);
	}

	{
		data_type_info* id_idx_type_info = malloc(sizeof_tuple_data_type_info(3));

		strcpy(id_idx_type_info->containees[0].field_name, "object_type");
		id_idx_type_info->containees[0].al.type_info = obj_type_dti_p;

		strcpy(id_idx_type_info->containees[1].field_name, "id");
		id_idx_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(id_idx_type_info->containees[2].field_name, "object.tuple_pointer");
		id_idx_type_info->containees[2].al.type_info = &(catmgr_engine->pam_p->pas.tuple_pointer_type_info);

		initialize_tuple_data_type_info(id_idx_type_info, "rhendb_id_idx_entry", 0, 900, 3);

		initialize_tuple_def(&(catmgr_p->id_idx.record_def), id_idx_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->id_idx.index_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->id_idx.record_def), key_element_ids0, cmp_dirs_all_asc, 3);
	}

	{
		data_type_info* table_to_indices_type_info = malloc(sizeof_tuple_data_type_info(2));

		strcpy(table_to_indices_type_info->containees[0].field_name, "table_id");
		table_to_indices_type_info->containees[0].al.type_info = id_dti_p;

		strcpy(table_to_indices_type_info->containees[1].field_name, "index.tuple_pointer");
		table_to_indices_type_info->containees[1].al.type_info = &(catmgr_engine->pam_p->pas.tuple_pointer_type_info);

		initialize_tuple_data_type_info(table_to_indices_type_info, "rhendb_table_to_indices_idx_entry", 0, 900, 2);

		initialize_tuple_def(&(catmgr_p->table_to_indices_idx.record_def), table_to_indices_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->table_to_indices_idx.index_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->table_to_indices_idx.record_def), key_element_ids0, cmp_dirs_all_asc, 2);
	}

	{
		data_type_info* owner_to_attributes_idx_type_info = malloc(sizeof_tuple_data_type_info(3));

		strcpy(owner_to_attributes_idx_type_info->containees[0].field_name, "owner_id");
		owner_to_attributes_idx_type_info->containees[0].al.type_info = id_dti_p;

		strcpy(owner_to_attributes_idx_type_info->containees[1].field_name, "rel_pos_in_owner");
		owner_to_attributes_idx_type_info->containees[1].al.type_info = rel_pos_in_owner_dti_p;

		strcpy(owner_to_attributes_idx_type_info->containees[2].field_name, "attributes.tuple_pointer");
		owner_to_attributes_idx_type_info->containees[2].al.type_info = &(catmgr_engine->pam_p->pas.tuple_pointer_type_info);

		initialize_tuple_data_type_info(owner_to_attributes_idx_type_info, "rhendb_owner_to_attributes_idx_entry", 0, 900, 3);

		initialize_tuple_def(&(catmgr_p->owner_to_attributes_idx.record_def), owner_to_attributes_idx_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->owner_to_attributes_idx.index_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->owner_to_attributes_idx.record_def), key_element_ids0, cmp_dirs_all_asc, 3);
	}


	pthread_mutex_init(&(catmgr_p->htan_lock), NULL);

	initialize_heap_table_accumulative_notifier(&(catmgr_p->htan), HTAN_ENTRIES_MAX);

	catmgr_p->catmgr_engine = catmgr_engine;

	pthread_mutex_init(&(catmgr_p->global_unique_schema_id_lock), NULL);
}