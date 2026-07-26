#include<rhendb/catalog_manager.h>

#include<tupleindexer/interface/page_access_methods.h>

#define ID_PART_ID_REL_POS_BYTES 8
#define NAME_BYTES 64

#define HTAN_ENTRIES_MAX    56
#define HTAN_ENTRIES_THRES  35

void initialize_catalog_manager(catalog_manager* catmgr_p, uint64_t* root_page_id, data_type_info* mvcc_hdr_dti_p, rage_engine* catmgr_engine)
{
	data_type_info* obj_type_dti_p = UINT_NON_NULLABLE[2];

	data_type_info* id_dti_p = UINT_NON_NULLABLE[ID_PART_ID_REL_POS_BYTES];

	data_type_info* name_dti_p = malloc(sizeof(data_type_info));
	(*name_dti_p) = get_variable_length_string_type("name", NAME_BYTES + 8);

	data_type_info* part_id_dti_p = UINT_NON_NULLABLE[ID_PART_ID_REL_POS_BYTES];

	data_type_info* rel_pos_in_owner_dti_p = UINT_NON_NULLABLE[ID_PART_ID_REL_POS_BYTES];

	data_type_info* root_page_id_dti_p = &(catmgr_engine->pam_p->pas.page_id_type_info);

	data_type_info* base_dti_p = UINT_NON_NULLABLE[2];

	{
		data_type_info* attributes_type_info = malloc(sizeof_tuple_data_type_info(11));

		strcpy(attributes_type_info->containees[0].field_name, "mvcc_hdr");
		attributes_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(attributes_type_info->containees[1].field_name, "owner_id");
		attributes_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(attributes_type_info->containees[2].field_name, "table_part_id");
		attributes_type_info->containees[2].al.type_info = part_id_dti_p;

		strcpy(attributes_type_info->containees[3].field_name, "rel_pos_in_owner");
		attributes_type_info->containees[3].al.type_info = rel_pos_in_owner_dti_p;

		strcpy(attributes_type_info->containees[4].field_name, "attribute_name");
		attributes_type_info->containees[4].al.type_info = name_dti_p;

		strcpy(attributes_type_info->containees[5].field_name, "base_type");
		attributes_type_info->containees[5].al.type_info = base_dti_p;

		strcpy(attributes_type_info->containees[6].field_name, "attribute_type_id");
		attributes_type_info->containees[6].al.type_info = id_dti_p;

		strcpy(attributes_type_info->containees[7].field_name, "count");
		attributes_type_info->containees[7].al.type_info = UINT_NULLABLE[4];

		strcpy(attributes_type_info->containees[8].field_name, "is_auto_increment");
		attributes_type_info->containees[8].al.type_info = BIT_FIELD_NON_NULLABLE[1];

		strcpy(attributes_type_info->containees[9].field_name, "is_nullable");
		attributes_type_info->containees[9].al.type_info = BIT_FIELD_NON_NULLABLE[1];

		strcpy(attributes_type_info->containees[10].field_name, "derived_from_expr");
		attributes_type_info->containees[10].al.type_info = catmgr_engine->text_extended_type_info;

		initialize_tuple_data_type_info(attributes_type_info, "rhendb_attribute", 0, 900, 11);

		initialize_tuple_def(&(catmgr_p->attributes_tuple_def), attributes_type_info);
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

		initialize_tuple_def(&(catmgr_p->types_tuple_def), types_type_info);
	}

	{
		data_type_info* indices_type_info = malloc(sizeof_tuple_data_type_info(8));

		strcpy(indices_type_info->containees[0].field_name, "mvcc_hdr");
		indices_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(indices_type_info->containees[1].field_name, "id");
		indices_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(indices_type_info->containees[2].field_name, "table_id");
		indices_type_info->containees[2].al.type_info = id_dti_p;

		strcpy(indices_type_info->containees[3].field_name, "table_part_id");
		indices_type_info->containees[3].al.type_info = part_id_dti_p;

		strcpy(indices_type_info->containees[4].field_name, "name");
		indices_type_info->containees[4].al.type_info = name_dti_p;

		strcpy(indices_type_info->containees[5].field_name, "access_type");
		indices_type_info->containees[5].al.type_info = UINT_NON_NULLABLE[2];

		strcpy(indices_type_info->containees[6].field_name, "root_page_id");
		indices_type_info->containees[6].al.type_info = root_page_id_dti_p;

		strcpy(indices_type_info->containees[7].field_name, "predicate_expr");
		indices_type_info->containees[7].al.type_info = catmgr_engine->text_extended_type_info;

		initialize_tuple_data_type_info(indices_type_info, "rhendb_index", 0, 900, 8);

		initialize_tuple_def(&(catmgr_p->indices_tuple_def), indices_type_info);
	}

	{
		data_type_info* tables_type_info = malloc(sizeof_tuple_data_type_info(6));

		strcpy(tables_type_info->containees[0].field_name, "mvcc_hdr");
		tables_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(tables_type_info->containees[1].field_name, "id");
		tables_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(tables_type_info->containees[2].field_name, "part_id");
		tables_type_info->containees[2].al.type_info = part_id_dti_p;

		strcpy(tables_type_info->containees[3].field_name, "name");
		tables_type_info->containees[3].al.type_info = name_dti_p;

		strcpy(tables_type_info->containees[4].field_name, "heap_root_page_id");
		tables_type_info->containees[4].al.type_info = root_page_id_dti_p;

		strcpy(tables_type_info->containees[5].field_name, "blobs_root_page_id");
		tables_type_info->containees[5].al.type_info = root_page_id_dti_p;

		initialize_tuple_data_type_info(tables_type_info, "rhendb_table", 0, 900, 6);

		initialize_tuple_def(&(catmgr_p->tables_tuple_def), tables_type_info);
	}

	pthread_mutex_init(&(catmgr_p->htan_lock), NULL);

	initialize_heap_table_accumulative_notifier(&(catmgr_p->htan), HTAN_ENTRIES_MAX);

	catmgr_p->catmgr_engine = catmgr_engine;

	pthread_mutex_init(&(catmgr_p->global_unique_schema_id_lock), NULL);
}