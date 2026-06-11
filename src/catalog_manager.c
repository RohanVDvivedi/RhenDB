#include<rhendb/catalog_manager.h>

#include<tupleindexer/interface/page_access_methods.h>

#define ID_PART_ID_BYTES 8
#define NAME_BYTES 64

void initialize_catalog_manager(catalog_manager* catmgr_p, uint64_t* root_page_id, data_type_info* mvcc_hdr_dti_p, rage_engine* catmgr_engine)
{
	data_type_info* obj_type_dti_p = UINT_NON_NULLABLE[2];

	data_type_info* id_dti_p = UINT_NON_NULLABLE[ID_PART_ID_BYTES];

	data_type_info* name_dti_p = malloc(sizeof(data_type_info));
	(*name_dti_p) = get_variable_length_string_type("name", NAME_BYTES + 8);

	data_type_info* part_id_dti_p = UINT_NON_NULLABLE[ID_PART_ID_BYTES];

	data_type_info* root_page_id_dti_p = &(catmgr_engine->pam_p->pas.page_id_type_info);
}