#include<rhendb/rhendb_types.h>

#include<rhendb/mvcc_header.h>

#include<tuplelargetypes/text_extended.h>
#include<tuplelargetypes/blob_extended.h>
#include<tuplelargetypes/numeric_extended.h>
#include<tuplelargetypes/jsonb_extended.h>

data_type_info* get_data_type_info_for_rhendb_type_info(const rhendb_type_info* rti_p, const rhendb* rdb)
{
	switch(rti_p->type)
	{
		case RHENDB_BIT_FIELD :
		{
			if(0 < rti_p->size && rti_p->size <= 64)
			{
				if(rti_p->is_nullable)
					return BIT_FIELD_NULLABLE[rti_p->size];
				else
					return BIT_FIELD_NON_NULLABLE[rti_p->size];
			}
			return NULL;
		}

		case RHENDB_UINT :
		{
			if(rti_p->is_nullable)
			{
				if(0 < rti_p->size && rti_p->size <= 8)
					return UINT_NULLABLE[rti_p->size];
				else if(0 < rti_p->size && rti_p->size <= 32)
					return LARGE_UINT_NULLABLE[rti_p->size];
			}
			else
			{
				if(0 < rti_p->size && rti_p->size <= 8)
					return UINT_NON_NULLABLE[rti_p->size];
				else if(0 < rti_p->size && rti_p->size <= 32)
					return LARGE_UINT_NON_NULLABLE[rti_p->size];
			}
			return NULL;
		}

		case RHENDB_INT :
		{
			if(rti_p->is_nullable)
			{
				if(0 < rti_p->size && rti_p->size <= 8)
					return INT_NULLABLE[rti_p->size];
				else if(0 < rti_p->size && rti_p->size <= 32)
					return LARGE_INT_NULLABLE[rti_p->size];
			}
			else
			{
				if(0 < rti_p->size && rti_p->size <= 8)
					return INT_NON_NULLABLE[rti_p->size];
				else if(0 < rti_p->size && rti_p->size <= 32)
					return LARGE_INT_NON_NULLABLE[rti_p->size];
			}
			return NULL;
		}

		case RHENDB_FLOAT :
		{
			if(rti_p->is_nullable)
			{
				if(rti_p->size == sizeof(float))
					return FLOAT_float_NULLABLE;
				else if(rti_p->size == sizeof(double))
					return FLOAT_double_NULLABLE;
			}
			else
			{
				if(rti_p->size == sizeof(float))
					return FLOAT_float_NON_NULLABLE;
				else if(rti_p->size == sizeof(double))
					return FLOAT_double_NON_NULLABLE;
			}
			return NULL;
		}

		case RHENDB_TUPLE_POINTER :
			return &(rdb->persistent_acid_rage_engine.pam_p->pas.tuple_pointer_type_info);

		case RHENDB_MVCC_HEADER :
			return rdb->mvcc_hdr_type_info;

		case RHENDB_TEXT :
			return get_text_extended_type_info(PERSISTENT_EXT_SUB_TYPE, MAX_EXTENDED_TYPE_SIZE, get_text_inline_type_info(MAX_EXTENDED_TYPE_SIZE), &(rdb->persistent_acid_rage_engine.pam_p->pas));

		case RHENDB_BLOB :
			return get_blob_extended_type_info(PERSISTENT_EXT_SUB_TYPE, MAX_EXTENDED_TYPE_SIZE, get_blob_inline_type_info(MAX_EXTENDED_TYPE_SIZE), &(rdb->persistent_acid_rage_engine.pam_p->pas));

		case RHENDB_NUMERIC :
			return get_numeric_extended_type_info(PERSISTENT_EXT_SUB_TYPE, MAX_EXTENDED_TYPE_SIZE, get_numeric_inline_type_info(MAX_EXTENDED_TYPE_SIZE), &(rdb->persistent_acid_rage_engine.pam_p->pas));

		case RHENDB_JSONB :
			return get_jsonb_extended_type_info(PERSISTENT_EXT_SUB_TYPE, MAX_EXTENDED_TYPE_SIZE, MAX_EXTENDED_TYPE_SIZE, &(rdb->persistent_acid_rage_engine.pam_p->pas));

		case RHENDB_ARRAY :
		{
			data_type_info* dti_p = malloc(sizeof(data_type_info));
			data_type_info* containee_dti_p = get_data_type_info_for_rhendb_type_info(rti_p->containee, rdb);
			if(rti_p->element_count == 0)
			{
				(*dti_p) = get_variable_element_count_array_type("rhendb_array", rdb->persistent_acid_rage_engine.pam_p->pas.page_size, containee_dti_p);
			}
			else
			{
				(*dti_p) = get_fixed_element_count_array_type("rhendb_array", rti_p->element_count, rdb->persistent_acid_rage_engine.pam_p->pas.page_size, rti_p->is_nullable, containee_dti_p);
			}
			return dti_p;
		}

		case RHENDB_TUPLE :
		{
			data_type_info* dti_p = malloc(sizeof_tuple_data_type_info(rti_p->element_count));
			for(uint32_t i = 0; i < rti_p->element_count; i++)
			{
				strncpy(dti_p->containees[i].field_name, rti_p->containees[i].attribute_name, sizeof(rti_p->containees[i].attribute_name));
				dti_p->containees[i].al.type_info = get_data_type_info_for_rhendb_type_info(rti_p->containee, rdb);
			}
			initialize_tuple_data_type_info(dti_p, rti_p->type_name, rti_p->is_nullable, rdb->persistent_acid_rage_engine.pam_p->pas.page_size, rti_p->element_count);
			return dti_p;
		}
	}

	return NULL;
}