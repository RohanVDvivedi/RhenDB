#include<rhendb/rhendb_types.h>

#include<rhendb/mvcc_header.h>

#include<tuplelargetypes/text_extended.h>
#include<tuplelargetypes/blob_extended.h>
#include<tuplelargetypes/numeric_extended.h>
#include<tuplelargetypes/jsonb_extended.h>

data_type_info* get_data_type_info(const rhendb_type_info* rti_p, const rhendb* rdb)
{
	switch(rti_p->type)
	{
		case RHENDB_BIT_FIELD :
		{
			if(rti_p->is_nullable)
				return BIT_FIELD_NULLABLE[rti_p->size];
			else
				return BIT_FIELD_NON_NULLABLE[rti_p->size];
		}

		case RHENDB_UINT :
		{
			if(rti_p->is_nullable)
			{
				if(rti_p->size <= 8)
					return UINT_NULLABLE[rti_p->size];
				else
					return LARGE_UINT_NULLABLE[rti_p->size];
			}
			else
			{
				if(rti_p->size <= 8)
					return UINT_NON_NULLABLE[rti_p->size];
				else
					return LARGE_UINT_NON_NULLABLE[rti_p->size];
			}
		}

		case RHENDB_INT :
		{
			if(rti_p->is_nullable)
			{
				if(rti_p->size <= 8)
					return INT_NULLABLE[rti_p->size];
				else
					return LARGE_INT_NULLABLE[rti_p->size];
			}
			else
			{
				if(rti_p->size <= 8)
					return INT_NON_NULLABLE[rti_p->size];
				else
					return LARGE_INT_NON_NULLABLE[rti_p->size];
			}
		}

		case RHENDB_FLOAT :
		{
			if(rti_p->is_nullable)
			{
				if(rti_p->size == sizeof(float))
					return FLOAT_float_NULLABLE;
				else
					return FLOAT_double_NULLABLE;
			}
			else
			{
				if(rti_p->size == sizeof(float))
					return FLOAT_float_NON_NULLABLE;
				else
					return FLOAT_double_NON_NULLABLE;
			}
		}

		case RHENDB_TUPLE_POINTER :
			return &(rdb->persistent_acid_rage_engine.pam_p->pas.tuple_pointer_type_info);

		case RHENDB_MVCC_HEADER :
		{
			tuple_def* mvcc_header_def = get_mvcc_header_tuple_definition(TRANSACTION_ID_WIDTH);
			data_type_info* mvcc_header_type_info = mvcc_header_def->type_info;
			free(mvcc_header_def);
			return mvcc_header_type_info;
		}


		case RHENDB_STRING :
		case RHENDB_TEXT :
			return get_text_extended_type_info(MAX_INLINE_SIZE + 20, get_text_inline_type_info(MAX_INLINE_SIZE + 8), &(rdb->persistent_acid_rage_engine.pam_p->pas));

		case RHENDB_BINARY :
		case RHENDB_BLOB :
			return get_blob_extended_type_info(MAX_INLINE_SIZE + 20, get_blob_inline_type_info(MAX_INLINE_SIZE + 8), &(rdb->persistent_acid_rage_engine.pam_p->pas));

		case RHENDB_NUMERIC :
			return get_numeric_extended_type_info(MAX_INLINE_SIZE + 20, get_numeric_inline_type_info(MAX_INLINE_SIZE + 8), &(rdb->persistent_acid_rage_engine.pam_p->pas));

		case RHENDB_JSONB :
			return get_jsonb_extended_type_info(MAX_INLINE_SIZE + 20, MAX_INLINE_SIZE + 8, &(rdb->persistent_acid_rage_engine.pam_p->pas));
	}

	return NULL;
}