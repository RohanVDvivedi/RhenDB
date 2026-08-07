#include<util_numeric_conversions.h>

mpd_t numeric_from_primitive_numeral(const data_type_info* dti, const datum* uval, int* error_code)
{
	(*error_code) = NUMERIC_CONVERSION_SUCCESSFULL;

	mpd_t numeric;

	switch(dti->type)
	{
		case BIT_FIELD :
		{
			// use bit_field_value
			break;
		}
		case UINT :
		{
			// use uint_value
			break;
		}
		case INT :
		{
			// use int_value
			break;
		}
		case LARGE_UINT :
		{
			// use large_uint_value
			break;
		}
		case LAGRE_INT :
		{
			// use large_int_value
			break;
		}
		case FLOAT :
		{
			if(dti->size == sizeof(float))
			{
				// use float_value
			}
			else if(dti->size == sizeof(double))
			{
				// use double_value
			}
			else
			{
				(*error_code) = NUMERIC_CONVERSION_TYPE_FAILURE;
			}
			break;
		}
		default:
		{
			(*error_code) = NUMERIC_CONVERSION_TYPE_FAILURE;
			break;
		}
	}

	return numeric;
}

datum numeric_to_primitive_numeral(const data_type_info* dti, mpd_t numeric, int* error_code)
{
	(*error_code) = NUMERIC_CONVERSION_SUCCESSFULL;

	datum num = NULL;

	switch(dti->type)
	{
		case BIT_FIELD :
		{
			// use bit_field_value
			break;
		}
		case UINT :
		{
			// use uint_value
			break;
		}
		case INT :
		{
			// use int_value
			break;
		}
		case LARGE_UINT :
		{
			// use large_uint_value
			break;
		}
		case LAGRE_INT :
		{
			// use large_int_value
			break;
		}
		case FLOAT :
		{
			if(dti->size == sizeof(float))
			{
				// use float_value
			}
			else if(dti->size == sizeof(double))
			{
				// use double_value
			}
			else
			{
				(*error_code) = NUMERIC_CONVERSION_TYPE_FAILURE;
			}
			break;
		}
		default:
		{
			(*error_code) = NUMERIC_CONVERSION_TYPE_FAILURE;
			break;
		}
	}

	return num;
}