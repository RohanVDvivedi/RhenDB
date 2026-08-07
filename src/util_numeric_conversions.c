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
		}
		case UINT :
		{
			// use uint_value
		}
		case INT :
		{
			// use int_value
		}
		case LARGE_UINT :
		{
			// use large_uint_value
		}
		case LAGRE_INT :
		{
			// use large_int_value
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
		}
		case UINT :
		{
			// use uint_value
		}
		case INT :
		{
			// use int_value
		}
		case LARGE_UINT :
		{
			// use large_uint_value
		}
		case LAGRE_INT :
		{
			// use large_int_value
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
		}
	}

	return num;
}