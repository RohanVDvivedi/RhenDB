#ifndef UTIL_NUMERIC_CONVERSIONS_H
#define UTIL_NUMERIC_CONVERSIONS_H

#include<mpdecimal.h>

#define NUMERIC_CONVERSION_SUCCESSFULL       0
#define NUMERIC_CONVERSION_TYPE_FAILURE      1
#define NUMERIC_CONVERSION_UN_REPRESENTABLE  2

mpd_t numeric_from_primitive_numeral(const data_type_info* dti, const datum* uval, int* error_code);

datum numeric_to_primitive_numeral(const data_type_info* dti, mpd_t numeric, int* error_code);

#endif