#ifndef UTIL_NUMERIC_CONVERSIONS_H
#define UTIL_NUMERIC_CONVERSIONS_H

#include<mpdecimal.h>

#include<tuplestore/data_type_info.h>
#include<tuplestore/datum.h>

#define NUMERIC_CONVERSION_SUCCESSFULL       0
#define NUMERIC_CONVERSION_TYPE_FAILURE      1
#define NUMERIC_CONVERSION_UN_REPRESENTABLE  2

// on returning error_code, no resource needs freeing

mpd_t numeric_from_primitive_numeral(const data_type_info* dti, const datum* uval, int* error_code);

datum numeric_to_primitive_numeral(const data_type_info* dti, const mpd_t* numeric, int* error_code);

#endif