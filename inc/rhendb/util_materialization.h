#ifndef UTIL_MATERIALIZATION_H
#define UTIL_MATERIALIZATION_H

#include<rhendb/transaction.h>

#include<tuplelargetypes/text_extended.h>
#include<tuplelargetypes/blob_extended.h>
#include<tuplelargetypes/numeric_extended.h>

#include<mpdecimal.h>

/*
	this utility materializes the text/blob into char* and numeric into mpd_t
	if you are materializing something from the persistent_acid_storage_engine make sure you have atleast 1 buffer from the bufferpool available
*/

#define MATERIALIZED_SUCCESSFULLY      0
#define MATERIALIZED_RESULT_TOO_BIG    1
#define MATERIALIZATION_TYPE_INVALID   2
#define MATERIALIZING_NULL_DATUM       3

/* if error_code is returned nothing needs to be freed/released all resources acquired in the functions are already taken cared of */

// dti must be a text/blob type, inline or extended
// directly returns NULL, if the dti is not a text or blob type, if dti is NULL, we expect uval to be a native tuple store string datum
// uval input parameter for this function must be not a NULL_DATUM
// if capacity is returned to be 0, nothing needs to be freed, and the returned pointer is owned by the datum or the tuple passed
char* materialize_tb(datum uval, const data_type_info* dti, transaction* tx, uint32_t* length, uint32_t* capacity, int* error_code);

#define materialize_text materialize_tb
#define materialize_blob materialize_tb

// dti must be a numeric type, inline or extended
// directly returns NAN, if the dti is not a numeric type
// uval input parameter for this function must be not a NULL_DATUM
materialized_numeric materialize_numeric1(datum uval, const data_type_info* dti, transaction* tx, int* error_code);
mpd_t materialize_numeric(datum uval, const data_type_info* dti, transaction* tx, int* error_code);

#endif