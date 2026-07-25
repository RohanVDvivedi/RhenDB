#ifndef MATERIALIZATION_UTIL_H
#define MATERIALIZATION_UTIL_H

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

// dti must be a text/blob type, inline or extended
// directly returns NULL, if the dti is not a text or blob type, if dti is NULL, we expect uval to be a native tuple store string datum
// uval input parameter for this function must be not a NULL_DATUM
char* materialize_tb(const datum uval, const data_type_info* dti, transaction* tx, uint32_t* length, uint32_t* capacity, int* error_code);

#define materialize_text materialize_tb
#define materialize_blob materialize_tb

// dti must be a numeric type, inline or extended
// directly returns NAN, if the dti is not a numeric type
// uval input parameter for this function must be not a NULL_DATUM
mpd_t materialize_numeric(const datum uval, const data_type_info* dti, transaction* tx, int* error_code);

#endif