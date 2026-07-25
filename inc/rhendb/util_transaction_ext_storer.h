#ifndef UTIL_TRANSACTION_EXT_STORER_H
#define UTIL_TRANSACTION_EXT_STORER_H

#include<rhendb/transaction.h>

#include<tuplelargetypes/text_extended.h>
#include<tuplelargetypes/blob_extended.h>
#include<tuplelargetypes/numeric_extended.h>

#include<mpdecimal.h>

/*
	this utility sotres the text/blob into transaction's extension_store
*/

// ext_type_info is always expected to be volatile_rage_engine->blob/text_extended_type_info
// data must not be NULL, and is left owned by the caller
// returned pointer if not NULL must be freed
void* tx_temp_store_tb(char* data, uint32_t data_size, data_type_info* ext_type_info, transaction* tx);

#define tx_temp_store_text tx_temp_store_tb
#define tx_temp_store_blob tx_temp_store_tb

// ext_type_info is always expected to be volatile_rage_engine->numeric_extended_type_info
// number must not be NULL, and is left owned by the caller
// returned pointer if not NULL must be freed
void* tx_temp_store_numeric(mpd_t* number, data_type_info* ext_type_info, transaction* tx);

#endif