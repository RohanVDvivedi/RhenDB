#ifndef FUNCTION_HASH_H
#define FUNCTION_HASH_H

#include<rhendb/transaction.h>

#include<tuplestore/datum.h>

/*
	For hashing any extended types you need atmost 1 buffer pool frame
	else the application crashes
*/

int do_these_types_on_being_equal_hash_to_same_value(const data_type_info* dti1, const data_type_info* dti2);

// it only reads data from the ex_engine for the extended atributes, provision atleast 1 buffer for each one of this function calls
// the ex_engine will/must be the min_tx_engine
// transaction_id is passed as NULL, to read extended types as a separate read-only transaction
uint64_t hash_datum_rhendb(const datum* uval, const data_type_info* dti, tuple_hasher* th, transaction* tx);

uint64_t hash_tuple_rhendb(const void* tup, const tuple_def* tpl_d, const positional_accessor* element_ids, tuple_hasher* th, uint32_t element_count, transaction* tx);

#endif