#ifndef FUNCTION_HASH_H
#define FUNCTION_HASH_H

#include<rhendb/rage_engine.h>

#include<tuplestore/datum.h>

// it only reads data from the ex_engine for the extended atributes, provision atleast 1 buffer for each one of this function calls
// the ex_engine will/must be the min_tx_engine
uint64_t hash_datum_rhendb(const datum* uval, const data_type_info* dti, tuple_hasher* th, rage_engine* ex_engine, int* abort_error);

#endif