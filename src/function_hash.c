#include<rhendb/function_hash.h>

uint64_t hash_datum_rhendb(const datum* uval, const data_type_info* dti, tuple_hasher* th, rage_engine* ex_engine, int* abort_error);