#ifndef INTERIM_TUPLE_STORE_SORT_H
#define INTERIM_TUPLE_STORE_SORT_H

#include<rhendb/interim_tuple_store.h>
#include<rhendb/rage_engine.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

interim_tuple_store* sort_interim_tuples(interim_tuple_store* its_p, uint32_t min_bytes_to_mmap, const tuple_def* tpl_d, const positional_accessor* element_ids, const compare_direction* cmp_dir, uint32_t element_count, rage_engine* ex_engine, const void* transaction_id, int* abort_error);

#endif