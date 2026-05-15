#ifndef INTERIM_TUPLE_STORE_SORT_H
#define INTERIM_TUPLE_STORE_SORT_H

#include<rhendb/interim_tuple_store.h>
#include<rhendb/rage_engine.h>
#include<rhendb/tuples_down_counter.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

// returns NULL on abort_error
interim_tuple_store* sort_interim_tuples(interim_tuple_store* its_p, tuples_down_counter result_counter, const tuple_def* tpl_d, const positional_accessor* element_ids, const compare_direction* cmp_dir, uint32_t element_count, rage_engine* ex_engine);

#endif