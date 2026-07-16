#ifndef FUNCTION_COMPARE_H
#define FUNCTION_COMPARE_H

#include<rhendb/transaction.h>

#include<tuplestore/datum.h>

/*
	For comparing any extended types you need atmost 2 buffer pool frames
	else the application crashes
*/

int can_compare_datum_rhendb(const data_type_info* dti1, const data_type_info* dti2);

// it only reads data from the ex_engine for the extended atributes, provision atleast 1 buffer for each one of this function calls
// the ex_engine will/must be the min_tx_engine

// transaction_id is passed as NULL, to read extended types as a separate read-only transaction

int compare_datum_rhendb(const datum* uval1, const data_type_info* dti1, const datum* uval2, const data_type_info* dti2, transaction* tx);

// this function is added to skip the checks that ensure that the datums can be compared, so that such checks can be discarded
int compare_datum2_rhendb(const datum* uval1, const datum* uval2, const data_type_info* dti, transaction* tx);

int compare_tuples_rhendb(const void* tup1, const tuple_def* tpl_d1, const positional_accessor* element_ids1, const void* tup2, const tuple_def* tpl_d2, const positional_accessor* element_ids2, const compare_direction* cmp_dir, uint32_t element_count, transaction* tx);

int compare_tuples2_rhendb(const void* tup1, const void* tup2, const tuple_def* tpl_d, const positional_accessor* element_ids, const compare_direction* cmp_dir, uint32_t element_count, transaction* tx);

int compare_datums3_rhendb(const datum* uvals1, const datum* uvals2, data_type_info const * const * dtis, const compare_direction* cmp_dir, uint32_t element_count, transaction* tx);

#endif