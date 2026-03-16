#ifndef FUNCTION_COMPARE_H
#define FUNCTION_COMPARE_H

#include<rhendb/rage_engine.h>

#include<tuplestore/datum.h>

int can_compare_datum_rhendb(const data_type_info* dti1, const data_type_info* dti2);

// it only reads data from the ex_engine for the extended atributes, provision atleast 1 buffer for each one of this function calls
// the ex_engine will/must be the min_tx_engine

int compare_datum_rhendb(const datum* uval1, const data_type_info* dti1, const datum* uval2, const data_type_info* dti2, rage_engine* ex_engine, int* abort_error);

// this function is added to skip the checks that ensure that the datums can be compared, so that such checks can be discarded
int compare_datum2_rhendb(const datum* uval1, const datum* uval2, const data_type_info* dti, rage_engine* ex_engine, int* abort_error);

#endif