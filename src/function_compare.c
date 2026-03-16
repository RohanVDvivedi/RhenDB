#include<rhendb/function_compare.h>

int can_compare_datum_rhendb(const data_type_info* dti1, const data_type_info* dti2);

int compare_datum_rhendb(const datum* uval1, const data_type_info* dti1, const datum* uval2, const data_type_info* dti2, rage_engine* ex_engine, int* abort_error);

int compare_datum2_rhendb(const datum* uval1, const datum* uval2, const data_type_info* dti, rage_engine* ex_engine, int* abort_error);