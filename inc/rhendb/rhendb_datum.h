#ifndef RHENDB_DATUM_H
#define RHENDB_DATUM_H

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>
#include<tuplestore/datum.h>

#include<rhendb/function_hash.h>
#include<rhendb/function_compare.h>

typedef enum rhendb_datum_type rhendb_datum_type;
enum rhendb_datum_type
{
	RAW_DATUM,
	REF_DATUM,
	ARR_DATUM,
};

// this is the struct that will be used by the operators for returning the result opf next_tuple()
// the memory allocation for this will be done by the returning operator
typedef struct rhendb_datum rhendb_datum;
struct rhendb_datum
{
	rhendb_datum_type type;

	union
	{
		struct
		{
			datum value;
		} as_raw_datum;

		struct
		{
			const data_type_info* src_dti;
			positional_accessor* src_pos;
			rhendb_datum* src_datum;
		} as_ref_datum;

		rhendb_datum** as_arr_datum; // only allowed for a TUPLE not an ARRAY, or BINARY or STRING types
		// pointer to the array of pointers to rhendb_datum
	};
};

int get_nested_containee_from_rhendb_datum(rhendb_datum* uval_c, const data_type_info** dti_c, const rhendb_datum* uval, const data_type_info* dti, positional_accessor pa);

uint64_t hash_rhendb_datum(const rhendb_datum* uval, const data_type_info* dti, tuple_hasher* th, transaction* tx);

int compare_rhendb_datum(const rhendb_datum* uval1, const data_type_info* dti1, const rhendb_datum* uval2, const data_type_info* dti2, transaction* tx);

// must be called only on the root rhendb_datum
void build_tuple_from_rhendb_datum(void* dest_tuple, const rhendb_datum* uval, const tuple_def* def);
void* make_tuple_from_rhendb_datum(const rhendb_datum* uval, const tuple_def* def);

#endif