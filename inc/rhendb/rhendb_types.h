#ifndef RHENDB_TYPES_H
#define RHENDB_TYPES_H

#include<rhendb/rhendb.h>
#include<rhendb/transaction.h>

#include<rhendb/max_intermediate_tuple_size.h>

#include<tuplestore/data_type_info.h>
/*
typedef struct rhendb_type_info rhendb_type_info;

typedef struct rhendb_attribute_info rhendb_attribute_info;
struct rhendb_attribute_info
{
	char attribute_name[64];
	rhendb_type_info* containee;
};

struct rhendb_type_info
{
	rhendb_base_type base_type;

	unsigned int is_nullable:1;

	uint32_t size; // number of bits for BIT_FIELD, or size in bytes for anyother numeric types

	char type_name[64]; // only used for TUPLE

	uint32_t element_count; // only for TUPLE (must be positive) or ARRAY (0 means variable length)

	union
	{
		rhendb_attribute_info* containees; // array of rhendb_attribute_info for TUPLE
		rhendb_type_info* containee; // single rhendb_type_info for ARRAY
	};
};*/

//data_type_info* get_data_type_info_for_rhendb_type_info(const rhendb_type_info* rti_p, const rhendb* rdb);

#endif