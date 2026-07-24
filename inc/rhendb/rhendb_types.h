#ifndef RHENDB_TYPES_H
#define RHENDB_TYPES_H

#include<rhendb/rhendb.h>
#include<rhendb/transaction.h>

#include<rhendb/max_intermediate_tuple_size.h>

#include<tuplestore/data_type_info.h>

#define MAX_EXTENDED_TYPE_SIZE  128
#define MAX_PREFIX_SIZE          90 // do not load more than this many in the prefix of the extended types

typedef enum rhendb_type rhendb_type;
enum rhendb_type
{
	// primitive numbers of fixed length
	RHENDB_BIT_FIELD = 0,    // size 1-64 in bits
	RHENDB_UINT = 1,         // size 1-32 in bytes
	RHENDB_INT = 2,          // size 1-32 in bytes
	RHENDB_FLOAT = 3,        // either sizeof(float) or sizeof(double)

	// default composite types
	RHENDB_TUPLE_POINTER = 4,
	RHENDB_MVCC_HEADER = 5,

	// total size of below attributes must be limited to MAX_INTERMEDIATE_TUPLE_SIZE, else we can not hold it in memory
	RHENDB_TEXT = 6,
	RHENDB_BLOB = 7,
	RHENDB_NUMERIC = 8,
	RHENDB_JSONB = 9,

	// composite inlined types, array or tuple too big itself will not spill to table's blob_store, but it's nested TEXT column will
	RHENDB_ARRAY = 10, // fully inlined type of (variable or fixed count) array of elements
	RHENDB_TUPLE = 11, // fully inlined type to hold composite elements
};

typedef struct rhendb_type_info rhendb_type_info;

typedef struct rhendb_attribute rhendb_attribute;
struct rhendb_attribute
{
	char attribute_name[64];
	rhendb_type_info* containee;
};

struct rhendb_type_info
{
	rhendb_type type;

	unsigned int is_nullable:1;

	uint32_t size; // number of bits for BIT_FIELD, or size in bytes for anyother numeric types

	char type_name[64]; // only used for TUPLE

	uint32_t element_count; // only for TUPLE (must be positive) or ARRAY (0 means variable length)

	union
	{
		rhendb_attribute* containees; // array of rhendb_attribute for TUPLE
		rhendb_type_info* containee; // single rhendb_type_info for ARRAY
	};
};

data_type_info* get_data_type_info_for_rhendb_type_info(const rhendb_type_info* rti_p, const rhendb* rdb);

#endif