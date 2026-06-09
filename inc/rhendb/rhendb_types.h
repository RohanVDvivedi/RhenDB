#ifndef RHENDB_TYPES_H
#define RHENDB_TYPES_H

#include<rhendb/rhendb.h>

#include<tuplestore/data_type_info.h>

#define MAX_INLINE_SIZE 230

#define SMALL_TYPE_SIZE 32000
#define LARGE_TYPE_SIZE 32000000

typedef enum rhendb_type rhendb_type;
enum rhendb_type
{
	// primitive numbers of fixed length
	RHENDB_BIT_FIELD, // size 1-64 in bits
	RHENDB_UINT, // size 1-32 in bytes
	RHENDB_INT, // size 1-32 in bytes
	RHENDB_FLOAT, // either sizeof(float) or sizeof(double)

	// default composite types
	RHENDB_TUPLE_POINTER,
	RHENDB_MVCC_HEADER,

	// composite inlined types
	RHENDB_ARRAY, // fully inlined type of variable or fixed count of elements
	RHENDB_TUPLE, // fully inlined type to hold composite elements

	// extended types with total bytes lesser than 32 KB
	RHENDB_STRING,
	RHENDB_BINARY,
	RHENDB_NUMERIC, // 76000 decimal digits, too big for any normal use case

	// extended types with total bytes lesser than 32 MB
	RHENDB_TEXT,
	RHENDB_BLOB,
	RHENDB_JSONB,
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

	int is_nullable;

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

typedef enum rhendb_alter_type rhendb_alter_type;
enum rhendb_alter_type
{
	ADD_COLUMN,
	DROP_COLUMN,
};

typedef struct rhendb_alter_operation rhendb_alter_operation;
struct rhendb_alter_operation
{
	rhendb_alter_type type;

	uint32_t real_position;
	uint64_t relative_position;

	char column_name[64];
	rhendb_type_info* column_type;
	char* computed_column_expression;
};

#endif