#ifndef RASH_TABLE_H
#define RASH_TABLE_H

#include<rhendb/rhendb.h>

#include<tupleindexer/hash_table/hash_table.h>

#include<tuplelargetypes/tuple_list_extended.h>
#include<tuplelargetypes/blob_extended.h>

/*
	rash_table is an in-memory hash table built with the sole purpose of execution of queries quickly

	it in reality stores the following, tuple

	hash_value(uint64_t actual_key), key(tuple_list, with prefix of 200 bytes for_compare_only), value(blob with prefix of 140 bytes)

	it will alays be ensured that the records ill alyas store unique values for (hash_value, key_size, key)
*/

#define PREFIX_BYTES_FOR_KEY           200
#define PREFIX_BYTES_FOR_VALUE         140

#define EXTENDED_TYPE_MAX_SIZE_FOR_KEY    ((4 + (4 + 8)) + (4 + PREFIX_BYTES_FOR_KEY))
#define EXTENDED_TYPE_MAX_SIZE_FOR_VALUE  ((4 + (4 + 8)) + (4 + PREFIX_BYTES_FOR_VALUE))

#define RASH_RECORD_MAX_SIZE              ((4 + (8 + 4 + 4)) + EXTENDED_TYPE_MAX_SIZE_FOR_KEY + EXTENDED_TYPE_MAX_SIZE_FOR_VALUE)

fail_build_on(RASH_RECORD_MAX_SIZE > 1024);

#define MIN_LOAD_FACTOR_IN_BYTES   7 // shrink if load-factor crosses
#define MAX_LOAD_FACTOR_IN_BYTES  20 // expand if load-factor crosses

// must be called right after rhendb is doen initializing it's volatile_engine
void initialize_hash_table_tuple_defs_for_using_rash_table(rhendb* rdb);

typedef struct rash_table_handle rash_table_handle;
struct rash_table_handle
{
	uint64_t root_page_id;

	uint64_t element_count; // total number of elements
	uint64_t bucket_count; // total number of buckets

	uint64_t total_inline_size; // total number of inline bytes used

	/*
		expand if (total_inline_size / (bucket_count * PAGE_SIZE)) > MAX_LOAD_FACTOR_IN_BYTES
		shrink if (total_inline_size / (bucket_count * PAGE_SIZE)) < MIN_LOAD_FACTOR_IN_BYTES
	*/

	tuple_def* key_tuple_defs; // array of tuple_defs, built with the handle
	uint32_t key_element_count;

	// to read contents of extended types, to hash and compare them
	rage_engine* ex_engine;

	// this is where we use volatile_engine and httd for storing and maintaining rash table
	rhendb* rdb;
};

rash_table_handle get_new_rash_table(const tuple_def* key_def, const positional_accessor* key_element_ids, uint32_t key_element_count, rage_engine* ex_engine, rhendb* rdb);

void destroy_rash_table(rash_table_handle* rth_p);

typedef struct rash_table_key rash_table_key;
struct rash_table_key
{
	const void* record;
	const tuple_def* record_def;
	const positional_accessor* key_element_ids;
	uint32_t key_element_count;

	// for hashing and comparing extended types in this record
	rage_engine* ex_engine;

	// to be used a key in the actual hash_table underneath rash_table
	char hash_value[8];
};

// returns true, if the rash_key initialization will succeed
int can_initialize_rash_table_key(const rash_table_handle* rth_p, const tuple_def* record_def, const positional_accessor* key_element_ids, uint32_t key_element_count);

void initialize_rash_table_key(rash_table_key* rkey_p, const void* record, const tuple_def* record_def, const positional_accessor* key_element_ids, uint32_t key_element_count, rage_engine* ex_engine);

uint64_t get_hash_value_for_rash_table_key(rash_table_key* rkey_p);

typedef struct rash_table_iterator rash_table_iterator;
struct rash_table_iterator
{
	rash_table_handle* rth_p;

	hash_table_iterator* hti_p;

	int is_read_only;

	// this rkey_p may be NULL, if you are iterating over all the keys in the rash_table
	const rash_table_key* rkey_p;
};

rash_table_iterator find_all_in_rash_table(rash_table_handle* rth_p, int is_read_only);

rash_table_iterator find_equals_in_rash_table(rash_table_handle* rth_p, const rash_table_key* rkey_p, int is_read_only);

binary_read_iterator* read_key_in_rash_table_iterator(const rash_table_iterator* rti_p);

int exists_in_rash_table_iterator(const rash_table_iterator* rti_p);

int remove_from_rash_table_iterator(rash_table_iterator* rti_p);

binary_read_iterator* read_value_in_rash_table_iterator(rash_table_iterator* rti_p);

binary_write_iterator* open_for_writing_value_in_rash_table_iterator(rash_table_iterator* rti_p);
void close_and_write_value_in_hash_table_iterator(rash_table_iterator* rti_p, binary_write_iterator* bwi_p);

int next_in_rash_table_iterator(rash_table_iterator* rti_p);

void delete_rash_table_iterator(rash_table_iterator* rti_p);

#endif