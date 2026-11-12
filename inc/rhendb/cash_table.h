#ifndef CASH_TABLE_H
#define CASH_TABLE_H

#include<rhendb/transaction.h>

#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<cutlery/hashmap.h>

typedef struct cash_table cash_table;
struct cash_table
{
	uint64_t memory_utilization;

	const tuple_def* value_def;

	// key elements to hash/compare on in the value_def
	const positional_accessor* key_element_ids;

	// number of elements in key_element_ids
	uint32_t key_element_count;

	hashmap value_set;

	// to hash/compare extended data in the keys of the value tuples
	transaction* tx;
};

typedef struct cash_table_entry cash_table_entry;
struct cash_table_entry
{
	cy_uint hash_value;

	unsigned int is_key:1;

	union
	{
		struct // if is_key == 1, used only for a find
		{
			void* key_tuple;

			tuple_def* key_tuple_def;

			positional_accessor* key_element_ids; // key_element_count taken from the cash_table
		};
		struct // used for inserts and iterated value
		{
			uint8_t state;

			void* value;

			bstnode embed_node;
		};
	};
};

cash_table* get_new_cash_table(uint64_t bucket_count, const tuple_def* value_def, const positional_accessor* key_element_ids, uint32_t key_element_count, transaction* tx);

cash_table_entry get_new_cash_table_value(const cash_table* ct_p, void* value);

// below function mallocs internally, only allows user to preserve and not recompute hash_value
// it will also do the calculations for maintaining the memory_utilization number, user must check this before/after insertions
// it internally does int insert_in_hashmap(&(ct_p->value_set), malloc+memcpy+initialize_bstnode(v));
int insert_in_cash_table(cash_table* ct_p, const cash_table_entry* v);

// predominantly for starting lookups with a tuple with identical key, but a different tuple_def like a hash join probes
cash_table_entry get_new_cash_table_key(const cash_table* ct_p, void* key_tuple, tuple_def* key_tuple_def, positional_accessor* key_element_ids);

#define find_equals_in_cash_table(ct_p, k)   ((cash_table_entry*)(find_equals_in_hashmap(&(ct_p->value_set), k)))
#define next_equals_in_cash_table(ct_p, v)   ((cash_table_entry*)(get_next_of_in_hashmap(&(ct_p->value_set), v, ANY_THAT_EQUALS)))

#define find_all_in_cash_table(ct_p)         ((cash_table_entry*)(get_first_of_in_hashmap(&(ct_p->value_set), FIRST_OF_HASHMAP)))
#define next_all_in_cash_table(ct_p, v)      ((cash_table_entry*)(get_next_of_in_hashmap(&(ct_p->value_set), v, ANY_IN_HASHMAP)))

void destroy_cash_table(cash_table* ct_p);

#endif