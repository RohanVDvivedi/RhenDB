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

	// keys for the tuple of value_def
	const positional_accessor* key_element_ids;
	uint32_t key_element_count;

	// stores the tuples here as_value
	hashmap value_set;

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

			const tuple_def* key_def;
			const positional_accessor* key_element_ids; // key_element_count is taken from the cash_table
		} as_key;
		struct // used for inserts and iterated value
		{
			uint8_t state;

			void* value;

			llnode embed_node;
		} as_value;
	};
};

cash_table* get_new_cash_table(uint64_t bucket_count, const tuple_def* value_def, const positional_accessor* key_element_ids, uint32_t key_element_count, transaction* tx);

// value tuple gets owned by cash_table_entry after this call
cash_table_entry* get_new_cash_table_value(const cash_table* ct_p, void* value);

int insert_in_cash_table(cash_table* ct_p, cash_table_entry* v);
int remove_from_cash_table(cash_table* ct_p, cash_table_entry* v);

// key_tuple is still not owned by this returned cash_table_entry
cash_table_entry get_new_cash_table_key(const cash_table* ct_p, void* key_tuple, const tuple_def* key_def, const positional_accessor* key_element_ids);

#define find_equals_in_cash_table(ct_p, k)   ((cash_table_entry*)(find_equals_in_hashmap(&(ct_p->value_set), k)))
#define next_equals_in_cash_table(ct_p, v)   ((cash_table_entry*)(get_next_of_in_hashmap(&(ct_p->value_set), v, ANY_THAT_EQUALS)))

#define find_all_in_cash_table(ct_p)         ((cash_table_entry*)(get_first_of_in_hashmap(&(ct_p->value_set), FIRST_OF_HASHMAP)))
#define next_all_in_cash_table(ct_p, v)      ((cash_table_entry*)(get_next_of_in_hashmap(&(ct_p->value_set), v, ANY_IN_HASHMAP)))

void destroy_cash_table(cash_table* ct_p);

#endif