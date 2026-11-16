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

	// derived key_dtis, using the value_def, key_element_ids and key_element_count
	const data_type_info** key_dtis;

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
			datum* keys;

			const data_type_info** key_dtis;
		} as_key;
		struct // used for inserts and iterated value
		{
			// 2 state values
			uint64_t state_u; // possibly to store bit_fields for right/full-outer join
			void* state_p; // possibly to store pointer array to states for hash aggregation

			int is_value_owned;
			void* value;

			datum* keys;

			llnode embed_node;
		} as_value;
	};
};

/*
	optional_pre_computed_hash_value may be passed as 0, if you have not precomputed it
*/

cash_table* get_new_cash_table(uint64_t bucket_count, const tuple_def* value_def, const positional_accessor* key_element_ids, uint32_t key_element_count, transaction* tx);

// value tuple gets owned by cash_table_entry after this call
cash_table_entry* get_new_cash_table_value_own(const cash_table* ct_p, void* value, uint64_t optional_pre_computed_hash_value); // value allocated by user but now the cash table owns it
cash_table_entry* get_new_cash_table_value_clone(const cash_table* ct_p, void* value, uint64_t optional_pre_computed_hash_value); // value allocated by user and a new clone has to be done for the cash_table to own it's new copy
cash_table_entry* get_new_cash_table_value_borrow(const cash_table* ct_p, void* value, uint64_t optional_pre_computed_hash_value); // value allocated by user and will be freed by the user

// borrow can be used for tuples in some already existing mmaped interim_tuple_store's region

int insert_in_cash_table(cash_table* ct_p, cash_table_entry* v);
int remove_from_cash_table(cash_table* ct_p, cash_table_entry* v);

// key_tuple is still not owned by this returned cash_table_entry
cash_table_entry get_new_cash_table_key(const cash_table* ct_p, void* key_tuple, const tuple_def* key_def, const positional_accessor* key_element_ids, uint64_t optional_pre_computed_hash_value);

// keeps the allocations intact and uses the new tuple
// key_tuple is still not owned by this returned cash_table_entry
void reset_cash_table_key(cash_table_entry* k, const cash_table* ct_p, void* key_tuple, const tuple_def* key_def, const positional_accessor* key_element_ids, uint64_t optional_pre_computed_hash_value);

#define find_equals_in_cash_table(ct_p, k)   ((cash_table_entry*)(find_equals_in_hashmap(&(ct_p->value_set), k)))
#define next_equals_in_cash_table(ct_p, v)   ((cash_table_entry*)(get_next_of_in_hashmap(&(ct_p->value_set), v, ANY_THAT_EQUALS)))

#define find_all_in_cash_table(ct_p)         ((cash_table_entry*)(get_first_of_in_hashmap(&(ct_p->value_set), FIRST_OF_HASHMAP)))
#define next_all_in_cash_table(ct_p, v)      ((cash_table_entry*)(get_next_of_in_hashmap(&(ct_p->value_set), v, ANY_IN_HASHMAP)))

void delete_cash_table_entry(cash_table_entry* e);

void delete_cash_table(cash_table* ct_p);

#endif