#include<rhendb/rash_table.h>

#include<rhendb/function_compare.h>
#include<rhendb/function_hash.h>

static rage_engine* volatile_engine;

static hash_table_tuple_defs httd;

void initialize_rash_table_engine(rage_engine* volatile_engine);

rash_table_handle get_new_rash_table1(const data_type_info** key_dtis, uint32_t key_element_count, rage_engine* volatile_engine, rage_engine* ex_engine);

rash_table_handle get_new_rash_table2(const tuple_def* record_def, const positional_accessor* key_element_ids, uint32_t key_element_count, rage_engine* volatile_engine, rage_engine* ex_engine);

void destroy_rash_table(rash_table_handle* rth_p);

void initialize_rash_table_key(rash_table_key* rkey_p, const void* record, const tuple_def* record_def, const positional_accessor* key_element_ids, uint32_t key_element_count);

uint64_t get_hash_value_for_rash_table_key(rash_table_key* rkey_p);

rash_table_iterator* find_all_in_rash_table(rash_table_handle* rth_p);

rash_table_iterator find_equals_in_rash_table(rash_table_handle* rth_p, const rash_table_key* rkey_p);

int exists_in_rash_table_iterator(const rash_table_iterator* rti_p);

int remove_from_rash_table_iterator(rash_table_iterator* rti_p);

binary_read_iterator* read_value_in_rash_table_iterator(rash_table_iterator* rti_p);

binary_write_iterator* open_for_writing_value_in_rash_table_iterator(rash_table_iterator* rti_p);

void close_and_write_value_in_hash_table_iterator(rash_table_iterator* rti_p, binary_write_iterator* bwi_p);

void next_in_rash_table_iterator(rash_table_iterator* rti_p);

void delete_rash_table_iterator(rash_table_iterator* rti_p);