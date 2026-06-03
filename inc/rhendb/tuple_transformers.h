#ifndef TUPLE_TRANSFORMERS_H
#define TUPLE_TRANSFORMERS_H

#include<rhendb/tuple_transformer_interface.h>

tuple_transformer* get_new_identity_tuple_transformer(const tuple_def* input_def);

tuple_transformer* get_new_clone_tuple_transformer(const tuple_def* input_def);

tuple_transformer* get_new_is_readable_for_mvcc_snapshot_transformer(const mvcc_snapshot* mvccsnp_p, transaction_status_getter* tsg_p, int* were_hints_updated, const tuple_def* input_def);

tuple_transformer* get_new_simple_projection_transformer(const char* output_table_name, const tuple_def* input_def, uint32_t projections_count, positional_accessor** projections, const char** field_names);

tuple_transformer* get_new_simple_projection_transformer(const tuple_def* input_def, uint64_t next_row_number);

#endif