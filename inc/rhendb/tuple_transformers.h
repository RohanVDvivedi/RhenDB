#include<rhendb/tuple_transformers.h>

tuple_transformer* get_new_identity_tuple_transformer(const void* input_def);

tuple_transformer* get_new_clone_tuple_transformer(const void* input_def);