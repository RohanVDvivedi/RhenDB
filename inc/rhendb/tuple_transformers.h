#ifndef TUPLE_TRANSFORMERS_H
#define TUPLE_TRANSFORMERS_H

#include<rhendb/tuple_transformer_interface.h>

tuple_transformer* get_new_identity_tuple_transformer(const void* input_def);

tuple_transformer* get_new_clone_tuple_transformer(const void* input_def);

#endif