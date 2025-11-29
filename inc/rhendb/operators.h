#ifndef OPERATORS_H
#define OPERATORS_H

#include<rhendb/query_plan_interface.h>

void setup_generator_operator(operator* o, executor* thread_pool, operator_buffer* output, void* (*generator)(void* generator_context), void* generator_context, tuple_def* generator_tuple_def);

void setup_identity_operator(operator* o, executor* thread_pool, operator_buffer* output, operator_buffer* input);

void setup_result_writer_operator(operator* o, executor* thread_pool, stream* s, operator_buffer* input, tuple_def* input_tuple_def);

#endif