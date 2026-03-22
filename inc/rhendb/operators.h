#ifndef OPERATORS_H
#define OPERATORS_H

#include<rhendb/query_plan_interface.h>

#include<boompar/executor.h>
#include<cutlery/stream.h>

void setup_generator_operator(operator* o, void* (*generator)(void* generator_context, tuple_def* generator_tuple_def), void* generator_context, tuple_def* generator_tuple_def);

void setup_identity_operator(operator* o, operator* input_operator, uint64_t consume_only_after_bytes_count);

void setup_printf_operator(operator* o, operator* input_operator, tuple_def* input_tuple_def);

#endif