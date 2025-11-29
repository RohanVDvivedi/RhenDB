#include<rhendb/query_plan_interface.h>

void setup_generator_operator(operator* o, executor* thread_pool, operator_buffer* output, void* (*generator)(void* generator_context), void* generator_context, tuple_def* generator_tuple_def);