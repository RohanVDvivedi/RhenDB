#include<rhendb/query_plan_interface.h>

void setup_result_writer_operator(operator* o, executor* thread_pool, stream* s, operator_buffer* input, tuple_def* input_tuple_def);