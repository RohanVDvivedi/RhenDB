#include<rhendb/query_plan_interface.h>

#include<boompar/executor.h>

void setup_identity_operator(operator* o, executor* thread_pool, operator_buffer* output, operator_buffer* input);