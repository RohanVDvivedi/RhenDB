#include<rhendb/query_plan_interface.h>

void print_job(operator* o, void* param)
{
	printf("printing from %p : %s\n", o, (char*)param);
}