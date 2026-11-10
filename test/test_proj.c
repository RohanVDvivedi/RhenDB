#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
#include<rhendb/query_plan.h>

#include<sqltoast/sqltoast.h>
#include<sqltoast/sql_expression_eval.h>

#include<test_dataset_tuple_def.h>

#include<cutlery/stream_for_file_descriptor.h>
#include<cutlery/stream_for_dstring.h>

#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<signal.h>
#include<fcntl.h>

#define USERS_COUNT 10

#define PRINT_DATA

int main(int argc, char** argv)
{
	sql** proj_expr_sql = malloc(sizeof(sql*) * (argc - 1));
	for(int i = 0; i < (argc - 1); i++)
	{
		dstring proj_expr = get_dstring_pointing_to_cstring(argv[i+1]);

		stream strm;
		initialize_dstring_stream(&strm, &proj_expr);
		int error = 0;
		proj_expr_sql[i] = parse_sql(&strm, &error);
		if(error || proj_expr_sql[i]->type != EXPR)
		{
			printf("ERROR PARSING SQL %s\n", (((!error) && proj_expr_sql[i]->type == EXPR) ? "EXPR" : "not EXPR"));
			exit(-1);
		}
	}

	stream rs, ws;
	initialize_stream_for_fd(&rs, 0);
	initialize_stream_for_fd(&ws, 1);

	rhendb rdb;
	initialize_rhendb(&rdb, "./test.db",
		5,
		512, 8, 80, 80,
			10000ULL, 100000ULL,
			10000000ULL,
		4096,
			10000000ULL,
		USERS_COUNT);
	printf("database initialized\n\n");

	initialize_tuple_defs();

	transaction tx;
	initialize_transaction(&tx, &rdb);

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	{
		operator_desc* strm_in = insert_stream_input_operator_descriptor(qpd, &rs, &record_def);

		uint32_t count1 = 11;
		uint32_t count2 = argc - 1;
		projection_description* pd = alloca(sizeof(projection_description) * (count1 + count2));
		uint32_t p = 0;
		pd[p++] = project_from(NULL, SELF);
		pd[p++] = project_from(NULL, STATIC_POSITION(0));
		pd[p++] = project_from(NULL, STATIC_POSITION(1));
		pd[p++] = project_from(NULL, STATIC_POSITION(2));
		pd[p++] = project_from(NULL, STATIC_POSITION(3));
		pd[p++] = project_from(NULL, STATIC_POSITION(4));
		pd[p++] = project_from(NULL, STATIC_POSITION(5));
		pd[p++] = project_from(NULL, STATIC_POSITION(6));
		pd[p++] = project_from(NULL, STATIC_POSITION(2,0));
		pd[p++] = project_from(NULL, STATIC_POSITION(2,1));
		pd[p++] = project_from(NULL, STATIC_POSITION(2,2));
		for(; p < count1 + count2; p++)
			pd[p] = project_using(NULL, proj_expr_sql[p-count1]->expr);

		operator_desc* proj = insert_projection_operator_descriptor(qpd, strm_in, "record", pd, (count1 + count2));
	}

	query_plan* qp = get_new_query_plan(qpd);

	while(1)
	{
		int error_code = 0;
		void* tuple = next_tuple_from_query_plan(qp, &error_code);
		if(tuple == NULL)
			break;
		#ifdef PRINT_DATA
			print_tuple(tuple, qp->desc->output_tuple_def);
		#endif
		free(tuple);
	}

	destroy_query_plan(qp);

	destroy_query_plan_descriptor(qpd);

	for(int i = 0; i < (argc - 1); i++)
		delete_sql(proj_expr_sql[i]);
	free(proj_expr_sql);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}