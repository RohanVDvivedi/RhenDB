#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>
#include<rhendb/tuple_transformers.h>

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

#define PRINT_DATA 1

query_plan* qp = NULL;

void intHandler(int dummy)
{
	printf("\nCaught Ctrl+C! shuting down query plan\n");
	shutdown_query_plan(qp, get_dstring_pointing_to_literal_cstring("CTRL+C pressed!!"));
}

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

	signal(SIGINT, intHandler);

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

	transaction tx = initialize_transaction(&rdb);

	qp = get_new_query_plan(&tx, 30);

	// make operators

	printf("Building pipeline :\n");
	{
		operator* input_operator = get_new_registered_operator_for_query_plan(qp);
		setup_stream_input_operator(input_operator, &rs, &record_def);
		printf("source operator %p\n", input_operator);

		uint32_t count1 = 11;
		uint32_t count2 = argc - 1;
		projection_description* pd = alloca(sizeof(projection_description) * (count1 + count2));
		uint32_t p = 0;
		pd[p++] = project_from(SELF);
		pd[p++] = project_from(STATIC_POSITION(0));
		pd[p++] = project_from(STATIC_POSITION(1));
		pd[p++] = project_from(STATIC_POSITION(2));
		pd[p++] = project_from(STATIC_POSITION(3));
		pd[p++] = project_from(STATIC_POSITION(4));
		pd[p++] = project_from(STATIC_POSITION(5));
		pd[p++] = project_from(STATIC_POSITION(6));
		pd[p++] = project_from(STATIC_POSITION(2,0));
		pd[p++] = project_from(STATIC_POSITION(2,1));
		pd[p++] = project_from(STATIC_POSITION(2,2));
		for(; p < count1 + count2; p++)
			pd[p] = project_using(proj_expr_sql[p-count1]->expr);

		operator* projection_operator = get_new_registered_operator_for_query_plan(qp);
		setup_projection_operator(projection_operator, input_operator, pd, (count1 + count2));
		printf("projection operator %p\n", projection_operator);

		operator* print_operator = get_new_registered_operator_for_query_plan(qp);
		setup_consumer_operator(print_operator, projection_operator, PRINT_DATA ? print_consumer : NULL, NULL);
		printf("output print operator %p\n", print_operator);
	}
	printf("\n\n");

	// make operators completed

	start_all_operators_for_query_plan(qp);

	wait_for_shutdown_of_query_plan(qp);

	dstring kill_reasons = new_dstring("", 0);
	destroy_query_plan(qp, &kill_reasons);

	printf("\n\nKILL REASONS : \n");
	printf_dstring(&kill_reasons);
	deinit_dstring(&kill_reasons);
	printf("\n\nKILL REASONS END\n\n");

	for(int i = 0; i < (argc - 1); i++)
		delete_sql(proj_expr_sql[i]);
	free(proj_expr_sql);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}