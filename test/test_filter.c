#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
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
	dstring filter_expr = get_dstring_pointing_to_literal_cstring("true");
	if(argc >= 2)
		filter_expr = get_dstring_pointing_to_cstring(argv[1]);
	sql* filter_expr_sql = NULL;
	{
		stream strm;
		initialize_dstring_stream(&strm, &filter_expr);
		int error = 0;
		filter_expr_sql = parse_sql(&strm, &error);
		if(error || filter_expr_sql->type != EXPR)
		{
			printf("ERROR PARSING SQL %s\n", (((!error) && filter_expr_sql->type == EXPR) ? "EXPR" : "not EXPR"));
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

	// make query

	query_plan_desc* qpd = get_new_query_plan_descriptor(&rdb, NULL);

	printf("Building pipeline :\n");
	{
		uint64_t op_index = 0;

		insert_stream_input_operator_descriptor(qpd, &rs, &record_def); op_index++;

		insert_selection_operator_descriptor(qpd, 0, filter_expr_sql->expr); op_index++;

		insert_consumer_operator_descriptor(qpd, 1, PRINT_DATA ? print_consumer : NULL, NULL); op_index++;
	}
	printf("\n\n");

	qp = create_query_plan(qpd, &tx, NULL);

	printf("Pipeline built\n\n");

	// make query completed

	start_all_operators_for_query_plan(qp);

	wait_for_shutdown_of_query_plan(qp);

	if(qp->query_errored)
	{
		dstring kill_reasons = new_dstring("", 0);
		destroy_query_plan(qp, &kill_reasons);

		printf("\n\nKILL REASONS : \n");
		printf_dstring(&kill_reasons);
		deinit_dstring(&kill_reasons);
		printf("\n\nKILL REASONS END\n\n");
	}
	else
	{
		printf("QUERY SUCCESSFULL\n");
		destroy_query_plan(qp, NULL);
	}

	destroy_query_plan_descriptor(qpd);

	delete_sql(filter_expr_sql);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}