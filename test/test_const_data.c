#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
#include<rhendb/query_plan.h>

#include<sqltoast/sqltoast.h>
#include<sqltoast/sql_expression_eval.h>

#include<test_dataset_tuple_def.h>

#include<cutlery/stream_for_dstring.h>

#include<stdlib.h>
#include<unistd.h>
#include<signal.h>

#define USERS_COUNT 10

#define PRINT_DATA

int main(int argc, char** argv)
{
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

	interim_tuple_store* its_p = get_new_interim_tuple_store(4096);
	for(int i = 40; i < 151; i++)
	{
		char buffer[300];
		construct_record(buffer, i, 0, (i % 3 == 0) ? NULL : "Rohan Dvivedi");

		append_tuple_to_interim_tuple_store(its_p, buffer, &(record_def.size_def));
	}

	sql* filter_expr_sql = NULL;
	if(argc >= 2)
	{
		dstring filter_expr = get_dstring_pointing_to_cstring(argv[1]);
		stream strm;
		initialize_dstring_stream(&strm, &filter_expr);
		int error = 0;
		filter_expr_sql = parse_sql(&strm, &error);
		if(error || filter_expr_sql->type != EXPR)
		{
			printf("ERROR PARSING SQL %s\n", (((!error) && filter_expr_sql->type == EXPR) ? "EXPR" : "not EXPR"));
			exit(-1);
		}
		if(has_sub_query_in_sql_exp(filter_expr_sql->expr))
		{
			printf("ERROR PARSING SQL EXPR HAS SUB QUERY\n");
			exit(-1);
		}
	}

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	{
		operator_desc* cdata = insert_constant_dataset_operator_descriptor(qpd, its_p, &record_def);

		operator_desc* filtered = cdata;
		if(filter_expr_sql != NULL)
			filtered = insert_selection_operator_descriptor(qpd, cdata, filter_expr_sql->expr);
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

	if(filter_expr_sql != NULL)
		delete_sql(filter_expr_sql);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}