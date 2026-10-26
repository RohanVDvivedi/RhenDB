#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
#include<rhendb/tuple_transformers.h>

#include<sqltoast/sqltoast.h>
#include<sqltoast/sql_expression_eval.h>

#include<test_dataset_tuple_def.h>

#include<cutlery/stream_for_dstring.h>

#include<stdlib.h>
#include<unistd.h>
#include<signal.h>

#define USERS_COUNT 10

#define TOTAL_OPERATORS_COUNT 10

query_plan* qp = NULL;

void intHandler(int dummy)
{
	printf("\nCaught Ctrl+C! shuting down query plan\n");
	shutdown_query_plan(qp, get_dstring_pointing_to_literal_cstring("CTRL+C pressed!!"));
}

int main(int argc, char** argv)
{
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

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	printf("Building pipeline :\n");

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

	const char* field_names[] = {
		"row_id",
		"row_no",
		"data",
	};

	positional_accessor projections[] = {
		STATIC_POSITION(1,0),
		STATIC_POSITION(0),
		STATIC_POSITION(1,1),
	};

	{
		uint64_t op_index = 0;

		insert_constant_dataset_operator_descriptor(qpd, its_p, &record_def); op_index++;
		if(filter_expr_sql != NULL)
			insert_expressioned_selection_tuple_transformer_descriptor(qpd, filter_expr_sql->expr);
		insert_row_identifier_prepender_tuple_transformer_descriptor(qpd, 7);
		insert_row_number_prepender_tuple_transformer_descriptor(qpd, 1);
		insert_simple_projection_tuple_transformer_descriptor(qpd, "output", sizeof(field_names)/sizeof(field_names[0]), projections, field_names);

		insert_consumer_operator_descriptor(qpd, 0, print_consumer, NULL); op_index++;
	}
	printf("\n\n");

	qp = create_query_plan(qpd, NULL);

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

	if(filter_expr_sql != NULL)
		delete_sql(filter_expr_sql);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}