#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
#include<rhendb/tuple_transformers.h>

#include<sqltoast/sqltoast.h>
#include<sqltoast/sql_expression_eval.h>

#include<rhendb/function_compare.h>

#include<test_dataset_tuple_def.h>
#include<test_dataset_2_tuple_def.h>

#include<cutlery/stream_for_file_descriptor.h>
#include<cutlery/stream_for_dstring.h>

#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<signal.h>
#include<fcntl.h>

#define USERS_COUNT 10

#define SMALLEST_RUN_SIZE              (1 * 1024 * 1024)
#define PARALLEL_SORTING_JOBS_COUNT    8
#define N_WAY_SORT                     16

#define PARTITIONS_COUNT                      64
#define PARALLEL_HASH_JOIN_JOBS_COUNT         6
#define PARALLEL_HASH_JOIN_JOBS_QUEUE_SIZE    6
#define MIN_BUFFER_SIZE                       (1 * 1024 * 1024)

#define RECORD_S_KEY_ELEMENT_COUNT 1

#define MIN_BLOCK_SIZE (1024 * 1024)

positional_accessor KEY_POS[1] = {STATIC_POSITION(2)};
compare_direction CMP_DIR[1] = {ASC};

transaction tx;

void intHandler(int dummy)
{
	printf("\nCaught Ctrl+C! shuting down query plan\n");
	shutdown_all_query_plans(&tx, get_dstring_pointing_to_literal_cstring("CTRL+C pressed!!"));
}

int main(int argc, char** argv)
{
	dstring join_expr = get_dstring_pointing_to_literal_cstring("record.num_in_words = record2.num_in_words");
	sql* join_expr_sql = NULL;
	{
		stream strm;
		initialize_dstring_stream(&strm, &join_expr);
		int error = 0;
		join_expr_sql = parse_sql(&strm, &error);
		if(error || join_expr_sql->type != EXPR)
		{
			printf("ERROR PARSING SQL %s\n", (((!error) && join_expr_sql->type == EXPR) ? "EXPR" : "not EXPR"));
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

	initialize_transaction(&tx, &rdb);

	// make operators

	positional_accessor O_KEY_POS[2] = {STATIC_POSITION(0,0), STATIC_POSITION(1,0)};
	compare_direction O_CMP_DIR[2] = {ASC, ASC};

	// make query

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	printf("Building pipeline :\n");
	{
		uint64_t op_index = 0;

		// 0
		insert_stream_input_operator_descriptor(qpd, &rs, &record_def); op_index++;

		{
			// 1
			insert_identity_operator_descriptor(qpd, 0); op_index++;
			insert_simple_projection_tuple_transformer_descriptor(qpd, "record2", 7, (positional_accessor[]){
				STATIC_POSITION(0), STATIC_POSITION(1), STATIC_POSITION(2), STATIC_POSITION(3), STATIC_POSITION(4), STATIC_POSITION(5), STATIC_POSITION(6)
			}, (const char* []){
				"num", "order", "num_in_words", "digits", "value_in_string", "some_numeric", "some_float"
			});

			// 2
			insert_block_nested_loop_join_operator_descriptor(qpd, 0, 1, join_expr_sql->expr, PRESERVE_NONE, MIN_BLOCK_SIZE); op_index++;
		}

		{
			// 3
			insert_sort_operator_descriptor(qpd, TUPLES_DOWN_COUNTER_INF, 0, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT); op_index++;

			// 4
			insert_sort_merge_join_operator_descriptor(qpd, 3, KEY_POS, 3, KEY_POS, CMP_DIR, RECORD_S_KEY_ELEMENT_COUNT, PRESERVE_NONE, MIN_BLOCK_SIZE); op_index++;
		}

		{
			// 5
			insert_hash_join_operator_descriptor(qpd, 0, KEY_POS, 0, KEY_POS, RECORD_S_KEY_ELEMENT_COUNT, PRESERVE_NONE, PARTITIONS_COUNT, PARALLEL_HASH_JOIN_JOBS_COUNT, PARALLEL_HASH_JOIN_JOBS_QUEUE_SIZE, MIN_BUFFER_SIZE); op_index++;
		}

		// match phase
		{
			// 6
			insert_sort_operator_descriptor(qpd, TUPLES_DOWN_COUNTER_INF, 2, 2, O_KEY_POS, O_CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT); op_index++;

			// 7
			insert_sort_operator_descriptor(qpd, TUPLES_DOWN_COUNTER_INF, 4, 2, O_KEY_POS, O_CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT); op_index++;

			// 8
			insert_sort_operator_descriptor(qpd, TUPLES_DOWN_COUNTER_INF, 5, 2, O_KEY_POS, O_CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT); op_index++;

			// 9
			insert_result_match_operator_descriptor(qpd, 6, 7); op_index++;

			// 10
			insert_result_match_operator_descriptor(qpd, 7, 8); op_index++;

			// 11
			insert_result_match_operator_descriptor(qpd, 6, 8); op_index++;
		}
	}
	printf("\n\n");

	query_plan* qp = create_query_plan(qpd, NULL);
	register_query_plan(&tx, qp);

	printf("Pipeline built\n\n");

	// make query completed

	start_all_operators_for_query_plan(qp);

	wait_for_shutdown_of_query_plan(qp);

	deregister_query_plan(&tx, qp);
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

	delete_sql(join_expr_sql);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}