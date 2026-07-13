#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>
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

query_plan* qp = NULL;

void intHandler(int dummy)
{
	printf("\nCaught Ctrl+C! shuting down query plan\n");
	shutdown_query_plan(qp, get_dstring_pointing_to_literal_cstring("CTRL+C pressed!!"));
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
	initialize_tuple_defs2();

	transaction tx = initialize_transaction(&rdb);

	qp = get_new_query_plan(&tx, 30);

	// make operators

	positional_accessor O_KEY_POS[2] = {STATIC_POSITION(0,0), STATIC_POSITION(1,0)};
	compare_direction O_CMP_DIR[2] = {ASC, ASC};

	const char* field_names_input_operator2[] = {
		"num", "order", "num_in_words", "digits", "value_in_string", "some_numeric", "some_float"
	};

	positional_accessor* projections_input_operator2[] = {
		&(STATIC_POSITION(0)), &(STATIC_POSITION(1)), &(STATIC_POSITION(2)), &(STATIC_POSITION(3)), &(STATIC_POSITION(4)), &(STATIC_POSITION(5)), &(STATIC_POSITION(6))
	};

	printf("Building pipeline :\n");
	{
		operator* input_operator = get_new_registered_operator_for_query_plan(qp);
		setup_stream_input_operator(input_operator, &rs, &record_def);
		printf("source operator %p\n", input_operator);

		operator* bnlj = NULL;
		{
			// input_operator2 is just a rename to record2
			operator* input_operator2 = get_new_registered_operator_for_query_plan(qp);
			setup_identity_operator(input_operator2, input_operator);
			append_tuple_transformer(&(input_operator2->output_tuple_transformers), get_new_simple_projection_transformer("record2", get_tuple_def_for_tuples_to_be_consumed_from(input_operator2), 5, projections_input_operator2, field_names_input_operator2));
			printf("source operator2 only for bnlj %p\n", input_operator2);

			bnlj = get_new_registered_operator_for_query_plan(qp);
			setup_block_nested_loop_join_operator(bnlj, input_operator, input_operator2, join_expr_sql->expr, PRESERVE_NONE, MIN_BLOCK_SIZE);
			printf("block nested loop join operator %p\n", bnlj);
		}

		operator* smj = NULL;
		{
			operator* input_sorted_operator = get_new_registered_operator_for_query_plan(qp);
			setup_external_sort_operator(input_sorted_operator, TUPLES_DOWN_COUNTER_INF, input_operator, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT);
			printf("sorter for smj operator %p\n", input_sorted_operator);

			smj = get_new_registered_operator_for_query_plan(qp);
			setup_sort_merge_join_operator(smj, input_sorted_operator, KEY_POS, input_sorted_operator, KEY_POS, CMP_DIR, RECORD_S_KEY_ELEMENT_COUNT, PRESERVE_NONE, MIN_BLOCK_SIZE);
			printf("sort merge join operator %p\n", smj);
		}

		operator* hj = NULL;
		{
			hj = get_new_registered_operator_for_query_plan(qp);
			setup_hash_join_operator(hj, input_operator, KEY_POS, input_operator, KEY_POS, RECORD_S_KEY_ELEMENT_COUNT, PRESERVE_NONE, PARTITIONS_COUNT, PARALLEL_HASH_JOIN_JOBS_COUNT, PARALLEL_HASH_JOIN_JOBS_QUEUE_SIZE, MIN_BUFFER_SIZE);
			printf("hash join operator %p\n", hj);
		}

		// match phase
		{
			operator* output_bnlj_operator = get_new_registered_operator_for_query_plan(qp);
			setup_external_sort_operator(output_bnlj_operator, TUPLES_DOWN_COUNTER_INF, bnlj, 2, O_KEY_POS, O_CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT);
			printf("sorter for output of bnlj operator %p\n", output_bnlj_operator);

			operator* output_smj_operator = get_new_registered_operator_for_query_plan(qp);
			setup_external_sort_operator(output_smj_operator, TUPLES_DOWN_COUNTER_INF, smj, 2, O_KEY_POS, O_CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT);
			printf("sorter for output of smj operator %p\n", output_smj_operator);

			operator* output_hj_operator = get_new_registered_operator_for_query_plan(qp);
			setup_external_sort_operator(output_hj_operator, TUPLES_DOWN_COUNTER_INF, hj, 2, O_KEY_POS, O_CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT);
			printf("sorter for output of hj operator %p\n", output_hj_operator);

			operator* matcher1 = get_new_registered_operator_for_query_plan(qp);
			setup_result_match_operator(matcher1, (operator* []){output_bnlj_operator, output_smj_operator});
			printf("result match1 operator %p\n", matcher1);

			operator* matcher2 = get_new_registered_operator_for_query_plan(qp);
			setup_result_match_operator(matcher2, (operator* []){output_bnlj_operator, output_hj_operator});
			printf("result match2 operator %p\n", matcher2);

			operator* matcher3 = get_new_registered_operator_for_query_plan(qp);
			setup_result_match_operator(matcher3, (operator* []){output_smj_operator, output_hj_operator});
			printf("result match3 operator %p\n", matcher3);
		}
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

	delete_sql(join_expr_sql);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();
	deinitialize_tuple_defs2();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}