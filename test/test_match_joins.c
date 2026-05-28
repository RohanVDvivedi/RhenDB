#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>
#include<rhendb/tuple_transformers.h>

#include<rhendb/function_compare.h>

#include<test_dataset_tuple_def.h>
#include<test_dataset_2_tuple_def.h>

#include<cutlery/stream_for_file_descriptor.h>

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

#define RECORD_S_KEY_ELEMENT_COUNT 1

#define MAX_BLOCK_SIZE (1024 * 1024)

positional_accessor KEY_POS[1] = {STATIC_POSITION(2)};
compare_direction CMP_DIR[1] = {ASC};

query_plan* qp = NULL;

void intHandler(int dummy)
{
	printf("\nCaught Ctrl+C! shuting down query plan\n");
	shutdown_query_plan(qp, get_dstring_pointing_to_literal_cstring("CTRL+C pressed!!"));
}

int join_matcher(const void* join_match_context_p, const void* left_tuple, const tuple_def* left_tuple_def, const void* right_tuple, const tuple_def* right_tuple_def)
{
	return (0 == compare_tuples_rhendb(left_tuple, left_tuple_def, KEY_POS, right_tuple, right_tuple_def, KEY_POS, NULL, RECORD_S_KEY_ELEMENT_COUNT, (rage_engine*)join_match_context_p));
}

int main(int argc, char** argv)
{
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

	printf("Building pipeline :\n");
	{
		operator* input_operator = get_new_registered_operator_for_query_plan(qp);
		setup_stream_input_operator(input_operator, &rs, &record_def);
		printf("source operator %p\n", input_operator);

		operator* bnlj = NULL;
		{
			bnlj = get_new_registered_operator_for_query_plan(qp);
			setup_block_nested_loop_join_operator(bnlj, input_operator, input_operator, &(rdb.persistent_acid_rage_engine), join_matcher, PRESERVE_NONE, MAX_BLOCK_SIZE);
			printf("join operator %p\n", bnlj);
		}

		operator* smj = NULL;
		{
			operator* input_sorted_operator = get_new_registered_operator_for_query_plan(qp);
			setup_external_sort_operator(input_sorted_operator, TUPLES_DOWN_COUNTER_INF, input_operator, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT);
			printf("sorter for smj operator %p\n", input_sorted_operator);

			smj = get_new_registered_operator_for_query_plan(qp);
			setup_sort_merge_join_operator(smj, input_sorted_operator, KEY_POS, input_sorted_operator, KEY_POS, CMP_DIR, RECORD_S_KEY_ELEMENT_COUNT, PRESERVE_NONE, MAX_BLOCK_SIZE);
			printf("join operator %p\n", smj);
		}

		// match phase
		{
			operator* output_bnlj_operator = get_new_registered_operator_for_query_plan(qp);
			setup_external_sort_operator(output_bnlj_operator, TUPLES_DOWN_COUNTER_INF, bnlj, 2, O_KEY_POS, O_CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT);
			printf("sorter for output of bnlj operator %p\n", output_bnlj_operator);

			operator* output_smj_operator = get_new_registered_operator_for_query_plan(qp);
			setup_external_sort_operator(output_smj_operator, TUPLES_DOWN_COUNTER_INF, smj, 2, O_KEY_POS, O_CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT);
			printf("sorter for output of bnlj operator %p\n", output_smj_operator);

			operator* matcher = get_new_registered_operator_for_query_plan(qp);
			setup_result_match_operator(matcher, (operator* []){output_bnlj_operator, output_smj_operator});
			printf("result match operator %p\n", matcher);
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

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();
	deinitialize_tuple_defs2();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}