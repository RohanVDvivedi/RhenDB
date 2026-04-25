#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>

#include<test_dataset_tuple_def.h>

#include<cutlery/stream_for_file_descriptor.h>

#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<signal.h>
#include<fcntl.h>

#define USERS_COUNT 100

#define PRINT_DATA 0

#define TESTCASE_SIZE 1000000
#define INPUT_OPERATORS_COUNT 30

#define SMALLEST_RUN_SIZE              (1 * 1024 * 1024)
#define PARALLEL_SORTING_JOBS_COUNT    8
#define N_WAY_SORT                     16

#define RECORD_S_KEY_ELEMENT_COUNT 2

positional_accessor KEY_POS[2] = {STATIC_POSITION(0), STATIC_POSITION(2)};
compare_direction CMP_DIR[2] = {ASC, ASC};

int generator_contexts[INPUT_OPERATORS_COUNT];

#define BUFFER_SIZE 300

void* generator(void* generator_context, const tuple_def* generator_tuple_def)
{
	int* generator_number = generator_context;

	if((*generator_number) >= TESTCASE_SIZE)
		return NULL;

	void* generated = malloc(BUFFER_SIZE);

	construct_record(generated, (*generator_number), 0, "Rohan Dvivedi");
	(*generator_number) += INPUT_OPERATORS_COUNT;

	return generated;
}

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
		512, 8, 10, 10,
			10000ULL, 100000ULL,
			10000000ULL,
		4096,
			10000000ULL,
		USERS_COUNT);
	printf("database initialized\n\n");

	initialize_tuple_defs();

	transaction tx = initialize_transaction(&rdb);

	qp = get_new_query_plan(&tx, (INPUT_OPERATORS_COUNT) * 2 + 30);

	// make operators

	printf("Building pipeline :\n");
	{
		// source operators
		operator* input_operators[INPUT_OPERATORS_COUNT];
		for(int i = 0; i < INPUT_OPERATORS_COUNT; i++)
		{
			generator_contexts[i] = i;
			input_operators[i] = get_new_registered_operator_for_query_plan(qp);
			setup_generator_operator(input_operators[i], generator, &(generator_contexts[i]), &record_def);
			printf("source operator %p\n", input_operators[i]);
		}

		// first pipeline first union then sort
		operator* u = get_new_registered_operator_for_query_plan(qp);
		setup_union_operator(u, input_operators, INPUT_OPERATORS_COUNT);
		printf("union operator %p\n", u);

		operator* s = get_new_registered_operator_for_query_plan(qp);
		setup_external_sort_operator(s, TUPLES_DOWN_COUNTER_INF, u, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT);
		printf("sorter operator %p\n", s);

		// second pipeline first sort then merge
		operator* sorted_input_operators[INPUT_OPERATORS_COUNT];
		for(int i = 0; i < INPUT_OPERATORS_COUNT; i++)
		{
			sorted_input_operators[i] = get_new_registered_operator_for_query_plan(qp);
			setup_external_sort_operator(sorted_input_operators[i], TUPLES_DOWN_COUNTER_INF, input_operators[i], RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT);
			printf("sorted operator - %d %p\n", i, sorted_input_operators[i]);
		}

		operator* m  = get_new_registered_operator_for_query_plan(qp);
		setup_sorted_inputs_operator(m, sorted_input_operators, INPUT_OPERATORS_COUNT, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR);
		printf("merge sorted operator %p\n", m);

		// finally match both the output of the pipelines
		operator* r = get_new_registered_operator_for_query_plan(qp);
		setup_result_match_operator(r, (operator* []){s, m});
		printf("result match operator %p\n", r);
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

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}