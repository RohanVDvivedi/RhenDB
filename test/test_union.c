#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>

#include<test_dataset_tuple_def.h>

#include<cutlery/stream_for_file_descriptor.h>

#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<signal.h>
#include<fcntl.h>

#define USERS_COUNT 100

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

// generates sorted tuples with incremental values of % INPUT_OPERATORS_COUNT
void* generator(void* generator_context, const tuple_def* generator_tuple_def)
{
	int* generator_number = generator_context;

	if((*generator_number) >= TESTCASE_SIZE)
		return NULL;

	void* generated = malloc(BUFFER_SIZE);

	construct_record(generated, (*generator_number), 0, ((*generator_number) % 3 == 0) ? NULL : "Rohan Dvivedi");
	(*generator_number) += INPUT_OPERATORS_COUNT;

	return generated;
}

transaction tx;

void intHandler(int dummy)
{
	printf("\nCaught Ctrl+C! shuting down query plan\n");
	shutdown_all_query_plans(&tx, get_dstring_pointing_to_literal_cstring("CTRL+C pressed!!"));
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

	initialize_transaction(&tx, &rdb);

	// make query

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	printf("Building pipeline :\n");
	{
		uint64_t op_index = 0;

		uint64_t sorted_input_operator_indices[INPUT_OPERATORS_COUNT];
		for(int i = 0; i < INPUT_OPERATORS_COUNT; i++)
		{
			generator_contexts[i] = i;
			insert_generator_operator_descriptor(qpd, generator, &(generator_contexts[i]), &record_def); op_index++;
			sorted_input_operator_indices[i] = i;
		}

		insert_union_operator_descriptor(qpd, sorted_input_operator_indices, INPUT_OPERATORS_COUNT); op_index++;

		insert_sort_operator_descriptor(qpd, TUPLES_DOWN_COUNTER_INF, op_index-1, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT); op_index++;

		insert_merge_sorted_inputs_operator_descriptor(qpd, sorted_input_operator_indices, INPUT_OPERATORS_COUNT, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR); op_index++;

		insert_result_match_operator_descriptor(qpd, op_index - 1, op_index - 2); op_index++;
	}

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

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}