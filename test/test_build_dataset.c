#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>
#include<rhendb/tuple_transformers.h>

#include<test_dataset_tuple_def.h>

#include<cutlery/stream_for_file_descriptor.h>

#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<signal.h>
#include<fcntl.h>

#define USERS_COUNT 10

#define TESTCASE_SIZE 1000000

#define RANDOMIZE_DATA

//#define PRINT_DATA 1

#define SMALLEST_RUN_SIZE              (1 * 1024 * 1024)
#define PARALLEL_SORTING_JOBS_COUNT    8
#define N_WAY_SORT                     16

uint32_t inputs[TESTCASE_SIZE];
void generate_random_inputs()
{
	for(uint32_t i = 0; i < TESTCASE_SIZE; i++)
		inputs[i] = i;
	for(uint32_t i = 0; i < TESTCASE_SIZE; i++)
		memory_swap(inputs + (((uint32_t)rand())  % TESTCASE_SIZE), inputs + (((uint32_t)rand()) % TESTCASE_SIZE), sizeof(uint32_t));
}

#define BUFFER_SIZE 300

void* random_generator(void* generator_context, const tuple_def* generator_tuple_def)
{
	static uint32_t index = 0;

	if(index >= TESTCASE_SIZE)
		return NULL;

	void* generated = malloc(BUFFER_SIZE);
	construct_record(generated, inputs[index++], 0, "Rohan Dvivedi");

	return generated;
}

void* sorted_generator(void* generator_context, const tuple_def* generator_tuple_def)
{
	static uint32_t index = 0;

	if(index >= TESTCASE_SIZE)
		return NULL;

	void* generated = malloc(BUFFER_SIZE);
	construct_record(generated, index++, 0, "Rohan Dvivedi");

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
	stream rs, ws;
	initialize_stream_for_fd(&rs, 0);
	initialize_stream_for_fd(&ws, 1);

	stream out_file_stream;

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

#ifdef RANDOMIZE_DATA
	generate_random_inputs();
#endif

	initialize_tuple_defs();

	transaction tx = initialize_transaction(&rdb);

	qp = get_new_query_plan(&tx, 3);

	// make operators

	printf("Building pipeline :\n");
	{
		operator* input_operator = get_new_registered_operator_for_query_plan(qp);
		setup_generator_operator(input_operator,
		#ifdef RANDOMIZE_DATA
			random_generator
		#else
			sorted_generator
		#endif
		, NULL, &record_def);
		printf("source operator %s %p\n",
		#ifdef RANDOMIZE_DATA
			"random_generator"
		#else
			"sorted_generator"
		#endif
		, input_operator);

		#ifdef PRINT_DATA
			operator* print_operator = get_new_registered_operator_for_query_plan(qp);
			setup_printf_operator(print_operator, input_operator, PRINT_DATA);
			printf("output print operator %p\n", print_operator);
		#endif

		if(argc >= 2)
		{
			int out_fd = open(argv[1], O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
			initialize_stream_for_fd(&out_file_stream, out_fd);

			operator* output_operator = get_new_registered_operator_for_query_plan(qp);
			setup_stream_output_operator(output_operator, input_operator, &out_file_stream);
			printf("output stream operator %p\n", output_operator);
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

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}