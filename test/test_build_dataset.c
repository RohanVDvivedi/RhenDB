#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
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

//#define RANDOMIZE_DATA

#define PRINT_DATA 0

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
	construct_record(generated, inputs[index], 0, (inputs[index] % 3 == 0) ? NULL : "Rohan Dvivedi"); // every 3rd tuple has NULL instead of my name
	index++;

	return generated;
}

void* sorted_generator(void* generator_context, const tuple_def* generator_tuple_def)
{
	static uint32_t index = 0;

	if(index >= TESTCASE_SIZE)
		return NULL;

	void* generated = malloc(BUFFER_SIZE);
	construct_record(generated, index, 0, (index % 3 == 0) ? NULL : "Rohan Dvivedi"); // every 3rd tuple has NULL instead of my name
	index++;

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
		512, 8, 80, 80,
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

	// make query

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	printf("Building pipeline :\n");
	{
		uint64_t op_index = 0;

		insert_generator_operator_descriptor(qpd,
		#ifdef RANDOMIZE_DATA
			random_generator
		#else
			sorted_generator
		#endif
		, NULL, &record_def); op_index++;

		#ifdef PRINT_DATA
			insert_consumer_operator_descriptor(qpd, 0, PRINT_DATA ? print_consumer : NULL, NULL); op_index++;
		#endif

		if(argc >= 2)
		{
			int out_fd = open(argv[1], O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
			initialize_stream_for_fd(&out_file_stream, out_fd);

			insert_stream_output_operator_descriptor(qpd, 0, &out_file_stream);
		}
	}

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

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}