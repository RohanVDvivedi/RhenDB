#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
#include<rhendb/query_plan.h>

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

int main(int argc, char** argv)
{
	stream rs, ws;
	initialize_stream_for_fd(&rs, 0);
	initialize_stream_for_fd(&ws, 1);

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

	transaction tx;
	initialize_transaction(&tx, &rdb);

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	{
		const operator_desc* gen = insert_generator_operator_descriptor(qpd,
		#ifdef RANDOMIZE_DATA
			random_generator
		#else
			sorted_generator
		#endif
		, NULL, &record_def);
	}

	query_plan* qp = get_new_query_plan(qpd);

	stream out_file_stream;
	int out_fd = open(argv[1], O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
	initialize_stream_for_fd(&out_file_stream, out_fd);

	while(1)
	{
		int error_code = 0;
		void* tuple = next_tuple_from_query_plan(qp, &error_code);
		if(tuple == NULL)
			break;
		uint32_t tuple_size = get_tuple_size(qp->desc->output_tuple_def, tuple);
		int stream_error = 0;
		write_to_stream(&out_file_stream, tuple, tuple_size, &stream_error);
		free(tuple);
		if(stream_error)
			break;
	}

	int stream_error = 0;
	flush_all_from_stream(&out_file_stream, &stream_error);
	close_stream(&out_file_stream, &stream_error);
	deinitialize_stream(&out_file_stream);

	destroy_query_plan(qp);

	destroy_query_plan_descriptor(qpd);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}