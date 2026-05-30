#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>
#include<rhendb/tuple_transformers.h>

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

#define PRINT_DATA 1

#define MIN_LEFT 6
#define JUMP_LEFT 2

#define COUNT_LEFT 30000

uint32_t left_inputs[COUNT_LEFT];

#define MIN_RIGHT 12
#define JUMP_RIGHT 3

#define COUNT_RIGHT 30000

#define MAX_BLOCK_SIZE 4096

uint32_t right_inputs[COUNT_RIGHT];

void generate_random_inputs()
{
	for(uint32_t i = 0; i < COUNT_LEFT;)
	{
		uint32_t val = MIN_LEFT +  i;
		for(uint32_t j = 0; j < JUMP_LEFT && i < COUNT_LEFT; j++, i++)
			left_inputs[i] = val;
	}
	for(uint32_t i = 0; i < COUNT_LEFT; i++)
		memory_swap(left_inputs + (((uint32_t)rand()) % COUNT_LEFT), left_inputs + (((uint32_t)rand()) % COUNT_LEFT), sizeof(uint32_t));

	for(uint32_t i = 0; i < COUNT_RIGHT;)
	{
		uint32_t val = MIN_RIGHT +  i;
		for(uint32_t j = 0; j < JUMP_RIGHT && i < COUNT_RIGHT; j++, i++)
			right_inputs[i] = val;
	}
	for(uint32_t i = 0; i < COUNT_RIGHT; i++)
		memory_swap(right_inputs + (((uint32_t)rand()) % COUNT_RIGHT), right_inputs + (((uint32_t)rand()) % COUNT_RIGHT), sizeof(uint32_t));
}

#define BUFFER_SIZE 300

void* left_generator(void* generator_context, const tuple_def* generator_tuple_def)
{
	static uint32_t index = 0;

	if(index >= COUNT_LEFT)
		return NULL;

	void* generated = malloc(BUFFER_SIZE);
	construct_record(generated, left_inputs[index], 0, "LEFT");
	index++;

	return generated;
}

void* right_generator(void* generator_context, const tuple_def* generator_tuple_def)
{
	static uint32_t index = 0;

	if(index >= COUNT_RIGHT)
		return NULL;

	void* generated = malloc(BUFFER_SIZE);
	construct_record2(generated, right_inputs[index]);
	index++;

	return generated;
}

#define PARTITIONS_COUNT                      64
#define BUCKETS_PER_PARTITION                 32
#define PARALLEL_HASH_JOIN_JOBS_COUNT         6
#define PARALLEL_HASH_JOIN_JOBS_QUEUE_SIZE    6
#define MIN_BUFFER_SIZE                       (1 * 1024 * 1024)

#define SMALLEST_RUN_SIZE              (1 * 1024 * 1024)
#define PARALLEL_SORTING_JOBS_COUNT    8
#define N_WAY_SORT                     16

#define RECORD_S_KEY_ELEMENT_COUNT 1

positional_accessor LEFT_KEY_POS[1] = {STATIC_POSITION(2)};
positional_accessor RIGHT_KEY_POS[1] = {STATIC_POSITION(1)};

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

	generate_random_inputs();

	initialize_tuple_defs();
	initialize_tuple_defs2();

	transaction tx = initialize_transaction(&rdb);

	qp = get_new_query_plan(&tx, 30);

	// make operators

	positional_accessor FINAL_KEY_POS[2] = {STATIC_POSITION(0, 0), STATIC_POSITION(1, 0)};
	compare_direction FINAL_CMP_DIR[2] = {ASC, ASC};

	printf("Building pipeline :\n");
	{
		operator* left_input_operator = get_new_registered_operator_for_query_plan(qp);
		setup_generator_operator(left_input_operator, left_generator, NULL, &record_def);
		printf("source left operator %p\n", left_input_operator);

		operator* right_input_operator = get_new_registered_operator_for_query_plan(qp);
		setup_generator_operator(right_input_operator, right_generator, NULL, &record_def2);
		printf("source right operator %p\n", right_input_operator);

		operator* join_operator = get_new_registered_operator_for_query_plan(qp);
		setup_hash_join_operator(join_operator, left_input_operator, LEFT_KEY_POS, right_input_operator, RIGHT_KEY_POS, RECORD_S_KEY_ELEMENT_COUNT, PRESERVE_BOTH, PARTITIONS_COUNT, BUCKETS_PER_PARTITION, PARALLEL_HASH_JOIN_JOBS_COUNT, PARALLEL_HASH_JOIN_JOBS_QUEUE_SIZE, MIN_BUFFER_SIZE);
		printf("join operator %p\n", join_operator);

		operator* sorter_operator = get_new_registered_operator_for_query_plan(qp);
		setup_external_sort_operator(sorter_operator, TUPLES_DOWN_COUNTER_INF, join_operator, 2, FINAL_KEY_POS, FINAL_CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT);
		printf("sorter for output operator %p\n", sorter_operator);

		operator* print_operator = get_new_registered_operator_for_query_plan(qp);
		setup_consumer_operator(print_operator, sorter_operator, PRINT_DATA ? print_consumer : NULL, NULL);
		printf("output print operator %p\n", print_operator);
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