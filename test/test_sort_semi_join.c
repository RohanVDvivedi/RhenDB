#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
#include<rhendb/query_plan.h>

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

#define PRINT_DATA

#define MIN_LEFT 6
#define JUMP_LEFT 2

#define COUNT_LEFT 30000

uint32_t left_inputs[COUNT_LEFT];

#define MIN_RIGHT 12
#define JUMP_RIGHT 3

#define COUNT_RIGHT 30000

#define MIN_BLOCK_SIZE 4096

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

#define SMALLEST_RUN_SIZE              (1 * 1024 * 1024)
#define N_WAY_SORT                     16

#define RECORD_S_KEY_ELEMENT_COUNT 1

positional_accessor LEFT_KEY_POS[1] = {STATIC_POSITION(0)};
positional_accessor RIGHT_KEY_POS[1] = {STATIC_POSITION(0)};

/*positional_accessor LEFT_KEY_POS[1] = {STATIC_POSITION(2)};
positional_accessor RIGHT_KEY_POS[1] = {STATIC_POSITION(1)};*/

compare_direction CMP_DIR[1] = {ASC};

transaction tx;

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

	generate_random_inputs();

	initialize_tuple_defs();
	initialize_tuple_defs2();

	transaction tx;
	initialize_transaction(&tx, &rdb);

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	{
		operator_desc* left = insert_generator_operator_descriptor(qpd, left_generator, NULL, &record_def);

		operator_desc* left_sorted = insert_sort_operator_descriptor(qpd, TUPLES_DOWN_COUNTER_INF, left, RECORD_S_KEY_ELEMENT_COUNT, LEFT_KEY_POS, CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);

		operator_desc* right = insert_generator_operator_descriptor(qpd, right_generator, NULL, &record_def2);

		operator_desc* right_sorted = insert_sort_operator_descriptor(qpd, TUPLES_DOWN_COUNTER_INF, right, RECORD_S_KEY_ELEMENT_COUNT, RIGHT_KEY_POS, CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);

		operator_desc* join = insert_sort_merge_semi_join_operator_descriptor(qpd, left_sorted, LEFT_KEY_POS, right_sorted, RIGHT_KEY_POS, CMP_DIR, RECORD_S_KEY_ELEMENT_COUNT, LEFT_SEMI_JOIN);
	}

	query_plan* qp = get_new_query_plan(qpd);

	while(1)
	{
		int error_code = 0;
		void* tuple = next_tuple_from_query_plan(qp, &error_code);
		if(tuple == NULL)
			break;
		#ifdef PRINT_DATA
			print_tuple(tuple, qp->desc->output_tuple_def);
		#endif
		free(tuple);
	}

	destroy_query_plan(qp);

	destroy_query_plan_descriptor(qpd);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();
	deinitialize_tuple_defs2();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}