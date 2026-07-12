#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>
#include<rhendb/tuple_transformers.h>

#include<sqltoast/sqltoast.h>
#include<sqltoast/sql_expression_eval.h>

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

#define PRINT_DATA 1

#define MIN_LEFT 20
#define MAX_LEFT 3200

#define COUNT_LEFT (MAX_LEFT - MIN_LEFT + 1)

uint32_t left_inputs[COUNT_LEFT];

#define MIN_RIGHT 10
#define MAX_RIGHT 3000

#define COUNT_RIGHT (MAX_RIGHT - MIN_RIGHT + 1)

#define MIN_BLOCK_SIZE 4096

uint32_t right_inputs[COUNT_RIGHT];

void generate_random_inputs()
{
	for(uint32_t i = 0; i < COUNT_LEFT; i++)
		left_inputs[i] = MIN_LEFT + i;
	for(uint32_t i = 0; i < COUNT_LEFT; i++)
		memory_swap(left_inputs + (((uint32_t)rand()) % COUNT_LEFT), left_inputs + (((uint32_t)rand()) % COUNT_LEFT), sizeof(uint32_t));

	for(uint32_t i = 0; i < COUNT_RIGHT; i++)
		right_inputs[i] = MIN_RIGHT + i;
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
#define PARALLEL_SORTING_JOBS_COUNT    8
#define N_WAY_SORT                     16

#define RECORD_S_KEY_ELEMENT_COUNT 1

positional_accessor KEY_POS[1] = {STATIC_POSITION(0)};
compare_direction CMP_DIR[1] = {ASC};

query_plan* qp = NULL;

void intHandler(int dummy)
{
	printf("\nCaught Ctrl+C! shuting down query plan\n");
	shutdown_query_plan(qp, get_dstring_pointing_to_literal_cstring("CTRL+C pressed!!"));
}

int join_matcher(const void* join_match_context_p, const void* left_tuple, const tuple_def* left_tuple_def, const void* right_tuple, const tuple_def* right_tuple_def)
{
	datum num_left;
	get_value_from_element_from_tuple(&num_left, left_tuple_def, STATIC_POSITION(0), left_tuple);

	datum num_right;
	get_value_from_element_from_tuple(&num_right, right_tuple_def, STATIC_POSITION(0), right_tuple);

	return num_left.uint_value == num_right.uint_value;

	/*if(num_left.uint_value >= num_right.uint_value)
		return (num_left.uint_value - num_right.uint_value) <= 3;
	else
		return (num_right.uint_value - num_left.uint_value) <= 3;*/
}

int main(int argc, char** argv)
{
	dstring join_expr = get_dstring_pointing_to_literal_cstring("record.num = record2.num2");
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

	generate_random_inputs();

	initialize_tuple_defs();
	initialize_tuple_defs2();

	transaction tx = initialize_transaction(&rdb);

	qp = get_new_query_plan(&tx, 30);

	// make operators

	printf("Building pipeline :\n");
	{
		operator* left_input_operator = get_new_registered_operator_for_query_plan(qp);
		setup_generator_operator(left_input_operator, left_generator, NULL, &record_def);
		printf("source left operator %p\n", left_input_operator);

		operator* right_input_operator = get_new_registered_operator_for_query_plan(qp);
		setup_generator_operator(right_input_operator, right_generator, NULL, &record_def2);
		printf("source right operator %p\n", right_input_operator);

		operator* join_operator = get_new_registered_operator_for_query_plan(qp);
		setup_block_nested_loop_semi_join_operator(join_operator, left_input_operator, right_input_operator, join_expr_sql->expr, LEFT_SEMI_JOIN, MIN_BLOCK_SIZE);
		printf("join operator %p\n", join_operator);

		operator* sorter_operator = get_new_registered_operator_for_query_plan(qp);
		setup_external_sort_operator(sorter_operator, TUPLES_DOWN_COUNTER_INF, join_operator, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT);
		printf("sorter operator %p\n", sorter_operator);

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

	delete_sql(join_expr_sql);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();
	deinitialize_tuple_defs2();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}