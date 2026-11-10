#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
#include<rhendb/query_plan.h>

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

#define PRINT_DATA

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
#define N_WAY_SORT                     16

#define RECORD_S_KEY_ELEMENT_COUNT 1

positional_accessor KEY_POS[1] = {STATIC_POSITION(0)};
compare_direction CMP_DIR[1] = {ASC};

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

		operator_desc* right = insert_generator_operator_descriptor(qpd, right_generator, NULL, &record_def2);

		operator_desc* join = insert_block_nested_loop_semi_join_operator_descriptor(qpd, left, right, join_expr_sql->expr, LEFT_SEMI_JOIN, MIN_BLOCK_SIZE);

		operator_desc* sort = insert_sort_operator_descriptor(qpd, TUPLES_DOWN_COUNTER_INF, join, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);
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

	delete_sql(join_expr_sql);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();
	deinitialize_tuple_defs2();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}