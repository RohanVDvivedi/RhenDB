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

#define PRINT_DATA 1

// OFFSET_CLAUSE can not be infinity (UINT64_MAX), undef it if you want behaviour of it being 0
#define OFFSET_CLAUSE  UINT64_C(55)
// LIMIT_CLAUSE can not be 0, undef it if you want behaviour of it being infinity (UINT64_MAX)
#define LIMIT_CLAUSE   UINT64_C(155)

#define SMALLEST_RUN_SIZE              (1 * 1024 * 1024)
#define PARALLEL_SORTING_JOBS_COUNT    8
#define N_WAY_SORT                     16

#define RECORD_S_KEY_ELEMENT_COUNT 2

positional_accessor KEY_POS[2] = {STATIC_POSITION(0), STATIC_POSITION(2)};
compare_direction CMP_DIR[2] = {ASC, ASC};

transaction tx;

void intHandler(int dummy)
{
	printf("\nCaught Ctrl+C! shuting down query plan\n");
	shutdown_all_query_plans(&tx, get_dstring_pointing_to_literal_cstring("CTRL+C pressed!!"));
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

	initialize_tuple_defs();

	initialize_transaction(&tx, &rdb);

	// make query

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	printf("Building pipeline :\n");
	{
		uint64_t op_index = 0;

		insert_stream_input_operator_descriptor(qpd, &rs, &record_def); op_index++;

		tuples_down_counter sorter_result_counter = TUPLES_DOWN_COUNTER_INF;
		#if defined(LIMIT_CLAUSE) && !defined(OFFSET_CLAUSE)
			sorter_result_counter = TUPLES_DOWN_COUNTER_FIN(LIMIT_CLAUSE);
		#elif defined(LIMIT_CLAUSE) && defined(OFFSET_CLAUSE)
			sorter_result_counter = TUPLES_DOWN_COUNTER_FIN(OFFSET_CLAUSE + LIMIT_CLAUSE);
		#endif

		uint64_t result_index = op_index;
		insert_sort_operator_descriptor(qpd, sorter_result_counter, op_index - 1, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT); op_index++;

		#if defined(OFFSET_CLAUSE) || defined(LIMIT_CLAUSE)
			tuples_down_counter offset;
			#ifdef OFFSET_CLAUSE
				offset = TUPLES_DOWN_COUNTER_FIN(OFFSET_CLAUSE);
			#else
				offset = TUPLES_DOWN_COUNTER_ZERO;
			#endif
			tuples_down_counter limit;
			#ifdef LIMIT_CLAUSE
				limit = TUPLES_DOWN_COUNTER_FIN(LIMIT_CLAUSE);
			#else
				limit = TUPLES_DOWN_COUNTER_INF;
			#endif
			result_index = op_index;
			insert_offset_limit_operator_descriptor(qpd, op_index - 1, offset, limit); op_index++;
		#endif

		#ifdef PRINT_DATA
			insert_consumer_operator_descriptor(qpd, result_index, PRINT_DATA ? print_consumer : NULL, NULL); op_index++;
		#endif

		if(argc >= 2)
		{
			int out_fd = open(argv[1], O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
			initialize_stream_for_fd(&out_file_stream, out_fd);

			insert_stream_output_operator_descriptor(qpd, result_index, &out_file_stream); op_index++;
		}
	}
	printf("\n\n");

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