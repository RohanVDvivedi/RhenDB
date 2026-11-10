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

#define PRINT_DATA

// OFFSET_CLAUSE can not be infinity (UINT64_MAX), undef it if you want behaviour of it being 0
#define OFFSET_CLAUSE  UINT64_C(55)
// LIMIT_CLAUSE can not be 0, undef it if you want behaviour of it being infinity (UINT64_MAX)
#define LIMIT_CLAUSE   UINT64_C(155)

#define SMALLEST_RUN_SIZE              (1 * 1024 * 1024)
#define N_WAY_SORT                     16

#define RECORD_S_KEY_ELEMENT_COUNT 2

positional_accessor KEY_POS[2] = {STATIC_POSITION(0), STATIC_POSITION(2)};
compare_direction CMP_DIR[2] = {ASC, ASC};

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

	initialize_tuple_defs();

	transaction tx;
	initialize_transaction(&tx, &rdb);

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);
	{
		operator_desc* strm_in = insert_stream_input_operator_descriptor(qpd, &rs, &record_def);

		tuples_down_counter sorter_result_counter = TUPLES_DOWN_COUNTER_INF;
		#if defined(LIMIT_CLAUSE) && !defined(OFFSET_CLAUSE)
			sorter_result_counter = TUPLES_DOWN_COUNTER_FIN(LIMIT_CLAUSE);
		#elif defined(LIMIT_CLAUSE) && defined(OFFSET_CLAUSE)
			sorter_result_counter = TUPLES_DOWN_COUNTER_FIN(OFFSET_CLAUSE + LIMIT_CLAUSE);
		#endif

		operator_desc* sorted = insert_sort_operator_descriptor(qpd, sorter_result_counter, strm_in, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);

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
			insert_offset_limit_operator_descriptor(qpd, sorted, offset, limit);
		#endif
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

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}