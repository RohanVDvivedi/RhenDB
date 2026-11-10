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

#define SMALLEST_RUN_SIZE              (1 * 1024 * 1024)
#define N_WAY_SORT                     16

#define RECORD_S_KEY_ELEMENT_COUNT 1

positional_accessor KEY_POS[1] = {STATIC_POSITION(2)};
compare_direction CMP_DIR[1] = {ASC};

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

	aggregate_function* const AGGREGATES[] = {
		get_count_aggregate_function(record_def.type_info),
		get_count_aggregate_function(record_def.type_info->containees[4].al.type_info),

		get_min_max_aggregate_function(&tx, record_def.type_info->containees[0].al.type_info, 1), // 1 is min
		get_min_max_aggregate_function(&tx, record_def.type_info->containees[0].al.type_info, 0), // 0 as last param means max

		get_sum_aggregate_function(&tx, record_def.type_info->containees[0].al.type_info),

		get_gcd_aggregate_function(record_def.type_info->containees[0].al.type_info),

		get_bitwise_aggregate_function(record_def.type_info->containees[0].al.type_info, 1, 0),
		get_bitwise_aggregate_function(record_def.type_info->containees[0].al.type_info, 0, 1),
		get_bitwise_aggregate_function(record_def.type_info->containees[0].al.type_info, 0, 0),

		get_logical_aggregate_function(record_def.type_info->containees[0].al.type_info, 1, 0),
		get_logical_aggregate_function(record_def.type_info->containees[0].al.type_info, 0, 1),
		get_logical_aggregate_function(record_def.type_info->containees[0].al.type_info, 0, 0),
	};

	const positional_accessor aggregate_input_positions_SELF[] = {SELF};
	const positional_accessor aggregate_input_positions_0[] = {STATIC_POSITION(0)};
	const positional_accessor aggregate_input_positions_1[] = {STATIC_POSITION(1)};
	const positional_accessor aggregate_input_positions_2[] = {STATIC_POSITION(2)};
	const positional_accessor aggregate_input_positions_3[] = {STATIC_POSITION(3)};
	const positional_accessor aggregate_input_positions_4[] = {STATIC_POSITION(4)};

	const positional_accessor* AGGREGATE_INPUTS[] = {
		aggregate_input_positions_SELF,
		aggregate_input_positions_4,

		aggregate_input_positions_0,
		aggregate_input_positions_0,

		aggregate_input_positions_0,

		aggregate_input_positions_0,

		aggregate_input_positions_0,
		aggregate_input_positions_0,
		aggregate_input_positions_0,

		aggregate_input_positions_0,
		aggregate_input_positions_0,
		aggregate_input_positions_0,
	};

	positional_accessor FINAL_SORT_KEY[1] = {STATIC_POSITION(3)};

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	{
		operator_desc* strm_in = insert_stream_input_operator_descriptor(qpd, &rs, &record_def);

		operator_desc* sort = insert_sort_operator_descriptor(qpd, TUPLES_DOWN_COUNTER_INF, strm_in, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);

		operator_desc* agg = insert_sorted_aggregation_operator_descriptor(qpd, sort, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, sizeof(AGGREGATES) / sizeof(aggregate_function*), AGGREGATES, AGGREGATE_INPUTS);

		operator_desc* sort_final = insert_sort_operator_descriptor(qpd, TUPLES_DOWN_COUNTER_INF, agg, 1, FINAL_SORT_KEY, CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);
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