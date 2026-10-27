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

#define PARTITIONS_COUNT                      64
#define PARALLEL_AGGREGATION_JOBS_COUNT       6
#define PARALLEL_AGGREGATION_JOBS_QUEUE_SIZE  6
#define MIN_BUILD_QUEUE_BUFFER_SIZE           (1 * 1024 * 1024)

#define SMALLEST_RUN_SIZE              (1 * 1024 * 1024)
#define PARALLEL_SORTING_JOBS_COUNT    8
#define N_WAY_SORT                     16

#define RECORD_S_KEY_ELEMENT_COUNT 1

positional_accessor KEY_POS[1] = {STATIC_POSITION(2)};
compare_direction CMP_DIR[1] = {ASC};

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

	{
		uint64_t op_index = 0;

		insert_stream_input_operator_descriptor(qpd, &rs, &record_def); op_index++;

		insert_hash_aggregation_operator_descriptor(qpd, 0, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, sizeof(AGGREGATES) / sizeof(aggregate_function*), AGGREGATES, AGGREGATE_INPUTS, PARTITIONS_COUNT, PARALLEL_AGGREGATION_JOBS_COUNT, PARALLEL_AGGREGATION_JOBS_QUEUE_SIZE, MIN_BUILD_QUEUE_BUFFER_SIZE); op_index++;

		insert_sort_operator_descriptor(qpd, TUPLES_DOWN_COUNTER_INF, 1, 1, FINAL_SORT_KEY, CMP_DIR, SMALLEST_RUN_SIZE, N_WAY_SORT, PARALLEL_SORTING_JOBS_COUNT); op_index++;

		insert_consumer_operator_descriptor(qpd, 2, print_consumer, NULL); op_index++;
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