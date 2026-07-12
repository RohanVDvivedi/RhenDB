#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>
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

	initialize_tuple_defs();

	transaction tx = initialize_transaction(&rdb);

	qp = get_new_query_plan(&tx, 30);

	// make operators

	aggregate_function* const AGGREGATES[] = {
		get_count_aggregate_function(record_def.type_info),
		get_count_aggregate_function(record_def.type_info->containees[4].al.type_info),

		get_min_max_aggregate_function(&rdb, record_def.type_info->containees[0].al.type_info, 1), // 1 is min
		get_min_max_aggregate_function(&rdb, record_def.type_info->containees[0].al.type_info, 0), // 0 as last param means max

		get_sum_aggregate_function(record_def.type_info->containees[0].al.type_info),

		get_min_max_aggregate_function(&rdb, record_def.type_info->containees[1].al.type_info, 1),
		get_min_max_aggregate_function(&rdb, record_def.type_info->containees[1].al.type_info, 0),

		get_min_max_aggregate_function(&rdb, record_def.type_info->containees[2].al.type_info, 1),
		get_min_max_aggregate_function(&rdb, record_def.type_info->containees[2].al.type_info, 0),

		get_min_max_aggregate_function(&rdb, record_def.type_info->containees[3].al.type_info, 1),
		get_min_max_aggregate_function(&rdb, record_def.type_info->containees[3].al.type_info, 0),

		get_min_max_aggregate_function(&rdb, record_def.type_info->containees[4].al.type_info, 1),
		get_min_max_aggregate_function(&rdb, record_def.type_info->containees[4].al.type_info, 0),

		get_min_max_aggregate_function(&rdb, record_def.type_info, 1),
		get_min_max_aggregate_function(&rdb, record_def.type_info, 0),
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

		aggregate_input_positions_1,
		aggregate_input_positions_1,

		aggregate_input_positions_2,
		aggregate_input_positions_2,

		aggregate_input_positions_3,
		aggregate_input_positions_3,

		aggregate_input_positions_4,
		aggregate_input_positions_4,

		aggregate_input_positions_SELF,
		aggregate_input_positions_SELF,
	};

	printf("Building pipeline :\n");
	{
		operator* input_operator = get_new_registered_operator_for_query_plan(qp);
		setup_stream_input_operator(input_operator, &rs, &record_def);
		printf("source operator %p\n", input_operator);

		operator* aggregate_operator = get_new_registered_operator_for_query_plan(qp);
		setup_simple_aggregation_operator(aggregate_operator, input_operator, sizeof(AGGREGATES) / sizeof(aggregate_function*), AGGREGATES, AGGREGATE_INPUTS);
		printf("aggregate operator %p\n", aggregate_operator);

		operator* print_operator = get_new_registered_operator_for_query_plan(qp);
		setup_consumer_operator(print_operator, aggregate_operator, print_consumer, NULL);
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

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}