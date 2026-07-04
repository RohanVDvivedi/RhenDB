#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>
#include<rhendb/tuple_transformers.h>

#include<test_dataset_tuple_def.h>

#include<stdlib.h>
#include<unistd.h>
#include<signal.h>

#define USERS_COUNT 10

#define TOTAL_OPERATORS_COUNT 10

query_plan* qp = NULL;

void intHandler(int dummy)
{
	printf("\nCaught Ctrl+C! shuting down query plan\n");
	shutdown_query_plan(qp, get_dstring_pointing_to_literal_cstring("CTRL+C pressed!!"));
}

int main()
{
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

	qp = get_new_query_plan(&tx, TOTAL_OPERATORS_COUNT);

	// make operators

	interim_tuple_store* its_p = get_new_interim_tuple_store(4096);
	for(int i = 40; i < 151; i++)
	{
		char buffer[300];
		construct_record(buffer, i, 0, (i % 3 == 0) ? NULL : "Rohan Dvivedi");

		append_tuple_to_interim_tuple_store(its_p, buffer, &(record_def.size_def));
	}

	const char* field_names[] = {
		"row_id",
		"row_no",
		"data",
	};

	positional_accessor* projections[] = {
		&STATIC_POSITION(1,0),
		&STATIC_POSITION(0),
		&STATIC_POSITION(1,1),
	};

	printf("Building pipeline :\n");
	{
		operator* source = get_new_registered_operator_for_query_plan(qp);
		setup_constant_dataset_operator(source, its_p, &record_def);
		printf("source operator %p\n", source);
		append_tuple_transformer(&(source->output_tuple_transformers), get_new_row_identifier_prepender_transformer(get_tuple_def_for_tuples_to_be_consumed_from(source), 7));
		append_tuple_transformer(&(source->output_tuple_transformers), get_new_row_number_prepender_transformer(get_tuple_def_for_tuples_to_be_consumed_from(source), 1));
		append_tuple_transformer(&(source->output_tuple_transformers), get_new_simple_projection_transformer("output", get_tuple_def_for_tuples_to_be_consumed_from(source), sizeof(field_names)/sizeof(field_names[0]), projections, field_names));

		operator* sink = get_new_registered_operator_for_query_plan(qp);
		setup_consumer_operator(sink, source, print_consumer, NULL);
		printf("sink operator %p\n", sink);
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