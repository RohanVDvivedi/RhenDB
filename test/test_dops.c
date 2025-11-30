#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>

#include<stdlib.h>

#define USERS_COUNT 10

#define OPERATOR_BUFFERS_COUNT 2
#define IDENTITY_OPERATORS_COUNT (OPERATOR_BUFFERS_COUNT-1)
#define TOTAL_OPERATORS_COUNT (IDENTITY_OPERATORS_COUNT+2)

void* generator(void* generator_context, tuple_def* generator_tuple_def)
{
	return NULL;
}

int main()
{
	rhendb rdb;
	initialize_rhendb(&rdb, "./test.db",
		5,
		512, 8, 10, 10,
			10000ULL, 100000ULL,
			10000000ULL,
		4096,
			10000000ULL,
		USERS_COUNT);
	printf("database initialized\n\n");

	void* generator_context = NULL;
	tuple_def* generator_tuple_def = NULL;

	transaction tx = initialize_transaction(&rdb);

	query_plan* qp = get_new_query_plan(&tx, TOTAL_OPERATORS_COUNT, OPERATOR_BUFFERS_COUNT);

	// make operators

	{
		operator_buffer* input = get_new_registered_operator_buffer_for_query_plan(qp);
		operator* o = get_new_registered_operator_for_query_plan(qp);

		setup_generator_operator(o, input, generator, generator_context, generator_tuple_def);

		for(int i = 0; i < IDENTITY_OPERATORS_COUNT; i++)
		{
			operator_buffer* output = get_new_registered_operator_buffer_for_query_plan(qp);
			o = get_new_registered_operator_for_query_plan(qp);
			setup_identity_operator(o, output, input);
			input = output;
		}

		o = get_new_registered_operator_for_query_plan(qp);
		setup_printf_operator(o, input, generator_tuple_def);
	}

	// make operators completed

	start_all_operators_for_query_plan(qp);

	wait_for_shutdown_of_query_plan(qp);

	dstring kill_reasons = new_dstring("", 0);
	destroy_query_plan(qp, &kill_reasons);

	printf("\n\nKILL REASONE : \n");
	printf_dstring(&kill_reasons);
	printf("\n\nKILL REASONS END\n\n");

	deinitialize_transaction(&tx);

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED");

	return 0;
}