#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
#include<rhendb/query_plan.h>

#include<rhendb/function_compare.h>

#include<test_dataset_tuple_def.h>

#include<cutlery/stream_for_file_descriptor.h>

#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<signal.h>
#include<fcntl.h>

#define USERS_COUNT 100

#define TESTCASE_SIZE 1000000
#define INPUT_OPERATORS_COUNT 30

#define SMALLEST_RUN_SIZE              (1 * 1024 * 1024)
#define N_WAY_SORT                     16

#define RECORD_S_KEY_ELEMENT_COUNT 2

positional_accessor KEY_POS[2] = {STATIC_POSITION(0), STATIC_POSITION(2)};
compare_direction CMP_DIR[2] = {ASC, ASC};

#define BUFFER_SIZE 300

// generates sorted tuples with incremental values of % INPUT_OPERATORS_COUNT
void* generator(void* generator_context, const tuple_def* generator_tuple_def)
{
	int* generator_number = generator_context;

	if((*generator_number) >= TESTCASE_SIZE)
		return NULL;

	void* generated = malloc(BUFFER_SIZE);

	construct_record(generated, (*generator_number), 0, ((*generator_number) % 3 == 0) ? NULL : "Rohan Dvivedi");
	(*generator_number) += INPUT_OPERATORS_COUNT;

	return generated;
}

int main(int argc, char** argv)
{
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

	// make query

	query_plan_desc* qpd1 = get_new_query_plan_descriptor(&tx, NULL);
	int generator_contexts1[INPUT_OPERATORS_COUNT];
	{
		operator_desc* sorted_inputs1[INPUT_OPERATORS_COUNT];
		for(int i = 0; i < INPUT_OPERATORS_COUNT; i++)
		{
			generator_contexts1[i] = i;
			sorted_inputs1[i] = insert_generator_operator_descriptor(qpd1, generator, &(generator_contexts1[i]), &record_def);
		}

		operator_desc* union_op = insert_union_operator_descriptor(qpd1, sorted_inputs1, INPUT_OPERATORS_COUNT);

		operator_desc* sort_union_op = insert_sort_operator_descriptor(qpd1, TUPLES_DOWN_COUNTER_INF, union_op, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);
	}

	query_plan_desc* qpd2 = get_new_query_plan_descriptor(&tx, NULL);
	int generator_contexts2[INPUT_OPERATORS_COUNT];
	{
		operator_desc* sorted_inputs2[INPUT_OPERATORS_COUNT];
		for(int i = 0; i < INPUT_OPERATORS_COUNT; i++)
		{
			generator_contexts2[i] = i;
			sorted_inputs2[i] = insert_generator_operator_descriptor(qpd2, generator, &(generator_contexts2[i]), &record_def);
		}

		operator_desc* merge_sorted_op = insert_merge_sorted_inputs_operator_descriptor(qpd2, sorted_inputs2, INPUT_OPERATORS_COUNT, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR);
	}

	query_plan* qp1 = get_new_query_plan(qpd1);
	query_plan* qp2 = get_new_query_plan(qpd2);

	uint64_t matches_count = 0;
	while(1)
	{
		int error_code1 = 0;
		void* tuple1 = next_tuple_from_query_plan(qp1, &error_code1);

		int error_code2 = 0;
		void* tuple2 = next_tuple_from_query_plan(qp2, &error_code2);

		if(tuple1 == NULL && tuple2 == NULL)
		{
			printf("%"PRIu64" all matched\n", matches_count);
			break;
		}

		if(tuple1 == NULL || tuple2 == NULL)
		{
			printf("only %"PRIu64" all matched\n", matches_count);

			printf("tuple1 : ");
			if(tuple1 != NULL)
			{
				print_tuple(tuple1, qp1->desc->output_tuple_def);
				printf("\n");
			}
			else
				printf("NULL\n");

			printf("tuple2 : ");
			if(tuple2 != NULL)
			{
				print_tuple(tuple2, qp2->desc->output_tuple_def);
				printf("\n");
			}
			else
				printf("NULL\n");

			if(tuple1 != NULL)
				free(tuple1);
			if(tuple2 != NULL)
				free(tuple2);

			break;
		}

		if(0 == compare_tuples_rhendb(tuple1, qp1->desc->output_tuple_def, &SELF, tuple2, qp2->desc->output_tuple_def, &SELF, NULL, 1, &tx))
			matches_count++;
		else
		{
			printf("only %"PRIu64" all matched\n", matches_count);
			print_tuple(tuple1, qp1->desc->output_tuple_def);
			print_tuple(tuple2, qp2->desc->output_tuple_def);
		}

		free(tuple1);
		free(tuple2);
	}

	destroy_query_plan(qp1);
	destroy_query_plan(qp2);

	destroy_query_plan_descriptor(qpd1);
	destroy_query_plan_descriptor(qpd2);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}