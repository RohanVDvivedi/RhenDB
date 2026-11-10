#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
#include<rhendb/query_plan.h>

#include<rhendb/function_compare.h>

#include<sqltoast/sqltoast.h>
#include<sqltoast/sql_expression_eval.h>

#include<rhendb/function_compare.h>

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

#define SMALLEST_RUN_SIZE              (1 * 1024 * 1024)
#define N_WAY_SORT                     16

#define RECORD_S_KEY_ELEMENT_COUNT 1

#define MIN_BLOCK_SIZE (1024 * 1024)

positional_accessor KEY_POS[1] = {STATIC_POSITION(2)};
compare_direction CMP_DIR[1] = {ASC};

int main(int argc, char** argv)
{
	dstring join_expr = get_dstring_pointing_to_literal_cstring("record.num_in_words = record2.num_in_words");
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

	initialize_tuple_defs();

	transaction tx;
	initialize_transaction(&tx, &rdb);

	positional_accessor O_KEY_POS[2] = {STATIC_POSITION(0,0), STATIC_POSITION(1,0)};
	compare_direction O_CMP_DIR[2] = {ASC, ASC};

	query_plan_desc* qpd1 = get_new_query_plan_descriptor(&tx, NULL);
	stream s1_1, s1_2;
	initialize_stream_for_fd(&s1_1, open(argv[1], O_RDONLY));
	initialize_stream_for_fd(&s1_2, open(argv[1], O_RDONLY));
	{
		operator_desc* input_0 = insert_stream_input_operator_descriptor(qpd1, &s1_1, &record_def);

		operator_desc* input_1 = insert_stream_input_operator_descriptor(qpd1, &s1_2, &record_def);

		operator_desc* input_1_ = insert_row_rename_operator_descriptor(qpd1, input_1, "record2", NULL, 0);

		operator_desc* bnlj = insert_block_nested_loop_join_operator_descriptor(qpd1, input_0, input_1_, join_expr_sql->expr, PRESERVE_NONE, MIN_BLOCK_SIZE);

		operator_desc* bnlj_sorted = insert_sort_operator_descriptor(qpd1, TUPLES_DOWN_COUNTER_INF, bnlj, 2, O_KEY_POS, O_CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);
	}

	query_plan_desc* qpd2 = get_new_query_plan_descriptor(&tx, NULL);
	stream s2_1, s2_2;
	initialize_stream_for_fd(&s2_1, open(argv[1], O_RDONLY));
	initialize_stream_for_fd(&s2_2, open(argv[1], O_RDONLY));
	{
		operator_desc* input_0 = insert_stream_input_operator_descriptor(qpd2, &s2_1, &record_def);

		operator_desc* input_1 = insert_stream_input_operator_descriptor(qpd2, &s2_2, &record_def);

		operator_desc* sort_0 = insert_sort_operator_descriptor(qpd2, TUPLES_DOWN_COUNTER_INF, input_0, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);

		operator_desc* sort_1 = insert_sort_operator_descriptor(qpd2, TUPLES_DOWN_COUNTER_INF, input_1, RECORD_S_KEY_ELEMENT_COUNT, KEY_POS, CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);

		operator_desc* sort_join = insert_sort_merge_join_operator_descriptor(qpd2, sort_0, KEY_POS, sort_1, KEY_POS, CMP_DIR, RECORD_S_KEY_ELEMENT_COUNT, PRESERVE_NONE, MIN_BLOCK_SIZE);

		operator_desc* sort_join_sorder = insert_sort_operator_descriptor(qpd2, TUPLES_DOWN_COUNTER_INF, sort_join, 2, O_KEY_POS, O_CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);
	}

	query_plan_desc* qpd3 = get_new_query_plan_descriptor(&tx, NULL);
	stream s3_1, s3_2;
	initialize_stream_for_fd(&s3_1, open(argv[1], O_RDONLY));
	initialize_stream_for_fd(&s3_2, open(argv[1], O_RDONLY));
	{
		operator_desc* input_0 = insert_stream_input_operator_descriptor(qpd3, &s3_1, &record_def);

		operator_desc* input_1 = insert_stream_input_operator_descriptor(qpd3, &s3_2, &record_def);

		operator_desc* hash_join = insert_hash_join_operator_descriptor(qpd3, input_0, KEY_POS, input_1, KEY_POS, RECORD_S_KEY_ELEMENT_COUNT, PRESERVE_NONE);

		operator_desc* hash_join_sorted = insert_sort_operator_descriptor(qpd3, TUPLES_DOWN_COUNTER_INF, hash_join, 2, O_KEY_POS, O_CMP_DIR, N_WAY_SORT, SMALLEST_RUN_SIZE);
	}


	query_plan* qp1 = get_new_query_plan(qpd1);
	query_plan* qp2 = get_new_query_plan(qpd2);
	query_plan* qp3 = get_new_query_plan(qpd3);

	uint64_t matches_count = 0;
	while(1)
	{
		int error_code1 = 0;
		void* tuple1 = next_tuple_from_query_plan(qp1, &error_code1);

		int error_code2 = 0;
		void* tuple2 = next_tuple_from_query_plan(qp2, &error_code2);

		int error_code3 = 0;
		void* tuple3 = next_tuple_from_query_plan(qp3, &error_code3);

		if(tuple1 == NULL && tuple2 == NULL && tuple3 == NULL)
		{
			printf("%"PRIu64" all matched\n", matches_count);
			break;
		}

		if(tuple1 == NULL || tuple2 == NULL || tuple3 == NULL)
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

			printf("tuple3 : ");
			if(tuple3 != NULL)
			{
				print_tuple(tuple3, qp3->desc->output_tuple_def);
				printf("\n");
			}
			else
				printf("NULL\n");

			if(tuple1 != NULL)
				free(tuple1);
			if(tuple2 != NULL)
				free(tuple2);
			if(tuple3 != NULL)
				free(tuple3);

			break;
		}

		if(0 == compare_tuples_rhendb(tuple1, qp1->desc->output_tuple_def, &SELF, tuple2, qp2->desc->output_tuple_def, &SELF, NULL, 1, &tx) &&
			0 == compare_tuples_rhendb(tuple2, qp2->desc->output_tuple_def, &SELF, tuple3, qp3->desc->output_tuple_def, &SELF, NULL, 1, &tx))
			matches_count++;
		else
		{
			printf("only %"PRIu64" all matched\n", matches_count);
			print_tuple(tuple1, qp1->desc->output_tuple_def);
			print_tuple(tuple2, qp2->desc->output_tuple_def);
			print_tuple(tuple3, qp3->desc->output_tuple_def);
			break;
		}

		free(tuple1);
		free(tuple2);
		free(tuple3);
	}

	destroy_query_plan(qp1);
	destroy_query_plan(qp2);
	destroy_query_plan(qp3);

	destroy_query_plan_descriptor(qpd1);
	destroy_query_plan_descriptor(qpd2);
	destroy_query_plan_descriptor(qpd3);

	delete_sql(join_expr_sql);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}