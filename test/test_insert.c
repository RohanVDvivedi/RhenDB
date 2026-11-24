#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan.h>
#include<rhendb/dml.h>

#include<test_dataset_tuple_def.h>

#include<cutlery/stream_for_file_descriptor.h>
#include<cutlery/stream_for_dstring.h>

#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<signal.h>
#include<fcntl.h>

#define USERS_COUNT 10

//#define SCAN_POST_INSERTS

#define MIN_RUN_SIZE  (1024 * 1024)
#define MIN_PART_SIZE (1024 * 1024)

int main()
{
	stream rs, ws;
	initialize_stream_for_fd(&rs, 0);
	initialize_stream_for_fd(&ws, 1);

	// create rdb
	rhendb rdb;
	initialize_rhendb(&rdb, "./test.db",
		8,
		8192, 16, 128 * 1024 * 1024 / 4096, 32 * 1024 * 1024 / 512,
			10000ULL, 100000ULL,
			10000000ULL,
		8192,
			10000000ULL,
		USERS_COUNT);
	printf("database initialized\n\n");

	initialize_tuple_defs();

	// create a write only transaction
	transaction tx;
	initialize_transaction(&tx, &rdb);

	// make it have a writer snapshot capable of writing
	tx.snapshot = get_new_transaction_id(&(rdb.tx_table), NULL);

	// create a table
	uint64_t table_id = create_table(&(rdb.cat_mgr), tx.snapshot, "records", (const rhendb_attribute[13]){
		{
			.attribute_name = "mvcc_hdr",
			.base_type = RHENDB_MVCC_HEADER,
		},
		{
			.attribute_name = "num",
			.base_type = RHENDB_UINT,
			.size = 8,
		},
		{
			.attribute_name = "order",
			.base_type = RHENDB_INT,
			.size = 1,
		},
		{
			.attribute_name = "num_in_words",
			.base_type = RHENDB_TEXT,
		},
		{
			.attribute_name = "digits_0",
			.base_type = RHENDB_UINT,
			.size = 1,
		},
		{
			.attribute_name = "digits_1",
			.base_type = RHENDB_UINT,
			.size = 1,
		},
		{
			.attribute_name = "digits_2",
			.base_type = RHENDB_UINT,
			.size = 1,
		},
		{
			.attribute_name = "digits_3",
			.base_type = RHENDB_UINT,
			.size = 1,
		},
		{
			.attribute_name = "digits_4",
			.base_type = RHENDB_UINT,
			.size = 1,
		},
		{
			.attribute_name = "digits_5",
			.base_type = RHENDB_UINT,
			.size = 1,
		},
		{
			.attribute_name = "value_in_string",
			.base_type = RHENDB_TEXT,
		},
		{
			.attribute_name = "some_numeric",
			.base_type = RHENDB_NUMERIC,
		},
		{
			.attribute_name = "some_float",
			.base_type = RHENDB_FLOAT,
			.size = sizeof(float),
		},
	}, 13);

	// read cache version of the table called ftbl
	fetched_table* ftbl = fetch_table_from_catalog_manager(&rdb, tx.snapshot, "records", table_id);

	// create sample interim tuple store for constant dataset operator for insertion
	interim_tuple_store* its_p = get_new_interim_tuple_store(4096);

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	{
		{
			operator_desc* strm_in = insert_stream_input_operator_descriptor(qpd, &rs, &record_def);

			projection_description* pd = alloca(sizeof(projection_description) * 12);
			pd[0] = project_from(NULL, STATIC_POSITION(0));
			pd[1] = project_from(NULL, STATIC_POSITION(1));
			pd[2] = project_from(NULL, STATIC_POSITION(2));
			pd[3] = project_from(NULL, STATIC_POSITION(3, 0));
			pd[4] = project_from(NULL, STATIC_POSITION(3, 1));
			pd[5] = project_from(NULL, STATIC_POSITION(3, 2));
			pd[6] = project_from(NULL, STATIC_POSITION(3, 3));
			pd[7] = project_from(NULL, STATIC_POSITION(3, 4));
			pd[8] = project_from(NULL, STATIC_POSITION(3, 5));
			pd[9] = project_from(NULL, STATIC_POSITION(4));
			pd[10] = project_from(NULL, STATIC_POSITION(5));
			pd[11] = project_from(NULL, STATIC_POSITION(6));
			operator_desc* proj = insert_projection_operator_descriptor(qpd, strm_in, "record", pd, 12);
		}

		query_plan* qp = get_new_query_plan(qpd);

		while(1)
		{
			int error_code = 0;
			void* tuple = next_tuple_from_query_plan(qp, &error_code);
			if(tuple == NULL)
				break;
			//print_tuple(tuple, qp->desc->output_tuple_def);
			append_tuple_to_interim_tuple_store2(its_p, &(its_p->embed_regions[0]), tuple, &(qp->desc->output_tuple_def->size_def), 1024 * 1024);
			free(tuple);
		}

		destroy_query_plan(qp);
	}

	int dml_error = 0;
	uint64_t inserts_done = dml_insert(
							NULL,
							its_p,
							qpd->output_tuple_def,
							(positional_accessor[]){STATIC_POSITION(0),STATIC_POSITION(1),STATIC_POSITION(2),STATIC_POSITION(3),STATIC_POSITION(4),STATIC_POSITION(5),STATIC_POSITION(6),STATIC_POSITION(7),STATIC_POSITION(8),STATIC_POSITION(9),STATIC_POSITION(10),STATIC_POSITION(11)},
							ftbl,
							&tx,
							0,
							0,
							&dml_error);
	printf("inserts_done = %"PRIu64", dml_error = %d\n", inserts_done, dml_error);

	destroy_query_plan_descriptor(qpd);

	delete_interim_tuple_store(its_p);

	// a query completed so reset it's ext stores
	reset_temp_ext_stores_in_transaction(&tx);

	#ifdef SCAN_POST_INSERTS
	{
		query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

		insert_scan_operator_descriptor(qpd, ftbl, HEAP_TUPLE_IN_OUTPUT, 0);

		query_plan* qp = get_new_query_plan(qpd);

		uint64_t scan_counter = 0;
		while(1)
		{
			int error_code = 0;
			void* tuple = next_tuple_from_query_plan(qp, &error_code);
			if(tuple == NULL)
				break;
			scan_counter++;
			print_tuple(tuple, qp->desc->output_tuple_def);
			free(tuple);
		}
		printf("post scan counted %"PRIu64" tuples\n", scan_counter);

		destroy_query_plan(qp);

		destroy_query_plan_descriptor(qpd);
	}
	#endif

	update_transaction_status(&(rdb.tx_table), tx.snapshot, TX_COMMITTED);
	tx.snapshot = NULL;

	// wind up

	destroy_fetched_table(ftbl);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);
}