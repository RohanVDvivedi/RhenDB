#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>
#include<rhendb/tuple_transformers.h>

#include<rhendb/util_transaction_ext_storer.h>

#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<signal.h>
#include<fcntl.h>

#define USERS_COUNT 10

rhendb rdb = {};
transaction* txn;
query_plan* qp = NULL;

tuple_def record_def;

void initialize_tuple_defs()
{
	data_type_info* record_type_info = malloc(sizeof_tuple_data_type_info(3));
	initialize_tuple_data_type_info(record_type_info, "record", 0, 900, 3);

	strcpy(record_type_info->containees[0].field_name, "key");
	record_type_info->containees[0].al.type_info = rdb.volatile_rage_engine.text_extended_type_info;

	strcpy(record_type_info->containees[1].field_name, "value_blob");
	record_type_info->containees[1].al.type_info = rdb.volatile_rage_engine.blob_extended_type_info;

	strcpy(record_type_info->containees[2].field_name, "value_numeric");
	record_type_info->containees[2].al.type_info = rdb.volatile_rage_engine.numeric_extended_type_info;

	initialize_tuple_def(&record_def, record_type_info);

	print_tuple_def(&record_def);
	printf("\n\n");
}

void insert_in_interim_tuple_store(interim_tuple_store* its_p, const char* key, const char* value_blob, const char* value_numeric)
{
	char record[900];
	init_tuple(&record_def, record);

	void* k = tx_temp_store_tb(key, strlen(key), record_def.type_info->containees[0].al.type_info, txn);
	void* vb = tx_temp_store_tb(value_blob, strlen(value_blob), record_def.type_info->containees[1].al.type_info, txn);
	mpd_context_t ctx;
    mpd_maxcontext(&ctx);
	mpd_t* number = mpd_new(&ctx);
	mpd_set_string(number, value_numeric, &ctx);
	void* vn = tx_temp_store_numeric(number, record_def.type_info->containees[2].al.type_info, txn);

	set_element_in_tuple(&record_def, STATIC_POSITION(0), record, &(datum){.tuple_value = k}, UINT32_MAX);
	set_element_in_tuple(&record_def, STATIC_POSITION(1), record, &(datum){.tuple_value = vb}, UINT32_MAX);
	set_element_in_tuple(&record_def, STATIC_POSITION(2), record, &(datum){.tuple_value = vn}, UINT32_MAX);

	append_tuple_to_interim_tuple_store(its_p, record, &(record_def.size_def));

	mpd_del(number);
	free(k);
	free(vb);
	free(vn);
}

int main()
{
	// create rdb
	initialize_rhendb(&rdb, "./test.db",
		6,
		4096, 8, 80, 80,
			10000ULL, 100000ULL,
			10000000ULL,
		4096,
			10000000ULL,
		USERS_COUNT);
	printf("database initialized\n\n");

	initialize_tuple_defs();

	// create a write only transaction
	transaction tx = initialize_transaction(&rdb);
	txn = &tx;
	// make it have a writer snapshot capable of writing
	tx.snapshot = get_new_transaction_id(&(rdb.tx_table), NULL);
	tx.transaction_id = &(tx.snapshot->self_transaction_id);

	// create a table
	uint64_t table_id = create_table(&(rdb.cat_mgr), tx.snapshot, "my_table", (const rhendb_attribute[4]){
		{
			.attribute_name = "mvcc_hdr",
			.base_type = RHENDB_MVCC_HEADER,
		},
		{
			.attribute_name = "key",
			.base_type = RHENDB_TEXT,
		},
		{
			.attribute_name = "value_blob",
			.base_type = RHENDB_BLOB,
		},
		{
			.attribute_name = "value_numeric",
			.base_type = RHENDB_NUMERIC,
		},
	}, 4);

	// read cache version of the table called ftbl
	fetched_table* ftbl = fetch_table_from_catalog_manager(&(rdb.cat_mgr), tx.snapshot, "my_table", table_id);

	// create sample interim tuple store for constant dataset operator for insertion
	interim_tuple_store* its_p = get_new_interim_tuple_store(4096);
	insert_in_interim_tuple_store(its_p, "Rohan", "\xdd\x11\xdd\x11", "29.5");
	insert_in_interim_tuple_store(its_p, "Devashree", "\x44\x10\x01\x55\x11\x11", "40.2");
	insert_in_interim_tuple_store(its_p, "Rupa", "\x44\x10\x01\x55\x11\x11\xdd\x11\xdd\x11", "60");

	{
		qp = get_new_query_plan(&tx, 30);

		positional_accessor input_positions[3] = {STATIC_POSITION(0), STATIC_POSITION(1), STATIC_POSITION(2)};
		// insert these rows into the table
		{
			operator* input_operator = get_new_registered_operator_for_query_plan(qp);
			setup_constant_dataset_operator(input_operator, its_p, &record_def);

			operator* insertion_operator = get_new_registered_operator_for_query_plan(qp);
			setup_insertion_operator(insertion_operator, input_operator, input_positions, ftbl, TABLE_ID_IN_OUTPUT | PARTITION_ID_IN_OUTPUT | TUPLE_POINTER_IN_OUTPUT | HEAP_TUPLE_IN_OUTPUT, NULL, 0);

			operator* print_operator = get_new_registered_operator_for_query_plan(qp);
			setup_consumer_operator(print_operator, insertion_operator, print_consumer, NULL);
		}

		start_all_operators_for_query_plan(qp);

		wait_for_shutdown_of_query_plan(qp);

		dstring kill_reasons = new_dstring("", 0);
		destroy_query_plan(qp, &kill_reasons);

		printf("\n\nKILL REASONS : \n");
		printf_dstring(&kill_reasons);
		deinit_dstring(&kill_reasons);
		printf("\n\nKILL REASONS END\n\n");
	}

	// a query completed so reset it's ext stores
	reset_temp_ext_stores_in_transaction(&tx);

	// run a sample pipeline
	{
		qp = get_new_query_plan(&tx, 30);

		{
			operator* input_operator = get_new_registered_operator_for_query_plan(qp);
			setup_scan_operator(input_operator, qp, ftbl, 1, HEAP_TUPLE_IN_OUTPUT, 0);

			operator* print_operator = get_new_registered_operator_for_query_plan(qp);
			setup_consumer_operator(print_operator, input_operator, print_consumer, NULL);
		}

		start_all_operators_for_query_plan(qp);

		wait_for_shutdown_of_query_plan(qp);

		dstring kill_reasons = new_dstring("", 0);
		destroy_query_plan(qp, &kill_reasons);

		printf("\n\nKILL REASONS : \n");
		printf_dstring(&kill_reasons);
		deinit_dstring(&kill_reasons);
		printf("\n\nKILL REASONS END\n\n");
	}

	// a query completed so reset it's ext stores
	reset_temp_ext_stores_in_transaction(&tx);

	// create another dataset for constant dataset operator
	its_p = get_new_interim_tuple_store(4096);
	insert_in_interim_tuple_store(its_p, "Rohan", "\xdd\x11\xdd\x11", "29.5");
	insert_in_interim_tuple_store(its_p, "Devashree", "\x44\x10\x01\x55\x11\x11", "40.2");
	insert_in_interim_tuple_store(its_p, "Vipulkumar", "\x44\xdd\x11\xdd\x11", "59.6");

	// run a sample pipeline
	{
		qp = get_new_query_plan(&tx, 30);

		const positional_accessor key_pos[] = {STATIC_POSITION(0)};

		aggregate_function* const AGGREGATES[] = {
			get_count_aggregate_function(rdb.union_numeric_type_info),

			get_min_max_aggregate_function(&tx, rdb.union_numeric_type_info, 1), // 1 is min
			get_min_max_aggregate_function(&tx, rdb.union_numeric_type_info, 0), // 0 as last param means max

			get_sum_aggregate_function(&tx, rdb.union_numeric_type_info),

			get_concat_aggregate_function(&tx, rdb.union_text_type_info, get_dstring_pointing_to_literal_cstring("[ "), get_dstring_pointing_to_literal_cstring(" , "), get_dstring_pointing_to_literal_cstring(" ]")),
			get_concat_aggregate_function(&tx, rdb.union_blob_type_info, get_dstring_pointing_to_literal_cstring("\x12\x23"), get_dstring_pointing_to_literal_cstring("\x45\x56"), get_dstring_pointing_to_literal_cstring("\x78\x89")),
		};

		const positional_accessor aggregate_input_positions_0[] = {STATIC_POSITION(0)};
		const positional_accessor aggregate_input_positions_1[] = {STATIC_POSITION(1)};
		const positional_accessor aggregate_input_positions_2[] = {STATIC_POSITION(2)};

		const positional_accessor* AGGREGATE_INPUTS[] = {
			aggregate_input_positions_2,
			aggregate_input_positions_2,
			aggregate_input_positions_2,
			aggregate_input_positions_2,
			aggregate_input_positions_0,
			aggregate_input_positions_1,
		};

		compare_direction CMP_DIR[1] = {ASC};

		positional_accessor* projections_for_table[3] = {&STATIC_POSITION(0,1), &STATIC_POSITION(0,2), &STATIC_POSITION(0,3)};
		{
			operator* input_operator1 = get_new_registered_operator_for_query_plan(qp);
			setup_scan_operator(input_operator1, qp, ftbl, 1, HEAP_TUPLE_IN_OUTPUT, 0);
			tuple_transformer* projs = get_new_simple_projection_transformer("projecteds", get_tuple_def_for_tuples_to_be_consumed_from(input_operator1), 3, projections_for_table, ((char const *[]){"column1", "column2", "column3"}));
			append_tuple_transformer(&(input_operator1->output_tuple_transformers), projs);
			append_tuple_transformer(&(input_operator1->output_tuple_transformers), get_new_unified_typing_transformer(get_tuple_def_for_tuples_to_be_consumed_from(input_operator1), &tx, (uint32_t[]){0,1,2}, 3));

			operator* input_operator2 = get_new_registered_operator_for_query_plan(qp);
			setup_constant_dataset_operator(input_operator2, its_p, &record_def);
			append_tuple_transformer(&(input_operator2->output_tuple_transformers), get_new_unified_typing_transformer(get_tuple_def_for_tuples_to_be_consumed_from(input_operator2), &tx, (uint32_t[]){0,1,2}, 3));

			operator* union_operator = get_new_registered_operator_for_query_plan(qp);
			setup_union_operator(union_operator, (operator* []){input_operator1, input_operator2}, 2);

			operator* aggregate_operator = get_new_registered_operator_for_query_plan(qp);
			setup_hash_aggregation_operator(aggregate_operator, union_operator, 1, key_pos, sizeof(AGGREGATES) / sizeof(aggregate_function*), AGGREGATES, AGGREGATE_INPUTS, 4, 1, 1, 1000);

			operator* sorter_operator = get_new_registered_operator_for_query_plan(qp);
			setup_sort_operator(sorter_operator, TUPLES_DOWN_COUNTER_INF, aggregate_operator, 1, key_pos, CMP_DIR, 10, 2, 1);

			operator* print_operator = get_new_registered_operator_for_query_plan(qp);
			setup_consumer_operator(print_operator, sorter_operator, print_consumer, NULL);
		}

		start_all_operators_for_query_plan(qp);

		wait_for_shutdown_of_query_plan(qp);

		dstring kill_reasons = new_dstring("", 0);
		destroy_query_plan(qp, &kill_reasons);

		printf("\n\nKILL REASONS : \n");
		printf_dstring(&kill_reasons);
		deinit_dstring(&kill_reasons);
		printf("\n\nKILL REASONS END\n\n");
	}

	// a query completed so reset it's ext stores
	reset_temp_ext_stores_in_transaction(&tx);

	// wind up

	destroy_fetched_table(ftbl);

	deinitialize_transaction(&tx);

	// let the system know that it was a crashed transaction, by not committing or aborting it

	free(record_def.type_info);

	deinitialize_rhendb(&rdb);
}