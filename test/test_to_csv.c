// test_to_csv.c
//
// Reads a .vu dataset file (produced by test_build_dataset.c) and converts every
// record into a single CSV line, written to stdout (printf), ready to be loaded
// into PostgreSQL with:
//
//     COPY my_table FROM 'out.csv' WITH (FORMAT csv, NULL '');
//
// It uses a stream-input operator over the .vu file exactly like the other tests,
// pulls one tuple at a time from the query plan, and serializes it to CSV.
//
//     ./test_to_csv.out  s500.vu  > s500.csv
//     ./test_to_csv.out  < s500.vu > s500.csv      (reads stdin if no file arg)

#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/query_plan_descriptor.h>
#include<rhendb/query_plan.h>

#include<test_dataset_tuple_def.h>

#include<tuplelargetypes/materialized_numeric.h>

#include<cutlery/stream_for_file_descriptor.h>

#include<inttypes.h>
#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<fcntl.h>

#define USERS_COUNT 10

// ---- CSV field helpers ----------------------------------------------------

// write a text value as an RFC-4180 quoted CSV field (internal quotes doubled)
static void write_csv_text(const void* s, uint32_t n)
{
	const char* p = (const char*) s;
	putchar('"');
	for(uint32_t i = 0; i < n; i++)
	{
		if(p[i] == '"')
			putchar('"');
		putchar(p[i]);
	}
	putchar('"');
}

// write the inline numeric (field 5) as a plain decimal literal that PostgreSQL's
// NUMERIC accepts. We reconstruct it through the engine's own materialized_numeric
// -> libmpdec path, so the value printed is exactly what RhenDB stores.
static void write_csv_numeric(const void* record)
{
	datum nval; const data_type_info* ndti = NULL;
	get_value_from_element_from_tuple(&nval, &ndti, &record_def, STATIC_POSITION(5), record);
	if(is_datum_NULL(&nval))
		return; // empty field -> SQL NULL

	numeric_sign_bits sign_bits; int16_t exponent;
	extract_sign_bits_and_exponent_from_numeric(&sign_bits, &exponent, &nval, ndti);

	// base-10^12 digits live inline at sub-position 2 of the numeric; only finite
	// non-zero numerics carry them (zero / inf / nan are fully described by the prefix)
	uint64_t digits[64];
	uint32_t digits_count = 0;
	if(sign_bits == NEGATIVE_NUMERIC || sign_bits == POSITIVE_NUMERIC)
	{
		digits_count = get_element_count_for_element_from_tuple(&record_def, STATIC_POSITION(5, 2), record);
		if(digits_count > 64)
			digits_count = 64;
		for(uint32_t i = 0; i < digits_count; i++)
		{
			datum dd; const data_type_info* ddti = NULL;
			get_value_from_element_from_tuple(&dd, &ddti, &record_def, STATIC_POSITION(5, 2, i), record);
			digits[i] = dd.uint_value;
		}
	}

	materialized_numeric m;
	initialize_static_materialized_numeric(&m, sign_bits, exponent, digits, digits_count);

	mpd_t dec = decimal_from_materialized_numeric(&m);
	char* str = mpd_to_sci(&dec, 0);
	fputs(str, stdout);
	mpd_free(str); // frees the string returned by mpd_to_sci
	mpd_del(&dec); // frees the coefficient buffer of the returned-by-value mpd_t
}

// num, order, num_in_words, digits, value_in_string, some_numeric, some_float
static void record_to_csv(const void* record)
{
	datum uval; const data_type_info* dti = NULL;

	// 0) num : uint64
	get_value_from_element_from_tuple(&uval, &dti, &record_def, STATIC_POSITION(0), record);
	printf("%"PRIu64, uval.uint_value);

	// 1) order : int8
	get_value_from_element_from_tuple(&uval, &dti, &record_def, STATIC_POSITION(1), record);
	printf(",%"PRId64, uval.int_value);

	// 2) num_in_words : variable length string
	putchar(',');
	get_value_from_element_from_tuple(&uval, &dti, &record_def, STATIC_POSITION(2), record);
	if(!is_datum_NULL(&uval))
		write_csv_text(uval.string_value, uval.string_size);

	// 3) digits : array of uint8 -> PostgreSQL array literal "{d0,d1,...}"
	putchar(',');
	{
		uint32_t cnt = get_element_count_for_element_from_tuple(&record_def, STATIC_POSITION(3), record);
		putchar('"'); putchar('{');
		for(uint32_t i = 0; i < cnt; i++)
		{
			datum dd; const data_type_info* ddti = NULL;
			get_value_from_element_from_tuple(&dd, &ddti, &record_def, STATIC_POSITION(3, i), record);
			if(i != 0)
				putchar(',');
			printf("%"PRIu64, dd.uint_value);
		}
		putchar('}'); putchar('"');
	}

	// 4) value_in_string : nullable variable length string
	putchar(',');
	get_value_from_element_from_tuple(&uval, &dti, &record_def, STATIC_POSITION(4), record);
	if(!is_datum_NULL(&uval))
		write_csv_text(uval.string_value, uval.string_size);

	// 5) some_numeric : inline numeric
	putchar(',');
	write_csv_numeric(record);

	// 6) some_float : nullable float
	putchar(',');
	get_value_from_element_from_tuple(&uval, &dti, &record_def, STATIC_POSITION(6), record);
	if(!is_datum_NULL(&uval))
		printf("%g", uval.float_value);

	putchar('\n');
}

// ---------------------------------------------------------------------------

int main(int argc, char** argv)
{
	// input .vu stream: argv[1] if given, else stdin (fd 0)
	int in_fd = 0;
	if(argc > 1)
	{
		in_fd = open(argv[1], O_RDONLY);
		if(in_fd < 0)
		{
			fprintf(stderr, "could not open input file %s\n", argv[1]);
			return -1;
		}
	}
	stream rs;
	initialize_stream_for_fd(&rs, in_fd);

	// the engine's initialization chatter (print_tuple_def, etc.) goes to stdout;
	// keep stdout clean for CSV by diverting fd 1 to stderr for the duration of init
	int saved_stdout = dup(1);
	dup2(2, 1);

	rhendb rdb;
	initialize_rhendb(&rdb, "./test.db",
		5,
		512, 8, 80, 80,
			10000ULL, 100000ULL,
			10000000ULL,
		4096,
			10000000ULL,
		USERS_COUNT);

	initialize_tuple_defs();

	fflush(stdout);
	dup2(saved_stdout, 1); // restore the real stdout for CSV output
	close(saved_stdout);

	transaction tx;
	initialize_transaction(&tx, &rdb);

	query_plan_desc* qpd = get_new_query_plan_descriptor(&tx, NULL);

	insert_stream_input_operator_descriptor(qpd, &rs, &record_def);

	query_plan* qp = get_new_query_plan(qpd);

	while(1)
	{
		int error_code = 0;
		void* tuple = next_tuple_from_query_plan(qp, &error_code);
		if(tuple == NULL)
			break;
		record_to_csv(tuple);
		free(tuple);
	}

	destroy_query_plan(qp);
	destroy_query_plan_descriptor(qpd);

	deinitialize_transaction(&tx);
	deinitialize_tuple_defs();
	deinitialize_rhendb(&rdb);

	int stream_error = 0;
	close_stream(&rs, &stream_error);
	deinitialize_stream(&rs);

	fprintf(stderr, "TEST COMPLETED\n");
	return 0;
}