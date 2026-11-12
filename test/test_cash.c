#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/cash_table.h>

#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<signal.h>

#define USERS_COUNT 10

#define TESTCASE_SIZE 500000

#define BUCKET_COUNT (TESTCASE_SIZE / 30)

//#define DEBUG_PRINT

uint32_t inputs[TESTCASE_SIZE];
void generate_random_inputs()
{
	for(uint32_t i = 0; i < TESTCASE_SIZE; i++)
		inputs[i] = i;
	for(uint32_t i = 0; i < TESTCASE_SIZE; i++)
		memory_swap(inputs + (((uint32_t)rand())  % TESTCASE_SIZE), inputs + (((uint32_t)rand()) % TESTCASE_SIZE), sizeof(uint32_t));
}

data_type_info digits_type_info;
data_type_info num_in_words_type_info;
data_type_info value_string_type_info;
data_type_info* record_type_info;
tuple_def record_def;

#define RECORD_S_KEY_ELEMENT_COUNT 2

positional_accessor KEY_POS[2] = {STATIC_POSITION(0), STATIC_POSITION(2)};

void initialize_tuple_defs()
{
	record_type_info = malloc(sizeof_tuple_data_type_info(5));
	initialize_tuple_data_type_info(record_type_info, "record", 0, 900, 5);

	strcpy(record_type_info->containees[0].field_name, "num");
	record_type_info->containees[0].al.type_info = UINT_NON_NULLABLE[8];

	strcpy(record_type_info->containees[1].field_name, "order");
	record_type_info->containees[1].al.type_info = INT_NON_NULLABLE[1];

	num_in_words_type_info = get_variable_length_string_type("num_in_words", 70);
	strcpy(record_type_info->containees[2].field_name, "num_in_words");
	record_type_info->containees[2].al.type_info = &num_in_words_type_info;

	digits_type_info = get_variable_element_count_array_type("digits", 16, UINT_NON_NULLABLE[1]);
	strcpy(record_type_info->containees[3].field_name, "digits");
	record_type_info->containees[3].al.type_info = &digits_type_info;

	value_string_type_info = get_variable_length_string_type("value_in_string", 100);
	strcpy(record_type_info->containees[4].field_name, "value_in_string");
	record_type_info->containees[4].al.type_info = &value_string_type_info;

	initialize_tuple_def(&record_def, record_type_info);

	print_tuple_def(&record_def);
	printf("\n\n");
}

void deinitialize_tuple_defs()
{
	free(record_type_info);
}

const char *ones[] = {
  "zero", "one", "two", "three", "four", "five", "six", "seven",
  "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen",
  "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"
};

const char *tens[] = {
  "", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"
};

void num_in_words(char* output, uint16_t n) {
  if (n < 20) {
    strcpy(output, ones[n]);
  } else if (n < 100) {
    strcpy(output, tens[(n / 10) % 10]);
    if((n % 10) != 0) {
    	strcat(output, " ");
    	strcat(output, ones[n % 10]);
    }
  } else if (n < 1000) {
  	strcpy(output, ones[(n / 100) % 10]);
  	strcat(output, " hundred");
  	if(n % 100 != 0) {
  		strcat(output, " ");
			char temp[100];
			num_in_words(temp, n % 100);
  		strcat(output, temp);
  	}
  } else {
  	strcpy(output, "TOO00-BIG");
  }
}

uint16_t find_order(uint64_t num, int order)
{
	switch(order)
	{
		case 0:
			return (num / 1ULL) % 1000;
		case 1:
			return (num / 1000ULL) % 1000;
		case 2:
			return (num / 1000000ULL) % 1000;
		case 3:
			return (num / 1000000000ULL) % 1000;
		case 4:
		{
			printf("ORDER TOO BIG\n");
			exit(-1);
		}
	}
	return 0;
}

void construct_record(void* buffer, uint64_t num, int order, char* value)
{
	init_tuple(&record_def, buffer);

	set_element_in_tuple(&record_def, STATIC_POSITION(0), buffer, &(datum){.uint_value = num}, UINT32_MAX);

	uint16_t o = find_order(num, order);
	set_element_in_tuple(&record_def, STATIC_POSITION(1), buffer, &(datum){.int_value = order}, UINT32_MAX);

	char temp[100];
	num_in_words(temp, o);
	set_element_in_tuple(&record_def, STATIC_POSITION(2), buffer, &(datum){.string_value = temp, .string_size = strlen(temp)}, UINT32_MAX);

	{
		set_element_in_tuple(&record_def, STATIC_POSITION(3), buffer, EMPTY_DATUM, UINT32_MAX);
		uint32_t size = 0;
		uint32_t digits[64];
		while(num > 0)
		{
			digits[size++] = num % 10;
			num = num / 10;
		}
		expand_element_count_for_element_in_tuple(&record_def, STATIC_POSITION(3), buffer, 0, size, UINT32_MAX);
		for(uint32_t i = 0; i < size; i++)
			set_element_in_tuple(&record_def, STATIC_POSITION(3,i), buffer, &(datum){.uint_value = digits[i]}, UINT32_MAX);
	}

	if(value == NULL)
		set_element_in_tuple(&record_def, STATIC_POSITION(4), buffer, NULL_DATUM, UINT32_MAX);
	else
		set_element_in_tuple(&record_def, STATIC_POSITION(4), buffer, &(datum){.string_value = value, .string_size = strlen(value)}, UINT32_MAX);
}

void insert_ct(cash_table* ct_p, uint32_t v)
{
	char* record = malloc(300);
	construct_record(record, v, 0, "Rohan Dvivedi");

	cash_table_entry e = get_new_cash_table_value(ct_p, record);

	insert_in_cash_table(ct_p, &e);
}

void find_and_print(const cash_table* ct_p, uint32_t v)
{
	char record[300];
	construct_record(record, v, 0, "Rohan Dvivedi");

	cash_table_entry e = get_new_cash_table_key(ct_p, record, ct_p->value_tuple_def, ct_p->key_element_ids);

	printf("%"PRIu32"\n", v);
	for(cash_table_entry* it = find_equals_in_cash_table(ct_p, &e); it != NULL; it = next_equals_in_cash_table(ct_p, it))
	{
		printf("\t");
		print_tuple(it->value, &record_def);
	}
	printf("\n");
}

void print_cash_table(const cash_table* ct_p)
{
	for(cash_table_entry* it = find_all_in_cash_table(ct_p); it != NULL; it = next_all_in_cash_table(ct_p, it))
	{
		printf("\t");
		print_tuple(it->value, &record_def);
	}
	printf("\n");
}

int main()
{
	rhendb rdb;
	initialize_rhendb(&rdb, "./test.db",
		5,
		512, 8, 80, 80,
			10000ULL, 100000ULL,
			10000000ULL,
		4096,
			60000000ULL,
		USERS_COUNT);
	printf("database initialized\n\n");

	generate_random_inputs();

	initialize_tuple_defs();

	transaction tx;
	initialize_transaction(&tx, &rdb);

	// create cash table
	cash_table* ct_p = get_new_cash_table(200000, &record_def, KEY_POS, RECORD_S_KEY_ELEMENT_COUNT, &tx);

	// print all
	#ifdef DEBUG_PRINT
		print_cash_table(ct_p, print_value);
	#endif

	// insert all
	printf("INSERTIONS STARTED\n");
	for(uint32_t i = 0; i < TESTCASE_SIZE; i++)
		insert_ct(ct_p, inputs[i]);
	printf("INSERTIONS ENDED\n");

	// insert all again, duplicating the even values
	printf("INSERTIONS STARTED\n");
	for(uint32_t i = 0; i < TESTCASE_SIZE; i+=2)
		insert_ct(ct_p, i);
	printf("INSERTIONS ENDED\n");

	// print all
	#ifdef DEBUG_PRINT
		print_cash_table(ct_p, print_value);
	#endif

	#define FINDERS_SIZE 30
	uint32_t find_these[FINDERS_SIZE];
	for(uint32_t i = 0; i < FINDERS_SIZE; i++)
		find_these[i] = ((((uint32_t)rand()) % TESTCASE_SIZE) & (UINT32_MAX << 1)) | (i & 1);

	// find some of them
	for(uint32_t i = 0; i < FINDERS_SIZE; i++)
		find_and_print(ct_p, find_these[i]);

	// find some of them
	for(uint32_t i = 0; i < FINDERS_SIZE; i++)
		find_and_print(ct_p, find_these[i]);

	// print all
	#ifdef DEBUG_PRINT
		print_cash_table(ct_p, print_value);
	#endif

	// find some of them
	for(uint32_t i = 0; i < FINDERS_SIZE; i++)
		find_and_print(ct_p, find_these[i]);

	// print all
	#ifdef DEBUG_PRINT
		print_cash_table(ct_p, print_value);
	#endif

	// destroy cash table
	destroy_cash_table(ct_p);

	deinitialize_transaction(&tx);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}