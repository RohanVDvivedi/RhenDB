#include<rhendb/rhendb.h>
#include<rhendb/rash_table.h>

#include<string.h>
#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<signal.h>

#define USERS_COUNT 10

#define TESTCASE_SIZE 1000000

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

void print_value(binary_read_iterator* value_bri_p)
{
	int abort_error_dummy = 0;
	while(1)
	{
		int finish = 0;
		consume_tuple_from_tuple_list(tuple, &record_def, value_bri_p, NULL, &abort_error_dummy, {
			if(tuple == NULL)
				finish = 1;
			else
			{
				printf("\t\t\t");print_tuple(tuple, &record_def);
			}
		});
		if(finish)
			break;
	}
}

void insert_rth(rash_table_handle* rth_p, uint32_t v)
{
	int abort_error = 0;

	char record[300];
	construct_record(record, v, 0, "Rohan Dvivedi");

	rash_table_key rtk = get_new_rash_table_key(record, &record_def, KEY_POS, RECORD_S_KEY_ELEMENT_COUNT, &(rth_p->rdb->persistent_acid_rage_engine), NULL, &abort_error);

	rash_table_iterator rti = find_equals_in_rash_table(rth_p, &rtk, 0, NULL, &abort_error);

	binary_write_iterator* bwi_p = open_for_writing_value_in_rash_table_iterator(&rti, NULL, &abort_error);

	int abort_error_dummy = 0;
	append_to_binary_write_iterator(bwi_p, record, get_tuple_size(&record_def, record), &(rth_p->rdb->volatile_rage_engine), &abort_error_dummy);

	close_and_write_value_in_hash_table_iterator(&rti, bwi_p);

	delete_rash_table_iterator(&rti);
}

int remove_rth(rash_table_handle* rth_p, uint32_t v)
{
	int abort_error = 0;

	char record[300];
	construct_record(record, v, 0, "Rohan Dvivedi");

	rash_table_key rtk = get_new_rash_table_key(record, &record_def, KEY_POS, RECORD_S_KEY_ELEMENT_COUNT, &(rth_p->rdb->persistent_acid_rage_engine), NULL, &abort_error);

	rash_table_iterator rti = find_equals_in_rash_table(rth_p, &rtk, 0, NULL, &abort_error);

	int removed = remove_from_rash_table_iterator(&rti);

	delete_rash_table_iterator(&rti);

	return removed;
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

	generate_random_inputs();

	initialize_tuple_defs();

	// create rash table
	rash_table_handle rth = get_new_rash_table(&record_def, KEY_POS, RECORD_S_KEY_ELEMENT_COUNT, &(rdb.persistent_acid_rage_engine), &rdb);

	// print all
	print_rash_table(&rth, print_value);

	// insert all
	printf("INSERTIONS STARTED\n");
	for(uint32_t i = 0; i < TESTCASE_SIZE; i++)
		insert_rth(&rth, inputs[i]);
	printf("INSERTIONS ENDED\n");

	// print all
	print_rash_table(&rth, print_value);

	// find all

	// remove all
	printf("REMOVES STARTED\n");
	uint32_t removes_success = 0;
	for(uint32_t i = 0; i < TESTCASE_SIZE; i+=2)
		removes_success += remove_rth(&rth, i);
	printf("REMOVES ENDED (%u)\n", removes_success);

	// print all
	print_rash_table(&rth, print_value);

	// remove all
	printf("REMOVES STARTED\n");
	removes_success = 0;
	for(uint32_t i = 1; i < TESTCASE_SIZE; i+=2)
		removes_success += remove_rth(&rth, i);
	printf("REMOVES ENDED (%u)\n", removes_success);

	// print all
	print_rash_table(&rth, print_value);

	// destroy rash table
	destroy_rash_table(&rth);

	deinitialize_tuple_defs();

	deinitialize_rhendb(&rdb);

	printf("TEST COMPLETED\n");

	return 0;
}