#include<rhendb/rhendb.h>

#include<rhendb/transaction.h>
#include<rhendb/operators.h>

#include<stdlib.h>
#include<unistd.h>
#include<signal.h>

#define USERS_COUNT 10

#define OPERATOR_BUFFERS_COUNT 3
#define IDENTITY_OPERATORS_COUNT (OPERATOR_BUFFERS_COUNT-1)
#define TOTAL_OPERATORS_COUNT (IDENTITY_OPERATORS_COUNT+2)

data_type_info digits_type_info;
data_type_info num_in_words_type_info;
data_type_info value_string_type_info;
data_type_info* record_type_info;
tuple_def record_def;

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

	set_element_in_tuple(&record_def, STATIC_POSITION(0), buffer, &(user_value){.uint_value = num}, UINT32_MAX);

	uint16_t o = find_order(num, order);
	set_element_in_tuple(&record_def, STATIC_POSITION(1), buffer, &(user_value){.int_value = order}, UINT32_MAX);

	char temp[100];
	num_in_words(temp, o);
	set_element_in_tuple(&record_def, STATIC_POSITION(2), buffer, &(user_value){.string_value = temp, .string_size = strlen(temp)}, UINT32_MAX);

	{
		set_element_in_tuple(&record_def, STATIC_POSITION(3), buffer, EMPTY_USER_VALUE, UINT32_MAX);
		uint32_t size = 0;
		uint32_t digits[64];
		while(num > 0)
		{
			digits[size++] = num % 10;
			num = num / 10;
		}
		expand_element_count_for_element_in_tuple(&record_def, STATIC_POSITION(3), buffer, 0, size, UINT32_MAX);
		for(uint32_t i = 0; i < size; i++)
			set_element_in_tuple(&record_def, STATIC_POSITION(3,i), buffer, &(user_value){.uint_value = digits[i]}, UINT32_MAX);
	}

	if(value == NULL)
		set_element_in_tuple(&record_def, STATIC_POSITION(4), buffer, NULL_USER_VALUE, UINT32_MAX);
	else
		set_element_in_tuple(&record_def, STATIC_POSITION(4), buffer, &(user_value){.string_value = value, .string_size = strlen(value)}, UINT32_MAX);
}

#define BUFFER_SIZE 300

void* generator(void* generator_context, tuple_def* generator_tuple_def)
{
	int* generator_number = generator_context;

	if((*generator_number) >= 50)
		return NULL;

	void* generated = malloc(BUFFER_SIZE);

	construct_record(generated, (*generator_number), 0, "Rohan Dvivedi");
	usleep(100000);

	(*generator_number)++;
	return generated;
}

query_plan* qp = NULL;

void intHandler(int dummy)
{
	printf("\nCaught Ctrl+C! shuting down query plan\n");
	shutdown_query_plan(qp, get_dstring_pointing_to_literal_cstring("CTRL+C pressed!!"));
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

	int generator_number = 0;
	initialize_tuple_defs();

	transaction tx = initialize_transaction(&rdb);

	qp = get_new_query_plan(&tx, TOTAL_OPERATORS_COUNT, OPERATOR_BUFFERS_COUNT);

	// make operators

	{
		operator_buffer* input = get_new_registered_operator_buffer_for_query_plan(qp);
		operator* o = get_new_registered_operator_for_query_plan(qp);

		setup_generator_operator(o, input, generator, &generator_number, &record_def);

		for(int i = 0; i < IDENTITY_OPERATORS_COUNT; i++)
		{
			operator_buffer* output = get_new_registered_operator_buffer_for_query_plan(qp);
			o = get_new_registered_operator_for_query_plan(qp);
			setup_identity_operator(o, output, input);
			input = output;
		}

		o = get_new_registered_operator_for_query_plan(qp);
		setup_printf_operator(o, input, &record_def);
	}

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

	printf("TEST COMPLETED");

	return 0;
}