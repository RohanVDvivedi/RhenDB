
data_type_info digits_type_info;
data_type_info num_in_words_type_info;
data_type_info value_string_type_info;
data_type_info* record_type_info;
tuple_def record_def;

#include<tuplelargetypes/numeric_extended.h>

void initialize_tuple_defs()
{
	record_type_info = malloc(sizeof_tuple_data_type_info(7));
	initialize_tuple_data_type_info(record_type_info, "record", 0, 900, 7);

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

	strcpy(record_type_info->containees[5].field_name, "some_numeric");
	record_type_info->containees[5].al.type_info = get_numeric_inline_type_info(64);

	strcpy(record_type_info->containees[6].field_name, "some_float");
	record_type_info->containees[6].al.type_info = FLOAT_float_NULLABLE;

	initialize_tuple_def(&record_def, record_type_info);

	print_tuple_def(&record_def);
	printf("\n\n");
}

void deinitialize_tuple_defs()
{
	free(record_type_info->containees[5].al.type_info->containees[2].al.type_info);
	free(record_type_info->containees[5].al.type_info);
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

	set_element_in_tuple(&record_def, STATIC_POSITION(5), buffer, EMPTY_DATUM, UINT32_MAX);
	if(num == 0)
		set_sign_bits_and_exponent_for_numeric(ZERO_NUMERIC, 0, buffer, &record_def, STATIC_POSITION(5));
	else if(num & 1)
		set_sign_bits_and_exponent_for_numeric(NEGATIVE_NUMERIC, 2, buffer, &record_def, STATIC_POSITION(5));
	else
		set_sign_bits_and_exponent_for_numeric(POSITIVE_NUMERIC, 2, buffer, &record_def, STATIC_POSITION(5));
	if(num != 0)
	{
		set_element_in_tuple(&record_def, STATIC_POSITION(5,2), buffer, EMPTY_DATUM, UINT32_MAX);
		expand_element_count_for_element_in_tuple(&record_def, STATIC_POSITION(5,2), buffer, 0, 3, UINT32_MAX);
		set_element_in_tuple(&record_def, STATIC_POSITION(5,2,0), buffer, &(datum){.uint_value = num % 1000000000000ULL}, UINT32_MAX);
		set_element_in_tuple(&record_def, STATIC_POSITION(5,2,0), buffer, &(datum){.uint_value = (((uint64_t)num) * num) % 1000000000000ULL}, UINT32_MAX);
		set_element_in_tuple(&record_def, STATIC_POSITION(5,2,0), buffer, &(datum){.uint_value = (((uint64_t)num) * num * num) % 1000000000000ULL}, UINT32_MAX);
	}

	set_element_in_tuple(&record_def, STATIC_POSITION(6), buffer, &(datum){.float_value = (((float)num)/100) * ((num & 1) ? -1 : 1)}, UINT32_MAX);
}