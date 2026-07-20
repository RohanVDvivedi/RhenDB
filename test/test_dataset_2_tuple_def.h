
data_type_info num_in_words_type_info2;
data_type_info* record_type_info2;
tuple_def record_def2;

void initialize_tuple_defs2()
{
	record_type_info2 = malloc(sizeof_tuple_data_type_info(2));
	initialize_tuple_data_type_info(record_type_info2, "record2", 0, 900, 2);

	strcpy(record_type_info2->containees[0].field_name, "num2");
	record_type_info2->containees[0].al.type_info = LARGE_INT_NON_NULLABLE[8];

	num_in_words_type_info2 = get_variable_length_string_type("num_in_words2", 70);
	strcpy(record_type_info2->containees[1].field_name, "num_in_words2");
	record_type_info2->containees[1].al.type_info = &num_in_words_type_info2;

	initialize_tuple_def(&record_def2, record_type_info2);

	print_tuple_def(&record_def2);
	printf("\n\n");
}

void deinitialize_tuple_defs2()
{
	free(record_type_info2);
}

void construct_record2(void* buffer, uint64_t num)
{
	init_tuple(&record_def2, buffer);

	set_element_in_tuple(&record_def2, STATIC_POSITION(0), buffer, &(datum){.large_int_value = num}, UINT32_MAX);

	char temp[100];
	num_in_words(temp, find_order(num, 0));
	set_element_in_tuple(&record_def2, STATIC_POSITION(1), buffer, &(datum){.string_value = temp, .string_size = strlen(temp)}, UINT32_MAX);
}