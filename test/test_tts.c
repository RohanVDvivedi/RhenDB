#include<rhendb/temp_tuple_store.h>

#include<unistd.h>
#include<stdio.h>
#include<stdlib.h>

tuple_def tpl_d;
data_type_info dti;

int main()
{
	printf("page_size = %"PRIu64"\n\n", sysconf(_SC_PAGE_SIZE));

	dti = get_variable_length_string_type("var_string_8192", sysconf(_SC_PAGE_SIZE) * 5);
	initialize_tuple_def(&tpl_d, &dti);

	print_tuple_def(&tpl_d);

	temp_tuple_store* tts_p = get_new_temp_tuple_store(".");

	delete_temp_tuple_store(tts_p);
	return 0;
}