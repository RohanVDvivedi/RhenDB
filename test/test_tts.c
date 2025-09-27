#include<rhendb/temp_tuple_store.h>

int main()
{
	temp_tuple_store* tts_p = get_new_temp_tuple_store(".");


	delete_temp_tuple_store(tts_p);
	return 0;
}