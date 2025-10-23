#include<rhendb/sub_operator.h>

typed_user_value transform(transformer* t, uint32_t input_count, const typed_user_value** input)
{
	return t->transform(t->context, input_count, input);
}