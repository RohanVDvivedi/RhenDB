#ifndef NULLABLE_TYPE_INFO_MAKER_H
#define NULLABLE_TYPE_INFO_MAKER_H

#include<tuplestore/data_type_info.h>

// shanllow clones the passed data_type_info into a nullable clone
// to destroy it just free() it
data_type_info* shallow_clone_into_nullable_type(const data_type_info* dti_p);

/*
	This function can be used to dti_p clone into an identical type that can also hold a NULL_DATUM
	It can be used to create keys for aggregate operators or output tuple pars of join operators
*/

#endif