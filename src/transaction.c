#include<rhendb/transaction.h>

int compare_transaction_by_transaction_id(const void* tx1, const void* tx2)
{
	return compare_uint256_with_ptrs(((const transaction*)tx1)->transaction_id, ((const transaction*)tx2)->transaction_id);
}

cy_uint hash_transaction_by_transaction_id(const void* tx)
{
	return ((const transaction*)tx)->transaction_id->limbs[0];
}