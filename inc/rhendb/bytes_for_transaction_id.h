#ifndef BYTES_FOR_TRANSACTION_ID_H
#define BYTES_FOR_TRANSACTION_ID_H

#include<serint/large_uints.h>

static inline uint32_t get_bytes_required_for_transaction_id(uint256 overflow_transaction_id)
{
	// iterate over all the possible sizes (except 0) in reverse and return the size which suggests that its last byte is set to non-zero
	for(uint32_t possible_size = sizeof(uint256); possible_size != 0; possible_size--)
		if(get_byte_from_uint256(overflow_transaction_id, possible_size-1) != 0)
			return possible_size;

	printf("BUG :: system identified 0 bytes needed to hold a transaction_id\n");
	exit(-1);
	return 0;
}

#endif