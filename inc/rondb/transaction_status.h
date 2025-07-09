#ifndef TRANSACTION_STATUS_H
#define TRANSACTION_STATUS_H

typedef enum transaction_status transaction_status;
enum transaction_status
{
	TX_IN_PROGRESS = 0b01,
	TX_ABORTED     = 0b10,
	TX_COMMITTED   = 0b11,
};

extern char const * const transaction_status_string[];

#endif