#ifndef TRANSACTION_STATUS_H
#define TRANSACTION_STATUS_H

typedef enum transaction_status transaction_status;
enum transaction_status
{
	TX_IN_PROGRESS,
	TX_ABORTED,
	TX_COMMITTED,
};

extern char const * const transaction_status_string[];

#endif