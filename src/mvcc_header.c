#include<rondb/mvcc_header.h>

#include<stdlib.h>

tuple_def* get_mvcc_header_tuple_definition(uint8_t transaction_id_width)
{
	data_type_info* dti_p = malloc(sizeof_tuple_data_type_info(6));

	initialize_tuple_data_type_info(dti_p, "mvcc_header", 0, 0, 6);

	strcpy(dti_p->containees[0].field_name, "is_xmin_committed");
	dti_p->containees[0].al.type_info = BIT_FIELD_NON_NULLABLE[1];

	strcpy(dti_p->containees[1].field_name, "is_xmin_aborted");
	dti_p->containees[1].al.type_info = BIT_FIELD_NON_NULLABLE[1];

	strcpy(dti_p->containees[2].field_name, "xmin");
	dti_p->containees[2].al.type_info = LARGE_UINT_NON_NULLABLE[transaction_id_width];

	strcpy(dti_p->containees[3].field_name, "is_xmax_committed");
	dti_p->containees[3].al.type_info = BIT_FIELD_NON_NULLABLE[1];

	strcpy(dti_p->containees[4].field_name, "is_xmax_aborted");
	dti_p->containees[4].al.type_info = BIT_FIELD_NON_NULLABLE[1];

	strcpy(dti_p->containees[5].field_name, "xmax");
	dti_p->containees[5].al.type_info = LARGE_UINT_NULLABLE[transaction_id_width];

	tuple_def* mvcchdr_def = malloc(sizeof(tuple_def));

	initialize_tuple_def(mvcchdr_def, dti_p);

	return mvcchdr_def;
}

void read_mvcc_header(mvcc_header* mvcchdr_p, const void* mvcchdr_uval, const tuple_def* mvcchdr_def);

void write_mvcc_header(void* mvcchdr_uval, const tuple_def* mvcchdr_def, const mvcc_header* mvcchdr_p);

transaction_status fetch_status_for_transaction_id_with_hints(transaction_id_with_hints* transaction_id, transaction_status (*get_transaction_status)(uint256 transaction_id), int* were_hints_updated)
{
	// first try and answer from hints if possible
	if(transaction_id->is_committed)
		return TX_COMMITTED;

	if(transaction_id->is_aborted)
		return TX_ABORTED;

	// go to the table and try to fetch it
	transaction_status status = get_transaction_status(transaction_id->transaction_id);

	// update the hints
	switch(status)
	{
		case TX_COMMITTED :
		{
			transaction_id->is_committed = 1;
			(*were_hints_updated) = 1;
			break;
		}
		case TX_ABORTED :
		{
			transaction_id->is_aborted = 1;
			(*were_hints_updated) = 1;
			break;
		}
		default :
		{
			break;
		}
	}

	return status;
}