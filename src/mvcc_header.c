#include<rhendb/mvcc_header.h>

#include<tuplestore/tuple.h>

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
	dti_p->containees[2].al.type_info = LARGE_UINT_NULLABLE[transaction_id_width];

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

void read_mvcc_header(mvcc_header* mvcchdr_p, const void* mvcchdr_tup, const tuple_def* mvcchdr_def)
{
	datum uval;

	if(!get_value_from_element_from_tuple(&uval, mvcchdr_def, STATIC_POSITION(2), mvcchdr_tup))
		exit(-1);
	if(is_datum_NULL(&uval))
	{
		mvcchdr_p->is_xmin_NULL = 1;
	}
	else
	{
		mvcchdr_p->is_xmin_NULL = 0;
		mvcchdr_p->xmin.transaction_id = uval.large_uint_value;

		if(!get_value_from_element_from_tuple(&uval, mvcchdr_def, STATIC_POSITION(0), mvcchdr_tup))
			exit(-1);
		mvcchdr_p->xmin.is_committed = uval.bit_field_value;

		if(!get_value_from_element_from_tuple(&uval, mvcchdr_def, STATIC_POSITION(1), mvcchdr_tup))
			exit(-1);
		mvcchdr_p->xmin.is_aborted = uval.bit_field_value;
	}

	if(!get_value_from_element_from_tuple(&uval, mvcchdr_def, STATIC_POSITION(5), mvcchdr_tup))
		exit(-1);
	if(is_datum_NULL(&uval))
	{
		mvcchdr_p->is_xmax_NULL = 1;
	}
	else
	{
		mvcchdr_p->is_xmax_NULL = 0;
		mvcchdr_p->xmax.transaction_id = uval.large_uint_value;

		if(!get_value_from_element_from_tuple(&uval, mvcchdr_def, STATIC_POSITION(3), mvcchdr_tup))
			exit(-1);
		mvcchdr_p->xmax.is_committed = uval.bit_field_value;

		if(!get_value_from_element_from_tuple(&uval, mvcchdr_def, STATIC_POSITION(4), mvcchdr_tup))
			exit(-1);
		mvcchdr_p->xmax.is_aborted = uval.bit_field_value;
	}
}

void write_mvcc_header(void* mvcchdr_tup, const tuple_def* mvcchdr_def, const mvcc_header* mvcchdr_p)
{
	init_tuple(mvcchdr_def, mvcchdr_tup);

	if(mvcchdr_p->is_xmin_NULL)
	{
		if(!set_element_in_tuple(mvcchdr_def, STATIC_POSITION(2), mvcchdr_tup, NULL_DATUM, 0))
			exit(-1);
	}
	else
	{
		if(!set_element_in_tuple(mvcchdr_def, STATIC_POSITION(0), mvcchdr_tup, &((datum){.bit_field_value = mvcchdr_p->xmin.is_committed}), 0))
			exit(-1);

		if(!set_element_in_tuple(mvcchdr_def, STATIC_POSITION(1), mvcchdr_tup, &((datum){.bit_field_value = mvcchdr_p->xmin.is_aborted}), 0))
			exit(-1);

		if(!set_element_in_tuple(mvcchdr_def, STATIC_POSITION(2), mvcchdr_tup, &((datum){.large_uint_value = mvcchdr_p->xmin.transaction_id}), 0))
			exit(-1);
	}

	if(mvcchdr_p->is_xmax_NULL)
	{
		if(!set_element_in_tuple(mvcchdr_def, STATIC_POSITION(5), mvcchdr_tup, NULL_DATUM, 0))
			exit(-1);
	}
	else
	{
		if(!set_element_in_tuple(mvcchdr_def, STATIC_POSITION(3), mvcchdr_tup, &((datum){.bit_field_value = mvcchdr_p->xmax.is_committed}), 0))
			exit(-1);

		if(!set_element_in_tuple(mvcchdr_def, STATIC_POSITION(4), mvcchdr_tup, &((datum){.bit_field_value = mvcchdr_p->xmax.is_aborted}), 0))
			exit(-1);

		if(!set_element_in_tuple(mvcchdr_def, STATIC_POSITION(5), mvcchdr_tup, &((datum){.large_uint_value = mvcchdr_p->xmax.transaction_id}), 0))
			exit(-1);
	}
}

void print_mvcc_header(const mvcc_header* mvcchdr_p)
{
	if(mvcchdr_p->is_xmin_NULL)
		printf("xmin => NULL\n");
	else
	{
		char temp[80] = {};
		serialize_to_decimal_uint256(temp, mvcchdr_p->xmin.transaction_id);
		printf("xmin => c=%d a=%d tx_id=%s\n", mvcchdr_p->xmin.is_committed, mvcchdr_p->xmin.is_aborted, temp);
	}

	if(mvcchdr_p->is_xmax_NULL)
		printf("xmax => NULL\n");
	else
	{
		char temp[80] = {};
		serialize_to_decimal_uint256(temp, mvcchdr_p->xmax.transaction_id);
		printf("xmax => c=%d a=%d tx_id=%s\n", mvcchdr_p->xmax.is_committed, mvcchdr_p->xmax.is_aborted, temp);
	}
}

transaction_status fetch_status_for_transaction_id_with_hints(transaction_id_with_hints* transaction_id, transaction_status_getter* tsg_p, int* were_hints_updated)
{
	// first try and answer from hints if possible
	if(transaction_id->is_committed)
		return TX_COMMITTED;

	if(transaction_id->is_aborted)
		return TX_ABORTED;

	// go to the table and try to fetch it
	transaction_status status = get_transaction_status_for_transaction_id(tsg_p, transaction_id->transaction_id);

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