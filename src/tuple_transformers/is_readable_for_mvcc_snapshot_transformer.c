#include<rhendb/tuple_transformer_interface.h>

#include<rhendb/mvcc_snapshot.h>

#include<tuplestore/tuple.h>
#include<tuplestore/tuple_def.h>

#include<stdlib.h>

/*
	it is expected that mvcc_header is the first attribute in the tuple for input_def
*/

#define MVCC_HEADER_POSITION STATIC_POSITION(0)

typedef struct is_readable_mvcc_snapshot_context is_readable_mvcc_snapshot_context;
struct is_readable_mvcc_snapshot_context
{
	const mvcc_snapshot* mvccsnp_p;
	transaction_status_getter* tsg_p;
	int* were_hints_updated;
	tuple_def mvcchdr_def;
};

static void* process(tuple_transformer* tt_p, void* tuple)
{
	is_readable_mvcc_snapshot_context* c_p = tt_p->context;

	const void* mvcchdr_tup = NULL;
	{
		datum uval;
		if(!get_value_from_element_from_tuple(&uval, tt_p->input_def, MVCC_HEADER_POSITION, tuple) && !is_datum_NULL(&uval))
			mvcchdr_tup = uval.tuple_value;
	}

	mvcc_header mvcchdr;
	read_mvcc_header(&mvcchdr, mvcchdr_tup, &(c_p->mvcchdr_def));

	if(is_tuple_visible_to_mvcc_snapshot(c_p->mvccsnp_p, &mvcchdr, c_p->tsg_p, c_p->were_hints_updated))
		return tuple;

	return NULL;
}

static void destroy(tuple_transformer* tt_p)
{
	free(tt_p->context);
}

tuple_transformer* get_new_is_readable_for_mvcc_snapshot_transformer(const mvcc_snapshot* mvccsnp_p, transaction_status_getter* tsg_p, int* were_hints_updated, const tuple_def* input_def)
{
	data_type_info* mvcchdr_dti = (data_type_info*) get_type_info_for_element_from_tuple_def(input_def, MVCC_HEADER_POSITION);

	is_readable_mvcc_snapshot_context* c_p = malloc(sizeof(is_readable_mvcc_snapshot_context));
	c_p->mvccsnp_p = mvccsnp_p;
	c_p->tsg_p = tsg_p;
	c_p->were_hints_updated = were_hints_updated;
	initialize_tuple_def(&(c_p->mvcchdr_def), mvcchdr_dti);

	return get_new_tuple_transformer(c_p, input_def, input_def, process, destroy);
}