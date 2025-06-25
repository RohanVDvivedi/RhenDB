#include<rondb/rondb.h>

#include<rondb/mvcc_snapshot.h>

uint256 self;
uint256* in_progress_list;
uint32_t in_progress_count;

transaction_status get_transaction_status(uint256 transaction_id)
{
	// self is always in_progress
	if(compare_uint256(transaction_id, self) >= 0)
		return TX_IN_PROGRESS;

	// anything above 830 has not happenned yet so they are in progress
	if(compare_uint256(transaction_id, get_uint256(830)) >= 0)
		return TX_IN_PROGRESS;

	// in_progress list transaction are still in progress
	for(uint32_t i = 0; i < in_progress_count; i++)
		if(are_equal_uint256(in_progress_list[i], transaction_id))
			return TX_IN_PROGRESS;

	// for any other transaction it is aborted if it gives 0 on mod 5
	uint256 rem;
	uint256 quo;
	rem = div_uint256(&quo, transaction_id, get_uint256(5));
	if(are_equal_uint256(rem, get_uint256(0)))
		return TX_ABORTED;
	else
		return TX_COMMITTED;
}

int main()
{
	{
		tuple_def* mvcc_header_def = get_mvcc_header_tuple_definition(20);

		mvcc_header a = {
			.xmin = {
				.is_committed = 0,
				.is_aborted = 1,
				.transaction_id = get_uint256(345),
			},
			.is_xmax_NULL = 0,
			.xmax = {
				.is_committed = 1,
				.is_aborted = 0,
				.transaction_id = get_uint256(543),
			},
		};

		char mvcc_tuple[65];

		print_mvcc_header(&a);printf("\n\n");

		write_mvcc_header(mvcc_tuple, mvcc_header_def, &a);

		mvcc_header b;

		read_mvcc_header(&b, mvcc_tuple, mvcc_header_def);

		print_mvcc_header(&b);printf("\n\n");

		destroy_non_static_type_info_recursively(mvcc_header_def->type_info);
		free(mvcc_header_def);
	}

	{
		self = get_uint256(777);
		uint256 in_progress[] = {
			get_uint256(777),
			get_uint256(800),
			get_uint256(7),
			get_uint256(77),
			get_uint256(70),
			get_uint256(777),
			get_uint256(900),
			get_uint256(77),
			get_uint256(500),
			get_uint256(555),
			get_uint256(555),
			get_uint256(444),
			get_uint256(600),
			get_uint256(700),
		};
		in_progress_list = in_progress;
		in_progress_count = sizeof(in_progress)/sizeof(uint256);

		mvcc_snapshot snap;

		initialize_mvcc_snapshot(&snap, get_uint256(777));

		for(uint32_t i = 0; i < in_progress_count; i++)
		{
			char temp[80] = {};
			serialize_to_decimal_uint256(temp, in_progress_list[i]);
			printf("insert (%s) => %d\n", temp, insert_in_progress_transaction_in_mvcc_snapshot(&snap, in_progress_list[i]));
		}
		printf("\n");

		finalize_mvcc_snapshot(&snap);

		print_mvcc_snapshot(&snap);

		deinitialize_mvcc_snapshot(&snap);
	}

	return 0;
}