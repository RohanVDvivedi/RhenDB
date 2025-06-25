#include<rondb/rondb.h>

#include<rondb/mvcc_snapshot.h>

int main()
{
	tuple_def* mvcc_header_def = get_mvcc_header_tuple_definition(20);

	mvcc_header a = {
		.xmin = {
			.is_committed = 1,
			.is_aborted = 0,
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

	print_mvcc_header(&a);

	write_mvcc_header(mvcc_tuple, mvcc_header_def, &a);

	mvcc_header b;

	read_mvcc_header(&b, mvcc_tuple, mvcc_header_def);

	print_mvcc_header(&b);

	return 0;
}