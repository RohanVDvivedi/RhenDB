#include<rondb/rondb.h>

#include<rondb/mvcc_snapshot.h>

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
		mvcc_snapshot snap;

		initialize_mvcc_snapshot(&snap, get_uint256(777));

		finalize_mvcc_snapshot(&snap);

		deinitialize_mvcc_snapshot(&snap);
	}

	return 0;
}