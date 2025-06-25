#include<rondb/rondb.h>

#include<rondb/mvcc_snapshot.h>

int main()
{
	mvcc_header a = {
		.xmin = {
			.is_committed = 1;
			.is_aborted = 0;
			.transaction_id = get_uint256(345);
		},
		.is_xmax_NULL = 0,
		.xmax = {
			.is_committed = 1;
			.is_aborted = 0;
			.transaction_id = get_uint256(543);
		},
	};



	write_mvcc_header(void* mvcchdr_tup, const tuple_def* mvcchdr_def, const mvcc_header* mvcchdr_p);

	read_mvcc_header(mvcc_header* mvcchdr_p, const void* mvcchdr_tup, const tuple_def* mvcchdr_def);

	return 0;
}