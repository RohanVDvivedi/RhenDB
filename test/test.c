#include<rondb/rondb.h>

#include<rondb/mvcc_snapshot.h>

uint256 self;
uint256* in_progress_list;
uint32_t in_progress_count;

transaction_status get_transaction_status_internal(uint256 transaction_id)
{
	// self is always in_progress
	if(are_equal_uint256(transaction_id, self))
		return TX_IN_PROGRESS;

	// anything above 830 has not happenned yet so they are in progress
	if(compare_uint256(transaction_id, get_uint256(830)) >= 0)
		return TX_IN_PROGRESS;

	// 500 is now committed, after the snapshot was taken
	if(are_equal_uint256(transaction_id, get_uint256(500)))
		return TX_COMMITTED;

	// 600 is now aborted, after the snapshot was taken
	if(are_equal_uint256(transaction_id, get_uint256(600)))
		return TX_ABORTED;

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

transaction_status get_transaction_status(uint256 transaction_id)
{
	transaction_status res = get_transaction_status_internal(transaction_id);
	char temp[80] = {};
	serialize_to_decimal_uint256(temp, transaction_id);
	printf("status(%s) => %s\n", temp, transaction_status_string[res]);
	return res;
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

		uint256 test_tx_ids[] = {
			get_uint256(1),
			get_uint256(5),
			get_uint256(7),
			get_uint256(22),
			get_uint256(25),
			get_uint256(77),
			get_uint256(500),
			get_uint256(550),
			get_uint256(600),
			get_uint256(603),
			get_uint256(605),
			get_uint256(777),
			get_uint256(801),
			get_uint256(805),
		};
		for(uint32_t i = 0; i < sizeof(test_tx_ids)/sizeof(uint256); i++)
		{
			char temp[80] = {};
			int is_self = is_self_transaction_for_mvcc_snapshot(&snap, test_tx_ids[i]);
			int was_completed = was_completed_transaction_at_mvcc_snapshot(&snap, test_tx_ids[i]);
			int were_hints_updated = 0;
			int is_visible = are_changes_for_transaction_id_visible_at_mvcc_snapshot(&snap, &((transaction_id_with_hints){0,0,test_tx_ids[i]}), get_transaction_status, &were_hints_updated);
			serialize_to_decimal_uint256(temp, test_tx_ids[i]);
			printf("%s => self=%d, completed=%d, visible=%d, hints_updated=%d\n", temp, is_self, was_completed, is_visible, were_hints_updated);
		}
		printf("\n");

		transaction_id_with_hints header_ids[] = {
			{0,0,get_uint256(1)}, // PAST COMMITTED
			{0,0,get_uint256(5)}, // PAST ABORTED
			{0,0,get_uint256(500)}, // PRESENT
			{0,0,get_uint256(543)}, // PAST COMMITTED
			{0,0,get_uint256(555)}, // PAST ABORTED
			{0,0,get_uint256(600)}, // PRESENT ABORTED
			{0,0,get_uint256(777)}, // SELF
			{0,0,get_uint256(801)}, // FUTURE COMMITTED
			{0,0,get_uint256(805)}, // FUTURE ABORTED
			{0,0,get_uint256(0)}, // NULL (c0nsider it as NULL)
		};
		uint32_t header_ids_count = sizeof(header_ids)/sizeof(uint256);

		for(uint32_t xmin_i = 0; xmin_i < header_ids_count; xmin_i++)
		{
			for(uint32_t xmax_i = xmin_i + 1; xmax_i < header_ids_count; xmax_i++)
			{
				mvcc_header hdr = {.xmin = header_ids[xmin_i], .is_xmax_NULL = are_equal_uint256(header_ids[xmax_i].transaction_id, get_uint256(0)), .xmax = header_ids[xmax_i]};
				int were_hints_updated = 0;
				int is_visible = is_tuple_visible_to_mvcc_snapshot(&snap, &hdr, get_transaction_status, &were_hints_updated);
				int can_delete = can_delete_tuple_for_mvcc_snapshot(&snap, &hdr, get_transaction_status, &were_hints_updated);
				print_mvcc_header(&hdr);printf("\n\n");
				printf("is_visble=%d can_delete=%d\n\n\n", is_visible, can_delete);
			}
		}

		deinitialize_mvcc_snapshot(&snap);
	}

	return 0;
}