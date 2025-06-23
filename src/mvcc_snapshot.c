#include<rondb/mvcc_snapshot.h>

declarations_value_arraylist(sorted_transaction_list, uint256, static inline)
#define EXPANSION_FACTOR 1.5
function_definitions_value_arraylist(sorted_transaction_list, uint256, static inline)

void initialize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 transaction_id);

int insert_in_progress_transaction_in_mvcc_snapshot(mvcc_snapshot* mvccsnp_p, uint256 in_progress_transaction_id);

void finalize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p);

int is_self_transaction_for_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id);

int was_completed_transaction_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id);

void deinitialize_mvcc_snapshot(mvcc_snapshot* mvccsnp_p);

int are_changes_for_transaction_id_visible_at_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, uint256 transaction_id, transaction_status (*get_transaction_status)(uint256 transaction_id));

int is_mvcc_header_visible_to_mvcc_snapshot(const mvcc_snapshot* mvccsnp_p, mvcc_header* mvcchdr_p, transaction_status (*get_transaction_status)(uint256 transaction_id), int* can_delete, int* was_mvcc_header_updated);