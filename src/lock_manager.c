#include<rondb/lock_manager.h>

#include<tuplestore/tuple.h>
#include<tuplestore/data_type_info.h>

#include<tupleindexer/interface/page_access_methods.h>

#include<stdlib.h>

char const * const lock_result_strings[] = {
	[LOCK_ACQUIRED] = "LOCK_ACQUIRED",
	[LOCK_TRANSITIONED] = "LOCK_TRANSITIONED",
	[LOCK_ALREADY_HELD] = "LOCK_ALREADY_HELD",
	[LOCKING_FAILED] = "LOCKING_FAILED",
	[MUST_BLOCK_FOR_LOCK] = "MUST_BLOCK_FOR_LOCK",
};

int abort_error = 0;

// all the bplus_tree-s used are ascending ordered by their keys, so we need this global array to pass in all the bplus_tree tuple_defs
static compare_direction all_ascending[] = {ASC, ASC, ASC, ASC, ASC, ASC, ASC, ASC};

/*
** internal structures
*/

#define MAX_SERIALIZED_LOCK_ENTRY_SIZE (sizeof(uint256) + sizeof(uint32_t) + MAX_RESOURCE_ID_SIZE + sizeof(uint32_t) + 2) // +2 for the size and offset of resource_id

typedef struct lock_entry lock_entry;
struct lock_entry
{
	// transaction_id that holds the lock
	uint256 transaction_id;

	// this is the resource_type for the below resource_id
	// this dictates what lock_modes can be used for this resource_type
	// this implies it is same as the type of the lock to be used on this resource
	// i.e. resource_type = lock_type, resulting into fixed usable number of lock_mode-s that you can acquire on it
	uint32_t resource_type;

	// the resource_id that is locked, it is atmost 16 bytes wide, i.e. 128 bits
	uint8_t resource_id_size;
	uint8_t resource_id[MAX_RESOURCE_ID_SIZE];

	// lock mode is the mode of the lock, for instance a (resource_type ==) reader_writer_lock has 2 modes, READ_MODE and WRITE_MODE
	uint32_t lock_mode;
};

static void serialize_lock_entry_record(void* to, const lock_entry* from, const lock_manager* lckmgr_p)
{
	init_tuple(lckmgr_p->lock_record_def, to);

	set_element_in_tuple(lckmgr_p->lock_record_def, STATIC_POSITION(0), to, &((user_value){.large_uint_value = from->transaction_id}), 0);
	set_element_in_tuple(lckmgr_p->lock_record_def, STATIC_POSITION(1), to, &((user_value){.uint_value = from->resource_type}), 0);
	set_element_in_tuple(lckmgr_p->lock_record_def, STATIC_POSITION(2), to, &((user_value){.blob_size = from->resource_id_size, .blob_value = from->resource_id}), MAX_RESOURCE_ID_SIZE);
	set_element_in_tuple(lckmgr_p->lock_record_def, STATIC_POSITION(3), to, &((user_value){.uint_value = from->lock_mode}), 0);
}

static void deserialize_lock_entry_record(const void* from, lock_entry* to, const lock_manager* lckmgr_p)
{
	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, lckmgr_p->lock_record_def, STATIC_POSITION(0), from);
		to->transaction_id = uval.large_uint_value;
	}

	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, lckmgr_p->lock_record_def, STATIC_POSITION(1), from);
		to->resource_type = uval.uint_value;
	}

	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, lckmgr_p->lock_record_def, STATIC_POSITION(2), from);
		to->resource_id_size = uval.blob_size;
		memory_move(to->resource_id, uval.blob_value, to->resource_id_size);
	}

	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, lckmgr_p->lock_record_def, STATIC_POSITION(3), from);
		to->lock_mode = uval.uint_value;
	}
}

static positional_accessor tx_locks_keys[] = {STATIC_POSITION(0), STATIC_POSITION(1), STATIC_POSITION(2)};

static positional_accessor rs_locks_keys[] = {STATIC_POSITION(1), STATIC_POSITION(2), STATIC_POSITION(0)};

#define MAX_SERIALIZED_WAIT_ENTRY_SIZE (sizeof(uint256) + sizeof(uint32_t) + sizeof(uint256) + sizeof(uint32_t) + MAX_RESOURCE_ID_SIZE + 2) // +2 for the size and offset of resource_id

typedef struct wait_entry wait_entry;
struct wait_entry
{
	// transaction_id that is waiting for the lock
	uint256 waiting_transaction_id;

	// task_id of the above transaction that is waiting for the lock
	uint32_t waiting_task_id;

	// ON the transaction_id and the resource attributes below

	// transaction_id that holds the lock
	uint256 transaction_id;

	// this is the resource_type for the below resource_id
	// this dictates what lock_modes can be used for this resource_type
	// this implies it is same as the type of the lock to be used on this resource
	// i.e. resource_type = lock_type, resulting into fixed usable number of lock_mode-s that you can acquire on it
	uint32_t resource_type;

	// the resource_id that is locked, it is atmost 16 bytes wide, i.e. 128 bits
	uint8_t resource_id_size;
	uint8_t resource_id[MAX_RESOURCE_ID_SIZE];
};

static void serialize_wait_entry_record(void* to, const wait_entry* from, const lock_manager* lckmgr_p)
{
	init_tuple(lckmgr_p->wait_record_def, to);

	set_element_in_tuple(lckmgr_p->wait_record_def, STATIC_POSITION(0), to, &((user_value){.large_uint_value = from->waiting_transaction_id}), 0);
	set_element_in_tuple(lckmgr_p->wait_record_def, STATIC_POSITION(1), to, &((user_value){.uint_value = from->waiting_task_id}), 0);
	set_element_in_tuple(lckmgr_p->wait_record_def, STATIC_POSITION(2), to, &((user_value){.large_uint_value = from->transaction_id}), 0);
	set_element_in_tuple(lckmgr_p->wait_record_def, STATIC_POSITION(3), to, &((user_value){.uint_value = from->resource_type}), 0);
	set_element_in_tuple(lckmgr_p->wait_record_def, STATIC_POSITION(4), to, &((user_value){.blob_size = from->resource_id_size, .blob_value = from->resource_id}), MAX_RESOURCE_ID_SIZE);
}

static void deserialize_wait_entry_record(const void* from, wait_entry* to, const lock_manager* lckmgr_p)
{
	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, lckmgr_p->wait_record_def, STATIC_POSITION(0), from);
		to->waiting_transaction_id = uval.large_uint_value;
	}

	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, lckmgr_p->wait_record_def, STATIC_POSITION(1), from);
		to->waiting_task_id = uval.uint_value;
	}

	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, lckmgr_p->wait_record_def, STATIC_POSITION(2), from);
		to->transaction_id = uval.large_uint_value;
	}

	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, lckmgr_p->wait_record_def, STATIC_POSITION(3), from);
		to->resource_type = uval.uint_value;
	}

	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, lckmgr_p->wait_record_def, STATIC_POSITION(4), from);
		to->resource_id_size = uval.blob_size;
		memory_move(to->resource_id, uval.blob_value, to->resource_id_size);
	}
}

static positional_accessor waits_for_keys[] = {STATIC_POSITION(0), STATIC_POSITION(1), STATIC_POSITION(2), STATIC_POSITION(3), STATIC_POSITION(4)};

static positional_accessor waits_back_keys[] = {STATIC_POSITION(3), STATIC_POSITION(4), STATIC_POSITION(0), STATIC_POSITION(1), STATIC_POSITION(2)};

// --

/*
** utility functions
*/

// 1 - basic functionality for the wait_entries
static int insert_wait_entry(lock_manager* lckmgr_p, const wait_entry* we_p)
{
	char wait_entry_tuple[MAX_SERIALIZED_WAIT_ENTRY_SIZE];

	serialize_wait_entry_record(wait_entry_tuple, we_p, lckmgr_p);

	int res = 1;

	res = res && insert_in_bplus_tree(lckmgr_p->waits_for_root_page_id, wait_entry_tuple, lckmgr_p->waits_for_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);
	res = res && insert_in_bplus_tree(lckmgr_p->waits_back_root_page_id, wait_entry_tuple, lckmgr_p->waits_back_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);

	return res;
}

/*
static int remove_wait_entry(lock_manager* lckmgr_p, const wait_entry* we_p)
{
	char wait_entry_tuple[MAX_SERIALIZED_WAIT_ENTRY_SIZE];

	serialize_wait_entry_record(wait_entry_tuple, we_p, lckmgr_p);

	int res = 1;

	if(res)
	{
		char wait_entry_key[MAX_SERIALIZED_WAIT_ENTRY_SIZE];
		extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->waits_for_td, wait_entry_tuple, wait_entry_key);
		res = res && delete_from_bplus_tree(lckmgr_p->waits_for_root_page_id, wait_entry_key, lckmgr_p->waits_for_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);
	}

	if(res)
	{
		char wait_entry_key[MAX_SERIALIZED_WAIT_ENTRY_SIZE];
		extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->waits_back_td, wait_entry_tuple, wait_entry_key);
		res = res && delete_from_bplus_tree(lckmgr_p->waits_back_root_page_id, wait_entry_key, lckmgr_p->waits_back_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);
	}

	return res;
}
*/

// 2 - remove wait_entries for a particular waiters
static void remove_all_wait_entries_for_task_id(lock_manager* lckmgr_p, uint256 waiting_transaction_id, uint32_t waiting_task_id)
{
	// we need to construct the wait_entry_key
	char wait_entry_key[MAX_SERIALIZED_WAIT_ENTRY_SIZE];

	{
		// construct wait_entry_tuple
		char wait_entry_tuple[MAX_SERIALIZED_WAIT_ENTRY_SIZE];
		{
			wait_entry we = {.waiting_transaction_id = waiting_transaction_id, .waiting_task_id = waiting_task_id};
			serialize_wait_entry_record(wait_entry_tuple, &we, lckmgr_p);
		}

		// extract key out of it
		extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->waits_for_td, wait_entry_tuple, wait_entry_key);
	}

	// create an iterator using the first 2 keys (waiting_transaction_id, waiting_task_id)
	bplus_tree_iterator* bpi_p = find_in_bplus_tree(lckmgr_p->waits_for_root_page_id, wait_entry_key, 2, GREATER_THAN_EQUALS, 1, WRITE_LOCK, lckmgr_p->waits_for_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);

	// keep looping while the bplus_tree is not empty and it has a current tuple to be processed
	while(!is_empty_bplus_tree(bpi_p) && !is_beyond_max_tuple_bplus_tree_iterator(bpi_p))
	{
		// deserialize the wait_entry into a struct
		wait_entry we;
		deserialize_wait_entry_record(get_tuple_bplus_tree_iterator(bpi_p), &we, lckmgr_p);

		// if it is not the right tuple, we break out
		if(!are_equal_uint256(waiting_transaction_id, we.waiting_transaction_id) || waiting_task_id != we.waiting_task_id)
			break;

		// remove the wait_entry
		{
			{
				char wait_entry_key[MAX_SERIALIZED_WAIT_ENTRY_SIZE];
				extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->waits_back_td, get_tuple_bplus_tree_iterator(bpi_p), wait_entry_key);
				delete_from_bplus_tree(lckmgr_p->waits_back_root_page_id, wait_entry_key, lckmgr_p->waits_back_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);
			}

			// then from the table with the iterator
			remove_from_bplus_tree_iterator(bpi_p, GO_NEXT_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION, NULL, &abort_error);
		}
	}

	delete_bplus_tree_iterator(bpi_p, NULL, &abort_error);
}

static void remove_all_wait_entries_for_transaction_id(lock_manager* lckmgr_p, uint256 waiting_transaction_id)
{
	// we need to construct the wait_entry_key
	char wait_entry_key[MAX_SERIALIZED_WAIT_ENTRY_SIZE];

	{
		// construct wait_entry_tuple
		char wait_entry_tuple[MAX_SERIALIZED_WAIT_ENTRY_SIZE];
		{
			wait_entry we = {.waiting_transaction_id = waiting_transaction_id};
			serialize_wait_entry_record(wait_entry_tuple, &we, lckmgr_p);
		}

		// extract key out of it
		extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->waits_for_td, wait_entry_tuple, wait_entry_key);
	}

	// create an iterator using the first 1 keys (waiting_transaction_id)
	bplus_tree_iterator* bpi_p = find_in_bplus_tree(lckmgr_p->waits_for_root_page_id, wait_entry_key, 1, GREATER_THAN_EQUALS, 1, WRITE_LOCK, lckmgr_p->waits_for_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);

	// keep looping while the bplus_tree is not empty and it has a current tuple to be processed
	while(!is_empty_bplus_tree(bpi_p) && !is_beyond_max_tuple_bplus_tree_iterator(bpi_p))
	{
		// deserialize the wait_entry into a struct
		wait_entry we;
		deserialize_wait_entry_record(get_tuple_bplus_tree_iterator(bpi_p), &we, lckmgr_p);

		// if it is not the right tuple, we break out
		if(!are_equal_uint256(waiting_transaction_id, we.waiting_transaction_id))
			break;

		// remove the wait_entry
		{
			{
				char wait_entry_key[MAX_SERIALIZED_WAIT_ENTRY_SIZE];
				extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->waits_back_td, get_tuple_bplus_tree_iterator(bpi_p), wait_entry_key);
				delete_from_bplus_tree(lckmgr_p->waits_back_root_page_id, wait_entry_key, lckmgr_p->waits_back_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);
			}

			// then from the table with the iterator
			remove_from_bplus_tree_iterator(bpi_p, GO_NEXT_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION, NULL, &abort_error);
		}
	}

	delete_bplus_tree_iterator(bpi_p, NULL, &abort_error);
}

// 3 - below function only calls notify_unblocked for all the waiters, that are blocked for this particular resource
// it will not remove those wait-entries
static void notify_all_wait_entries_for_resource_of_being_unblocked(lock_manager* lckmgr_p, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size)
{
	// we need to construct the wait_entry_key
	char wait_entry_key[MAX_SERIALIZED_WAIT_ENTRY_SIZE];

	{
		// construct wait_entry_tuple
		char wait_entry_tuple[MAX_SERIALIZED_WAIT_ENTRY_SIZE];
		{
			wait_entry we = {.resource_type = resource_type, .resource_id_size = resource_id_size};
			memory_move(we.resource_id, resource_id, resource_id_size);
			serialize_wait_entry_record(wait_entry_tuple, &we, lckmgr_p);
		}

		// extract key out of it
		extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->waits_back_td, wait_entry_tuple, wait_entry_key);
	}

	// create an iterator using the first 2 keys (resource_type, resource_id)
	bplus_tree_iterator* bpi_p = find_in_bplus_tree(lckmgr_p->waits_back_root_page_id, wait_entry_key, 2, GREATER_THAN_EQUALS, 0, READ_LOCK, lckmgr_p->waits_back_td, lckmgr_p->lckmgr_engine->pam_p, NULL, NULL, &abort_error);

	// keep looping while the bplus_tree is not empty and it has a current tuple to be processed
	while(!is_empty_bplus_tree(bpi_p) && !is_beyond_max_tuple_bplus_tree_iterator(bpi_p))
	{
		// deserialize the wait_entry into a struct
		wait_entry we;
		deserialize_wait_entry_record(get_tuple_bplus_tree_iterator(bpi_p), &we, lckmgr_p);

		// if it is not the right tuple, we break out
		if(we.resource_type != resource_type || we.resource_id_size != resource_id_size || memory_compare(we.resource_id, resource_id, resource_id_size))
			break;

		// wake up that transaction_id's task_id
		lckmgr_p->notifier.notify_unblocked(lckmgr_p->notifier.context_p, we.waiting_transaction_id, we.waiting_task_id);

		// continue ahead with the next tuple
		next_bplus_tree_iterator(bpi_p, NULL, &abort_error);
	}

	delete_bplus_tree_iterator(bpi_p, NULL, &abort_error);
}

// 4 - find function for the lock_entry, the primary key is transaction_id and the resource (type + id)
static uint32_t find_lock_entry(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size)
{
	// initialize the lock_mode, this is the return value
	uint32_t lock_mode = NO_LOCK_HELD_LOCK_MODE;

	char lock_entry_key[MAX_SERIALIZED_LOCK_ENTRY_SIZE];

	{
		// construct lock_entry_tuple
		char lock_entry_tuple[MAX_SERIALIZED_LOCK_ENTRY_SIZE];
		{
			lock_entry le = {.transaction_id = transaction_id, .resource_type = resource_type, .resource_id_size = resource_id_size};
			memory_move(le.resource_id, resource_id, resource_id_size);
			serialize_lock_entry_record(lock_entry_tuple, &le, lckmgr_p);
		}

		// extract key out of it
		extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->tx_locks_td, lock_entry_tuple, lock_entry_key);
	}

	// create an iterator using the first 3 keys (transaction_id, resource_type, resource_id)
	bplus_tree_iterator* bpi_p = find_in_bplus_tree(lckmgr_p->tx_locks_root_page_id, lock_entry_key, 3, GREATER_THAN_EQUALS, 0, READ_LOCK, lckmgr_p->tx_locks_td, lckmgr_p->lckmgr_engine->pam_p, NULL, NULL, &abort_error);

	// if the bplus_tree is not empty and it has a current tuple to be processed, then move forward with the test
	if(!is_empty_bplus_tree(bpi_p) && !is_beyond_max_tuple_bplus_tree_iterator(bpi_p))
	{
		// deserialize the lock_entry into a struct
		lock_entry le;
		deserialize_lock_entry_record(get_tuple_bplus_tree_iterator(bpi_p), &le, lckmgr_p);

		// if it is the right tuple, extract the lock_mode
		if(are_equal_uint256(le.transaction_id, transaction_id) && le.resource_type == resource_type && le.resource_id_size == resource_id_size && 0 == memory_compare(le.resource_id, resource_id, resource_id_size))
			lock_mode = le.lock_mode;
	}

	delete_bplus_tree_iterator(bpi_p, NULL, &abort_error);

	return lock_mode;
}

// 5 - below functions insert or remove the lock_entry and if a prior lock_mode existed, then a wake_up is also called on the corresponding wait_entries using the notify_all_wait_entries_*() function above
// uses utility functions of section 3

typedef struct insert_or_update_lock_entry_context insert_or_update_lock_entry_context;
struct insert_or_update_lock_entry_context
{
	int* was_updated;
	lock_manager* lckmgr_p;
};

static int insert_or_update_lock_entry(const void* context, const tuple_def* record_def, const void* old_record, void** new_record, void (*cancel_update_callback)(void* cancel_update_callback_context, const void* transaction_id, int* abort_error), void* cancel_update_callback_context, const void* transaction_id, int* abort_error)
{
	// if there no existing lock, insert directly
	if(old_record == NULL)
		return 1;

	// else analyze if an update has to be performed

	insert_or_update_lock_entry_context* cntxt = (void*) context;

	lock_entry old_lock_entry; deserialize_lock_entry_record(   old_record, &old_lock_entry, cntxt->lckmgr_p);
	lock_entry new_lock_entry; deserialize_lock_entry_record(*(new_record), &new_lock_entry, cntxt->lckmgr_p);

	// if both the lock_mode-s are same then do nothing
	if(old_lock_entry.lock_mode == new_lock_entry.lock_mode)
		return 0;

	// else say we update and ask the callback to go ahead with the update
	*(cntxt->was_updated) = 1;
	return 1;
}

static int insert_or_update_lock_entry_and_wake_up_waiters(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode)
{
	// construct lock_entry_tuple, that we aim to insert or update with
	char lock_entry_tuple[MAX_SERIALIZED_LOCK_ENTRY_SIZE];
	{
		lock_entry le = {.transaction_id = transaction_id, .resource_type = resource_type, .resource_id_size = resource_id_size, .lock_mode = new_lock_mode};
		memory_move(le.resource_id, resource_id, resource_id_size);
		serialize_lock_entry_record(lock_entry_tuple, &le, lckmgr_p);
	}

	int inserted_OR_updated = 1;
	int was_updated = 0;

	inserted_OR_updated = inserted_OR_updated && inspected_update_in_bplus_tree(lckmgr_p->tx_locks_root_page_id, lock_entry_tuple, &((update_inspector){.context = &((insert_or_update_lock_entry_context){&was_updated, lckmgr_p}), .update_inspect = insert_or_update_lock_entry}), lckmgr_p->tx_locks_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);

	inserted_OR_updated = inserted_OR_updated && inspected_update_in_bplus_tree(lckmgr_p->rs_locks_root_page_id, lock_entry_tuple, &((update_inspector){.context = &((insert_or_update_lock_entry_context){&was_updated, lckmgr_p}), .update_inspect = insert_or_update_lock_entry}), lckmgr_p->rs_locks_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);

	if(inserted_OR_updated && was_updated)
		notify_all_wait_entries_for_resource_of_being_unblocked(lckmgr_p, resource_type, resource_id, resource_id_size);

	return inserted_OR_updated;
}

static int remove_lock_entry_and_wake_up_waiters(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size)
{
	lock_entry le = {.transaction_id = transaction_id, .resource_type = resource_type, .resource_id_size = resource_id_size};
	memory_move(le.resource_id, resource_id, resource_id_size);
	char lock_entry_tuple[MAX_SERIALIZED_LOCK_ENTRY_SIZE];

	serialize_lock_entry_record(lock_entry_tuple, &le, lckmgr_p);

	int res = 1;

	if(res)
	{
		char lock_entry_key[MAX_SERIALIZED_LOCK_ENTRY_SIZE];
		extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->tx_locks_td, lock_entry_tuple, lock_entry_key);
		res = res && delete_from_bplus_tree(lckmgr_p->tx_locks_root_page_id, lock_entry_key, lckmgr_p->tx_locks_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);
	}

	if(res)
	{
		char lock_entry_key[MAX_SERIALIZED_LOCK_ENTRY_SIZE];
		extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->rs_locks_td, lock_entry_tuple, lock_entry_key);
		res = res && delete_from_bplus_tree(lckmgr_p->rs_locks_root_page_id, lock_entry_key, lckmgr_p->rs_locks_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);
	}

	if(res)
		notify_all_wait_entries_for_resource_of_being_unblocked(lckmgr_p, resource_type, resource_id, resource_id_size);

	return res;
}

static void remove_all_lock_entries_and_wake_up_waiters(lock_manager* lckmgr_p, uint256 transaction_id)
{
	// we need to construct the lock_entry_key
	char lock_entry_key[MAX_SERIALIZED_LOCK_ENTRY_SIZE];

	{
		// construct lock_entry_tuple
		char lock_entry_tuple[MAX_SERIALIZED_LOCK_ENTRY_SIZE];
		{
			lock_entry le = {.transaction_id = transaction_id};
			serialize_lock_entry_record(lock_entry_tuple, &le, lckmgr_p);
		}

		// extract key out of it
		extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->tx_locks_td, lock_entry_tuple, lock_entry_key);
	}

	// create an iterator using the first 1 keys (transaction_id)
	bplus_tree_iterator* bpi_p = find_in_bplus_tree(lckmgr_p->tx_locks_root_page_id, lock_entry_key, 1, GREATER_THAN_EQUALS, 1, WRITE_LOCK, lckmgr_p->tx_locks_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);

	// keep looping while the bplus_tree is not empty and it has a current tuple to be processed
	while(!is_empty_bplus_tree(bpi_p) && !is_beyond_max_tuple_bplus_tree_iterator(bpi_p))
	{
		// deserialize the lock_entry into a struct
		lock_entry le;
		deserialize_lock_entry_record(get_tuple_bplus_tree_iterator(bpi_p), &le, lckmgr_p);

		// if it is not the right tuple, we break out
		if(!are_equal_uint256(transaction_id, le.transaction_id))
			break;

		// remove the lock_entry and wake up waiters waiting on this resource
		{
			{
				char lock_entry_key[MAX_SERIALIZED_LOCK_ENTRY_SIZE];
				extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->rs_locks_td, get_tuple_bplus_tree_iterator(bpi_p), lock_entry_key);
				delete_from_bplus_tree(lckmgr_p->rs_locks_root_page_id, lock_entry_key, lckmgr_p->rs_locks_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);
			}

			// then from the table with the iterator
			remove_from_bplus_tree_iterator(bpi_p, GO_NEXT_AFTER_BPLUS_TREE_ITERATOR_REMOVE_OPERATION, NULL, &abort_error);

			// remove was successfull, so wake up all other transactions that are waitinf for this resource
			notify_all_wait_entries_for_resource_of_being_unblocked(lckmgr_p, le.resource_type, le.resource_id, le.resource_id_size);
		}
	}

	delete_bplus_tree_iterator(bpi_p, NULL, &abort_error);
}

// 6 - below function inserts wait_entries for the corresponding conflicts only if the do_insert_wait_entries = 1,
// return value only suggests if there are any lock_conflicts or not
// return = 1, means there are lock conflicts, else it returns 0, if the lock can be readily taken
// please note that this function skips all the lock_entries that have the same transaction_id, and same resource
// uses utility functions of section 1
static int check_lock_conflicts(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, int do_insert_wait_entries)
{
	int has_conflicts = 0;

	// we need to construct the lock_entry_key
	char lock_entry_key[MAX_SERIALIZED_LOCK_ENTRY_SIZE];

	{
		// construct lock_entry_tuple
		char lock_entry_tuple[MAX_SERIALIZED_LOCK_ENTRY_SIZE];
		{
			lock_entry le = {.resource_type = resource_type, .resource_id_size = resource_id_size};
			memory_move(le.resource_id, resource_id, resource_id_size);
			serialize_lock_entry_record(lock_entry_tuple, &le, lckmgr_p);
		}

		// extract key out of it
		extract_key_from_record_tuple_using_bplus_tree_tuple_definitions(lckmgr_p->rs_locks_td, lock_entry_tuple, lock_entry_key);
	}

	// create an iterator using the first 2 keys (resource_type, resource_id)
	bplus_tree_iterator* bpi_p = find_in_bplus_tree(lckmgr_p->rs_locks_root_page_id, lock_entry_key, 2, GREATER_THAN_EQUALS, 0, READ_LOCK, lckmgr_p->rs_locks_td, lckmgr_p->lckmgr_engine->pam_p, NULL, NULL, &abort_error);

	// keep looping while the bplus_tree is not empty and it has a current tuple to be processed
	while(!is_empty_bplus_tree(bpi_p) && !is_beyond_max_tuple_bplus_tree_iterator(bpi_p))
	{
		// deserialize the lock_entry into a struct
		lock_entry le;
		deserialize_lock_entry_record(get_tuple_bplus_tree_iterator(bpi_p), &le, lckmgr_p);

		// if it is not the right tuple, we break out
		if(le.resource_id_size != resource_id_size || memory_compare(le.resource_id, resource_id, resource_id_size))
			break;

		// skip entries for the transaction_id that wants this lock
		if(are_equal_uint256(le.transaction_id, transaction_id))
			continue;

		// check for conflicts, if there are no conflicts, we continue
		if(are_glock_modes_compatible(&(lckmgr_p->lock_matrices[resource_type]), le.lock_mode, new_lock_mode))
			continue;

		// we have conflicts, so set the return value
		has_conflicts = 1;

		// break out, if we are instructed to not insert any wait entries
		if(!do_insert_wait_entries)
			break;

		// insert wait entry for this lock conflict
		{
			wait_entry to_ins = {.waiting_transaction_id = transaction_id, .waiting_task_id = task_id, .transaction_id = le.transaction_id, .resource_type = resource_type, .resource_id_size = resource_id_size};
			memory_move(to_ins.resource_id, resource_id, resource_id_size);
			insert_wait_entry(lckmgr_p, &to_ins);
		}

		// continue ahead with the next tuple
		next_bplus_tree_iterator(bpi_p, NULL, &abort_error);
	}

	delete_bplus_tree_iterator(bpi_p, NULL, &abort_error);

	return has_conflicts;
}

// --

void initialize_lock_manager(lock_manager* lckmgr_p, pthread_mutex_t* external_lock, lock_manager_notifier notifier, uint256 overflow_transaction_id, rage_engine* lckmgr_engine)
{
	lckmgr_p->external_lock = external_lock;

	lckmgr_p->notifier = notifier;

	lckmgr_p->locks_type_count = 0;
	lckmgr_p->lock_matrices = NULL;

	lckmgr_p->lckmgr_engine = lckmgr_engine;

	uint32_t transaction_id_bytes = 0;
	{
		for(uint32_t possible_size = sizeof(uint256); possible_size != 0; possible_size--)
		{
			if(get_byte_from_uint256(overflow_transaction_id, possible_size-1) != 0)
			{
				transaction_id_bytes = possible_size;
				break;
			}
		}
	}
	if(transaction_id_bytes == 0)
		exit(-1);
	printf("lock manager initialized to support %u number of bytes for transaction_id\n", transaction_id_bytes);

	lckmgr_p->resource_id_type_info = malloc(sizeof(data_type_info));
	if(lckmgr_p->resource_id_type_info == NULL)
		exit(-1);
	*(lckmgr_p->resource_id_type_info) = get_variable_length_blob_type("resource_id_type", MAX_RESOURCE_ID_SIZE + 2);

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(4));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "lock_entry", 0, MAX_SERIALIZED_LOCK_ENTRY_SIZE, 4);

		strcpy(dti->containees[0].field_name, "transaction_id");
		dti->containees[0].al.type_info = LARGE_UINT_NON_NULLABLE[transaction_id_bytes];

		strcpy(dti->containees[1].field_name, "resource_type");
		dti->containees[1].al.type_info = UINT_NON_NULLABLE[4];

		strcpy(dti->containees[2].field_name, "resource_id");
		dti->containees[2].al.type_info = lckmgr_p->resource_id_type_info;

		strcpy(dti->containees[3].field_name, "lock_mode");
		dti->containees[3].al.type_info = UINT_NON_NULLABLE[4];

		lckmgr_p->lock_record_def = malloc(sizeof(tuple_def));
		if(lckmgr_p->lock_record_def == NULL)
			exit(-1);
		initialize_tuple_def(lckmgr_p->lock_record_def, dti);
	}

	lckmgr_p->tx_locks_td = malloc(sizeof(bplus_tree_tuple_defs));
	if(lckmgr_p->tx_locks_td == NULL)
		exit(-1);
	if(!init_bplus_tree_tuple_definitions(lckmgr_p->tx_locks_td, &(lckmgr_p->lckmgr_engine->pam_p->pas), lckmgr_p->lock_record_def, tx_locks_keys, all_ascending, 3))
	{
		printf("BUG (in lock_manager) :: could not initialize tx_locks_td\n");
		exit(-1);
	}
	lckmgr_p->tx_locks_root_page_id = get_new_bplus_tree(lckmgr_p->tx_locks_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);

	lckmgr_p->rs_locks_td = malloc(sizeof(bplus_tree_tuple_defs));
	if(lckmgr_p->rs_locks_td == NULL)
		exit(-1);
	if(!init_bplus_tree_tuple_definitions(lckmgr_p->rs_locks_td, &(lckmgr_p->lckmgr_engine->pam_p->pas), lckmgr_p->lock_record_def, rs_locks_keys, all_ascending, 3))
	{
		printf("BUG (in lock_manager) :: could not initialize rs_locks_td\n");
		exit(-1);
	}
	lckmgr_p->rs_locks_root_page_id = get_new_bplus_tree(lckmgr_p->rs_locks_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "wait_entry", 0, MAX_SERIALIZED_WAIT_ENTRY_SIZE, 5);

		strcpy(dti->containees[0].field_name, "waiting_transaction_id");
		dti->containees[0].al.type_info = LARGE_UINT_NON_NULLABLE[transaction_id_bytes];

		strcpy(dti->containees[1].field_name, "waiting_task_id");
		dti->containees[1].al.type_info = UINT_NON_NULLABLE[4];

		strcpy(dti->containees[2].field_name, "transaction_id");
		dti->containees[2].al.type_info = LARGE_UINT_NON_NULLABLE[transaction_id_bytes];

		strcpy(dti->containees[3].field_name, "resource_type");
		dti->containees[3].al.type_info = UINT_NON_NULLABLE[4];

		strcpy(dti->containees[4].field_name, "resource_id");
		dti->containees[4].al.type_info = lckmgr_p->resource_id_type_info;

		lckmgr_p->wait_record_def = malloc(sizeof(tuple_def));
		if(lckmgr_p->wait_record_def == NULL)
			exit(-1);
		initialize_tuple_def(lckmgr_p->wait_record_def, dti);
	}

	lckmgr_p->waits_for_td = malloc(sizeof(bplus_tree_tuple_defs));
	if(lckmgr_p->waits_for_td == NULL)
		exit(-1);
	if(!init_bplus_tree_tuple_definitions(lckmgr_p->waits_for_td, &(lckmgr_p->lckmgr_engine->pam_p->pas), lckmgr_p->wait_record_def, waits_for_keys, all_ascending, 5))
	{
		printf("BUG (in lock_manager) :: could not initialize waits_for_td\n");
		exit(-1);
	}
	lckmgr_p->waits_for_root_page_id = get_new_bplus_tree(lckmgr_p->waits_for_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);

	lckmgr_p->waits_back_td = malloc(sizeof(bplus_tree_tuple_defs));
	if(lckmgr_p->waits_back_td == NULL)
		exit(-1);
	if(!init_bplus_tree_tuple_definitions(lckmgr_p->waits_back_td, &(lckmgr_p->lckmgr_engine->pam_p->pas), lckmgr_p->wait_record_def, waits_back_keys, all_ascending, 5))
	{
		printf("BUG (in lock_manager) :: could not initialize waits_back_td\n");
		exit(-1);
	}
	lckmgr_p->waits_back_root_page_id = get_new_bplus_tree(lckmgr_p->waits_back_td, lckmgr_p->lckmgr_engine->pam_p, lckmgr_p->lckmgr_engine->pmm_p, NULL, &abort_error);
}

uint32_t register_lock_type_with_lock_manager(lock_manager* lckmgr_p, glock_matrix lock_matrix)
{
	// expand the container for lock matrix to hold 1 more element
	lckmgr_p->lock_matrices = realloc(lckmgr_p->lock_matrices, sizeof(glock_matrix) * (lckmgr_p->locks_type_count + 1));
	if(lckmgr_p->lock_matrices == NULL)
		exit(-1);

	// add the element to the end and return it's index, then increment the counter
	lckmgr_p->lock_matrices[lckmgr_p->locks_type_count] = lock_matrix;
	return lckmgr_p->locks_type_count++;
}

uint32_t get_lock_mode_for_lock_from_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size)
{
	// task_id, for the transacton_id, is indeed calling this function, so it is no longer blocked or waiting, so remove it's wait entries
	remove_all_wait_entries_for_task_id(lckmgr_p, transaction_id, task_id);

	return find_lock_entry(lckmgr_p, transaction_id, resource_type, resource_id, resource_id_size);
}

lock_result acquire_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, int non_blocking)
{
	// task_id, for the transacton_id, is indeed calling this function, so it is no longer blocked or waiting, so remove it's wait entries
	remove_all_wait_entries_for_task_id(lckmgr_p, transaction_id, task_id);

	// get the old_lock_mode for the transaction_id and resource given
	uint32_t old_lock_mode = find_lock_entry(lckmgr_p, transaction_id, resource_type, resource_id, resource_id_size);

	// if the old_lock_mode is same as the one requested, then return success
	if(old_lock_mode == new_lock_mode)
		return LOCK_ALREADY_HELD;

	// the return value of this function suggests if we encountered any lock conflicts
	int has_conflicts = check_lock_conflicts(lckmgr_p, transaction_id, task_id, resource_type, resource_id, resource_id_size, new_lock_mode, !non_blocking); // do insert wait entries if it is a blocking call

	if(has_conflicts) // this means failure
	{
		if(non_blocking)
			return LOCKING_FAILED;
		else
			return MUST_BLOCK_FOR_LOCK;
	}

	// now we are sure that we can grab the lock
	// so add or update the lock entry with the new_lock_mode, and wake up waiters
	insert_or_update_lock_entry_and_wake_up_waiters(lckmgr_p, transaction_id, resource_type, resource_id, resource_id_size, new_lock_mode);

	// if the old_lock_mode was not held, then we acquired the lock, else we just transitioned
	if(old_lock_mode == NO_LOCK_HELD_LOCK_MODE)
		return LOCK_ACQUIRED;
	else
		return LOCK_TRANSITIONED;
}

void release_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size)
{
	// task_id, for the transacton_id, is indeed calling this function, so it is no longer blocked or waiting, so remove it's wait entries
	remove_all_wait_entries_for_task_id(lckmgr_p, transaction_id, task_id);

	// try to remove the lock, and wake up waiters if any
	remove_lock_entry_and_wake_up_waiters(lckmgr_p, transaction_id, resource_type, resource_id, resource_id_size);
}

void conclude_all_business_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id)
{
	// transacton_id, is indeed calling this function, so it is no longer expected to be blocked, it is aborting or committing, so we remove all it's wait_entries
	remove_all_wait_entries_for_transaction_id(lckmgr_p, transaction_id);

	// remove all lock_entries and wake up all waiters on those resources
	remove_all_lock_entries_and_wake_up_waiters(lckmgr_p, transaction_id);
}

void debug_print_lock_manager_tables(lock_manager* lckmgr_p)
{
	printf("======================= LOCK MANAGER TABLES ===========================\n\n");
	printf("TX_LOCKS : \n\n");
	print_bplus_tree(lckmgr_p->tx_locks_root_page_id, 1, lckmgr_p->tx_locks_td, lckmgr_p->lckmgr_engine->pam_p, NULL, &abort_error);
	printf("-----------------------------------------------------------------------\n\n");

	printf("RS_LOCKS : \n\n");
	print_bplus_tree(lckmgr_p->rs_locks_root_page_id, 1, lckmgr_p->rs_locks_td, lckmgr_p->lckmgr_engine->pam_p, NULL, &abort_error);
	printf("-----------------------------------------------------------------------\n\n");

	printf("WAITS_FOR : \n\n");
	print_bplus_tree(lckmgr_p->waits_for_root_page_id, 1, lckmgr_p->waits_for_td, lckmgr_p->lckmgr_engine->pam_p, NULL, &abort_error);
	printf("-----------------------------------------------------------------------\n\n");

	printf("WAITS_BACK : \n\n");
	print_bplus_tree(lckmgr_p->waits_back_root_page_id, 1, lckmgr_p->waits_back_td, lckmgr_p->lckmgr_engine->pam_p, NULL, &abort_error);
	printf("-----------------------------------------------------------------------\n\n");
}

const glock_matrix RW_DB_LOCK = {
	.lock_modes_count = 2,// there are 2 modes
	.matrix = (uint8_t[GLOCK_MATRIX_SIZE(2)]){
	//  R  W
		1,    // R
		0, 0, // W
	},
};