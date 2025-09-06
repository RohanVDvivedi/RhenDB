#include<rondb/lock_manager.h>

#include<tupleindexer/heap_page/heap_page.h>

#include<cutlery/bst.h>
#include<cutlery/linkedlist.h>

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

#define MAX_SERIALIZED_WAIT_ENTRY_SIZE (sizeof(uint256) + sizeof(uint256) + sizeof(uint32_t) + MAX_RESOURCE_ID_SIZE + sizeof(uint32_t) + 2) // +2 for the size and offset of resource_id

typedef struct wait_entry wait_entry;
struct wait_entry
{
	// transaction_id that is waiting for the lock
	uint256 waiting_transaction_id;

	// task_id of the above transaction that is waiting for the lock
	uint256 waiting_task_id;

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

int insert_wait_entry(lock_manager* lckmgr_p, const wait_entry* we_p);
int remove_wait_entry(lock_manager* lckmgr_p, const wait_entry* we_p);
int remove_all_wait_entries_for_task_id(lock_manager* lckmgr_p, uint256 waiting_transaction_id, uint32_t task_id);
int remove_all_wait_entries_for_transaction_id(lock_manager* lckmgr_p, uint256 waiting_transaction_id);

uint32_t find_lock_entry(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size);
int insert_lock_entry(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t lock_mode);
int remove_lock_entry(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size);

// below function only calls notify_unblocked for all the waiters, that are blocked for this particular resource
void notify_all_wait_entries_for_resource_of_being_unblocked(lock_manager* lckmgr_p, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size);

// below function inserts wait_entries for the corresponding conflicts only if the do_insert_wait_entries = 1,
// return value only suggests if there are any lock_conflicts or not
// return = 1, means there are lock conflicts, else it returns 0, if the lock can be readily taken
// please note that this function skips all the lock_entries that have the same transaction_id, and same resource
int check_lock_conflicts(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, int do_insert_wait_entries);

// --

void initialize_lock_manager(lock_manager* lckmgr_p, pthread_mutex_t* external_lock, lock_manager_notifier notifier, uint256 overflow_transaction_id, rage_engine* ltbl_engine);

uint32_t register_lock_type_with_lock_manager(lock_manager* lckmgr_p, glock_matrix lock_matrix);

lock_result acquire_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, int non_blocking);

void notify_task_unblocked_to_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id);

void release_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size);

void release_all_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id);