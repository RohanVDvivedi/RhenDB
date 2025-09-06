#include<rondb/lock_manager.h>

#include<tuplestore/tuple.h>
#include<tuplestore/data_type_info.h>

#include<tupleindexer/interface/page_access_methods.h>

#include<stdlib.h>

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

void initialize_lock_manager(lock_manager* lckmgr_p, pthread_mutex_t* external_lock, lock_manager_notifier notifier, uint256 overflow_transaction_id, rage_engine* ltbl_engine)
{
	lckmgr_p->external_lock = external_lock;

	lckmgr_p->notifier = notifier;

	lckmgr_p->locks_type_count = 0;
	lckmgr_p->lock_matrices = NULL;

	lckmgr_p->ltbl_engine = ltbl_engine;

	uint32_t transaction_id_bytes = 0;
	{
		for(uint32_t possible_size = sizeof(uint256); possible_size != 0; possible_size++)
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

	lckmgr_p->resource_id_type_info = malloc(sizeof(data_type_info));
	if(lckmgr_p->resource_id_type_info)
		exit(-1);
	*(lckmgr_p->resource_id_type_info) = get_variable_length_blob_type("resource_id_type", MAX_RESOURCE_ID_SIZE);

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(4));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "lock_entry", 0, MAX_SERIALIZED_LOCK_ENTRY_SIZE, 4);

		strcpy(dti->containees[0].field_name, "transaction_id");
		dti->containees[0].al.type_info = LARGE_UINT_NON_NULLABLE[transaction_id_bytes];

		strcpy(dti->containees[1].field_name, "resource_type");
		dti->containees[1].al.type_info = UINT_NON_NULLABLE[4];

		strcpy(dti->containees[2].field_name, "resoucre_id");
		dti->containees[2].al.type_info = lckmgr_p->resource_id_type_info;

		strcpy(dti->containees[3].field_name, "lock_mode");
		dti->containees[3].al.type_info = UINT_NON_NULLABLE[4];

		lckmgr_p->lock_record_def = malloc(sizeof(tuple_def));
		if(lckmgr_p->lock_record_def)
			exit(-1);
		initialize_tuple_def(lckmgr_p->lock_record_def, dti);
	}

	lckmgr_p->tx_locks_td = malloc(sizeof(bplus_tree_tuple_defs));
	if(lckmgr_p->tx_locks_td == NULL)
		exit(-1);
	init_bplus_tree_tuple_definitions(lckmgr_p->tx_locks_td, &(lckmgr_p->ltbl_engine->pam_p->pas), lckmgr_p->lock_record_def, tx_locks_keys, all_ascending, 3);
	lckmgr_p->tx_locks_root_page_id = get_new_bplus_tree(lckmgr_p->tx_locks_td, lckmgr_p->ltbl_engine->pam_p, lckmgr_p->ltbl_engine->pmm_p, NULL, NULL);

	lckmgr_p->rs_locks_td = malloc(sizeof(bplus_tree_tuple_defs));
	if(lckmgr_p->rs_locks_td == NULL)
		exit(-1);
	init_bplus_tree_tuple_definitions(lckmgr_p->rs_locks_td, &(lckmgr_p->ltbl_engine->pam_p->pas), lckmgr_p->lock_record_def, rs_locks_keys, all_ascending, 3);
	lckmgr_p->rs_locks_root_page_id = get_new_bplus_tree(lckmgr_p->rs_locks_td, lckmgr_p->ltbl_engine->pam_p, lckmgr_p->ltbl_engine->pmm_p, NULL, NULL);

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

		strcpy(dti->containees[4].field_name, "resoucre_id");
		dti->containees[4].al.type_info = lckmgr_p->resource_id_type_info;

		lckmgr_p->wait_record_def = malloc(sizeof(tuple_def));
		if(lckmgr_p->wait_record_def)
			exit(-1);
		initialize_tuple_def(lckmgr_p->wait_record_def, dti);
	}

	lckmgr_p->waits_for_td = malloc(sizeof(bplus_tree_tuple_defs));
	if(lckmgr_p->waits_for_td == NULL)
		exit(-1);
	init_bplus_tree_tuple_definitions(lckmgr_p->waits_for_td, &(lckmgr_p->ltbl_engine->pam_p->pas), lckmgr_p->wait_record_def, waits_for_keys, all_ascending, 5);
	lckmgr_p->waits_for_root_page_id = get_new_bplus_tree(lckmgr_p->waits_for_td, lckmgr_p->ltbl_engine->pam_p, lckmgr_p->ltbl_engine->pmm_p, NULL, NULL);

	lckmgr_p->waits_back_td = malloc(sizeof(bplus_tree_tuple_defs));
	if(lckmgr_p->waits_back_td == NULL)
		exit(-1);
	init_bplus_tree_tuple_definitions(lckmgr_p->waits_back_td, &(lckmgr_p->ltbl_engine->pam_p->pas), lckmgr_p->wait_record_def, waits_back_keys, all_ascending, 5);
	lckmgr_p->waits_back_root_page_id = get_new_bplus_tree(lckmgr_p->waits_back_td, lckmgr_p->ltbl_engine->pam_p, lckmgr_p->ltbl_engine->pmm_p, NULL, NULL);
}

uint32_t register_lock_type_with_lock_manager(lock_manager* lckmgr_p, glock_matrix lock_matrix);

lock_result acquire_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size, uint32_t new_lock_mode, int non_blocking);

void notify_task_unblocked_to_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t task_id);

void release_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id, uint32_t resource_type, uint8_t* resource_id, uint8_t resource_id_size);

void release_all_lock_with_lock_manager(lock_manager* lckmgr_p, uint256 transaction_id);