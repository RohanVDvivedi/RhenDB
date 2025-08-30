#include<rondb/lock_manager.h>

#include<tupleindexer/heap_page/heap_page.h>

#include<cutlery/bst.h>

/*
** internal structures
*/

typedef struct active_transaction_entry active_transaction_entry;
struct active_transaction_entry
{
	// the transaction_id in context
	uint256 transaction_id;

	// this transaction waits on this variable if it is waiting for an already locked resource
	// broadcast here if soem other transaction_id finds this transaction to be waiting on it
	// we will recheck the condition and may again go back to sleep
	pthread_cond_t wait_on;

	// number of transaction_id's threads waiting on wait_on condition variable
	uint64_t wait_on_thread_counter;

	// for deadlock detection(is_seen and is_on_path bits)
	// for releasing_all_locks(mark all transactions that could be woken up, using the flags here)
	int traversal_flags;

	bstnode embed_node; // embedded node for active transactions
};

// compare function using transaction_id for active_transaction_entry
static int compare_active_transaction_id_entry_by_transaction_id(const void* data1, const void* data2)
{
	const active_transaction_entry* ate1 = data1;
	const active_transaction_entry* ate2 = data2;
	return compare_uint256_with_ptrs(&(ate1->transaction_id), &(ate2->transaction_id));
}

// hash function using transaction_id for active_transaction_entry
static cy_uint hash_active_transaction_id_entry_by_transaction_id(const void* data)
{
	const active_transaction_entry* ate = data;
	return ate->transaction_id.limbs[0];
}

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

#define MAX_SERIALIZED_WAIT_ENTRY_SIZE (sizeof(uint256) + sizeof(uint256) + sizeof(uint32_t) + MAX_RESOURCE_ID_SIZE + sizeof(uint32_t) + 2) // +2 for the size and offset of resource_id

typedef struct wait_entry wait_entry;
struct wait_entry
{
	// transaction_id that is waiting for the lock
	uint256 waiting_transaction_id;

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
	set_element_in_tuple(lckmgr_p->wait_record_def, STATIC_POSITION(1), to, &((user_value){.large_uint_value = from->transaction_id}), 0);
	set_element_in_tuple(lckmgr_p->wait_record_def, STATIC_POSITION(2), to, &((user_value){.uint_value = from->resource_type}), 0);
	set_element_in_tuple(lckmgr_p->wait_record_def, STATIC_POSITION(3), to, &((user_value){.blob_size = from->resource_id_size, .blob_value = from->resource_id}), MAX_RESOURCE_ID_SIZE);
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
		to->transaction_id = uval.large_uint_value;
	}

	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, lckmgr_p->wait_record_def, STATIC_POSITION(2), from);
		to->resource_type = uval.uint_value;
	}

	{
		user_value uval;
		get_value_from_element_from_tuple(&uval, lckmgr_p->wait_record_def, STATIC_POSITION(3), from);
		to->resource_id_size = uval.blob_size;
		memory_move(to->resource_id, uval.blob_value, to->resource_id_size);
	}
}

// --

/*
** utility functions
*/

// --