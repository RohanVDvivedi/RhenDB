#include<rondb/lock_manager.h>

#include<tupleindexer/heap_page/heap_page.h>

/*
** internal structures
*/

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

#define MAX_SERIALIZED_LOCK_ENTRY_SIZE (sizeof(uint256) + sizeof(uint32_t) + MAX_RESOURCE_ID_SIZE + sizeof(uint32_t) + 2) // +2 for the size and offset of resource_id

typedef struct active_transaction_entry active_transaction_entry;
struct active_transaction_entry
{
	// the transaction_id in context
	uint256 transaction_id;

	// this transaction waits on this variable if it is waiting for an already locked resource
	pthread_cond_t wait_on;

	int is_waiting:1;

	// for deadlock detection(is_seen and is_on_path bits)
	// for releasing_all_locks(mark all transactions that could be woken up, using the flags here)
	int traversal_flags;

	// below attribites suggest what is it waiting for
	uint32_t resource_type;
	uint8_t resource_id_size;
	uint8_t resource_id[MAX_RESOURCE_ID_SIZE];
	uint32_t lock_mode;

	// when did it start waiting
	uint64_t waiting_from_in_seconds;

	bstnode at_embed_node; // embedded node for active transactions
	bstnode wt_embed_node; // embedded node for waiting transactions
};

// --

/*
** lock_table functions
*/

// insert to heap_table and both the indices

// --

/*
** tx_index functions (transaction_id, resource_type, resource_id, lock_state, lock_mode) -> lock
** to be used to release all locks, upon abort or commit, or release a specific lock
*/

// start a scan

// get current

// go next

// remove from everywhere

// destroy the scan

// --

/*
** rt_index functions (resource_type, resource_id, lock_state, lock_mode) -> lock
** to be used to check lock conflicts with other lock modes on the same resource_type and resource_id, for acquire, modify and deadlock detection
*/

// start a scan

// get current

// go next

// remove from everywhere

// destroy the scan

// --