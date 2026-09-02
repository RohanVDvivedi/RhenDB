#ifndef TRANSACTION_H
#define TRANSACTION_H

#include<rhendb/rhendb.h>
#include<rhendb/mvcc_snapshot.h>

#include<lockking/rwlock.h>

#include<tuplestore/data_type_info.h>

#include<tupleindexer/hash_table/hash_table.h>
#include<tupleindexer/linked_page_list/linked_page_list.h>

#include<tupleindexer/utils/heap_table_accumulative_notifier.h>

#include<tuplelargetypes/extension_reader_iterator_callback.h>

// extended types when we are working with them, may be stored on disk blob_store using persistent_acid_rage_engine for the corresponding table
// or in the volatile_rage_engine 
// given below sub_type information helps us organize them and store them at the right place
// this type information for thsi tuplelargetypes itself helps us with that
// this is only used when the extended type is tex, numeric or blob

typedef struct temporary_extension_store temporary_extension_store;
struct temporary_extension_store
{
	uint64_t blob_store_root_page_id;

	// heap table notifier to notifier for the unused_space fixing in the blob_store
	// note: you must fix with write_lock held
	heap_table_accumulative_notifier htan;

	rwlock blob_store_lock;
};

// every tuple inserted (even update is a delete followed by insert) must have it's tuple_pointer inserted in this hashset
// this allows the source operators like table scans and index scans know which tuples to not return from the scan
// this enables scanning and then updating the tuple (effectively an insert), and now the scans can skip this particular tuple_pointer and avoid double processing it
// every insert or update (delete then insert), must register it's inserted tuple pointer in this hashset

typedef struct hashset_for_tuple_pointers hashset_for_tuple_pointers;
struct hashset_for_tuple_pointers
{
	hash_table_handle root_handle;

	uint64_t entries_count;

	// record_def is simply the tuple_pointer_tuple_def and it itself is the key
	hash_table_tuple_defs httd;

	rwlock hash_table_lock;
};

// this is the number of temporary extension stores that any 1 particular transaction will maintain
#define TEMPORARY_EXTENSION_STORE_COUNT 64

// savepoint log is the place where we store table_id, partition_id and tuple_pointer for the insert and deleted so that rollback to savepoint can undo it

typedef enum savepoint_log_type savepoint_log_type;
enum savepoint_log_type
{
	INSERTION_SAVEPOINT_LOG = 1,
	DELETION_SAVEPOINT_LOG = 2,
	NEW_SAVEPOINT_NAME_LOG = 3,
};

typedef struct savepoint_log savepoint_log;
struct savepoint_log
{
	pthread_mutex_t savepoint_lock;

	uint64_t savepoint_log_root;

	linked_page_list_tuple_defs savepoint_log_defs;

	uint64_t savepoint_logs_count;

	hash_table_handle savepoint_names_set_handle;

	hash_table_tuple_defs savepoint_names_set_tuple_defs;

	uint64_t savepoint_names_count;

	// below are the two tuple definitions that support the above 2 structures

	tuple_def* savepoint_log_def;
	data_type_info* savepoint_log_dti;

	tuple_def* savepoint_name_def;
	data_type_info* savepoint_name_dti;
};

typedef struct query_plan query_plan;

typedef struct transaction transaction;
struct transaction
{
	// database that this transaction belongs to
	rhendb* rdb;

	// mvcc snapshot, for isolation level
	mvcc_snapshot* snapshot;

	// transaction_id points to the transaction_id in the snapshot for this transaction
	uint256* transaction_id;

	// curr_query is the parent most query being run, hels up kill the whole query when needed
	query_plan* curr_query;

	// every inserted tuple pointer must be registered here, so the source operator scans can skip it
	hashset_for_tuple_pointers inserted_tuple_pointers;

	// every insert/delete is inserted here to allow undoing upon rollback to savepoint
	savepoint_log savepoint_logs;

	// the array that holds these temporary extension blobs, access them by the hash of the prefix
	temporary_extension_store temp_ext_stores[TEMPORARY_EXTENSION_STORE_COUNT];
};

// additional_flags for the insertion operator and scan operator, they toggle the additional book keeping
// that the operator is expected to perform, over and above the insert itself
#define RESCAN_PROTECTION_ENABLED   1
// used when the same query scans and inserts OR updates to the same table, only insertion or only scan on the same table does not need it
// when passed insertion operator inserts inserted tuple pointers to inserted_tuple_pointers, and the same query scans checks if it needs to skip them
#define SAVEPOINT_LOGGING_ENABLED   2
// used when there is a savepoint defined prior to this call
// when passed every insertion and deletion is logged in the savepoint_log as (takble_id, partition_id, tuple_pointer), so that it can be rolled back, by null-ing the xmin or xmax

#define IS_RESCAN_PROTECTION_ENABLED(flags)  ((flags) & RESCAN_PROTECTION_ENABLED)

#define IS_SAVEPOINT_LOGGING_ENABLED(flags)  ((flags) & SAVEPOINT_LOGGING_ENABLED)

transaction initialize_transaction(rhendb* rdb);

// registers that there was an insert at the given tuple_pointer for the current query
void register_inserted_tuple_pointer(transaction* tx, tuple_pointer tptr);

// checks if a given tuple_pointer was inserted for the current query, so that the source scan operators can skip it
int was_registered_as_inserted_tuple_pointer(transaction* tx, tuple_pointer tptr);

// deletes the old inserted_tuple_pointers and creates new hash_table for it
// this needs to be called after the current query is completed, so that this very same inserted tuple pointer are visible to the next query in the same transaction
void reset_inserted_tuple_pointers(transaction* tx);

// this function allows only type = INSERTION_SAVEPOINT_LOG or DELETION_SAVEPOINT_LOG
void log_to_savepoint_log(transaction* tx, savepoint_log_type type, uint64_t table_id, uint64_t partition_id, tuple_pointer tptr);

// the below 4 savepoint functions although guarded by savepoint_lock, must be called only while no query plan is active on the transaction

// this function inserts NEW_SAVEPOINT_NAME_LOG, and a savepoint name entry in savepoint_names_set
int insert_new_savepoint(transaction* tx, const dstring* savepoint_name); // fails if the savepoint name is more than 64 bytes long
void delete_savepoint(transaction* tx, const dstring* savepoint_name); // and this one only deleted from savepoint_names_set
int exists_savepoint(transaction* tx, const dstring* savepoint_name);

// if the return value is 1 and returned type is NEW_SAVEPOINT_NAME_LOG, then deinit_dstring must be called on the savepoint_name
int peek_top_of_savepoint_log(transaction* tx, savepoint_log_type* type, dstring* savepoint_name, uint64_t* table_id, uint64_t* partition_id, tuple_pointer* tptr);
int pop_top_of_savepoint_log(transaction* tx);
// same functions as above but work with the bottom
int peek_bottom_of_savepoint_log(transaction* tx, savepoint_log_type* type, dstring* savepoint_name, uint64_t* table_id, uint64_t* partition_id, tuple_pointer* tptr);
int pop_bottom_of_savepoint_log(transaction* tx);

// deletes the old temp_ext_stores and creates new blobs for them
// this needs to be done after completion of the current query, after which the temporary memory for the extended objects produced for this query is no longer needed
void reset_temp_ext_stores_in_transaction(transaction* tx);

// if dti_p is not extended then both the attributes are NULL,
// for persistent store only callback returned is NULL
// and for volatile_rage_engine based temp_ext_stores, everything both the return values are present
extension_reader_iterator_callback* get_callback_and_engine_for_extended_type(transaction* tx, const data_type_info* dti_p, rage_engine** ex_engine, extension_reader_iterator_callback* pass_through);

// utility function to be called with write lock on the store to fix unused space entries
void fix_unused_space_entries_in_store(transaction* tx, temporary_extension_store* temp_ext_store);

void deinitialize_transaction(transaction* tx);

#endif