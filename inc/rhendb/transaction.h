#ifndef TRANSACTION_H
#define TRANSACTION_H

#include<rhendb/rhendb.h>
#include<rhendb/mvcc_snapshot.h>

#include<lockking/rwlock.h>

#include<tuplestore/data_type_info.h>

#include<tupleindexer/utils/heap_table_accumulative_notifier.h>

#include<tuplelargetypes/extension_reader_iterator_callback.h>

// the transaction struct only consists of pointers to already created structs, and need to be managed by you
// it is basically a place holder for you (the user), and noone else, the application will not be handling or initializing it for you

// this struct must be 0 initiialized, before trying to use it

// extended types when we are working with them, may be stored on disk blob_store using persistent_acid_rage_engine for the corresponding table
// or in the volatile_rage_engine 
// given below sub_type information helps us organize them and store them at the right place
// this type information for thsi tuplelargetypes itself helps us with that
// this is only used when the extended type is tex, numeric or blob

#define PERSISTENT_EXT_SUB_TYPE    "P" // for values read from disk, i.e. from persistent_acid_rage_engine

#define VOLATILE_EXT_SUB_TYPE      "V" // for temporary data generated from expressions, are stored in this type, in volatile_rage_engine

typedef struct temporary_extension_store temporary_extension_store;
struct temporary_extension_store
{
	uint64_t blob_store_root_page_id;

	// heap table notifier to notifier for the unused_space fixing in the blob_store
	// note: you must fix with write_lock held
	heap_table_accumulative_notifier htan;

	rwlock blob_store_lock;
};

#define MAX_ENTRIES_IN_VOL_BLOBS_HTAN        56 // threshold should be something like 20 to 24 for fixing the accumulated entries
#define TEMP_EXT_BLOB_STORE_FIX_THRESHOLD    25

// this is the number of temporary extension stores that any 1 particular transaction will maintain
#define TEMPORARY_EXTENSION_STORE_COUNT 64

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

	// the array that holds these temporary extension blobs, access them by the hash of the prefix
	temporary_extension_store temp_ext_stores[TEMPORARY_EXTENSION_STORE_COUNT];
};

transaction initialize_transaction(rhendb* rdb);

// deletes the old temp_ext_stores and creates new blobs for them
void reset_temp_ext_stores_in_transaction(transaction* tx);

// if dti_p is not extended then both the attributes are NULL,
// for persistent store only callback returned is NULL
// and for volatile_rage_engine based temp_ext_stores, everything both the return values are present
extension_reader_iterator_callback* get_callback_and_engine_for_extended_type(transaction* tx, const data_type_info* dti_p, rage_engine** ex_engine, extension_reader_iterator_callback* pass_through);

// utility function to be called with write lock on the store to fix unused space entries
void fix_unused_space_entries_in_store(transaction* tx, temporary_extension_store* temp_ext_store);

void deinitialize_transaction(transaction* tx);

#endif