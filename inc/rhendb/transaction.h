#ifndef TRANSACTION_H
#define TRANSACTION_H

#include<rhendb/rhendb.h>
#include<rhendb/mvcc_snapshot.h>

#include<lockking/rwlock.h>

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

	rwlock blob_store_lock;
};

// this is the number of temporary extension stores that any 1 particular transaction will maintain
#define TEMPORARY_EXTENSION_STORE_COUNT 64

typedef struct transaction transaction;
struct transaction
{
	// database that this transaction belongs to
	rhendb* rdb;

	// mvcc snapshot, for isolation level
	mvcc_snapshot* snapshot;

	// transaction_id points to the transaction_id in the snapshot for this transaction
	uint256* transaction_id;

	// the array that holds these temporary extension blobs, access them by the hash of the prefix
	temporary_extension_store temp_ext_stores[TEMPORARY_EXTENSION_STORE_COUNT];
};

transaction initialize_transaction(rhendb* rdb);

// deletes the old temp_ext_stores and creates new blobs for them
void reset_temp_ext_stores_in_transaction(transaction* tx);

void deinitialize_transaction(transaction* tx);

#endif