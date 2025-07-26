#include<rondb/transaction_table.h>

#include<stdlib.h>
#include<unistd.h>

// entry for the currently_active_transaction_ids bst
typedef struct active_transaction_id_entry active_transaction_id_entry;
struct active_transaction_id_entry
{
	// this transaction_id will be in TX_IN_PROGRESS status
	uint256 transaction_id; // note: it should always be the first attribute

	bstnode embed_node;
};

static int compare_active_transaction_id_entry(const void* a, const void* b)
{
	return compare_uint256_with_ptrs(&(((const active_transaction_id_entry*)a)->transaction_id), &(((const active_transaction_id_entry*)b)->transaction_id));
}

// entry for the transaction_table_cache cachemap
typedef struct passive_transaction_id_entry passive_transaction_id_entry;
struct passive_transaction_id_entry
{
	// this transaction_id will be in either TX_ABORTED or TX_COMMITTED status, check the status attribute
	uint256 transaction_id; // note: it should always be the first attribute

	// this status will be either TX_ABORTED or TX_COMMITTED, only
	transaction_status status;

	cchnode embed_node;
};

static int compare_passive_transaction_id_entry(const void* a, const void* b)
{
	return compare_uint256_with_ptrs(&(((const passive_transaction_id_entry*)a)->transaction_id), &(((const passive_transaction_id_entry*)b)->transaction_id));
}

static cy_uint hash_passive_transaction_id_entry(const void* a)
{
	return ((const passive_transaction_id_entry*)a)->transaction_id.limbs[0];
}

/*
	internal table functions
	Note: You do not need lock to access the persistent transaction table as the MinTxEngine will take care of the ACID-compliant access to it
	Things become simple, as you only read transaction_ids of other tansactions and update your own
*/

// reads the transaction status as is from the table, fails if unassigned or if the entry does not exists
static int get_transaction_status_from_table(transaction_table* ttbl, uint256 transaction_id, transaction_status* status)
{
	uint64_t bucket_id;
	uint32_t sub_bucket_id;

	{
		uint256 q;
		uint256 r = div_uint256(&q, transaction_id, get_uint256(ttbl->transaction_statuses_per_bitmap_page));
		bucket_id = q.limbs[0];
		sub_bucket_id = r.limbs[0];
	}

	while(1)
	{
		// initialize result to 0, i.e. not yet produced
		int result = 0;

		int abort_error = 0;

		page_table_range_locker* ptrl_p = NULL;
		persistent_page bucket_page = get_NULL_persistent_page(ttbl->ttbl_engine->pam_p);

		ptrl_p = get_new_page_table_range_locker(ttbl->transaction_table_root_page_id, (bucket_range){.first_bucket_id = bucket_id, .last_bucket_id = bucket_id}, ttbl->pttd_p, ttbl->ttbl_engine->pam_p, NULL, NULL, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		uint64_t bucket_page_id = get_from_page_table(ptrl_p, bucket_id, NULL, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		if(bucket_page_id != ttbl->ttbl_engine->pam_p->pas.NULL_PAGE_ID)
		{
			bucket_page = acquire_persistent_page_with_lock(ttbl->ttbl_engine->pam_p, NULL, bucket_page_id, READ_LOCK, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			uint64_t status_field = get_bit_field_on_bitmap_page(&bucket_page, sub_bucket_id, &(ttbl->ttbl_engine->pam_p->pas), ttbl->bitmap_page_tuple_def_p);

			// then set the result if read
			if(status_field != 0)
			{
				(*status) = status_field;
				result = 1;
			}
		}

		// release all resources now
		ABORT_ERROR:;
		if(!is_persistent_page_NULL(&bucket_page, ttbl->ttbl_engine->pam_p))
		{
			release_lock_on_persistent_page(ttbl->ttbl_engine->pam_p, NULL, &bucket_page, NONE_OPTION, &abort_error);
			bucket_page = get_NULL_persistent_page(ttbl->ttbl_engine->pam_p);
		}
		if(ptrl_p != NULL)
		{
			delete_page_table_range_locker(ptrl_p, NULL, NULL, NULL, &abort_error);
			ptrl_p = NULL;
		}

		// if read done, i.e. no abort_error, then return result
		if(abort_error == 0)
			return result;

		// sleep for a second and try again
		sleep(1);
	}

	// never reaches here
	return 0;
}

// this is the maximum transaction_id that was never assigned
static void get_min_unassigned_transaction_id(transaction_table* ttbl, uint256* transaction_id)
{
	while(1)
	{
		int abort_error = 0;

		page_table_range_locker* ptrl_p = NULL;
		persistent_page bucket_page = get_NULL_persistent_page(ttbl->ttbl_engine->pam_p);

		// lock the complete table
		ptrl_p = get_new_page_table_range_locker(ttbl->transaction_table_root_page_id, (bucket_range){.first_bucket_id = 0, .last_bucket_id = UINT64_MAX}, ttbl->pttd_p, ttbl->ttbl_engine->pam_p, NULL, NULL, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		// find greatest non-NULL bucket_id
		uint64_t bucket_id = UINT64_MAX;
		uint64_t bucket_page_id = find_non_NULL_PAGE_ID_in_page_table(ptrl_p, &bucket_id, LESSER_THAN_EQUALS, NULL, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		if(bucket_page_id != ttbl->ttbl_engine->pam_p->pas.NULL_PAGE_ID)
		{
			bucket_page = acquire_persistent_page_with_lock(ttbl->ttbl_engine->pam_p, NULL, bucket_page_id, READ_LOCK, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			// I know, we locked the entire tree, but no one will access transaction table before or during this function call, as this will be the first call to the transaction table, hence we will not delete the page table range locker immediately after locking the bucket

			for(uint32_t sub_bucket_id = ttbl->transaction_statuses_per_bitmap_page; sub_bucket_id > 0; sub_bucket_id--)
			{
				// fetch the immediately previous sub_bucket_id
				// bucket_id * transaction_statuses_per_bitmap_page + (sub_bucket_id-1)
				// if it has status_field == 0, then (bucket_id * transaction_statuses_per_bitmap_page + sub_bucket_id) is the answer
				uint64_t status_field = get_bit_field_on_bitmap_page(&bucket_page, (sub_bucket_id - 1), &(ttbl->ttbl_engine->pam_p->pas), ttbl->bitmap_page_tuple_def_p);

				if(status_field != 0)
				{
					mul_uint256(transaction_id, get_uint256(bucket_id), get_uint256(ttbl->transaction_statuses_per_bitmap_page));
					add_uint256(transaction_id, get_uint256(sub_bucket_id), (*transaction_id));
					break;
				}
			}
		}
		else // if none exists ask the callee to start allotting new transactions from 0
			(*transaction_id) = get_0_uint256();

		// release all resources now
		ABORT_ERROR:;
		if(!is_persistent_page_NULL(&bucket_page, ttbl->ttbl_engine->pam_p))
		{
			release_lock_on_persistent_page(ttbl->ttbl_engine->pam_p, NULL, &bucket_page, NONE_OPTION, &abort_error);
			bucket_page = get_NULL_persistent_page(ttbl->ttbl_engine->pam_p);
		}
		if(ptrl_p != NULL)
		{
			delete_page_table_range_locker(ptrl_p, NULL, NULL, NULL, &abort_error);
			ptrl_p = NULL;
		}

		// if read done, i.e. no abort_error, then return result
		if(abort_error == 0)
			return ;

		// sleep for a second and try again
		sleep(1);
	}

	// never reaches here
	return ;
}

// updates transaction status for the transaction_id on the table, as is creates a new page_table entry and a bitmap page if required
// if a flush is set an immediate flush is performed while committing the mini transaction that performed the write
// must never fail
static int set_transaction_status_in_table(transaction_table* ttbl, uint256 transaction_id, transaction_status status, int flush)
{
	uint64_t bucket_id;
	uint32_t sub_bucket_id;

	{
		uint256 q;
		uint256 r = div_uint256(&q, transaction_id, get_uint256(ttbl->transaction_statuses_per_bitmap_page));
		bucket_id = q.limbs[0];
		sub_bucket_id = r.limbs[0];
	}

	uint64_t page_latches_to_be_borrowed = 0;

	while(1)
	{
		int abort_error = 0;

		page_table_range_locker* ptrl_p = NULL;
		persistent_page bucket_page = get_NULL_persistent_page(ttbl->ttbl_engine->pam_p);

		// we are fine with waiting for atmost a second, and we hold no latches
		void* sub_transaction_id = NULL;
		while(sub_transaction_id == NULL)
			sub_transaction_id = ttbl->ttbl_engine->allot_new_sub_transaction_id(ttbl->ttbl_engine->context, 1000000ULL, page_latches_to_be_borrowed);

		ptrl_p = get_new_page_table_range_locker(ttbl->transaction_table_root_page_id, (bucket_range){.first_bucket_id = bucket_id, .last_bucket_id = bucket_id}, ttbl->pttd_p, ttbl->ttbl_engine->pam_p, ttbl->ttbl_engine->pmm_p, sub_transaction_id, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		uint64_t bucket_page_id = get_from_page_table(ptrl_p, bucket_id, sub_transaction_id, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		if(bucket_page_id == ttbl->ttbl_engine->pam_p->pas.NULL_PAGE_ID)
		{
			// bucket_page for this bucket does not exists, so allocate 1 and insert it into the page_table
			bucket_page = get_new_bitmap_page_with_write_lock(&(ttbl->ttbl_engine->pam_p->pas), ttbl->bitmap_page_tuple_def_p, ttbl->ttbl_engine->pam_p, ttbl->ttbl_engine->pmm_p, sub_transaction_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			// set bucket_id -> bucket_page_id mapping for the new page
			set_in_page_table(ptrl_p, bucket_id, bucket_page.page_id, sub_transaction_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;
		}
		else
		{
			bucket_page = acquire_persistent_page_with_lock(ttbl->ttbl_engine->pam_p, sub_transaction_id, bucket_page_id, WRITE_LOCK, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;
		}

		set_bit_field_on_bitmap_page(&bucket_page, sub_bucket_id, (uint64_t)(status), &(ttbl->ttbl_engine->pam_p->pas), ttbl->bitmap_page_tuple_def_p, ttbl->ttbl_engine->pmm_p, sub_transaction_id, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		// release all resources now
		ABORT_ERROR:;
		if(!is_persistent_page_NULL(&bucket_page, ttbl->ttbl_engine->pam_p))
		{
			release_lock_on_persistent_page(ttbl->ttbl_engine->pam_p, sub_transaction_id, &bucket_page, NONE_OPTION, &abort_error);
			bucket_page = get_NULL_persistent_page(ttbl->ttbl_engine->pam_p);
		}
		if(ptrl_p != NULL)
		{
			delete_page_table_range_locker(ptrl_p, NULL, NULL, sub_transaction_id, &abort_error);
			ptrl_p = NULL;
		}
		ttbl->ttbl_engine->complete_sub_transaction(ttbl->ttbl_engine->context, sub_transaction_id, flush, NULL, 0, &page_latches_to_be_borrowed);

		// if read done, i.e. no abort_error, then return result
		if(abort_error == 0)
			return 1;

		// sleep for a second and try again
		sleep(1);
	}

	// never reaches here
	return 0;
}

// --

/*
	internal cache functions
	must be called with transaction_table_cache_lock held in write lock mode (exclusive lock)
*/

// reads the transaction status as is from the cache, fails if the entry does not exists in the cache
// if an entry is found it is bumped in the cache, so as to avoid immediate eviction
static int get_transaction_status_from_cache(transaction_table* ttbl, uint256 transaction_id, transaction_status* status)
{
	// find one that equals, else fail with 0
	const passive_transaction_id_entry* ptid_p = find_equals_in_cachemap(&(ttbl->transaction_table_cache), &transaction_id);
	if(ptid_p == NULL)
		return 0;

	// then bump the found entry, retrieve status and return 1
	bump_element_in_cachemap(&(ttbl->transaction_table_cache), ptid_p);
	(*status) = ptid_p->status;
	return 1;
}

// inserts a new entry in the cache for the given transaction_id OR updates and bumps it, if an entry for the transaction_id exists
static void set_transaction_status_in_cache(transaction_table* ttbl, uint256 transaction_id, transaction_status status)
{
	// find one that equals
	passive_transaction_id_entry* ptid_p = (passive_transaction_id_entry*) find_equals_in_cachemap(&(ttbl->transaction_table_cache), &transaction_id);

	if(ptid_p == NULL)
	{
		// if there is space for a new entry create a new one
		if(ttbl->transaction_table_cache_capacity < get_element_count_cachemap(&(ttbl->transaction_table_cache)))
		{
			// if find fails, allocate a new entry
			ptid_p = malloc(sizeof(passive_transaction_id_entry));
			if(ptid_p == NULL)
				exit(-1);
		}
		else // else evict an existing one
		{
			ptid_p = (passive_transaction_id_entry*) get_evictable_element_from_cachemap(&(ttbl->transaction_table_cache));
			remove_from_cachemap(&(ttbl->transaction_table_cache), ptid_p);
		}

		// initialize it
		ptid_p->transaction_id = transaction_id;
		ptid_p->status = status;
		initialize_cchnode(&(ptid_p->embed_node));

		// insert it
		insert_in_cachemap(&(ttbl->transaction_table_cache), ptid_p);
	}
	else
	{
		// else if found, set status and bump the element
		ptid_p->status = status;
		bump_element_in_cachemap(&(ttbl->transaction_table_cache), ptid_p);
	}
}

// --

/*
	internal functions for currently_active_transaction_ids
	must be called with transaction_table_cache_lock held in read OR write lock mode as per access type
*/

// checks if a transaction_id exists in currently_active_transaction_ids
static const active_transaction_id_entry* find_in_currently_active_transaction_ids(const transaction_table* ttbl, uint256 transaction_id)
{
	return find_equals_in_bst(&(ttbl->currently_active_transaction_ids), &transaction_id, FIRST_OCCURENCE);
}

// iterate over all the currently_active_transaction_ids
static void for_each_in_order_in_currently_active_transaction_ids(const transaction_table* ttbl, void (*operation)(const void* data, const void* additional_params), const void* additional_params)
{
	for_each_in_bst(&(ttbl->currently_active_transaction_ids), IN_ORDER, operation, additional_params);
}

// insert to currently_active_transaction_ids
static int insert_in_currently_active_transaction_ids(transaction_table* ttbl, uint256 transaction_id)
{
	active_transaction_id_entry* atid_p = (active_transaction_id_entry*) find_equals_in_bst(&(ttbl->currently_active_transaction_ids), &transaction_id, FIRST_OCCURENCE);
	if(atid_p != NULL)
		return 0;

	// malloc a new entry
	atid_p = malloc(sizeof(active_transaction_id_entry));
	if(atid_p == NULL)
		exit(-1);

	// initialize it
	atid_p->transaction_id = transaction_id;
	initialize_bstnode(&(atid_p->embed_node));

	// then insert it
	return insert_in_bst(&(ttbl->currently_active_transaction_ids), atid_p);
}

// remove a entry from the currently_active_transaction_ids
static int remove_from_currently_active_transaction_ids(transaction_table* ttbl, active_transaction_id_entry* atid_p)
{
	int removed = remove_from_bst(&(ttbl->currently_active_transaction_ids), atid_p);

	free(atid_p);

	return removed;
}

// --

void initialize_transaction_table(transaction_table* ttbl, uint64_t* root_page_id, rage_engine* ttbl_engine, uint32_t transaction_table_cache_capacity)
{
	// initialize the root_page_id of the persistent transaction table
	ttbl->transaction_table_root_page_id = (*root_page_id);

	// set the provided transaction table engine, the persistent rage engine possibly MinTxEngine
	ttbl->ttbl_engine = ttbl_engine;

	// intitialize the pttd_p
	ttbl->pttd_p = malloc(sizeof(page_table_tuple_defs));
	if(ttbl->pttd_p == NULL)
		exit(-1);
	if(!init_page_table_tuple_definitions(ttbl->pttd_p, &(ttbl->ttbl_engine->pam_p->pas)))
	{
		printf("BUG (in transaction_table) :: could not initialize page_table_tuple_defs\n");
		exit(-1);
	}

	// initialize the bitmap page tuple def for each of the buckets
	ttbl->bitmap_page_tuple_def_p = get_tuple_definition_for_bitmap_page(&(ttbl->ttbl_engine->pam_p->pas), 2, &(ttbl->transaction_statuses_per_bitmap_page));
	if(ttbl->bitmap_page_tuple_def_p == NULL)
		exit(-1);

	// finally ensure that the (*root_page_id) is not NULL_PAGE_ID
	if((*root_page_id) == ttbl->ttbl_engine->pam_p->pas.NULL_PAGE_ID)
	{
		// create and initialize the root page for the page table
		{
			uint64_t page_latches_to_be_borrowed = 0;
			while(1)
			{
				int abort_error = 0;

				// we are fine with waiting for atmost a second, and we hold no latches
				void* sub_transaction_id = NULL;
				for(int i = 0; i < 3 && sub_transaction_id == NULL; i++)
					sub_transaction_id = ttbl->ttbl_engine->allot_new_sub_transaction_id(ttbl->ttbl_engine->context, 1000000ULL, page_latches_to_be_borrowed);
				if(sub_transaction_id == NULL)
				{
					printf("FAILED (in transaction_table) :: spent 3 seconds trying to start a sub transaction to create a new transaction table but failed\n");
					exit(-1);
				}

				(*root_page_id) = get_new_page_table(ttbl->pttd_p, ttbl->ttbl_engine->pam_p, ttbl->ttbl_engine->pmm_p, sub_transaction_id, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;

				ABORT_ERROR:
				ttbl->ttbl_engine->complete_sub_transaction(ttbl->ttbl_engine->context, sub_transaction_id, 1, NULL, 0, &page_latches_to_be_borrowed);

				if(abort_error == 0)
					break;
			}
		}

		ttbl->transaction_table_root_page_id = (*root_page_id);
	}

	// initialize locks
	initialize_rwlock(&(ttbl->transaction_table_cache_lock), NULL);
	initialize_rwlock(&(ttbl->transaction_table_lock), NULL);

	// initialize active and passive transaction_id caches
	initialize_bst(&(ttbl->currently_active_transaction_ids), RED_BLACK_TREE, &simple_comparator(compare_active_transaction_id_entry), offsetof(active_transaction_id_entry, embed_node));
	ttbl->transaction_table_cache_capacity = transaction_table_cache_capacity;
	initialize_cachemap(&(ttbl->transaction_table_cache), NULL, NEVER_PINNED, ((transaction_table_cache_capacity / 5) + 5), &simple_hasher(hash_passive_transaction_id_entry), &simple_comparator(compare_passive_transaction_id_entry), offsetof(passive_transaction_id_entry, embed_node));

	// compute the overflow_transaction_id, that you not go at or beyond
	mul_uint256(&(ttbl->overflow_transaction_id), get_uint256(UINT64_MAX), get_uint256(ttbl->transaction_statuses_per_bitmap_page));

	// initialize transaction_ids that are assignable
	get_min_unassigned_transaction_id(ttbl, &(ttbl->next_assignable_transaction_id_at_boot));
	ttbl->next_assignable_transaction_id = ttbl->next_assignable_transaction_id_at_boot;
}

#include<rondb/mvcc_snapshot.h>

static void mvcc_snapshot_inserter(const void* data, const void* additional_params)
{
	mvcc_snapshot* snp = (mvcc_snapshot*) additional_params;
	const active_transaction_id_entry* atid_p = data;
	if(!insert_in_progress_transaction_in_mvcc_snapshot(snp, atid_p->transaction_id))
	{
		printf("BUG (in transaction_table) :: inserting an active transaction_id in the mvcc_snapshot failed\n");
		exit(-1);
	}
}

mvcc_snapshot* get_new_transaction_id(transaction_table* ttbl)
{
	// allocate a mvcc snapshot
	mvcc_snapshot* snp = malloc(sizeof(mvcc_snapshot));
	if(snp == NULL)
		exit(-1);

	write_lock(&(ttbl->transaction_table_cache_lock), BLOCKING);

	// initialize mvcc snapshot
	initialize_mvcc_snapshot(snp, ttbl->next_assignable_transaction_id);
	for_each_in_order_in_currently_active_transaction_ids(ttbl, mvcc_snapshot_inserter, ttbl);
	finalize_mvcc_snapshot(snp);

	// increment ttbl->next_assignable_transaction_id
	if(!add_overflow_safe_uint256(&(ttbl->next_assignable_transaction_id), ttbl->next_assignable_transaction_id, get_1_uint256(), ttbl->overflow_transaction_id))
	{
		// overflow occurred
		printf("BUG (in transaction_table) :: transaction id overflow occurred\n");
		exit(-1);
	}

	// insert it as an active new transaction_id
	insert_in_currently_active_transaction_ids(ttbl, snp->transaction_id);

	write_lock(&(ttbl->transaction_table_lock), BLOCKING);

	write_unlock(&(ttbl->transaction_table_cache_lock));

	// set the transaction status in the persistent table, with flush=1
	set_transaction_status_in_table(ttbl, snp->transaction_id, TX_IN_PROGRESS, 1);

	write_unlock(&(ttbl->transaction_table_lock));

	return snp;
}

transaction_status get_transaction_status(transaction_table* ttbl, uint256 transaction_id)
{
	// if transaction_id >= overflow_transaction_id, then there is a bug
	if(compare_uint256(transaction_id, ttbl->overflow_transaction_id) >= 0)
	{
		printf("BUG (in transaction_table) :: out-of-bounds transaction id encountered\n");
		exit(-1);
	}

	write_lock(&(ttbl->transaction_table_cache_lock), BLOCKING);

	// check if it is currently active
	if(NULL != find_in_currently_active_transaction_ids(ttbl, transaction_id))
	{
		write_unlock(&(ttbl->transaction_table_cache_lock));
		return TX_IN_PROGRESS;
	}

	// try to get the cached copy, now it can not be TX_IN_PROGRESS transaction
	transaction_status status = 0;
	if(get_transaction_status_from_cache(ttbl, transaction_id, &status))
	{
		write_unlock(&(ttbl->transaction_table_cache_lock));
		return status;
	}

	read_lock(&(ttbl->transaction_table_lock), READ_PREFERRING, BLOCKING);

	// try and fetch it from the disk
	if(!get_transaction_status_from_table(ttbl, transaction_id, &status))
	{
		// if you can not even find it in the table then it is a bug
		printf("BUG (in transaction_table) :: attempt to get transaction status for transaction id, that was never assigned\n");
		exit(-1);
	}

	// any transaction that was in status TX_IN_PROGRESS, before boot is considered to be committed
	if(compare_uint256(transaction_id, ttbl->next_assignable_transaction_id_at_boot) < 0 && status == TX_IN_PROGRESS)
	{
		set_transaction_status_in_table(ttbl, transaction_id, TX_ABORTED, 0); // this operation can be done in just read lock as it is idempotent, and concurrent reads will always eventually get the fixed value, so a flush is not necessary
		status = TX_ABORTED;
	}

	read_unlock(&(ttbl->transaction_table_lock));

	// insert a cached copy for it, to next time find it quickly
	set_transaction_status_in_cache(ttbl, transaction_id, status);

	write_unlock(&(ttbl->transaction_table_cache_lock));

	return status;
}

int update_transaction_status(transaction_table* ttbl, uint256 transaction_id, transaction_status status)
{
	// if transaction_id >= overflow_transaction_id, then there is a bug
	if(compare_uint256(transaction_id, ttbl->overflow_transaction_id) >= 0)
	{
		printf("BUG (in transaction_table) :: out-of-bounds transaction id encountered\n");
		exit(-1);
	}

	// you can voluntarily only write committed or aborted to the transaction_id
	if(status != TX_ABORTED && status != TX_COMMITTED)
	{
		printf("BUG (in transaction_table) :: caller attempting to set transaction status to something other than TX_ABORTED or TX_COMMITTED\n");
		exit(-1);
	}

	write_lock(&(ttbl->transaction_table_cache_lock), BLOCKING);

	// find a corresponsing active_transaction_id_entry and remove it from the active transactions
	active_transaction_id_entry* atid_p = (active_transaction_id_entry*) find_in_currently_active_transaction_ids(ttbl, transaction_id);
	if(NULL == atid_p)
	{
		printf("BUG (in transaction_table) :: caller attempting to update transaction status for a non-TX_IN_PROGRESS transaction\n");
		exit(-1);
	}
	remove_from_currently_active_transaction_ids(ttbl, atid_p);

	// insert a cached copy for it
	set_transaction_status_in_cache(ttbl, transaction_id, status);

	write_lock(&(ttbl->transaction_table_lock), BLOCKING);

	write_unlock(&(ttbl->transaction_table_cache_lock));

	// update the actual on-disk transaction table, with flush=1
	set_transaction_status_in_table(ttbl, transaction_id, status, 1);

	write_unlock(&(ttbl->transaction_table_lock));

	return 1;
}