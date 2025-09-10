#include<rhendb/rage_engine_min_tx_engine.h>
#include<volatilepagestore/volatile_page_store.h>
#include<tupleindexer/interface/page_access_methods.h>
#include<tupleindexer/interface/unWALed_page_modification_methods.h>

static void* get_new_page_with_write_lock_vps(void* context, const void* transaction_id, uint64_t* page_id_returned, int* abort_error)
{
	return get_new_page_for_vps(context, page_id_returned);
}
static void* acquire_page_with_reader_lock_vps(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	return acquire_page_for_vps(context, page_id);
}
static void* acquire_page_with_writer_lock_vps(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	return acquire_page_for_vps(context, page_id);
}
static int downgrade_writer_lock_to_reader_lock_on_page_vps(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	return 1;
}
static int upgrade_reader_lock_to_writer_lock_on_page_vps(void* context, const void* transaction_id, void* pg_ptr, int* abort_error)
{
	return 1;
}
static int release_reader_lock_on_page_vps(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	release_page_for_vps(context, pg_ptr, !!(opts & FREE_PAGE));
	return 1;
}
static int release_writer_lock_on_page_vps(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	release_page_for_vps(context, pg_ptr, !!(opts & FREE_PAGE));
	return 1;
}
static int free_page_vps(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	free_page_for_vps(context, page_id);
	return 1;
}

#include<stdlib.h>

rage_engine get_rage_engine_for_volatile_page_store(uint32_t page_size, uint8_t page_id_width, uint64_t truncator_period_in_microseconds)
{
	rage_engine e = {};

	// allocate/reset all the three components

	e.context = malloc(sizeof(volatile_page_store));
	if(e.context == NULL)
		exit(-1);

	e.pam_p = malloc(sizeof(page_access_methods));
	if(e.pam_p == NULL)
		exit(-1);

	e.pmm_p = NULL;

	// initialize all the three components

	if(!initialize_volatile_page_store(e.context, ".", page_size, page_id_width, truncator_period_in_microseconds))
	{
		printf("FAILED to initialize volatile page store\n");
		exit(-1);
	}

	e.pam_p->get_new_page_with_write_lock = get_new_page_with_write_lock_vps;
	e.pam_p->acquire_page_with_reader_lock = acquire_page_with_reader_lock_vps;
	e.pam_p->acquire_page_with_writer_lock = acquire_page_with_writer_lock_vps;
	e.pam_p->downgrade_writer_lock_to_reader_lock_on_page = downgrade_writer_lock_to_reader_lock_on_page_vps;
	e.pam_p->upgrade_reader_lock_to_writer_lock_on_page = upgrade_reader_lock_to_writer_lock_on_page_vps;
	e.pam_p->release_reader_lock_on_page = release_reader_lock_on_page_vps;
	e.pam_p->release_writer_lock_on_page = release_writer_lock_on_page_vps;
	e.pam_p->free_page = free_page_vps;
	e.pam_p->pas = (page_access_specs){};
	e.pam_p->context = e.context;

	if(!initialize_page_access_specs(&(e.pam_p->pas), ((volatile_page_store*)(e.context))->user_stats.page_id_width, ((volatile_page_store*)(e.context))->user_stats.page_size, ((volatile_page_store*)(e.context))->user_stats.NULL_PAGE_ID))
		exit(-1);

	e.pmm_p = get_new_unWALed_page_modification_methods();

	e.allot_new_sub_transaction_id = NULL;

	e.complete_sub_transaction = NULL;

	e.mark_sub_transaction_aborted = NULL;

	return e;
}