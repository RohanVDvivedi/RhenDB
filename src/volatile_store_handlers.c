#include<volatile_store_handlers.h>

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

void initialize_pam_for_vps(page_access_methods* pam_p, volatile_page_store* vps)
{
	pam_p->get_new_page_with_write_lock = get_new_page_with_write_lock_vps;
	pam_p->acquire_page_with_reader_lock = acquire_page_with_reader_lock_vps;
	pam_p->acquire_page_with_writer_lock = acquire_page_with_writer_lock_vps;
	pam_p->downgrade_writer_lock_to_reader_lock_on_page = downgrade_writer_lock_to_reader_lock_on_page_vps;
	pam_p->upgrade_reader_lock_to_writer_lock_on_page = upgrade_reader_lock_to_writer_lock_on_page_vps;
	pam_p->release_reader_lock_on_page = release_reader_lock_on_page_vps;
	pam_p->release_writer_lock_on_page = release_writer_lock_on_page_vps;
	pam_p->free_page = free_page_vps;
	pam_p->pas = (page_access_specs){};
	pam_p->context = vps;

	if(!initialize_page_access_specs(&(pam_p->pas), vps->user_stats.page_id_width, vps->user_stats.page_size, vps->user_stats.NULL_PAGE_ID))
		exit(-1);
}

#include<unWALed_page_modification_methods.h>

void initialize_pmm_for_vps(page_modification_methods* pmm_p, volatile_page_store* vps)
{
	page_modification_methods* pmm_p_c = get_new_unWALed_page_modification_methods();
	*pmm_p = *pmm_p_c;
	delete_unWALed_page_modification_methods(pmm_p_c);
}