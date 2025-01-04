#include<volatile_store_handlers.h>

void initialize_pam_for_vps(page_access_methods* pam_p, volatile_page_store* vps);

void initialize_pmm_for_vps(page_modification_methods* pmm_p, volatile_page_store* vps);