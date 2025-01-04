#include<persistent_store_handlers.h>

void initialize_pam_for_mte(page_access_methods* pam_p, mini_transaction_engine* mte);

void initialize_pmm_for_mte(page_modification_methods* pmm_p, mini_transaction_engine* mte);