#ifndef RAGE_ENIGNE_VOLATILE_PAGE_STORE_H
#define RAGE_ENGINE_VOLATILE_PAGE_STORE_H

#include<volatilepagestore/volatile_page_store.h>
#include<tupleindexer/interface/page_access_methods.h>
#include<tupleindexer/interface/page_modification_methods.h>

void initialize_pam_for_vps(page_access_methods* pam_p, volatile_page_store* vps);

void initialize_pmm_for_vps(page_modification_methods* pmm_p, volatile_page_store* vps);

#endif