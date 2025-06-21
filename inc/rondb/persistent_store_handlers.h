#ifndef PERSISTENT_STORE_HANDLERS_H
#define PERSISTENT_STORE_HANDLERS_H

#include<mintxengine/mini_transaction_engine.h>
#include<tupleindexer/interface/page_access_methods.h>
#include<tupleindexer/interface/page_modification_methods.h>

void initialize_pam_for_mte(page_access_methods* pam_p, mini_transaction_engine* mte);

void initialize_pmm_for_mte(page_modification_methods* pmm_p, mini_transaction_engine* mte);

#endif