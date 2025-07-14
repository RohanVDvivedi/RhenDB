#ifndef RAGE_ENIGNE_VOLATILE_PAGE_STORE_H
#define RAGE_ENGINE_VOLATILE_PAGE_STORE_H

#include<rondb/rage_engine.h>

#include<volatilepagestore/volatile_page_store.h>

// any allocation or initialization failure results in an exit(-1)
rage_engine get_rage_engine_for_volatile_page_store(uint32_t page_size, uint8_t page_id_width, uint64_t truncator_period_in_microseconds);

#endif