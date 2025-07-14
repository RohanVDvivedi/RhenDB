#ifndef RAGE_ENGINE_MIN_TX_ENGINE_H
#define RAGE_ENGINE_MIN_TX_ENGINE_H

#include<rondb/rage_engine.h>

// any allocation or initialization failure results in an exit(-1)
rage_engine get_rage_engine_for_min_tx_engine(const char* database_file_name, uint32_t page_size, uint32_t page_id_width, uint32_t log_sequence_number_width, uint64_t bufferpool_frame_count, uint64_t wale_append_only_buffer_block_count, uint64_t latch_wait_timeout_in_microseconds, uint64_t write_lock_wait_timeout_in_microseconds, uint64_t checkpointing_period_in_microseconds, uint64_t checkpointing_LSN_diff_in_bytes, uint64_t max_wal_file_size_in_bytes);

#endif