#include<rondb/lock_manager.h>

#include<tupleindexer/heap_page/heap_page.h>

/*
** lock_table functions
*/

// insert to heap_table and both the indices

// --

/*
** tx_index functions (transaction_id, resource_type, resource_id, lock_state, lock_mode) -> lock
** to be used to release all locks, upon abort or commit, or release a specific lock
*/

// start a scan

// get current

// go next

// remove from everywhere

// destroy the scan

// --

/*
** rt_index functions (resource_type, resource_id, lock_state, lock_mode) -> lock
** to be used to check lock conflicts with other lock modes on the same resource_type and resource_id, for acquire, modify and deadlock detection
*/

// start a scan

// get current

// go next

// remove from everywhere

// destroy the scan

// --