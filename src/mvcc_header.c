#include<rondb/mvcc_header.h>

tuple_def* get_mvcc_header_tuple_definition(uint8_t transaction_id_width);

void read_mvcc_header(mvcc_header* mvcchdr_p, const void* mvcchdr_uval, const tuple_def* mvcchdr_tdef);

void write_mvcc_header(void* mvcchdr_uval, const tuple_def* mvcchdr_tdef, const mvcc_header* mvcchdr_p);

int update_mvcc_header(mvcc_header* mvcchdr_p, transaction_status (*get_transaction_status)(uint256 transaction_id));