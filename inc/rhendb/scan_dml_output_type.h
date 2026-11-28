#ifndef TABLE_OPERATOR_OUTPUT_TYPE_H
#define TABLE_OPERATOR_OUTPUT_TYPE_H

#define HEAP_TUPLE_ONLY_IN_OUTPUT    1

#define TABLE_ID_IN_OUTPUT           2
#define PARTITION_ID_IN_OUTPUT       4
#define TUPLE_POINTER_IN_OUTPUT      8
#define MVCC_HEADER_IN_OUTPUT       16
#define HEAP_TUPLE_IN_OUTPUT        32

#define MUST_OUTPUT_HEAP_TUPLE_ONLY(flags)    ((flags) & HEAP_TUPLE_ONLY_IN_OUTPUT)

#define MUST_OUTPUT_TABLE_ID(flags)           ((flags) & TABLE_ID_IN_OUTPUT)
#define MUST_OUTPUT_PARTITION_ID(flags)       ((flags) & PARTITION_ID_IN_OUTPUT)
#define MUST_OUTPUT_TUPLE_POINTER_ID(flags)   ((flags) & TUPLE_POINTER_IN_OUTPUT)
#define MUST_OUTPUT_MVCC_HEADER(flags)        ((flags) & MVCC_HEADER_IN_OUTPUT)
#define MUST_OUTPUT_HEAP_TUPLE(flags)         ((flags) & HEAP_TUPLE_IN_OUTPUT)

#endif