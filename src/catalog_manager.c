#include<rhendb/catalog_manager.h>

#include<tupleindexer/page_table/page_table.h>
#include<tupleindexer/heap_page/heap_page.h>

#include<tupleindexer/interface/page_access_methods.h>
#include<tupleindexer/common/tuple_pointer.h>
#include<tupleindexer/common/invalid_tuple_indices.h>
#include<tupleindexer/interface/page_access_methods_options.h>
#include<tuplelargetypes/binary_write_iterator.h>
#include<tuplelargetypes/binary_read_iterator.h>

// index utility struct

typedef struct rhendb_name_idx_entry rhendb_name_idx_entry;
struct rhendb_name_idx_entry
{
	catalog_object_type object_type;

	char name[64];

	tuple_pointer object_tuple_pointer;
};

typedef struct rhendb_id_idx_entry rhendb_id_idx_entry;
struct rhendb_id_idx_entry
{
	catalog_object_type object_type;

	uint64_t id;

	tuple_pointer object_tuple_pointer;
};

typedef struct rhendb_table_to_indices_entry rhendb_table_to_indices_entry;
struct rhendb_table_to_indices_entry
{
	uint64_t table_id;

	tuple_pointer indices_tuple_pointer;
};

typedef struct rhendb_owner_to_attributes_idx_entry rhendb_owner_to_attributes_idx_entry;
struct rhendb_owner_to_attributes_idx_entry
{
	uint64_t owner_id;

	uint64_t rel_pos_in_owner;

	tuple_pointer attributes_tuple_pointer;
};

// ------

#define ID_n_REL_POS_BYTES 8
#define NAME_BYTES 64

#define HTAN_ENTRIES_MAX    56
#define HTAN_ENTRIES_THRES  35

static void fix_unused_space_entries_UNSAFE(catalog_manager* catmgr_p, uint64_t heap_table_root_page_id, heap_table_accumulative_notifier* htan_p, const heap_table_tuple_defs* httd_p, const void* min_tx_id, int* abort_error)
{
	uint32_t entries_to_fix = get_notification_count_for_heap_table_accumulative_notifier(htan_p);
	if(entries_to_fix < HTAN_ENTRIES_THRES)
		return;

	uint64_t _heap_table_root_page_id;
	uint32_t unused_bytes_in_entry;
	uint64_t page_id;
	while(pop_from_heap_table_accumulative_notifier(htan_p, &_heap_table_root_page_id, &unused_bytes_in_entry, &page_id))
	{
		if(heap_table_root_page_id == _heap_table_root_page_id)
		{
			fix_unused_space_in_heap_table(_heap_table_root_page_id, unused_bytes_in_entry, page_id, httd_p, catmgr_p->catmgr_engine->pam_p, catmgr_p->catmgr_engine->pmm_p, min_tx_id, abort_error);
			if(*abort_error)
				return;
		}
	}
}

// serialization (struct -> (void*)tuple) functions
// if should_blob is true, then serialize expression in the blob_store, in the current mini transaction passed
// it is expected that on should_blob = 1, this function calls may abort with a abort_error, and will return NULL for sure

static int catalog_write_extended_blob(catalog_manager* catmgr_p, void* tuple, const tuple_def* record_def, positional_accessor position, char* data, uint32_t data_size, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;

	set_element_in_tuple(record_def, position, tuple, EMPTY_DATUM, UINT32_MAX);

	binary_write_iterator* bwi_p = get_new_binary_write_iterator(tuple, record_def, position, catmgr_p->ext_store_root_page_id, get_NULL_tuple_pointer(&(engine->pam_p->pas)), engine->max_prefix_size_in_bytes, &(engine->bstd), engine->pam_p, engine->pmm_p);

	uint32_t bytes_written = 0;

	while(bytes_written < data_size)
	{
		uint32_t bytes_written_this_iteration = append_to_binary_write_iterator(bwi_p, data + bytes_written, data_size - bytes_written, &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(catmgr_p->htan)), min_tx_id, abort_error);
		if(*abort_error)
			break;
		if(bytes_written_this_iteration == 0)
			break;

		bytes_written += bytes_written_this_iteration;
	}

	delete_binary_write_iterator(bwi_p, min_tx_id, abort_error);
	if(*abort_error)
		return 0;

	fix_unused_space_entries_UNSAFE(catmgr_p, catmgr_p->ext_store_root_page_id, &(catmgr_p->htan), &(catmgr_p->catmgr_engine->bstd.httd), min_tx_id, abort_error);
	if(*abort_error)
		return 0;

	return 1;
}

static void catalog_write_mvcc_header(catalog_manager* catmgr_p, void* tuple, const tuple_def* record_def, const mvcc_header* mvcchdr_p)
{
	char mvcc_header_serialized[get_maximum_tuple_size(&(catmgr_p->mvcc_header_tuple_def))];

	write_mvcc_header(mvcc_header_serialized, &(catmgr_p->mvcc_header_tuple_def), mvcchdr_p);

	set_element_in_tuple(record_def, STATIC_POSITION(0), tuple, &((datum){.tuple_value = mvcc_header_serialized}), UINT32_MAX);
}

// mvcchdr_p may be NULL, if so do not need to set it

static void* serialize_rhendb_attribute(catalog_manager* catmgr_p, const mvcc_header* mvcchdr_p, const rhendb_attribute* attr, int should_blob, const void* min_tx_engine, int* abort_error)
{
	const tuple_def* record_def = &(catmgr_p->attributes_table.record_def);

	void* tuple = malloc(get_maximum_tuple_size(record_def));

	init_tuple(record_def, tuple);

	if(mvcchdr_p != NULL)
		catalog_write_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	set_element_in_tuple(record_def, STATIC_POSITION(1), tuple, &((datum){.uint_value = attr->owner_id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(2), tuple, &((datum){.uint_value = attr->rel_pos_in_owner}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(3), tuple, &((datum){.uint_value = attr->table_part_id_from}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(4), tuple, &((datum){.uint_value = attr->table_part_id_to}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(5), tuple, &((datum){.string_value = attr->attribute_name, .string_size = strnlen(attr->attribute_name, 64)}), UINT32_MAX);

	set_element_in_tuple(record_def, STATIC_POSITION(6), tuple, &((datum){.uint_value = attr->base_type}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(7), tuple, &((datum){.uint_value = attr->attribute_type_id}), 0);

	if(attr->count == NULL)
		set_element_in_tuple(record_def, STATIC_POSITION(8), tuple, NULL_DATUM, 0);
	else
		set_element_in_tuple(record_def, STATIC_POSITION(8), tuple, &((datum){.uint_value = *(attr->count)}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(9), tuple, &((datum){.bit_field_value = attr->is_auto_increment}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(10), tuple, &((datum){.bit_field_value = attr->is_nullable}), 0);

	if(should_blob && attr->derived_from_expr != NULL)
	{
		if(!catalog_write_extended_blob(catmgr_p, tuple, record_def, STATIC_POSITION(11), attr->derived_from_expr, strlen(attr->derived_from_expr), min_tx_engine, abort_error))
		{
			free(tuple);
			return NULL;
		}
	}
	else
		set_element_in_tuple(record_def, STATIC_POSITION(11), tuple, NULL_DATUM, UINT32_MAX);

	return tuple;
}

static void* serialize_rhendb_type(catalog_manager* catmgr_p, const mvcc_header* mvcchdr_p, const rhendb_type* typ)
{
	const tuple_def* record_def = &(catmgr_p->types_table.record_def);

	void* tuple = malloc(get_maximum_tuple_size(record_def));

	init_tuple(record_def, tuple);

	if(mvcchdr_p != NULL)
		catalog_write_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	set_element_in_tuple(record_def, STATIC_POSITION(1), tuple, &((datum){.uint_value = typ->id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(2), tuple, &((datum){.string_value = typ->name, .string_size = strnlen(typ->name, 64)}), UINT32_MAX);

	return tuple;
}

static void* serialize_rhendb_index_fragment(catalog_manager* catmgr_p, const mvcc_header* mvcchdr_p, const rhendb_index_fragment* ifrag)
{
	const tuple_def* record_def = &(catmgr_p->index_fragments_table.record_def);

	void* tuple = malloc(get_maximum_tuple_size(record_def));

	init_tuple(record_def, tuple);

	if(mvcchdr_p != NULL)
		catalog_write_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	set_element_in_tuple(record_def, STATIC_POSITION(1), tuple, &((datum){.uint_value = ifrag->table_id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(2), tuple, &((datum){.uint_value = ifrag->index_id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(3), tuple, &((datum){.uint_value = ifrag->partition_id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(4), tuple, &((datum){.uint_value = ifrag->root_page_id}), 0);

	return tuple;
}

static void* serialize_rhendb_index(catalog_manager* catmgr_p, const mvcc_header* mvcchdr_p, const rhendb_index* idx, int should_blob, const void* min_tx_engine, int* abort_error)
{
	const tuple_def* record_def = &(catmgr_p->indices_table.record_def);

	void* tuple = malloc(get_maximum_tuple_size(record_def));

	init_tuple(record_def, tuple);

	if(mvcchdr_p != NULL)
		catalog_write_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	set_element_in_tuple(record_def, STATIC_POSITION(1), tuple, &((datum){.uint_value = idx->id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(2), tuple, &((datum){.string_value = idx->name, .string_size = strnlen(idx->name, 64)}), UINT32_MAX);

	set_element_in_tuple(record_def, STATIC_POSITION(3), tuple, &((datum){.uint_value = idx->table_id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(4), tuple, &((datum){.uint_value = idx->access_methos}), 0);

	if(should_blob && idx->predicate_expr != NULL)
	{
		if(!catalog_write_extended_blob(catmgr_p, tuple, record_def, STATIC_POSITION(5), idx->predicate_expr, strlen(idx->predicate_expr), min_tx_engine, abort_error))
		{
			free(tuple);
			return NULL;
		}
	}
	else
		set_element_in_tuple(record_def, STATIC_POSITION(5), tuple, NULL_DATUM, UINT32_MAX);

	return tuple;
}

static void* serialize_rhendb_table_partition(catalog_manager* catmgr_p, const mvcc_header* mvcchdr_p, const rhendb_table_partition* tpart)
{
	const tuple_def* record_def = &(catmgr_p->table_partitions_table.record_def);

	void* tuple = malloc(get_maximum_tuple_size(record_def));

	init_tuple(record_def, tuple);

	if(mvcchdr_p != NULL)
		catalog_write_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	set_element_in_tuple(record_def, STATIC_POSITION(1), tuple, &((datum){.uint_value = tpart->table_id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(2), tuple, &((datum){.uint_value = tpart->partition_id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(3), tuple, &((datum){.uint_value = tpart->heap_root_page_id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(4), tuple, &((datum){.uint_value = tpart->blobs_root_page_id}), 0);

	return tuple;
}

static void* serialize_rhendb_table(catalog_manager* catmgr_p, const mvcc_header* mvcchdr_p, const rhendb_table* tbl)
{
	const tuple_def* record_def = &(catmgr_p->tables_table.record_def);

	void* tuple = malloc(get_maximum_tuple_size(record_def));

	init_tuple(record_def, tuple);

	if(mvcchdr_p != NULL)
		catalog_write_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	set_element_in_tuple(record_def, STATIC_POSITION(1), tuple, &((datum){.uint_value = tbl->id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(2), tuple, &((datum){.string_value = tbl->name, .string_size = strnlen(tbl->name, 64)}), UINT32_MAX);

	return tuple;
}

static void* serialize_rhendb_name_idx_entry(catalog_manager* catmgr_p, const rhendb_name_idx_entry* nidx)
{
	const tuple_def* record_def = &(catmgr_p->name_idx.record_def);

	void* tuple = malloc(get_maximum_tuple_size(record_def));

	init_tuple(record_def, tuple);

	set_element_in_tuple(record_def, STATIC_POSITION(0), tuple, &((datum){.uint_value = nidx->object_type}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(1), tuple, &((datum){.string_value = nidx->name, .string_size = strnlen(nidx->name, 64)}), UINT32_MAX);

	char object_tuple_pointer_serialized[sizeof(tuple_pointer)];
	set_tuple_pointer(object_tuple_pointer_serialized, nidx->object_tuple_pointer, &(catmgr_p->catmgr_engine->pam_p->pas));
	set_element_in_tuple(record_def, STATIC_POSITION(2), tuple, &((datum){.tuple_value = object_tuple_pointer_serialized}), 0);

	return tuple;
}

static void* serialize_rhendb_id_idx_entry(catalog_manager* catmgr_p, const rhendb_id_idx_entry* ididx)
{
	const tuple_def* record_def = &(catmgr_p->id_idx.record_def);

	void* tuple = malloc(get_maximum_tuple_size(record_def));

	init_tuple(record_def, tuple);

	set_element_in_tuple(record_def, STATIC_POSITION(0), tuple, &((datum){.uint_value = ididx->object_type}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(1), tuple, &((datum){.uint_value = ididx->id}), 0);

	char object_tuple_pointer_serialized[sizeof(tuple_pointer)];
	set_tuple_pointer(object_tuple_pointer_serialized, ididx->object_tuple_pointer, &(catmgr_p->catmgr_engine->pam_p->pas));
	set_element_in_tuple(record_def, STATIC_POSITION(2), tuple, &((datum){.tuple_value = object_tuple_pointer_serialized}), 0);

	return tuple;
}

static void* serialize_rhendb_table_to_indices_entry(catalog_manager* catmgr_p, const rhendb_table_to_indices_entry* t2iidx)
{
	const tuple_def* record_def = &(catmgr_p->table_to_indices_idx.record_def);

	void* tuple = malloc(get_maximum_tuple_size(record_def));

	init_tuple(record_def, tuple);

	set_element_in_tuple(record_def, STATIC_POSITION(0), tuple, &((datum){.uint_value = t2iidx->table_id}), 0);

	char indices_tuple_pointer_serialized[sizeof(tuple_pointer)];
	set_tuple_pointer(indices_tuple_pointer_serialized, t2iidx->indices_tuple_pointer, &(catmgr_p->catmgr_engine->pam_p->pas));
	set_element_in_tuple(record_def, STATIC_POSITION(1), tuple, &((datum){.tuple_value = indices_tuple_pointer_serialized}), 0);

	return tuple;
}

static void* serialize_rhendb_owner_to_attributes_idx_entry(catalog_manager* catmgr_p, const rhendb_owner_to_attributes_idx_entry* o2aidx)
{
	const tuple_def* record_def = &(catmgr_p->owner_to_attributes_idx.record_def);

	void* tuple = malloc(get_maximum_tuple_size(record_def));

	init_tuple(record_def, tuple);

	set_element_in_tuple(record_def, STATIC_POSITION(0), tuple, &((datum){.uint_value = o2aidx->owner_id}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(1), tuple, &((datum){.uint_value = o2aidx->rel_pos_in_owner}), 0);

	char attributes_tuple_pointer_serialized[sizeof(tuple_pointer)];
	set_tuple_pointer(attributes_tuple_pointer_serialized, o2aidx->attributes_tuple_pointer, &(catmgr_p->catmgr_engine->pam_p->pas));
	set_element_in_tuple(record_def, STATIC_POSITION(2), tuple, &((datum){.tuple_value = attributes_tuple_pointer_serialized}), 0);

	return tuple;
}

// deserialization ((void*)tuple -> struct) functions
// if should_blob is true, then deserialize expression in the blob_store, in a separate NULL read only transaction
// else leave the blob of the expression as NULL

static char* catalog_read_extended_blob(catalog_manager* catmgr_p, const void* tuple, const tuple_def* record_def, positional_accessor position, const data_type_info* field_dti, uint32_t* bytes_read, const void* min_tx_id, int* abort_error)
{
	(*bytes_read) = 0;
	rage_engine* engine = catmgr_p->catmgr_engine;

	datum uval;
	get_value_from_element_from_tuple(&uval, record_def, position, tuple);

	if(is_datum_NULL(&uval))
		return NULL;

	binary_read_iterator* bri_p = get_new_binary_read_iterator(&uval, field_dti, &(engine->bstd), engine->pam_p, NULL);

	uint64_t capacity = 64;
	char* data = malloc(capacity);

	while(1)
	{
		if((*bytes_read) == capacity)
		{
			if(capacity == UINT32_MAX)
			{
				printf("catalog expression too big\n");
				exit(-1);
			}
			capacity = min(UINT32_MAX, 2 * capacity);
			data = realloc(data, capacity);
		}

		uint32_t bytes_read_this_iteration = read_from_binary_read_iterator(bri_p, data + (*bytes_read), capacity - (*bytes_read), min_tx_id, abort_error);
		if(*abort_error)
			break;
		if(bytes_read_this_iteration == 0)
			break;

		(*bytes_read) += bytes_read_this_iteration;
	}

	delete_binary_read_iterator(bri_p, min_tx_id, abort_error);
	if(*abort_error)
	{
		free(data);
		return NULL;
	}

	return data;
}

static void catalog_read_mvcc_header(catalog_manager* catmgr_p, const void* tuple, const tuple_def* record_def, mvcc_header* mvcchdr_p)
{
	datum uval;
	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(0), tuple);

	read_mvcc_header(mvcchdr_p, uval.tuple_value, &(catmgr_p->mvcc_header_tuple_def));
}

// if mvcchdr_p is not NULL, then it also needs to be read and deserialized

static rhendb_attribute deserialize_rhendb_attribute(catalog_manager* catmgr_p, mvcc_header* mvcchdr_p, const void* tuple, int should_blob, const void* min_tx_engine, int* abort_error)
{
	const tuple_def* record_def = &(catmgr_p->attributes_table.record_def);

	rhendb_attribute attr = {};

	datum uval;

	if(mvcchdr_p != NULL)
		catalog_read_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(1), tuple);
	attr.owner_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(2), tuple);
	attr.rel_pos_in_owner = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(3), tuple);
	attr.table_part_id_from = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(4), tuple);
	attr.table_part_id_to = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(5), tuple);
	memory_move(attr.attribute_name, uval.string_value, uval.string_size);

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(6), tuple);
	attr.base_type = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(7), tuple);
	attr.attribute_type_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(8), tuple);
	if(uval.is_NULL)
		attr.count = NULL;
	else
	{
		attr._count = uval.uint_value;
		attr.count = &(attr._count);
	}

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(9), tuple);
	attr.is_auto_increment = uval.bit_field_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(10), tuple);
	attr.is_nullable = uval.bit_field_value;

	if(should_blob)
	{
		attr.derived_from_expr = catalog_read_extended_blob(catmgr_p, tuple, record_def, STATIC_POSITION(11), record_def->type_info->containees[11].al.type_info, &(attr.derived_from_expr_size), min_tx_engine, abort_error);
		if(*abort_error)
			return attr;
	}
	else
	{
		attr.derived_from_expr = NULL;
		attr.derived_from_expr_size = 0;
	}

	return attr;
}

static rhendb_type deserialize_rhendb_type(catalog_manager* catmgr_p, mvcc_header* mvcchdr_p, const void* tuple)
{
	const tuple_def* record_def = &(catmgr_p->types_table.record_def);

	rhendb_type typ = {};

	datum uval;

	if(mvcchdr_p != NULL)
		catalog_read_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(1), tuple);
	typ.id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(2), tuple);
	memory_move(typ.name, uval.string_value, uval.string_size);

	return typ;
}

static rhendb_index_fragment deserialize_rhendb_index_fragment(catalog_manager* catmgr_p, mvcc_header* mvcchdr_p, const void* tuple)
{
	const tuple_def* record_def = &(catmgr_p->index_fragments_table.record_def);

	rhendb_index_fragment ifrag = {};

	datum uval;

	if(mvcchdr_p != NULL)
		catalog_read_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(1), tuple);
	ifrag.table_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(2), tuple);
	ifrag.index_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(3), tuple);
	ifrag.partition_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(4), tuple);
	ifrag.root_page_id = uval.uint_value;

	return ifrag;
}

static rhendb_index deserialize_rhendb_index(catalog_manager* catmgr_p, mvcc_header* mvcchdr_p, const void* tuple, int should_blob, const void* min_tx_engine, int* abort_error)
{
	const tuple_def* record_def = &(catmgr_p->indices_table.record_def);

	rhendb_index idx = {};

	datum uval;

	if(mvcchdr_p != NULL)
		catalog_read_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(1), tuple);
	idx.id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(2), tuple);
	memory_move(idx.name, uval.string_value, uval.string_size);

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(3), tuple);
	idx.table_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(4), tuple);
	idx.access_methos = uval.uint_value;

	if(should_blob)
	{
		idx.predicate_expr = catalog_read_extended_blob(catmgr_p, tuple, record_def, STATIC_POSITION(5), record_def->type_info->containees[5].al.type_info, &(idx.predicate_expr_size), min_tx_engine, abort_error);
		if(*abort_error)
			return idx;
	}
	else
	{
		idx.predicate_expr = NULL;
		idx.predicate_expr_size = 0;
	}

	return idx;
}

static rhendb_table_partition deserialize_rhendb_table_partition(catalog_manager* catmgr_p, mvcc_header* mvcchdr_p, const void* tuple)
{
	const tuple_def* record_def = &(catmgr_p->table_partitions_table.record_def);

	rhendb_table_partition tpart = {};

	datum uval;

	if(mvcchdr_p != NULL)
		catalog_read_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(1), tuple);
	tpart.table_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(2), tuple);
	tpart.partition_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(3), tuple);
	tpart.heap_root_page_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(4), tuple);
	tpart.blobs_root_page_id = uval.uint_value;

	return tpart;
}

static rhendb_table deserialize_rhendb_table(catalog_manager* catmgr_p, mvcc_header* mvcchdr_p, const void* tuple)
{
	const tuple_def* record_def = &(catmgr_p->tables_table.record_def);

	rhendb_table tbl = {};

	datum uval;

	if(mvcchdr_p != NULL)
		catalog_read_mvcc_header(catmgr_p, tuple, record_def, mvcchdr_p);

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(1), tuple);
	tbl.id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(2), tuple);
	memory_move(tbl.name, uval.string_value, uval.string_size);

	return tbl;
}

static rhendb_name_idx_entry deserialize_rhendb_name_idx_entry(catalog_manager* catmgr_p, const void* tuple)
{
	const tuple_def* record_def = &(catmgr_p->name_idx.record_def);

	rhendb_name_idx_entry nidx = {};

	datum uval;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(0), tuple);
	nidx.object_type = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(1), tuple);
	memory_move(nidx.name, uval.string_value, uval.string_size);

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(2), tuple);
	nidx.object_tuple_pointer = get_tuple_pointer(uval.tuple_value, &(catmgr_p->catmgr_engine->pam_p->pas));

	return nidx;
}

static rhendb_id_idx_entry deserialize_rhendb_id_idx_entry(catalog_manager* catmgr_p, const void* tuple)
{
	const tuple_def* record_def = &(catmgr_p->id_idx.record_def);

	rhendb_id_idx_entry ididx = {};

	datum uval;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(0), tuple);
	ididx.object_type = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(1), tuple);
	ididx.id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(2), tuple);
	ididx.object_tuple_pointer = get_tuple_pointer(uval.tuple_value, &(catmgr_p->catmgr_engine->pam_p->pas));

	return ididx;
}

static rhendb_table_to_indices_entry deserialize_rhendb_table_to_indices_entry(catalog_manager* catmgr_p, const void* tuple)
{
	const tuple_def* record_def = &(catmgr_p->table_to_indices_idx.record_def);

	rhendb_table_to_indices_entry t2iidx = {};

	datum uval;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(0), tuple);
	t2iidx.table_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(1), tuple);
	t2iidx.indices_tuple_pointer = get_tuple_pointer(uval.tuple_value, &(catmgr_p->catmgr_engine->pam_p->pas));

	return t2iidx;
}

static rhendb_owner_to_attributes_idx_entry deserialize_rhendb_owner_to_attributes_idx_entry(catalog_manager* catmgr_p, const void* tuple)
{
	const tuple_def* record_def = &(catmgr_p->owner_to_attributes_idx.record_def);

	rhendb_owner_to_attributes_idx_entry o2aidx = {};

	datum uval;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(0), tuple);
	o2aidx.owner_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(1), tuple);
	o2aidx.rel_pos_in_owner = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(2), tuple);
	o2aidx.attributes_tuple_pointer = get_tuple_pointer(uval.tuple_value, &(catmgr_p->catmgr_engine->pam_p->pas));

	return o2aidx;
}

// functions to get serialized key from the rhendb_table_partition and rhendb_index_fragment, this is needed as they are clustered btree tables, with uniqueness governed by these keys

static void* serialize_rhendb_index_fragment_key(catalog_manager* catmgr_p, const rhendb_index_fragment* ifrag)
{
	const tuple_def* key_def = catmgr_p->index_fragments_table.clust_table_defs.key_def;

	void* key_tuple = malloc(get_maximum_tuple_size(key_def));

	init_tuple(key_def, key_tuple);

	set_element_in_tuple(key_def, STATIC_POSITION(0), key_tuple, &((datum){.uint_value = ifrag->table_id}), 0);

	set_element_in_tuple(key_def, STATIC_POSITION(1), key_tuple, &((datum){.uint_value = ifrag->index_id}), 0);

	set_element_in_tuple(key_def, STATIC_POSITION(2), key_tuple, &((datum){.uint_value = ifrag->partition_id}), 0);

	return key_tuple;
}

static void* serialize_rhendb_table_partition_key(catalog_manager* catmgr_p, const rhendb_table_partition* tpart)
{
	const tuple_def* key_def = catmgr_p->table_partitions_table.clust_table_defs.key_def;

	void* key_tuple = malloc(get_maximum_tuple_size(key_def));

	init_tuple(key_def, key_tuple);

	set_element_in_tuple(key_def, STATIC_POSITION(0), key_tuple, &((datum){.uint_value = tpart->table_id}), 0);

	set_element_in_tuple(key_def, STATIC_POSITION(1), key_tuple, &((datum){.uint_value = tpart->partition_id}), 0);

	return key_tuple;
}

// --

static const compare_direction cmp_dirs_all_asc[] = {ASC, ASC, ASC, ASC, ASC, ASC};

const positional_accessor key_element_ids0[] = {STATIC_POSITION(0), STATIC_POSITION(1), STATIC_POSITION(2), STATIC_POSITION(3), STATIC_POSITION(4), STATIC_POSITION(5)};

const positional_accessor key_element_ids1[] = {STATIC_POSITION(1), STATIC_POSITION(2), STATIC_POSITION(3), STATIC_POSITION(4), STATIC_POSITION(5), STATIC_POSITION(6)};

void initialize_catalog_manager(catalog_manager* catmgr_p, uint64_t* root_page_id, data_type_info* mvcc_hdr_dti_p, rage_engine* catmgr_engine)
{
	data_type_info* obj_type_dti_p = UINT_NON_NULLABLE[2];

	data_type_info* id_dti_p = UINT_NON_NULLABLE[ID_n_REL_POS_BYTES];

	data_type_info* name_dti_p = malloc(sizeof(data_type_info));
	(*name_dti_p) = get_variable_length_string_type("name", NAME_BYTES + 8);

	data_type_info* rel_pos_in_owner_dti_p = UINT_NON_NULLABLE[ID_n_REL_POS_BYTES];

	data_type_info* root_page_id_dti_p = &(catmgr_engine->pam_p->pas.page_id_type_info);

	data_type_info* base_dti_p = UINT_NON_NULLABLE[2];

	{
		data_type_info* attributes_type_info = malloc(sizeof_tuple_data_type_info(12));

		strcpy(attributes_type_info->containees[0].field_name, "mvcc_hdr");
		attributes_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(attributes_type_info->containees[1].field_name, "owner_id");
		attributes_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(attributes_type_info->containees[2].field_name, "rel_pos_in_owner");
		attributes_type_info->containees[2].al.type_info = rel_pos_in_owner_dti_p;

		strcpy(attributes_type_info->containees[3].field_name, "table_part_id_from");
		attributes_type_info->containees[3].al.type_info = id_dti_p;

		strcpy(attributes_type_info->containees[4].field_name, "table_part_id_to");
		attributes_type_info->containees[4].al.type_info = id_dti_p;

		strcpy(attributes_type_info->containees[5].field_name, "attribute_name");
		attributes_type_info->containees[5].al.type_info = name_dti_p;

		strcpy(attributes_type_info->containees[6].field_name, "base_type");
		attributes_type_info->containees[6].al.type_info = base_dti_p;

		strcpy(attributes_type_info->containees[7].field_name, "attribute_type_id");
		attributes_type_info->containees[7].al.type_info = id_dti_p;

		strcpy(attributes_type_info->containees[8].field_name, "count");
		attributes_type_info->containees[8].al.type_info = UINT_NULLABLE[4];

		strcpy(attributes_type_info->containees[9].field_name, "is_auto_increment");
		attributes_type_info->containees[9].al.type_info = BIT_FIELD_NON_NULLABLE[1];

		strcpy(attributes_type_info->containees[10].field_name, "is_nullable");
		attributes_type_info->containees[10].al.type_info = BIT_FIELD_NON_NULLABLE[1];

		strcpy(attributes_type_info->containees[11].field_name, "derived_from_expr");
		attributes_type_info->containees[11].al.type_info = catmgr_engine->text_extended_type_info;

		initialize_tuple_data_type_info(attributes_type_info, "rhendb_attribute", 0, 900, 12);

		initialize_tuple_def(&(catmgr_p->attributes_table.record_def), attributes_type_info);

		init_heap_table_tuple_definitions(&(catmgr_p->attributes_table.heap_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->attributes_table.record_def));

		initialize_heap_table_accumulative_notifier(&(catmgr_p->attributes_table.htan), HTAN_ENTRIES_MAX);
	}

	{
		data_type_info* types_type_info = malloc(sizeof_tuple_data_type_info(3));

		strcpy(types_type_info->containees[0].field_name, "mvcc_hdr");
		types_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(types_type_info->containees[1].field_name, "id");
		types_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(types_type_info->containees[2].field_name, "name");
		types_type_info->containees[2].al.type_info = name_dti_p;

		initialize_tuple_data_type_info(types_type_info, "rhendb_type", 0, 900, 3);

		initialize_tuple_def(&(catmgr_p->types_table.record_def), types_type_info);

		init_heap_table_tuple_definitions(&(catmgr_p->types_table.heap_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->types_table.record_def));

		initialize_heap_table_accumulative_notifier(&(catmgr_p->types_table.htan), HTAN_ENTRIES_MAX);
	}

	{
		data_type_info* index_fragments_type_info = malloc(sizeof_tuple_data_type_info(5));

		strcpy(index_fragments_type_info->containees[0].field_name, "mvcc_hdr");
		index_fragments_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(index_fragments_type_info->containees[1].field_name, "table_id");
		index_fragments_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(index_fragments_type_info->containees[2].field_name, "index_id");
		index_fragments_type_info->containees[2].al.type_info = id_dti_p;

		strcpy(index_fragments_type_info->containees[3].field_name, "partition_id");
		index_fragments_type_info->containees[3].al.type_info = id_dti_p;

		strcpy(index_fragments_type_info->containees[4].field_name, "root_page_id");
		index_fragments_type_info->containees[4].al.type_info = root_page_id_dti_p;

		initialize_tuple_data_type_info(index_fragments_type_info, "rhendb_index", 0, 900, 5);

		initialize_tuple_def(&(catmgr_p->index_fragments_table.record_def), index_fragments_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->index_fragments_table.clust_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->index_fragments_table.record_def), key_element_ids1, cmp_dirs_all_asc, 3);
	}

	{
		data_type_info* indices_type_info = malloc(sizeof_tuple_data_type_info(6));

		strcpy(indices_type_info->containees[0].field_name, "mvcc_hdr");
		indices_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(indices_type_info->containees[1].field_name, "id");
		indices_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(indices_type_info->containees[2].field_name, "name");
		indices_type_info->containees[2].al.type_info = name_dti_p;

		strcpy(indices_type_info->containees[3].field_name, "table_id");
		indices_type_info->containees[3].al.type_info = id_dti_p;

		strcpy(indices_type_info->containees[4].field_name, "access_method");
		indices_type_info->containees[4].al.type_info = UINT_NON_NULLABLE[2];

		strcpy(indices_type_info->containees[5].field_name, "predicate_expr");
		indices_type_info->containees[5].al.type_info = catmgr_engine->text_extended_type_info;

		initialize_tuple_data_type_info(indices_type_info, "rhendb_index", 0, 900, 6);

		initialize_tuple_def(&(catmgr_p->indices_table.record_def), indices_type_info);

		init_heap_table_tuple_definitions(&(catmgr_p->indices_table.heap_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->indices_table.record_def));

		initialize_heap_table_accumulative_notifier(&(catmgr_p->indices_table.htan), HTAN_ENTRIES_MAX);
	}

	{
		data_type_info* table_partitions_type_info = malloc(sizeof_tuple_data_type_info(5));

		strcpy(table_partitions_type_info->containees[0].field_name, "mvcc_hdr");
		table_partitions_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(table_partitions_type_info->containees[1].field_name, "table_id");
		table_partitions_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(table_partitions_type_info->containees[2].field_name, "partition_id");
		table_partitions_type_info->containees[2].al.type_info = id_dti_p;

		strcpy(table_partitions_type_info->containees[3].field_name, "heap_root_page_id");
		table_partitions_type_info->containees[3].al.type_info = root_page_id_dti_p;

		strcpy(table_partitions_type_info->containees[4].field_name, "blobs_root_page_id");
		table_partitions_type_info->containees[4].al.type_info = root_page_id_dti_p;

		initialize_tuple_data_type_info(table_partitions_type_info, "rhendb_table_partition", 0, 900, 5);

		initialize_tuple_def(&(catmgr_p->table_partitions_table.record_def), table_partitions_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->table_partitions_table.clust_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->table_partitions_table.record_def), key_element_ids1, cmp_dirs_all_asc, 2);
	}

	{
		data_type_info* tables_type_info = malloc(sizeof_tuple_data_type_info(3));

		strcpy(tables_type_info->containees[0].field_name, "mvcc_hdr");
		tables_type_info->containees[0].al.type_info = mvcc_hdr_dti_p;

		strcpy(tables_type_info->containees[1].field_name, "id");
		tables_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(tables_type_info->containees[2].field_name, "name");
		tables_type_info->containees[2].al.type_info = name_dti_p;

		initialize_tuple_data_type_info(tables_type_info, "rhendb_table", 0, 900, 3);

		initialize_tuple_def(&(catmgr_p->tables_table.record_def), tables_type_info);

		init_heap_table_tuple_definitions(&(catmgr_p->tables_table.heap_table_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->tables_table.record_def));

		initialize_heap_table_accumulative_notifier(&(catmgr_p->tables_table.htan), HTAN_ENTRIES_MAX);
	}

	{
		data_type_info* name_idx_type_info = malloc(sizeof_tuple_data_type_info(3));

		strcpy(name_idx_type_info->containees[0].field_name, "object_type");
		name_idx_type_info->containees[0].al.type_info = obj_type_dti_p;

		strcpy(name_idx_type_info->containees[1].field_name, "name");
		name_idx_type_info->containees[1].al.type_info = name_dti_p;

		strcpy(name_idx_type_info->containees[2].field_name, "object.tuple_pointer");
		name_idx_type_info->containees[2].al.type_info = &(catmgr_engine->pam_p->pas.tuple_pointer_type_info);

		initialize_tuple_data_type_info(name_idx_type_info, "rhendb_name_idx_entry", 0, 900, 3);

		initialize_tuple_def(&(catmgr_p->name_idx.record_def), name_idx_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->name_idx.index_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->name_idx.record_def), key_element_ids0, cmp_dirs_all_asc, 3);
	}

	{
		data_type_info* id_idx_type_info = malloc(sizeof_tuple_data_type_info(3));

		strcpy(id_idx_type_info->containees[0].field_name, "object_type");
		id_idx_type_info->containees[0].al.type_info = obj_type_dti_p;

		strcpy(id_idx_type_info->containees[1].field_name, "id");
		id_idx_type_info->containees[1].al.type_info = id_dti_p;

		strcpy(id_idx_type_info->containees[2].field_name, "object.tuple_pointer");
		id_idx_type_info->containees[2].al.type_info = &(catmgr_engine->pam_p->pas.tuple_pointer_type_info);

		initialize_tuple_data_type_info(id_idx_type_info, "rhendb_id_idx_entry", 0, 900, 3);

		initialize_tuple_def(&(catmgr_p->id_idx.record_def), id_idx_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->id_idx.index_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->id_idx.record_def), key_element_ids0, cmp_dirs_all_asc, 3);
	}

	{
		data_type_info* table_to_indices_type_info = malloc(sizeof_tuple_data_type_info(2));

		strcpy(table_to_indices_type_info->containees[0].field_name, "table_id");
		table_to_indices_type_info->containees[0].al.type_info = id_dti_p;

		strcpy(table_to_indices_type_info->containees[1].field_name, "index.tuple_pointer");
		table_to_indices_type_info->containees[1].al.type_info = &(catmgr_engine->pam_p->pas.tuple_pointer_type_info);

		initialize_tuple_data_type_info(table_to_indices_type_info, "rhendb_table_to_indices_idx_entry", 0, 900, 2);

		initialize_tuple_def(&(catmgr_p->table_to_indices_idx.record_def), table_to_indices_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->table_to_indices_idx.index_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->table_to_indices_idx.record_def), key_element_ids0, cmp_dirs_all_asc, 2);
	}

	{
		data_type_info* owner_to_attributes_idx_type_info = malloc(sizeof_tuple_data_type_info(3));

		strcpy(owner_to_attributes_idx_type_info->containees[0].field_name, "owner_id");
		owner_to_attributes_idx_type_info->containees[0].al.type_info = id_dti_p;

		strcpy(owner_to_attributes_idx_type_info->containees[1].field_name, "rel_pos_in_owner");
		owner_to_attributes_idx_type_info->containees[1].al.type_info = rel_pos_in_owner_dti_p;

		strcpy(owner_to_attributes_idx_type_info->containees[2].field_name, "attributes.tuple_pointer");
		owner_to_attributes_idx_type_info->containees[2].al.type_info = &(catmgr_engine->pam_p->pas.tuple_pointer_type_info);

		initialize_tuple_data_type_info(owner_to_attributes_idx_type_info, "rhendb_owner_to_attributes_idx_entry", 0, 900, 3);

		initialize_tuple_def(&(catmgr_p->owner_to_attributes_idx.record_def), owner_to_attributes_idx_type_info);

		init_bplus_tree_tuple_definitions(&(catmgr_p->owner_to_attributes_idx.index_defs), &(catmgr_engine->pam_p->pas), &(catmgr_p->owner_to_attributes_idx.record_def), key_element_ids0, cmp_dirs_all_asc, 3);
	}

	initialize_heap_table_accumulative_notifier(&(catmgr_p->htan), HTAN_ENTRIES_MAX);

	catmgr_p->catmgr_engine = catmgr_engine;

	initialize_tuple_def(&(catmgr_p->mvcc_header_tuple_def), mvcc_hdr_dti_p);

	pthread_mutex_init(&(catmgr_p->global_unique_schema_id_lock), NULL);
}

// utilities for the catalog objects

static int insert_in_catalog_heap_table(catalog_manager* catmgr_p, catalog_heap_table* hpt_p, tuple_pointer* tptr, const void* heap_tuple, const void* min_tx_engine, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;
	const tuple_def* record_def = &(hpt_p->record_def);

	uint32_t required_space = get_space_to_be_occupied_by_tuple_on_persistent_page(engine->pam_p->pas.page_size, &(record_def->size_def), heap_tuple);

	// 1) try to insert into an already existing heap_page that claims enough unused_space
	uint32_t unused_space_in_entry = 0;
	persistent_page ppage = find_heap_page_with_enough_unused_space_from_heap_table(hpt_p->root_page_id, required_space, &unused_space_in_entry, &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(hpt_p->htan)), &(hpt_p->heap_table_defs), engine->pam_p, min_tx_engine, abort_error);
	if(*abort_error)
		return 0;

	if(!is_persistent_page_NULL(&ppage, engine->pam_p))
	{
		uint32_t possible_insertion_index = 0;
		uint32_t tuple_index = insert_in_heap_page(&ppage, heap_tuple, &possible_insertion_index, record_def, &(engine->pam_p->pas), engine->pmm_p, min_tx_engine, abort_error);
		if(*abort_error)
		{
			release_lock_on_persistent_page(engine->pam_p, min_tx_engine, &ppage, NONE_OPTION, abort_error);
			return 0;
		}

		if(tuple_index != INVALID_TUPLE_INDEX)
		{
			(*tptr) = (tuple_pointer){.page_id = ppage.page_id, .tuple_index = tuple_index};

			release_lock_on_persistent_page(engine->pam_p, min_tx_engine, &ppage, NONE_OPTION, abort_error);
			if(*abort_error)
				return 0;

			// the found page's entry is now stale, fix the accumulated entries after releasing the page lock
			fix_unused_space_entries_UNSAFE(catmgr_p, hpt_p->root_page_id, &(hpt_p->htan), &(hpt_p->heap_table_defs), min_tx_engine, abort_error);
			if(*abort_error)
				return 0;

			return 1;
		}
		else
		{
			// could not insert into the found page (its unused_space entry was a stale over-estimate), fall through to a new page
			release_lock_on_persistent_page(engine->pam_p, min_tx_engine, &ppage, NONE_OPTION, abort_error);
			if(*abort_error)
				return 0;

			// fall back to insert a new page and insert tuple there
		}
	}

	// 2) no usable existing page, create a new heap_page, insert into it, and track it in the heap_table
	persistent_page new_page = get_new_heap_page_with_write_lock(&(engine->pam_p->pas), record_def, engine->pam_p, engine->pmm_p, min_tx_engine, abort_error);
	if(*abort_error)
		return 0;

	uint32_t possible_insertion_index = 0;
	uint32_t tuple_index = insert_in_heap_page(&new_page, heap_tuple, &possible_insertion_index, record_def, &(engine->pam_p->pas), engine->pmm_p, min_tx_engine, abort_error);
	if(*abort_error)
	{
		release_lock_on_persistent_page(engine->pam_p, min_tx_engine, &new_page, NONE_OPTION, abort_error);
		return 0;
	}

	// tuple_index will always be valid here, if this insert fails we can not proceed further
	if(tuple_index == INVALID_TUPLE_INDEX)
	{
		printf("failed to insert a catalog heap table row in a new page\n");
		exit(-1);
	}

	(*tptr) = (tuple_pointer){.page_id = new_page.page_id, .tuple_index = tuple_index};

	// track the new page in the heap_table (reads its unused_space), then release the page lock
	track_unused_space_in_heap_table(hpt_p->root_page_id, &new_page, &(hpt_p->heap_table_defs), engine->pam_p, engine->pmm_p, min_tx_engine, abort_error);
	if(*abort_error)
		return 0;

	release_lock_on_persistent_page(engine->pam_p, min_tx_engine, &new_page, NONE_OPTION, abort_error);
	if(*abort_error)
		return 0;

	return 1;
}

// --