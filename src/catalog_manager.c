#include<rhendb/catalog_manager.h>

#include<tupleindexer/page_table/page_table.h>
#include<tupleindexer/heap_page/heap_page.h>
#include<tupleindexer/hash_table/hash_table.h>

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

static void* serialize_rhendb_attribute(catalog_manager* catmgr_p, const mvcc_header* mvcchdr_p, const rhendb_attribute* attr, int should_blob, const void* min_tx_id, int* abort_error)
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

	set_element_in_tuple(record_def, STATIC_POSITION(7), tuple, &((datum){.uint_value = attr->size}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(8), tuple, &((datum){.uint_value = attr->attribute_type_id}), 0);

	if(!attr->has_count)
		set_element_in_tuple(record_def, STATIC_POSITION(9), tuple, NULL_DATUM, 0);
	else
		set_element_in_tuple(record_def, STATIC_POSITION(9), tuple, &((datum){.uint_value = attr->count}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(10), tuple, &((datum){.bit_field_value = attr->is_auto_increment}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(11), tuple, &((datum){.bit_field_value = attr->is_nullable}), 0);

	set_element_in_tuple(record_def, STATIC_POSITION(12), tuple, &((datum){.int_value = attr->cmp_dir}), 0);

	if(should_blob && attr->derived_from_expr != NULL)
	{
		if(!catalog_write_extended_blob(catmgr_p, tuple, record_def, STATIC_POSITION(13), attr->derived_from_expr, strlen(attr->derived_from_expr), min_tx_id, abort_error))
		{
			free(tuple);
			return NULL;
		}
	}
	else
		set_element_in_tuple(record_def, STATIC_POSITION(13), tuple, NULL_DATUM, UINT32_MAX);

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

static void* serialize_rhendb_index(catalog_manager* catmgr_p, const mvcc_header* mvcchdr_p, const rhendb_index* idx, int should_blob, const void* min_tx_id, int* abort_error)
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
		if(!catalog_write_extended_blob(catmgr_p, tuple, record_def, STATIC_POSITION(5), idx->predicate_expr, strlen(idx->predicate_expr), min_tx_id, abort_error))
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
				printf("FAILED (in catalog_manager) :: catalog expression too big\n");
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

static rhendb_attribute deserialize_rhendb_attribute(catalog_manager* catmgr_p, mvcc_header* mvcchdr_p, const void* tuple, int should_blob, const void* min_tx_id, int* abort_error)
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
	attr.size = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(8), tuple);
	attr.attribute_type_id = uval.uint_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(9), tuple);
	if(uval.is_NULL)
		attr.has_count = 0;
	else
	{
		attr.has_count = 1;
		attr.count = uval.uint_value;
	}

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(10), tuple);
	attr.is_auto_increment = uval.bit_field_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(11), tuple);
	attr.is_nullable = uval.bit_field_value;

	get_value_from_element_from_tuple(&uval, record_def, STATIC_POSITION(12), tuple);
	attr.cmp_dir = uval.int_value;

	if(should_blob)
	{
		attr.derived_from_expr = catalog_read_extended_blob(catmgr_p, tuple, record_def, STATIC_POSITION(13), record_def->type_info->containees[13].al.type_info, &(attr.derived_from_expr_size), min_tx_id, abort_error);
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

static rhendb_index deserialize_rhendb_index(catalog_manager* catmgr_p, mvcc_header* mvcchdr_p, const void* tuple, int should_blob, const void* min_tx_id, int* abort_error)
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
		idx.predicate_expr = catalog_read_extended_blob(catmgr_p, tuple, record_def, STATIC_POSITION(5), record_def->type_info->containees[5].al.type_info, &(idx.predicate_expr_size), min_tx_id, abort_error);
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

#define ATTRIBUTES_TABLE_ROOT_PAGE_ID_POS          0
#define TYPES_TABLE_ROOT_PAGE_ID_POS               1
#define INDEX_FRAGMENTS_TABLE_ROOT_PAGE_ID_POS     2
#define INDICES_TABLE_ROOT_PAGE_ID_POS             3
#define TABLE_PARTITIONS_TABLE_ROOT_PAGE_ID_POS    4
#define TABLES_TABLE_ROOT_PAGE_ID_POS              5
#define NAME_IDX_ROOT_PAGE_ID_POS                  6
#define ID_IDX_ROOT_PAGE_ID_POS                    7
#define TABLE_TO_INDICES_IDX_ROOT_PAGE_ID_POS      8
#define OWNER_TO_ATTRIBUTES_IDX_ROOT_PAGE_ID_POS   9
#define EXT_STORE_ROOT_PAGE_ID_ROOT_PAGE_ID_POS   10

void initialize_catalog_manager(catalog_manager* catmgr_p, uint64_t* root_page_id, data_type_info* mvcc_hdr_dti_p, rage_engine* catmgr_engine, transaction_status_getter* tsg_p)
{
	data_type_info* obj_type_dti_p = UINT_NON_NULLABLE[2];

	data_type_info* id_dti_p = UINT_NON_NULLABLE[ID_n_REL_POS_BYTES];

	data_type_info* name_dti_p = malloc(sizeof(data_type_info));
	(*name_dti_p) = get_variable_length_string_type("name", NAME_BYTES + 8);

	data_type_info* rel_pos_in_owner_dti_p = UINT_NON_NULLABLE[ID_n_REL_POS_BYTES];

	data_type_info* root_page_id_dti_p = &(catmgr_engine->pam_p->pas.page_id_type_info);

	data_type_info* base_dti_p = UINT_NON_NULLABLE[2];

	{
		data_type_info* attributes_type_info = malloc(sizeof_tuple_data_type_info(14));

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

		strcpy(attributes_type_info->containees[7].field_name, "size");
		attributes_type_info->containees[7].al.type_info = UINT_NON_NULLABLE[4];

		strcpy(attributes_type_info->containees[8].field_name, "attribute_type_id");
		attributes_type_info->containees[8].al.type_info = id_dti_p;

		strcpy(attributes_type_info->containees[9].field_name, "count");
		attributes_type_info->containees[9].al.type_info = UINT_NULLABLE[4];

		strcpy(attributes_type_info->containees[10].field_name, "is_auto_increment");
		attributes_type_info->containees[10].al.type_info = BIT_FIELD_NON_NULLABLE[1];

		strcpy(attributes_type_info->containees[11].field_name, "is_nullable");
		attributes_type_info->containees[11].al.type_info = BIT_FIELD_NON_NULLABLE[1];

		strcpy(attributes_type_info->containees[12].field_name, "cmp_dir");
		attributes_type_info->containees[12].al.type_info = INT_NON_NULLABLE[1];

		strcpy(attributes_type_info->containees[13].field_name, "derived_from_expr");
		attributes_type_info->containees[13].al.type_info = catmgr_engine->text_extended_type_info;

		initialize_tuple_data_type_info(attributes_type_info, "rhendb_attribute", 0, 900, 14);

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

		initialize_tuple_data_type_info(index_fragments_type_info, "rhendb_index_fragment", 0, 900, 5);

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

	catmgr_p->tsg_p = tsg_p;

	pthread_mutex_init(&(catmgr_p->global_unique_schema_id_lock), NULL);

	page_table_tuple_defs pttd;
	init_page_table_tuple_definitions(&pttd, &(catmgr_engine->pam_p->pas));

	if((*root_page_id) == catmgr_engine->pam_p->pas.NULL_PAGE_ID) // create a catalog
	{
		// create and initialize the root page for the page table
		{
			uint64_t page_latches_to_be_borrowed = 0;
			while(1)
			{
				int abort_error = 0;

				page_table_range_locker* ptrl_p = NULL;
				uint64_t vaccum_bucket_id; int vaccum_needed;

				// we are fine with waiting for atmost a second, and we hold no latches
				void* min_tx_id = catmgr_engine->allot_new_sub_transaction_id(catmgr_engine->context, page_latches_to_be_borrowed);

				(*root_page_id) = get_new_page_table(&pttd, catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;

				ptrl_p = get_new_page_table_range_locker((*root_page_id), WHOLE_BUCKET_RANGE, &pttd, catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
				if(abort_error)
					goto ABORT_ERROR;

				{
					{
						catmgr_p->attributes_table.root_page_id = get_new_heap_table(&(catmgr_p->attributes_table.heap_table_defs), catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;

						set_in_page_table(ptrl_p, ATTRIBUTES_TABLE_ROOT_PAGE_ID_POS, catmgr_p->attributes_table.root_page_id, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
					}
					{
						catmgr_p->types_table.root_page_id = get_new_heap_table(&(catmgr_p->types_table.heap_table_defs), catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;

						set_in_page_table(ptrl_p, TYPES_TABLE_ROOT_PAGE_ID_POS, catmgr_p->types_table.root_page_id, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
					}
					{
						catmgr_p->index_fragments_table.root_page_id = get_new_bplus_tree(&(catmgr_p->index_fragments_table.clust_table_defs), catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;

						set_in_page_table(ptrl_p, INDEX_FRAGMENTS_TABLE_ROOT_PAGE_ID_POS, catmgr_p->index_fragments_table.root_page_id, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
					}
					{
						catmgr_p->indices_table.root_page_id = get_new_heap_table(&(catmgr_p->indices_table.heap_table_defs), catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;

						set_in_page_table(ptrl_p, INDICES_TABLE_ROOT_PAGE_ID_POS, catmgr_p->indices_table.root_page_id, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
					}
					{
						catmgr_p->table_partitions_table.root_page_id = get_new_bplus_tree(&(catmgr_p->table_partitions_table.clust_table_defs), catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;

						set_in_page_table(ptrl_p, TABLE_PARTITIONS_TABLE_ROOT_PAGE_ID_POS, catmgr_p->table_partitions_table.root_page_id, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
					}
					{
						catmgr_p->tables_table.root_page_id = get_new_heap_table(&(catmgr_p->tables_table.heap_table_defs), catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;

						set_in_page_table(ptrl_p, TABLES_TABLE_ROOT_PAGE_ID_POS, catmgr_p->tables_table.root_page_id, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
					}
					{
						catmgr_p->name_idx.root_page_id = get_new_bplus_tree(&(catmgr_p->name_idx.index_defs), catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;

						set_in_page_table(ptrl_p, NAME_IDX_ROOT_PAGE_ID_POS, catmgr_p->name_idx.root_page_id, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
					}
					{
						catmgr_p->id_idx.root_page_id = get_new_bplus_tree(&(catmgr_p->id_idx.index_defs), catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;

						set_in_page_table(ptrl_p, ID_IDX_ROOT_PAGE_ID_POS, catmgr_p->id_idx.root_page_id, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
					}
					{
						catmgr_p->table_to_indices_idx.root_page_id = get_new_bplus_tree(&(catmgr_p->table_to_indices_idx.index_defs), catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;

						set_in_page_table(ptrl_p, TABLE_TO_INDICES_IDX_ROOT_PAGE_ID_POS, catmgr_p->table_to_indices_idx.root_page_id, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
					}
					{
						catmgr_p->owner_to_attributes_idx.root_page_id = get_new_bplus_tree(&(catmgr_p->owner_to_attributes_idx.index_defs), catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;

						set_in_page_table(ptrl_p, OWNER_TO_ATTRIBUTES_IDX_ROOT_PAGE_ID_POS, catmgr_p->owner_to_attributes_idx.root_page_id, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
					}
					{
						catmgr_p->ext_store_root_page_id = get_new_blob_store(&(catmgr_engine->bstd), catmgr_engine->pam_p, catmgr_engine->pmm_p, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;

						set_in_page_table(ptrl_p, EXT_STORE_ROOT_PAGE_ID_ROOT_PAGE_ID_POS, catmgr_p->ext_store_root_page_id, min_tx_id, &abort_error);
						if(abort_error)
							goto ABORT_ERROR;
					}
				}

				delete_page_table_range_locker(ptrl_p, &vaccum_bucket_id, &vaccum_needed, min_tx_id, &abort_error);
				ptrl_p = NULL;
				if(abort_error)
					goto ABORT_ERROR;

				if(abort_error == 0) // initialization done
				{
					catmgr_engine->complete_sub_transaction(catmgr_engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
					break;
				}

				ABORT_ERROR:
				if(ptrl_p != NULL)
					delete_page_table_range_locker(ptrl_p, &vaccum_bucket_id, &vaccum_needed, min_tx_id, &abort_error);
				catmgr_engine->complete_sub_transaction(catmgr_engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
			}

			catmgr_p->catalog_root_page_id = (*root_page_id);
		}

		catmgr_p->global_unique_schema_id = FIRST_SCHEMA_UNIQUE_ID;
	}
	else // init all root_page_ids
	{
		while(1)
		{
			int abort_error = 0;

			page_table_range_locker* ptrl_p = NULL;
			uint64_t vaccum_bucket_id; int vaccum_needed;

			ptrl_p = get_new_page_table_range_locker((*root_page_id), WHOLE_BUCKET_RANGE, &pttd, catmgr_engine->pam_p, NULL, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			catmgr_p->attributes_table.root_page_id = get_from_page_table(ptrl_p, ATTRIBUTES_TABLE_ROOT_PAGE_ID_POS, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			catmgr_p->types_table.root_page_id = get_from_page_table(ptrl_p, TYPES_TABLE_ROOT_PAGE_ID_POS, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			catmgr_p->index_fragments_table.root_page_id = get_from_page_table(ptrl_p, INDEX_FRAGMENTS_TABLE_ROOT_PAGE_ID_POS, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			catmgr_p->indices_table.root_page_id = get_from_page_table(ptrl_p, INDICES_TABLE_ROOT_PAGE_ID_POS, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			catmgr_p->table_partitions_table.root_page_id = get_from_page_table(ptrl_p, TABLE_PARTITIONS_TABLE_ROOT_PAGE_ID_POS, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			catmgr_p->tables_table.root_page_id = get_from_page_table(ptrl_p, TABLES_TABLE_ROOT_PAGE_ID_POS, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			catmgr_p->name_idx.root_page_id = get_from_page_table(ptrl_p, NAME_IDX_ROOT_PAGE_ID_POS, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			catmgr_p->id_idx.root_page_id = get_from_page_table(ptrl_p, ID_IDX_ROOT_PAGE_ID_POS, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			catmgr_p->table_to_indices_idx.root_page_id = get_from_page_table(ptrl_p, TABLE_TO_INDICES_IDX_ROOT_PAGE_ID_POS, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			catmgr_p->owner_to_attributes_idx.root_page_id = get_from_page_table(ptrl_p, OWNER_TO_ATTRIBUTES_IDX_ROOT_PAGE_ID_POS, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			catmgr_p->ext_store_root_page_id = get_from_page_table(ptrl_p, EXT_STORE_ROOT_PAGE_ID_ROOT_PAGE_ID_POS, NULL, &abort_error);
			if(abort_error)
				goto ABORT_ERROR_1;

			delete_page_table_range_locker(ptrl_p, &vaccum_bucket_id, &vaccum_needed, NULL, &abort_error);
			ptrl_p = NULL;
			if(abort_error)
				goto ABORT_ERROR_1;

			if(abort_error == 0) // initialization done
				break;

			ABORT_ERROR_1:
			if(ptrl_p != NULL)
				delete_page_table_range_locker(ptrl_p, &vaccum_bucket_id, &vaccum_needed, NULL, &abort_error);
		}

		while(1)
		{
			int abort_error = 0;

			bplus_tree_iterator* bpi_p = NULL;

			catmgr_p->global_unique_schema_id = FIRST_SCHEMA_UNIQUE_ID;
			for(catalog_object_type obj_type = 0; obj_type < 3; obj_type++)
			{
				void* key_obj_type = serialize_rhendb_id_idx_entry(catmgr_p, &((rhendb_id_idx_entry){.object_type = obj_type}));
				bpi_p = find_in_bplus_tree(catmgr_p->id_idx.root_page_id, key_obj_type, 1, LESSER_THAN_EQUALS, 0, READ_LOCK, &(catmgr_p->id_idx.index_defs), catmgr_engine->pam_p, NULL, NULL, &abort_error);
				free(key_obj_type);
				if(abort_error)
					goto ABORT_ERROR_2;

				if(is_empty_bplus_tree(bpi_p))
				{
					delete_bplus_tree_iterator(bpi_p, NULL, &abort_error);
					bpi_p = NULL;
					if(abort_error)
						goto ABORT_ERROR_2;

					break;
				}

				const void* id_idx_tuple = get_tuple_bplus_tree_iterator(bpi_p);
				if(id_idx_tuple != NULL)
				{
					rhendb_id_idx_entry id_idx_strct = deserialize_rhendb_id_idx_entry(catmgr_p, get_tuple_bplus_tree_iterator(bpi_p));
					if(id_idx_strct.object_type == obj_type)
						catmgr_p->global_unique_schema_id = max(id_idx_strct.id + 1, catmgr_p->global_unique_schema_id);
				}

				delete_bplus_tree_iterator(bpi_p, NULL, &abort_error);
				bpi_p = NULL;
				if(abort_error)
					goto ABORT_ERROR_2;
			}

			if(abort_error == 0) // initialization done
				break;

			ABORT_ERROR_2:
			if(bpi_p != NULL)
				delete_bplus_tree_iterator(bpi_p, NULL, &abort_error);
		}

		catmgr_p->catalog_root_page_id = (*root_page_id);
	}

	deinit_page_table_tuple_definitions(&pttd);
}

// utilities for the catalog objects

// part_id is used only if it is non-zero
// materialize the catalog object living at object_tuple_pointer (a row in the tables / types / indices heap, picked by
// object_type) into a freshly malloc-ed rhendb_table / rhendb_type / rhendb_index and return it. returns NULL on abort,
// or when the row is not visible to ss_p.
// hint bits are written back only when resolved and min_tx_id != NULL. for an index, should_blob reads its predicate_expr.
static void* get_catalog_object_at(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, catalog_object_type object_type, const tuple_pointer object_tuple_pointer, int should_blob, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;
	void* object = NULL;
	persistent_page ppage = get_NULL_persistent_page(engine->pam_p);

	const tuple_def* record_def = NULL;
	if(object_type == RHENDB_TYPE)
		record_def = &(catmgr_p->types_table.record_def);
	else if(object_type == RHENDB_INDEX)
		record_def = &(catmgr_p->indices_table.record_def);
	else if(object_type == RHENDB_TABLE)
		record_def = &(catmgr_p->tables_table.record_def);
	else
	{
		printf("BUG (in catalog_manager) :: unknown objec type passed\n");
		exit(-1);
	}

	ppage = acquire_persistent_page_with_lock(engine->pam_p, min_tx_id, object_tuple_pointer.page_id, (min_tx_id != NULL) ? WRITE_LOCK : READ_LOCK, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	const void* row = get_nth_tuple_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(record_def->size_def), object_tuple_pointer.tuple_index);
	if(row != NULL)
	{
		mvcc_header hdr;
		catalog_read_mvcc_header(catmgr_p, row, record_def, &hdr);

		int were_hints_updated = 0;
		int is_object_visible = is_tuple_visible_to_mvcc_snapshot(ss_p, &hdr, catmgr_p->tsg_p, &were_hints_updated);

		if(were_hints_updated && min_tx_id != NULL)
		{
			char mvcc_hdr_serialized[get_maximum_tuple_size(&(catmgr_p->mvcc_header_tuple_def))];
			write_mvcc_header(mvcc_hdr_serialized, &(catmgr_p->mvcc_header_tuple_def), &hdr);
			set_element_in_tuple_in_place_on_persistent_page(engine->pmm_p, min_tx_id, &ppage, engine->pam_p->pas.page_size, record_def, object_tuple_pointer.tuple_index, STATIC_POSITION(0), &((datum){.tuple_value = mvcc_hdr_serialized}), abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		if(is_object_visible)
		{
			if(object_type == RHENDB_TABLE)
			{
				rhendb_table* materialized_object = malloc(sizeof(rhendb_table));
				(*materialized_object) = deserialize_rhendb_table(catmgr_p, NULL, row);
				object = materialized_object;
			}
			else if(object_type == RHENDB_TYPE)
			{
				rhendb_type* materialized_object = malloc(sizeof(rhendb_type));
				(*materialized_object) = deserialize_rhendb_type(catmgr_p, NULL, row);
				object = materialized_object;
			}
			else if(object_type == RHENDB_INDEX)
			{
				rhendb_index* materialized_object = malloc(sizeof(rhendb_index));
				(*materialized_object) = deserialize_rhendb_index(catmgr_p, NULL, row, should_blob, min_tx_id, abort_error);
				object = materialized_object;
			}
		}
	}

	release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	return object;

	ABORT_ERROR :
	if(!is_persistent_page_NULL(&ppage, engine->pam_p))
		release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, abort_error);
	if(object != NULL)
	{
		if(object_type == RHENDB_INDEX && ((rhendb_index*)object)->predicate_expr != NULL)
			free(((rhendb_index*)object)->predicate_expr);
		free(object);
	}
	return NULL;
}

// look up a catalog object of object_type by its id via the id_idx, and materialize the version visible to us.
// returns a plain malloc-ed rhendb_table / rhendb_type / rhendb_index, or NULL on abort / not found / not visible.
static void* get_catalog_object_by_id(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, catalog_object_type object_type, uint64_t id, int should_blob, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;
	void* object = NULL;
	bplus_tree_iterator* bpi_p = NULL;

	rhendb_id_idx_entry id_key = (rhendb_id_idx_entry){.object_type = object_type, .id = id};
	void* id_key_tuple = serialize_rhendb_id_idx_entry(catmgr_p, &id_key);
	bpi_p = find_in_bplus_tree(catmgr_p->id_idx.root_page_id, id_key_tuple, 2, GREATER_THAN_EQUALS, 0, READ_LOCK, &(catmgr_p->id_idx.index_defs), engine->pam_p, NULL, min_tx_id, abort_error);
	free(id_key_tuple);
	if(*abort_error)
		goto ABORT_ERROR;

	if(!is_empty_bplus_tree(bpi_p))
	{
		// every heap update inserts a fresh id_idx entry, so more than one may share this (object_type, id),
		// walk the run until we materialize the version that is visible to us
		while(1)
		{
			const void* id_idx_record = get_tuple_bplus_tree_iterator(bpi_p);
			if(id_idx_record == NULL)
				break;
			rhendb_id_idx_entry id_idx_entry = deserialize_rhendb_id_idx_entry(catmgr_p, id_idx_record);

			// stop once we walk past the (object_type, id) run of the index
			if(id_idx_entry.object_type != object_type || id_idx_entry.id != id)
				break;

			object = get_catalog_object_at(catmgr_p, ss_p, object_type, id_idx_entry.object_tuple_pointer, should_blob, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			if(object != NULL)
				break;

			next_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	bpi_p = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	return object;

	ABORT_ERROR:;
	if(bpi_p != NULL)
		delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	if(object != NULL)
	{
		if(object_type == RHENDB_INDEX && ((rhendb_index*)object)->predicate_expr != NULL)
			free(((rhendb_index*)object)->predicate_expr);
		free(object);
		object = NULL;
	}
	return NULL;
}

// look up a catalog object of object_type by its name via the name_idx, and materialize the version visible to us.
// returns a plain malloc-ed rhendb_table / rhendb_type / rhendb_index, or NULL on abort / not found / not visible.
static void* get_catalog_object_by_name(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, catalog_object_type object_type, char* name, int should_blob, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;
	void* object = NULL;
	bplus_tree_iterator* bpi_p = NULL;

	rhendb_name_idx_entry name_key = (rhendb_name_idx_entry){.object_type = object_type};
	strncpy(name_key.name, name, 64);
	void* name_key_tuple = serialize_rhendb_name_idx_entry(catmgr_p, &name_key);
	bpi_p = find_in_bplus_tree(catmgr_p->name_idx.root_page_id, name_key_tuple, 2, GREATER_THAN_EQUALS, 0, READ_LOCK, &(catmgr_p->name_idx.index_defs), engine->pam_p, NULL, min_tx_id, abort_error);
	free(name_key_tuple);
	if(*abort_error)
		goto ABORT_ERROR;

	if(!is_empty_bplus_tree(bpi_p))
	{
		// every heap update inserts a fresh name_idx entry, so more than one may share this (object_type, name),
		// walk the run until we materialize the version that is visible to us
		while(1)
		{
			const void* name_idx_record = get_tuple_bplus_tree_iterator(bpi_p);
			if(name_idx_record == NULL)
				break;
			rhendb_name_idx_entry name_idx_entry = deserialize_rhendb_name_idx_entry(catmgr_p, name_idx_record);

			// stop once we walk past the (object_type, name) run of the index
			if(name_idx_entry.object_type != object_type || strncmp(name_idx_entry.name, name, 64) != 0)
				break;

			object = get_catalog_object_at(catmgr_p, ss_p, object_type, name_idx_entry.object_tuple_pointer, should_blob, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			if(object != NULL)
				break;

			next_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	bpi_p = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	return object;

	ABORT_ERROR:;
	if(bpi_p != NULL)
		delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	if(object != NULL)
	{
		if(object_type == RHENDB_INDEX && ((rhendb_index*)object)->predicate_expr != NULL)
			free(((rhendb_index*)object)->predicate_expr);
		free(object);
	}
	return NULL;
}

static rhendb_attribute* get_attributes_for_catalog_object(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t id, uint64_t part_id, int should_blob, uint64_t* attrs_count, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;

	bplus_tree_iterator* bpi_p = NULL;

	// init return attrs
	(*attrs_count) = 0;
	uint64_t attrs_capacity = 8;
	rhendb_attribute* attrs = malloc(sizeof(rhendb_attribute) * attrs_capacity);

	{
		rhendb_owner_to_attributes_idx_entry o2a_key = (rhendb_owner_to_attributes_idx_entry){.owner_id = id};
		void* o2a_key_tuple = serialize_rhendb_owner_to_attributes_idx_entry(catmgr_p, &o2a_key);
		bpi_p = find_in_bplus_tree(catmgr_p->owner_to_attributes_idx.root_page_id, o2a_key_tuple, 1, GREATER_THAN_EQUALS, 0, READ_LOCK, &(catmgr_p->owner_to_attributes_idx.index_defs), engine->pam_p, NULL, min_tx_id, abort_error);
		free(o2a_key_tuple);
		if(*abort_error)
			goto ABORT_ERROR;

		if(!is_empty_bplus_tree(bpi_p))
		{
			uint64_t expected_rel_pos_in_owner = 0;
			while(1)
			{
				const void* o2a_idx_record = get_tuple_bplus_tree_iterator(bpi_p);
				if(o2a_idx_record == NULL)
					break;
				rhendb_owner_to_attributes_idx_entry o2a_idx_entry = deserialize_rhendb_owner_to_attributes_idx_entry(catmgr_p, o2a_idx_record);

				// stop once we walk past the (id, ...) run of the index
				if(o2a_idx_entry.owner_id != id)
					break;

				if(o2a_idx_entry.rel_pos_in_owner < expected_rel_pos_in_owner)
				{
					next_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
					if(*abort_error)
						goto ABORT_ERROR;
					continue;
				}

				// follow this entry to its attributes_table row, write locked so we can persist resolved hint bits
				persistent_page ppage = acquire_persistent_page_with_lock(engine->pam_p, min_tx_id, o2a_idx_entry.attributes_tuple_pointer.page_id, (min_tx_id != NULL) ? WRITE_LOCK : READ_LOCK, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				const void* row = get_nth_tuple_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(catmgr_p->attributes_table.record_def.size_def), o2a_idx_entry.attributes_tuple_pointer.tuple_index);
				int is_row_visible = 0;
				{
					mvcc_header hdr;
					catalog_read_mvcc_header(catmgr_p, row, &(catmgr_p->attributes_table.record_def), &hdr);

					int were_hints_updated = 0;
					is_row_visible = is_tuple_visible_to_mvcc_snapshot(ss_p, &hdr, catmgr_p->tsg_p, &were_hints_updated);

					if(were_hints_updated && min_tx_id != NULL)
					{
						char mvcc_hdr_serialized[get_maximum_tuple_size(&(catmgr_p->mvcc_header_tuple_def))];
						write_mvcc_header(mvcc_hdr_serialized, &(catmgr_p->mvcc_header_tuple_def), &hdr);
						set_element_in_tuple_in_place_on_persistent_page(engine->pmm_p, min_tx_id, &ppage, engine->pam_p->pas.page_size, &(catmgr_p->attributes_table.record_def), o2a_idx_entry.attributes_tuple_pointer.tuple_index, STATIC_POSITION(0), &((datum){.tuple_value = mvcc_hdr_serialized}), abort_error);
					}
				}

				if(is_row_visible)
				{
					rhendb_attribute owned_attr = deserialize_rhendb_attribute(catmgr_p, NULL, row, 0, min_tx_id, abort_error);
					if(*abort_error)
					{
						release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, abort_error);
						goto ABORT_ERROR;
					}

					if(part_id == 0 || (owned_attr.table_part_id_from <= part_id && (owned_attr.table_part_id_to == 0 || part_id < owned_attr.table_part_id_to)))
					{
						if(should_blob)
						{
							owned_attr = deserialize_rhendb_attribute(catmgr_p, NULL, row, 1, min_tx_id, abort_error);
							if(*abort_error)
							{
								release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, abort_error);
								goto ABORT_ERROR;
							}
						}

						expected_rel_pos_in_owner = owned_attr.rel_pos_in_owner + 1;

						if((*attrs_count) == attrs_capacity)
						{
							attrs_capacity *= 2;
							attrs = realloc(attrs, sizeof(rhendb_attribute) * attrs_capacity);
						}
						attrs[(*attrs_count)++] = owned_attr;
					}
				}

				release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;

				next_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
			}
		}

		delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;
	}

	if((*attrs_count) == 0)
	{
		free(attrs);
		attrs = NULL;
		attrs_capacity = 0;
	}

	return attrs;

	ABORT_ERROR:;
	if(attrs != NULL)
	{
		for(uint64_t i = 0; i < (*attrs_count); i++)
			free(attrs[i].derived_from_expr);
		free(attrs);
	}
	if(bpi_p != NULL)
		delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	return NULL;
}

// get every index of a table via the table_to_indices_idx (keyed on table_id), following each entry to its indices_table
// row and returning the ones visible to us as a malloc-ed array. should_blob controls reading each index's predicate_expr.
static rhendb_index* get_indices_for_table(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t table_id, int should_blob, uint64_t* indices_count, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;

	bplus_tree_iterator* bpi_p = NULL;

	// init return indices
	(*indices_count) = 0;
	uint64_t indices_capacity = 8;
	rhendb_index* indices = malloc(sizeof(rhendb_index) * indices_capacity);

	rhendb_table_to_indices_entry t2i_key = (rhendb_table_to_indices_entry){.table_id = table_id};
	void* t2i_key_tuple = serialize_rhendb_table_to_indices_entry(catmgr_p, &t2i_key);
	bpi_p = find_in_bplus_tree(catmgr_p->table_to_indices_idx.root_page_id, t2i_key_tuple, 1, GREATER_THAN_EQUALS, 0, READ_LOCK, &(catmgr_p->table_to_indices_idx.index_defs), engine->pam_p, NULL, min_tx_id, abort_error);
	free(t2i_key_tuple);
	if(*abort_error)
		goto ABORT_ERROR;

	if(!is_empty_bplus_tree(bpi_p))
	{
		while(1)
		{
			const void* t2i_idx_record = get_tuple_bplus_tree_iterator(bpi_p);
			if(t2i_idx_record == NULL)
				break;
			rhendb_table_to_indices_entry t2i_idx_entry = deserialize_rhendb_table_to_indices_entry(catmgr_p, t2i_idx_record);

			// stop once we walk past the (table_id, ...) run of the index
			if(t2i_idx_entry.table_id != table_id)
				break;

			// materialize the index row via the shared getter (it acquires, checks visibility, fixes hints and mallocs),
			// then shallow copy it into our array and free that extra allocation, the predicate_expr is carried over as is
			rhendb_index* materialized_index = get_catalog_object_at(catmgr_p, ss_p, RHENDB_INDEX, t2i_idx_entry.indices_tuple_pointer, should_blob, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			if(materialized_index != NULL)
			{
				if((*indices_count) == indices_capacity)
				{
					indices_capacity *= 2;
					indices = realloc(indices, sizeof(rhendb_index) * indices_capacity);
				}
				indices[(*indices_count)++] = (*materialized_index);
				free(materialized_index);
			}

			next_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	bpi_p = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	if((*indices_count) == 0)
	{
		free(indices);
		indices = NULL;
	}

	return indices;

	ABORT_ERROR:;
	if(indices != NULL)
	{
		for(uint64_t i = 0; i < (*indices_count); i++)
			free(indices[i].predicate_expr);
		free(indices);
	}
	if(bpi_p != NULL)
		delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	return NULL;
}

// get every partition of a table from the clustered table_partitions_table (keyed on (table_id, partition_id)), scanning
// its (table_id, ...) run and returning the ones visible to us as a malloc-ed array. resolved hint bits are written back
// in place on the leaf when min_tx_id != NULL.
static rhendb_table_partition* get_partitions_for_table(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t table_id, uint64_t* partitions_count, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;

	bplus_tree_iterator* bpi_p = NULL;

	// init return partitions
	(*partitions_count) = 0;
	uint64_t partitions_capacity = 8;
	rhendb_table_partition* partitions = malloc(sizeof(rhendb_table_partition) * partitions_capacity);

	rhendb_table_partition partition_key = (rhendb_table_partition){.table_id = table_id};
	void* partition_key_tuple = serialize_rhendb_table_partition_key(catmgr_p, &partition_key);
	bpi_p = find_in_bplus_tree(catmgr_p->table_partitions_table.root_page_id, partition_key_tuple, 1, GREATER_THAN_EQUALS, 0, (min_tx_id != NULL) ? WRITE_LOCK : READ_LOCK, &(catmgr_p->table_partitions_table.clust_table_defs), engine->pam_p, (min_tx_id != NULL) ? engine->pmm_p : NULL, min_tx_id, abort_error);
	free(partition_key_tuple);
	if(*abort_error)
		goto ABORT_ERROR;

	if(!is_empty_bplus_tree(bpi_p))
	{
		while(1)
		{
			const void* partition_record = get_tuple_bplus_tree_iterator(bpi_p);
			if(partition_record == NULL)
				break;
			mvcc_header hdr;
			rhendb_table_partition partition_entry = deserialize_rhendb_table_partition(catmgr_p, &hdr, partition_record);

			// stop once we walk past the (table_id, ...) run of the clustered index
			if(partition_entry.table_id != table_id)
				break;

			int were_hints_updated = 0;
			int is_partition_visible = is_tuple_visible_to_mvcc_snapshot(ss_p, &hdr, catmgr_p->tsg_p, &were_hints_updated);

			// the mvcc_header is the non-key element at position 0, persist the resolved hints back in place on the leaf
			if(were_hints_updated && min_tx_id != NULL)
			{
				char mvcc_hdr_serialized[get_maximum_tuple_size(&(catmgr_p->mvcc_header_tuple_def))];
				write_mvcc_header(mvcc_hdr_serialized, &(catmgr_p->mvcc_header_tuple_def), &hdr);
				update_non_key_element_in_place_at_bplus_tree_iterator(bpi_p, STATIC_POSITION(0), &((datum){.tuple_value = mvcc_hdr_serialized}), min_tx_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
			}

			if(is_partition_visible)
			{
				if((*partitions_count) == partitions_capacity)
				{
					partitions_capacity *= 2;
					partitions = realloc(partitions, sizeof(rhendb_table_partition) * partitions_capacity);
				}
				partitions[(*partitions_count)++] = partition_entry;
			}

			next_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	bpi_p = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	if((*partitions_count) == 0)
	{
		free(partitions);
		partitions = NULL;
	}

	return partitions;

	ABORT_ERROR:;
	if(partitions != NULL)
		free(partitions);
	if(bpi_p != NULL)
		delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	return NULL;
}

// get the index fragments from the clustered index_fragments_table (keyed on (table_id, index_id, partition_id)).
// table_id is always concerned, index_id is concerned only when it is non zero, and partition_id only when both index_id
// and it are non zero. returns the fragments visible to us as a malloc-ed array, writing hints back when min_tx_id != NULL.
static rhendb_index_fragment* get_index_fragments(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t table_id, uint64_t index_id, uint64_t partition_id, uint64_t* fragments_count, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;

	bplus_tree_iterator* bpi_p = NULL;

	// init return fragments
	(*fragments_count) = 0;
	uint64_t fragments_capacity = 8;
	rhendb_index_fragment* fragments = malloc(sizeof(rhendb_index_fragment) * fragments_capacity);

	// decide how many of the leading key elements the lookup should be concerned with
	uint32_t key_element_count = 1;
	if(index_id != 0)
	{
		key_element_count = 2;
		if(partition_id != 0)
			key_element_count = 3;
	}

	rhendb_index_fragment fragment_key = (rhendb_index_fragment){.table_id = table_id, .index_id = index_id, .partition_id = partition_id};
	void* fragment_key_tuple = serialize_rhendb_index_fragment_key(catmgr_p, &fragment_key);
	bpi_p = find_in_bplus_tree(catmgr_p->index_fragments_table.root_page_id, fragment_key_tuple, key_element_count, GREATER_THAN_EQUALS, 0, (min_tx_id != NULL) ? WRITE_LOCK : READ_LOCK, &(catmgr_p->index_fragments_table.clust_table_defs), engine->pam_p, (min_tx_id != NULL) ? engine->pmm_p : NULL, min_tx_id, abort_error);
	free(fragment_key_tuple);
	if(*abort_error)
		goto ABORT_ERROR;

	if(!is_empty_bplus_tree(bpi_p))
	{
		while(1)
		{
			const void* fragment_record = get_tuple_bplus_tree_iterator(bpi_p);
			if(fragment_record == NULL)
				break;
			mvcc_header hdr;
			rhendb_index_fragment fragment_entry = deserialize_rhendb_index_fragment(catmgr_p, &hdr, fragment_record);

			// stop once we walk past the concerned (table_id [, index_id [, partition_id]]) key prefix
			if(fragment_entry.table_id != table_id)
				break;
			if(index_id != 0 && fragment_entry.index_id != index_id)
				break;
			if(index_id != 0 && partition_id != 0 && fragment_entry.partition_id != partition_id)
				break;

			int were_hints_updated = 0;
			int is_fragment_visible = is_tuple_visible_to_mvcc_snapshot(ss_p, &hdr, catmgr_p->tsg_p, &were_hints_updated);

			// the mvcc_header is the non-key element at position 0, persist the resolved hints back in place on the leaf
			if(were_hints_updated && min_tx_id != NULL)
			{
				char mvcc_hdr_serialized[get_maximum_tuple_size(&(catmgr_p->mvcc_header_tuple_def))];
				write_mvcc_header(mvcc_hdr_serialized, &(catmgr_p->mvcc_header_tuple_def), &hdr);
				update_non_key_element_in_place_at_bplus_tree_iterator(bpi_p, STATIC_POSITION(0), &((datum){.tuple_value = mvcc_hdr_serialized}), min_tx_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
			}

			if(is_fragment_visible)
			{
				if((*fragments_count) == fragments_capacity)
				{
					fragments_capacity *= 2;
					fragments = realloc(fragments, sizeof(rhendb_index_fragment) * fragments_capacity);
				}
				fragments[(*fragments_count)++] = fragment_entry;
			}

			next_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	bpi_p = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	if((*fragments_count) == 0)
	{
		free(fragments);
		fragments = NULL;
	}

	return fragments;

	ABORT_ERROR:;
	if(fragments != NULL)
		free(fragments);
	if(bpi_p != NULL)
		delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	return NULL;
}

// read the attribute at attribute_tuple_pointer if it is visible to us, into a malloc-ed rhendb_attribute (should_blob = 1).
// returns NULL on abort or when the row is not visible. hints are written back only when resolved and min_tx_id != NULL.
static rhendb_attribute* get_attribute_at(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, const tuple_pointer attribute_tuple_pointer, int should_blob, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;
	rhendb_attribute* attribute = NULL;
	persistent_page ppage = get_NULL_persistent_page(engine->pam_p);

	ppage = acquire_persistent_page_with_lock(engine->pam_p, min_tx_id, attribute_tuple_pointer.page_id, (min_tx_id != NULL) ? WRITE_LOCK : READ_LOCK, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	const void* row = get_nth_tuple_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(catmgr_p->attributes_table.record_def.size_def), attribute_tuple_pointer.tuple_index);
	if(row != NULL)
	{
		mvcc_header hdr;
		catalog_read_mvcc_header(catmgr_p, row, &(catmgr_p->attributes_table.record_def), &hdr);

		int were_hints_updated = 0;
		int is_attribute_visible = is_tuple_visible_to_mvcc_snapshot(ss_p, &hdr, catmgr_p->tsg_p, &were_hints_updated);

		if(were_hints_updated && min_tx_id != NULL)
		{
			char mvcc_hdr_serialized[get_maximum_tuple_size(&(catmgr_p->mvcc_header_tuple_def))];
			write_mvcc_header(mvcc_hdr_serialized, &(catmgr_p->mvcc_header_tuple_def), &hdr);
			set_element_in_tuple_in_place_on_persistent_page(engine->pmm_p, min_tx_id, &ppage, engine->pam_p->pas.page_size, &(catmgr_p->attributes_table.record_def), attribute_tuple_pointer.tuple_index, STATIC_POSITION(0), &((datum){.tuple_value = mvcc_hdr_serialized}), abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		if(is_attribute_visible)
		{
			attribute = malloc(sizeof(rhendb_attribute));
			(*attribute) = deserialize_rhendb_attribute(catmgr_p, NULL, row, should_blob, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	return attribute;

	ABORT_ERROR:;
	if(!is_persistent_page_NULL(&ppage, engine->pam_p))
		release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, abort_error);
	if(attribute != NULL)
	{
		free(attribute->derived_from_expr);
		free(attribute);
	}
	return NULL;
}

// the largest partition_id among the visible partitions of a table, 0 if it has none. found by seeking to (table_id, max)
// in the clustered table_partitions index and walking backwards to the newest visible partition.
static uint64_t get_last_partition_id_for_table(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t table_id, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;
	bplus_tree_iterator* bpi_p = NULL;
	uint64_t last_partition_id = 0;

	rhendb_table_partition partition_key = (rhendb_table_partition){.table_id = table_id, .partition_id = UINT64_MAX};
	void* partition_key_tuple = serialize_rhendb_table_partition_key(catmgr_p, &partition_key);
	bpi_p = find_in_bplus_tree(catmgr_p->table_partitions_table.root_page_id, partition_key_tuple, 2, LESSER_THAN_EQUALS, 0, (min_tx_id != NULL) ? WRITE_LOCK : READ_LOCK, &(catmgr_p->table_partitions_table.clust_table_defs), engine->pam_p, (min_tx_id != NULL) ? engine->pmm_p : NULL, min_tx_id, abort_error);
	free(partition_key_tuple);
	if(*abort_error)
		goto ABORT_ERROR;

	if(!is_empty_bplus_tree(bpi_p))
	{
		while(1)
		{
			const void* partition_record = get_tuple_bplus_tree_iterator(bpi_p);
			if(partition_record == NULL)
				break;
			mvcc_header hdr;
			rhendb_table_partition partition_entry = deserialize_rhendb_table_partition(catmgr_p, &hdr, partition_record);

			// stop once we walk back past this table's run of the clustered index
			if(partition_entry.table_id != table_id)
				break;

			int were_hints_updated = 0;
			int is_partition_visible = is_tuple_visible_to_mvcc_snapshot(ss_p, &hdr, catmgr_p->tsg_p, &were_hints_updated);
			if(were_hints_updated && min_tx_id != NULL)
			{
				char mvcc_hdr_serialized[get_maximum_tuple_size(&(catmgr_p->mvcc_header_tuple_def))];
				write_mvcc_header(mvcc_hdr_serialized, &(catmgr_p->mvcc_header_tuple_def), &hdr);
				update_non_key_element_in_place_at_bplus_tree_iterator(bpi_p, STATIC_POSITION(0), &((datum){.tuple_value = mvcc_hdr_serialized}), min_tx_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
			}
			if(is_partition_visible)
			{
				last_partition_id = partition_entry.partition_id;
				break;
			}

			prev_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	bpi_p = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	return last_partition_id;

	ABORT_ERROR:;
	if(bpi_p != NULL)
		delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	return 0;
}

// the visible attribute at (owner_id, rel_pos_in_owner) via the owner_to_attributes index, malloc-ed, or NULL if none is
// visible or on abort. when found and attribute_tuple_pointer is not NULL, its heap tuple_pointer is written there.
static rhendb_attribute* get_attribute_for_owner_at_rel_pos(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t owner_id, uint64_t rel_pos_in_owner, tuple_pointer* attribute_tuple_pointer, int should_blob, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;
	bplus_tree_iterator* bpi_p = NULL;
	rhendb_attribute* attribute = NULL;

	rhendb_owner_to_attributes_idx_entry owner_key = (rhendb_owner_to_attributes_idx_entry){.owner_id = owner_id, .rel_pos_in_owner = rel_pos_in_owner};
	void* owner_key_tuple = serialize_rhendb_owner_to_attributes_idx_entry(catmgr_p, &owner_key);
	bpi_p = find_in_bplus_tree(catmgr_p->owner_to_attributes_idx.root_page_id, owner_key_tuple, 2, GREATER_THAN_EQUALS, 0, READ_LOCK, &(catmgr_p->owner_to_attributes_idx.index_defs), engine->pam_p, NULL, min_tx_id, abort_error);
	free(owner_key_tuple);
	if(*abort_error)
		goto ABORT_ERROR;

	if(!is_empty_bplus_tree(bpi_p))
	{
		while(1)
		{
			const void* owner_idx_record = get_tuple_bplus_tree_iterator(bpi_p);
			if(owner_idx_record == NULL)
				break;
			rhendb_owner_to_attributes_idx_entry owner_idx_ent = deserialize_rhendb_owner_to_attributes_idx_entry(catmgr_p, owner_idx_record);

			// stop once we walk past the (owner_id, rel_pos_in_owner) run
			if(owner_idx_ent.owner_id != owner_id || owner_idx_ent.rel_pos_in_owner != rel_pos_in_owner)
				break;

			attribute = get_attribute_at(catmgr_p, ss_p, owner_idx_ent.attributes_tuple_pointer, should_blob, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
			if(attribute != NULL)
			{
				if(attribute_tuple_pointer != NULL)
					(*attribute_tuple_pointer) = owner_idx_ent.attributes_tuple_pointer;
				break;
			}

			next_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	bpi_p = NULL;
	if(*abort_error)
		goto ABORT_ERROR;

	return attribute;

	ABORT_ERROR:;
	if(attribute != NULL)
	{
		free(attribute->derived_from_expr);
		free(attribute);
	}
	if(bpi_p != NULL)
		delete_bplus_tree_iterator(bpi_p, min_tx_id, abort_error);
	return NULL;
}


// builds a tuple data_type_info from the attributes owned by owner_id, one containee per attribute, defined below
static data_type_info* get_tuple_data_type_info_from_attributes(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, char* type_name, int is_nullable, uint64_t owner_id, uint64_t part_id, const void* min_tx_id, int* abort_error);

static data_type_info* get_data_type_info_for_rhendb_attribute(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, rhendb_attribute attr, const void* min_tx_id, int* abort_error)
{
	data_type_info* inner_dti_p = NULL;

	switch(attr.base_type)
	{
		case RHENDB_BIT_FIELD :
		{
			if(0 < attr.size && attr.size <= 64)
			{
				if(attr.is_nullable)
					inner_dti_p = BIT_FIELD_NULLABLE[attr.size];
				else
					inner_dti_p = BIT_FIELD_NON_NULLABLE[attr.size];
			}
			break;
		}

		case RHENDB_UINT :
		{
			if(attr.is_nullable)
			{
				if(0 < attr.size && attr.size <= 8)
					inner_dti_p = UINT_NULLABLE[attr.size];
				else if(0 < attr.size && attr.size <= 32)
					inner_dti_p = LARGE_UINT_NULLABLE[attr.size];
			}
			else
			{
				if(0 < attr.size && attr.size <= 8)
					inner_dti_p = UINT_NON_NULLABLE[attr.size];
				else if(0 < attr.size && attr.size <= 32)
					inner_dti_p = LARGE_UINT_NON_NULLABLE[attr.size];
			}
			break;
		}

		case RHENDB_INT :
		{
			if(attr.is_nullable)
			{
				if(0 < attr.size && attr.size <= 8)
					inner_dti_p = INT_NULLABLE[attr.size];
				else if(0 < attr.size && attr.size <= 32)
					inner_dti_p = LARGE_INT_NULLABLE[attr.size];
			}
			else
			{
				if(0 < attr.size && attr.size <= 8)
					inner_dti_p = INT_NON_NULLABLE[attr.size];
				else if(0 < attr.size && attr.size <= 32)
					inner_dti_p = LARGE_INT_NON_NULLABLE[attr.size];
			}
			break;
		}

		case RHENDB_FLOAT :
		{
			if(attr.is_nullable)
			{
				if(attr.size == sizeof(float))
					inner_dti_p = FLOAT_float_NULLABLE;
				else if(attr.size == sizeof(double))
					inner_dti_p = FLOAT_double_NULLABLE;
			}
			else
			{
				if(attr.size == sizeof(float))
					inner_dti_p = FLOAT_float_NON_NULLABLE;
				else if(attr.size == sizeof(double))
					inner_dti_p = FLOAT_double_NON_NULLABLE;
			}
			break;
		}

		case RHENDB_TUPLE_POINTER :
			inner_dti_p = &(catmgr_p->catmgr_engine->pam_p->pas.tuple_pointer_type_info); break;

		case RHENDB_MVCC_HEADER :
			inner_dti_p = catmgr_p->mvcc_header_tuple_def.type_info; break;

		case RHENDB_STRING :
			inner_dti_p = get_text_inline_type_info(attr.size); break;

		case RHENDB_BINARY :
			inner_dti_p = get_blob_inline_type_info(attr.size); break;

		case RHENDB_NUMBER :
			inner_dti_p = get_numeric_inline_type_info(attr.size); break;

		case RHENDB_TEXT :
			inner_dti_p = catmgr_p->catmgr_engine->text_extended_type_info; break;

		case RHENDB_BLOB :
			inner_dti_p = catmgr_p->catmgr_engine->blob_extended_type_info; break;

		case RHENDB_NUMERIC :
			inner_dti_p = catmgr_p->catmgr_engine->numeric_extended_type_info; break;

		case RHENDB_JSONB :
			inner_dti_p = catmgr_p->catmgr_engine->jsonb_extended_type_info; break;

		case RHENDB_COMPOSITE_TYPE :
		{
			// first, a single id_idx lookup for this composite type's own row (must be visible to us) to get its name
			rhendb_type* composite_type = get_catalog_object_by_id(catmgr_p, ss_p, RHENDB_TYPE, attr.attribute_type_id, 0, min_tx_id, abort_error);
			if(*abort_error)
				return NULL;
			if(composite_type == NULL)
			{
				printf("BUG (in catalog_manager) :: mi9ssing type\n");
				exit(-1);
			}

			// materialize the composite type's tuple from the attributes owned by its type id, labelled with its name
			inner_dti_p = get_tuple_data_type_info_from_attributes(catmgr_p, ss_p, composite_type->name, attr.is_nullable, attr.attribute_type_id, 0, min_tx_id, abort_error);
			free(composite_type);
			if(*abort_error)
				return NULL;
			break;
		}
	}
	if(inner_dti_p == NULL)
	{
		printf("BUG (in catalog_manager) :: unmappable attribute type\n");
		exit(-1);
	}

	if(!attr.has_count)
		return inner_dti_p;

	data_type_info* dti_p = malloc(sizeof(data_type_info));
	if(attr.count == 0)
	{
		(*dti_p) = get_variable_element_count_array_type("rhendb_array", catmgr_p->catmgr_engine->pam_p->pas.page_size, inner_dti_p);
	}
	else
	{
		(*dti_p) = get_fixed_element_count_array_type("rhendb_array", attr.count, catmgr_p->catmgr_engine->pam_p->pas.page_size, 1, inner_dti_p); // always nullable
	}
	return dti_p;
}

// build a tuple data_type_info from the attributes owned by owner_id, materializing each attribute's type via
// get_data_type_info_for_rhendb_attribute. for a table pass the partition part_id (else 0). type_name labels the
// outermost tuple. returns NULL only on abort, having freed whatever it built so far.
static data_type_info* get_tuple_data_type_info_from_attributes(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, char* type_name, int is_nullable, uint64_t owner_id, uint64_t part_id, const void* min_tx_id, int* abort_error)
{
	uint64_t attrs_count = 0;
	rhendb_attribute* attrs = get_attributes_for_catalog_object(catmgr_p, ss_p, owner_id, part_id, 0, &attrs_count, min_tx_id, abort_error);
	if(*abort_error)
		return NULL;

	data_type_info* tuple_dti_p = malloc(sizeof_tuple_data_type_info(attrs_count));
	for(uint32_t i = 0; i < attrs_count; i++)
	{
		data_type_info* containee_dti_p = get_data_type_info_for_rhendb_attribute(catmgr_p, ss_p, attrs[i], min_tx_id, abort_error);
		if(*abort_error)
		{
			for(uint32_t j = 0; j < i; j++)
				destroy_type_info_recursively(tuple_dti_p->containees[j].al.type_info, NULL);
			free(tuple_dti_p);
			free(attrs);
			return NULL;
		}
		strncpy(tuple_dti_p->containees[i].field_name, attrs[i].attribute_name, 64);
		tuple_dti_p->containees[i].al.type_info = containee_dti_p;
	}
	initialize_tuple_data_type_info(tuple_dti_p, type_name, is_nullable, catmgr_p->catmgr_engine->pam_p->pas.page_size, attrs_count);
	free(attrs);
	return tuple_dti_p;
}

// tuple data_type_info describing the rows of a table partition, from the attributes valid in (table_id, partition_id).
// a table_partition carries no name of its own, so its row tuple is named after the owning table
static data_type_info* get_data_type_info_for_rhendb_table_partition(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, rhendb_table_partition table_partition, const void* min_tx_id, int* abort_error)
{
	rhendb_table* owning_table = get_catalog_object_by_id(catmgr_p, ss_p, RHENDB_TABLE, table_partition.table_id, 0, min_tx_id, abort_error);
	if(*abort_error)
		return NULL;
	if(owning_table == NULL)
	{
		printf("BUG (in catalog_manager) :: a table partition points at a missing table\n");
		exit(-1);
	}

	data_type_info* partition_dti_p = get_tuple_data_type_info_from_attributes(catmgr_p, ss_p, owning_table->name, 0, table_partition.table_id, table_partition.partition_id, min_tx_id, abort_error);
	free(owning_table);
	return partition_dti_p;
}

// tuple data_type_info describing a type, from the attributes owned by the type, labelled with the type's name
static data_type_info* get_data_type_info_for_rhendb_type(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, rhendb_type type, const void* min_tx_id, int* abort_error)
{
	return get_tuple_data_type_info_from_attributes(catmgr_p, ss_p, type.name, 0, type.id, 0, min_tx_id, abort_error);
}

// tuple data_type_info describing an index key, from the attributes owned by the index, labelled with the index's name
static data_type_info* get_data_type_info_for_rhendb_index(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, rhendb_index index, const void* min_tx_id, int* abort_error)
{
	return get_tuple_data_type_info_from_attributes(catmgr_p, ss_p, index.name, 0, index.id, 0, min_tx_id, abort_error);
}

static int insert_in_catalog_heap_table(catalog_manager* catmgr_p, catalog_heap_table* hpt_p, tuple_pointer* tptr, void const * const * heap_tuples, uint32_t heap_tuples_count, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;
	const tuple_def* record_def = &(hpt_p->record_def);

	// the heap_page we are currently filling, starts as a NULL page i.e. no page held
	persistent_page ppage = get_NULL_persistent_page(engine->pam_p);
	int is_new_page = 0;

	uint32_t inserted_tuples = 0;

	while(inserted_tuples < heap_tuples_count)
	{
		// if we do not hold a page, fetch one with enough unused_space for the next tuple, else allocate a fresh one
		if(is_persistent_page_NULL(&ppage, engine->pam_p))
		{
			uint32_t required_space = get_space_to_be_occupied_by_tuple_on_persistent_page(engine->pam_p->pas.page_size, &(record_def->size_def), heap_tuples[inserted_tuples]);

			is_new_page = 0;
			uint32_t unused_space_in_entry = 0;
			ppage = find_heap_page_with_enough_unused_space_from_heap_table(hpt_p->root_page_id, required_space, &unused_space_in_entry, &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(hpt_p->htan)), &(hpt_p->heap_table_defs), engine->pam_p, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			if(is_persistent_page_NULL(&ppage, engine->pam_p))
			{
				ppage = get_new_heap_page_with_write_lock(&(engine->pam_p->pas), record_def, engine->pam_p, engine->pmm_p, min_tx_id, abort_error);
				if(*abort_error)
					goto ABORT_ERROR;
				is_new_page = 1;
			}
		}

		// fill the held page with as many tuples as possible, until it is full (INVALID_TUPLE_INDEX)
		// possible_insertion_index is carried across the inserts into this page, insert_in_heap_page updates it to the next index to test
		uint32_t inserted_tuples_on_this_page = 0;
		uint32_t possible_insertion_index = 0;
		while(inserted_tuples < heap_tuples_count)
		{
			uint32_t tuple_index = insert_in_heap_page(&ppage, heap_tuples[inserted_tuples], &possible_insertion_index, record_def, &(engine->pam_p->pas), engine->pmm_p, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;

			// the page is full, stop filling it and go fetch another page
			if(tuple_index == INVALID_TUPLE_INDEX)
				break;

			tptr[inserted_tuples] = (tuple_pointer){.page_id = ppage.page_id, .tuple_index = tuple_index};
			inserted_tuples++;
			inserted_tuples_on_this_page++;
		}

		// a page fetched for the next tuple must accept at least that tuple, else it can not fit on any heap_page
		if(inserted_tuples_on_this_page == 0)
		{
			printf("FAILED (in catalog_manager) :: failed to insert a catalog heap table row, tuple too large for a heap page\n");
			exit(-1);
		}

		// a fresh page must be tracked by the heap_table (track reads the page, so it is done before releasing the lock)
		if(is_new_page)
		{
			track_unused_space_in_heap_table(hpt_p->root_page_id, &ppage, &(hpt_p->heap_table_defs), engine->pam_p, engine->pmm_p, min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}

		// release the page (this sets ppage back to a NULL page), then fix the now-stale unused_space entries
		release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		if(!is_new_page)
		{
			fix_unused_space_entries_UNSAFE(catmgr_p, hpt_p->root_page_id, &(hpt_p->htan), &(hpt_p->heap_table_defs), min_tx_id, abort_error);
			if(*abort_error)
				goto ABORT_ERROR;
		}
	}

	return 1;

	ABORT_ERROR:;
	// release the page if it is still held, a released page is already a NULL page, so this never double releases
	if(!is_persistent_page_NULL(&ppage, engine->pam_p))
		release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, abort_error);
	return 0;
}

// --

uint64_t create_table(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, char* name, const rhendb_attribute* attrs, uint32_t attrs_count)
{
	if(attrs_count == 0)
	{
		printf("BUG in (catalog_manager) :: create_table needs a non-zero attrs_count\n");
		exit(-1);
	}

	rage_engine* engine = catmgr_p->catmgr_engine;

	// we can only make persistent changes on behalf of a transaction that has a self transaction id
	if(!ss_p->has_self_transaction_id)
	{
		printf("ISSUE in (catalog_manager) :: create_table needs a snapshot that has a self transaction id\n");
		exit(-1);
	}

	// mvcc_header stamped on every new catalog row, born from our transaction and not yet deleted
	mvcc_header new_row_mvcc_hdr = (mvcc_header){
		.is_xmin_NULL = 0,
		.xmin = {.is_committed = 0, .is_aborted = 0, .transaction_id = ss_p->self_transaction_id},
		.is_xmax_NULL = 1,
	};

	// retry the whole mini transaction for as long as it aborts, we return only on success or a logical failure
	while(1)
	{
		uint64_t table_id = 0; // 0 == failure, real ids start at FIRST_SCHEMA_UNIQUE_ID
		tuple_pointer* attribute_tuple_pointers = NULL;

		int abort_error = 0;
		uint64_t page_latches_to_be_borrowed = 0;
		void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

		// (1) ensure no visible table already has this name, the by-name getter walks the (RHENDB_TABLE, name) run,
		//     checks visibility and fixes hints for us within this same mini transaction, existing_table is a localized
		//     resource, so it is freed here itself before we bail
		{
			rhendb_table* existing_table = get_catalog_object_by_name(catmgr_p, ss_p, RHENDB_TABLE, name, 0, min_tx_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;
			if(existing_table != NULL) // this is not abort, but we found a table with same name
			{
				free(existing_table);
				engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
				return 0;
			}
		}

		// (2) allocate a fresh globally unique id for the new table
		pthread_mutex_lock(&(catmgr_p->global_unique_schema_id_lock));
		if(catmgr_p->global_unique_schema_id == UINT64_MAX)
		{
			printf("ISSUE in (catalog_manager) :: create_table overflowed global_unique_schema_id\n");
			exit(-1);
		}
		table_id = catmgr_p->global_unique_schema_id++;
		pthread_mutex_unlock(&(catmgr_p->global_unique_schema_id_lock));

		// (3) insert the tables_table entry, remember its tuple_pointer for the name and id indexes
		tuple_pointer table_tuple_pointer;
		{
			rhendb_table tbl = (rhendb_table){.id = table_id};
			strncpy(tbl.name, name, 64);
			void* table_tuple = serialize_rhendb_table(catmgr_p, &new_row_mvcc_hdr, &tbl);
			insert_in_catalog_heap_table(catmgr_p, &(catmgr_p->tables_table), &table_tuple_pointer, (void const * const *)(&table_tuple), 1, min_tx_id, &abort_error);
			free(table_tuple);
			if(abort_error)
				goto ABORT_ERROR;
		}

		// (4) create the table's own heap for its rows and a blob store for its large values
		//     get_new_heap_table only lays the generic (unused_space, page_id) root, so any catalog httd works here
		uint64_t heap_root_page_id = get_new_heap_table(&(catmgr_p->tables_table.heap_table_defs), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;
		uint64_t blobs_root_page_id = get_new_blob_store(&(engine->bstd), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		// (5) a single partition entry with partition_id 1, into the clustered table_partitions_table
		{
			rhendb_table_partition tpart = (rhendb_table_partition){.table_id = table_id, .partition_id = 1, .heap_root_page_id = heap_root_page_id, .blobs_root_page_id = blobs_root_page_id};
			void* tpart_tuple = serialize_rhendb_table_partition(catmgr_p, &new_row_mvcc_hdr, &tpart);
			int inserted = insert_in_bplus_tree(catmgr_p->table_partitions_table.root_page_id, tpart_tuple, &(catmgr_p->table_partitions_table.clust_table_defs), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
			free(tpart_tuple);
			if(abort_error)
				goto ABORT_ERROR;
			if(!inserted)
			{
				printf("BUG in (catalog_manager) :: a catalog b+tree insert failed for an already existing key, this must never happen\n");
				exit(-1);
			}
		}

		// (6) all the attributes owned by this table, valid from partition 1 (to = 0, not yet dropped), bulk inserted for locality
		//     attribute_tuples is a localized resource, so it is unwound here itself on abort
		attribute_tuple_pointers = malloc(sizeof(tuple_pointer) * attrs_count);
		{
			void** attribute_tuples = malloc(sizeof(void*) * attrs_count);
			for(uint32_t i = 0; i < attrs_count; i++)
			{
				rhendb_attribute a = attrs[i];
				a.owner_id = table_id;
				a.rel_pos_in_owner = i;
				a.table_part_id_from = 1;
				a.table_part_id_to = 0;
				attribute_tuples[i] = serialize_rhendb_attribute(catmgr_p, &new_row_mvcc_hdr, &a, 1, min_tx_id, &abort_error);
				if(abort_error)
				{
					for(uint32_t j = 0; j < i; j++)
						free(attribute_tuples[j]);
					free(attribute_tuples);
					goto ABORT_ERROR;
				}
			}
			insert_in_catalog_heap_table(catmgr_p, &(catmgr_p->attributes_table), attribute_tuple_pointers, (void const * const *)attribute_tuples, attrs_count, min_tx_id, &abort_error);
			for(uint32_t i = 0; i < attrs_count; i++)
				free(attribute_tuples[i]);
			free(attribute_tuples);
			if(abort_error)
				goto ABORT_ERROR;
		}

		// (7) name_idx and id_idx entries for the new table, pointing at its tables_table row
		{
			rhendb_name_idx_entry name_idx_ent = (rhendb_name_idx_entry){.object_type = RHENDB_TABLE, .object_tuple_pointer = table_tuple_pointer};
			strncpy(name_idx_ent.name, name, 64);
			void* name_key_tuple = serialize_rhendb_name_idx_entry(catmgr_p, &name_idx_ent);
			int inserted = insert_in_bplus_tree(catmgr_p->name_idx.root_page_id, name_key_tuple, &(catmgr_p->name_idx.index_defs), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
			free(name_key_tuple);
			if(abort_error)
				goto ABORT_ERROR;
			if(!inserted)
			{
				printf("BUG in (catalog_manager) :: a catalog b+tree insert failed for an already existing key, this must never happen\n");
				exit(-1);
			}
		}
		{
			rhendb_id_idx_entry id_idx_ent = (rhendb_id_idx_entry){.object_type = RHENDB_TABLE, .id = table_id, .object_tuple_pointer = table_tuple_pointer};
			void* id_key_tuple = serialize_rhendb_id_idx_entry(catmgr_p, &id_idx_ent);
			int inserted = insert_in_bplus_tree(catmgr_p->id_idx.root_page_id, id_key_tuple, &(catmgr_p->id_idx.index_defs), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
			free(id_key_tuple);
			if(abort_error)
				goto ABORT_ERROR;
			if(!inserted)
			{
				printf("BUG in (catalog_manager) :: a catalog b+tree insert failed for an already existing key, this must never happen\n");
				exit(-1);
			}
		}

		// (8) owner_to_attributes_idx entries, one per attribute, pointing at each attribute's row
		for(uint32_t i = 0; i < attrs_count; i++)
		{
			rhendb_owner_to_attributes_idx_entry o2aidx_ent = (rhendb_owner_to_attributes_idx_entry){.owner_id = table_id, .rel_pos_in_owner = i, .attributes_tuple_pointer = attribute_tuple_pointers[i]};
			void* o2aidx_key_tuple = serialize_rhendb_owner_to_attributes_idx_entry(catmgr_p, &o2aidx_ent);
			int inserted = insert_in_bplus_tree(catmgr_p->owner_to_attributes_idx.root_page_id, o2aidx_key_tuple, &(catmgr_p->owner_to_attributes_idx.index_defs), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
			free(o2aidx_key_tuple);
			if(abort_error)
				goto ABORT_ERROR;
			if(!inserted)
			{
				printf("BUG in (catalog_manager) :: a catalog b+tree insert failed for an already existing key, this must never happen\n");
				exit(-1);
			}
		}

		// commit the mini transaction and hand back the new table id
		free(attribute_tuple_pointers);
		engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
		return table_id;

		ABORT_ERROR:;
		if(attribute_tuple_pointers != NULL)
			free(attribute_tuple_pointers);
		engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
	}
}

// build a fresh empty index fragment root for the given index : a b+tree or a hash table by its access method. the fragment
// record is exactly the index's attributes (the tuple_pointer is one of them by design). the key is all the attributes,
// except a hash index drops a trailing tuple_pointer attribute from its key. returns the root page_id, or NULL_PAGE_ID on abort.
static uint64_t create_index_fragment_root_page_id(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, rhendb_index index, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;
	uint64_t fragment_root_page_id = engine->pam_p->pas.NULL_PAGE_ID;
	data_type_info* fragment_record_data_type_info = NULL;
	rhendb_attribute* index_attributes = NULL;
	positional_accessor* key_element_ids = NULL;
	compare_direction* key_compare_directions = NULL;
	uint32_t* key_element_positions = NULL;

	fragment_record_data_type_info = get_data_type_info_for_rhendb_index(catmgr_p, ss_p, index, min_tx_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	// the per attribute compare directions come from the attributes themselves
	uint64_t index_attributes_count = 0;
	index_attributes = get_attributes_for_catalog_object(catmgr_p, ss_p, index.id, 0, 0, &index_attributes_count, min_tx_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	uint32_t attribute_count = fragment_record_data_type_info->element_count;

	// the key is all the attributes, but for a hash index a trailing tuple_pointer attribute is not part of the key
	uint32_t key_element_count = attribute_count;
	if(index.access_methos == RHENDB_HASH && attribute_count > 0 && fragment_record_data_type_info->containees[attribute_count - 1].al.type_info == &(engine->pam_p->pas.tuple_pointer_type_info))
		key_element_count = attribute_count - 1;

	tuple_def fragment_record_def;
	initialize_tuple_def(&fragment_record_def, fragment_record_data_type_info);

	key_element_ids = malloc(sizeof(positional_accessor) * key_element_count);
	key_compare_directions = malloc(sizeof(compare_direction) * key_element_count);
	// STATIC_POSITION builds a positional_accessor pointing at a block scoped temporary, so it cannot be stored,
	// we keep the positions in a persistent array of our own and point the accessors at it
	key_element_positions = malloc(sizeof(uint32_t) * key_element_count);
	for(uint32_t i = 0; i < key_element_count; i++)
	{
		key_element_positions[i] = i;
		key_element_ids[i] = (positional_accessor){.positions_length = 1, .positions = &(key_element_positions[i])};
		key_compare_directions[i] = index_attributes[i].cmp_dir;
	}

	if(index.access_methos == RHENDB_HASH)
	{
		hash_table_tuple_defs fragment_hash_table_defs;
		init_hash_table_tuple_definitions(&fragment_hash_table_defs, &(engine->pam_p->pas), &fragment_record_def, key_element_ids, key_element_count, FNV_64_TUPLE_HASHER);
		hash_table_handle fragment_hash_table = get_new_hash_table(1, &fragment_hash_table_defs, engine->pam_p, engine->pmm_p, min_tx_id, abort_error);
		fragment_root_page_id = fragment_hash_table.root_page_id;
		deinit_hash_table_tuple_definitions(&fragment_hash_table_defs);
	}
	else
	{
		bplus_tree_tuple_defs fragment_bplus_tree_defs;
		init_bplus_tree_tuple_definitions(&fragment_bplus_tree_defs, &(engine->pam_p->pas), &fragment_record_def, key_element_ids, key_compare_directions, key_element_count);
		fragment_root_page_id = get_new_bplus_tree(&fragment_bplus_tree_defs, engine->pam_p, engine->pmm_p, min_tx_id, abort_error);
		deinit_bplus_tree_tuple_definitions(&fragment_bplus_tree_defs);
	}
	if(*abort_error)
		goto ABORT_ERROR;

	// deinit only frees the internally built index_def and key_def, so we own and free the record type and the key arrays
	destroy_type_info_recursively(fragment_record_data_type_info, NULL);
	free(index_attributes);
	free(key_element_ids);
	free(key_compare_directions);
	free(key_element_positions);
	return fragment_root_page_id;

	ABORT_ERROR:;
	if(fragment_record_data_type_info != NULL)
		destroy_type_info_recursively(fragment_record_data_type_info, NULL);
	if(index_attributes != NULL)
		free(index_attributes);
	if(key_element_ids != NULL)
		free(key_element_ids);
	if(key_compare_directions != NULL)
		free(key_compare_directions);
	if(key_element_positions != NULL)
		free(key_element_positions);
	return engine->pam_p->pas.NULL_PAGE_ID;
}

// create a new partition of the table (one past its last partition) with its own heap and blob store, and a fresh index
// fragment for every index of the table, all in this same mini transaction. returns the new partition_id, or 0 on abort.
static uint64_t create_new_partition_and_index_fragments(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t table_id, const mvcc_header* new_row_mvcc_hdr, const void* min_tx_id, int* abort_error)
{
	rage_engine* engine = catmgr_p->catmgr_engine;
	uint64_t new_partition_id = 0;
	rhendb_index* indices = NULL;

	new_partition_id = get_last_partition_id_for_table(catmgr_p, ss_p, table_id, min_tx_id, abort_error) + 1;
	if(*abort_error)
		goto ABORT_ERROR;

	uint64_t heap_root_page_id = get_new_heap_table(&(catmgr_p->tables_table.heap_table_defs), engine->pam_p, engine->pmm_p, min_tx_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;
	uint64_t blobs_root_page_id = get_new_blob_store(&(engine->bstd), engine->pam_p, engine->pmm_p, min_tx_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;
	{
		rhendb_table_partition new_partition = (rhendb_table_partition){.table_id = table_id, .partition_id = new_partition_id, .heap_root_page_id = heap_root_page_id, .blobs_root_page_id = blobs_root_page_id};
		void* partition_tuple = serialize_rhendb_table_partition(catmgr_p, new_row_mvcc_hdr, &new_partition);
		int inserted = insert_in_bplus_tree(catmgr_p->table_partitions_table.root_page_id, partition_tuple, &(catmgr_p->table_partitions_table.clust_table_defs), engine->pam_p, engine->pmm_p, min_tx_id, abort_error);
		free(partition_tuple);
		if(*abort_error)
			goto ABORT_ERROR;
		if(!inserted)
		{
			printf("BUG in (catalog_manager) :: a catalog b+tree insert failed for an already existing key, this must never happen\n");
			exit(-1);
		}
	}

	uint64_t indices_count = 0;
	indices = get_indices_for_table(catmgr_p, ss_p, table_id, 0, &indices_count, min_tx_id, abort_error);
	if(*abort_error)
		goto ABORT_ERROR;

	for(uint64_t i = 0; i < indices_count; i++)
	{
		uint64_t fragment_root_page_id = create_index_fragment_root_page_id(catmgr_p, ss_p, indices[i], min_tx_id, abort_error);
		if(*abort_error)
			goto ABORT_ERROR;

		rhendb_index_fragment new_fragment = (rhendb_index_fragment){.table_id = table_id, .index_id = indices[i].id, .partition_id = new_partition_id, .root_page_id = fragment_root_page_id};
		void* fragment_tuple = serialize_rhendb_index_fragment(catmgr_p, new_row_mvcc_hdr, &new_fragment);
		int inserted = insert_in_bplus_tree(catmgr_p->index_fragments_table.root_page_id, fragment_tuple, &(catmgr_p->index_fragments_table.clust_table_defs), engine->pam_p, engine->pmm_p, min_tx_id, abort_error);
		free(fragment_tuple);
		if(*abort_error)
			goto ABORT_ERROR;
		if(!inserted)
		{
			printf("BUG in (catalog_manager) :: a catalog b+tree insert failed for an already existing key, this must never happen\n");
			exit(-1);
		}
	}

	for(uint64_t i = 0; i < indices_count; i++)
		free(indices[i].predicate_expr);
	free(indices);
	return new_partition_id;

	ABORT_ERROR:;
	if(indices != NULL)
	{
		for(uint64_t i = 0; i < indices_count; i++)
			free(indices[i].predicate_expr);
		free(indices);
	}
	return 0;
}

uint64_t alter_table_add_column(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t table_id, const rhendb_attribute* attr)
{
	rage_engine* engine = catmgr_p->catmgr_engine;

	if(!ss_p->has_self_transaction_id)
	{
		printf("ISSUE in (catalog_manager) :: alter_table_add_column needs a snapshot that has a self transaction id\n");
		exit(-1);
	}

	mvcc_header new_row_mvcc_hdr = (mvcc_header){
		.is_xmin_NULL = 0,
		.xmin = {.is_committed = 0, .is_aborted = 0, .transaction_id = ss_p->self_transaction_id},
		.is_xmax_NULL = 1,
	};

	// retry the whole mini transaction for as long as it aborts, we return only on success or a logical failure
	while(1)
	{
		uint64_t new_partition_id = 0; // returned, 0 == failure
		uint64_t new_rel_pos_in_owner = 0;

		int abort_error = 0;
		uint64_t page_latches_to_be_borrowed = 0;
		void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

		// (1) get the last partition's attributes, reject a duplicate name, and note the next rel_pos to use
		{
			uint64_t last_partition_id = get_last_partition_id_for_table(catmgr_p, ss_p, table_id, min_tx_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			uint64_t last_partition_attrs_count = 0;
			rhendb_attribute* last_partition_attrs = get_attributes_for_catalog_object(catmgr_p, ss_p, table_id, last_partition_id, 0, &last_partition_attrs_count, min_tx_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			int name_already_exists = 0;
			for(uint64_t i = 0; i < last_partition_attrs_count; i++)
			{
				// get_attributes only returns the visible attributes and the new one is not inserted yet, so any name
				// match here is a real duplicate, bail on the first such find
				if(strncmp(last_partition_attrs[i].attribute_name, attr->attribute_name, 64) == 0)
				{
					name_already_exists = 1;
					break;
				}
				if(last_partition_attrs[i].rel_pos_in_owner >= new_rel_pos_in_owner)
					new_rel_pos_in_owner = last_partition_attrs[i].rel_pos_in_owner + 1;
			}
			free(last_partition_attrs);
			if(name_already_exists) // not an abort, but a column of this name already exists
			{
				engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
				return 0;
			}
		}

		// (2) create the new partition, its storage and index fragments, this computes the new partition_id
		new_partition_id = create_new_partition_and_index_fragments(catmgr_p, ss_p, table_id, &new_row_mvcc_hdr, min_tx_id, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		// (3) insert the new attribute, valid from the new partition (to = 0), and its owner_to_attributes entry
		tuple_pointer new_attribute_tuple_pointer;
		{
			rhendb_attribute new_attribute = *attr;
			new_attribute.owner_id = table_id;
			new_attribute.rel_pos_in_owner = new_rel_pos_in_owner;
			new_attribute.table_part_id_from = new_partition_id;
			new_attribute.table_part_id_to = 0;
			void* attribute_tuple = serialize_rhendb_attribute(catmgr_p, &new_row_mvcc_hdr, &new_attribute, 1, min_tx_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;
			insert_in_catalog_heap_table(catmgr_p, &(catmgr_p->attributes_table), &new_attribute_tuple_pointer, (void const * const *)(&attribute_tuple), 1, min_tx_id, &abort_error);
			free(attribute_tuple);
			if(abort_error)
				goto ABORT_ERROR;
		}
		{
			rhendb_owner_to_attributes_idx_entry o2aidx_ent = (rhendb_owner_to_attributes_idx_entry){.owner_id = table_id, .rel_pos_in_owner = new_rel_pos_in_owner, .attributes_tuple_pointer = new_attribute_tuple_pointer};
			void* o2aidx_key_tuple = serialize_rhendb_owner_to_attributes_idx_entry(catmgr_p, &o2aidx_ent);
			int inserted = insert_in_bplus_tree(catmgr_p->owner_to_attributes_idx.root_page_id, o2aidx_key_tuple, &(catmgr_p->owner_to_attributes_idx.index_defs), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
			free(o2aidx_key_tuple);
			if(abort_error)
				goto ABORT_ERROR;
			if(!inserted)
			{
				printf("BUG in (catalog_manager) :: a catalog b+tree insert failed for an already existing key, this must never happen\n");
				exit(-1);
			}
		}

		engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
		return new_partition_id;

		ABORT_ERROR:;
		engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
	}
}

uint64_t alter_table_drop_column(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, uint64_t table_id, uint64_t rel_pos_in_owner_to_drop)
{
	rage_engine* engine = catmgr_p->catmgr_engine;

	if(!ss_p->has_self_transaction_id)
	{
		printf("ISSUE in (catalog_manager) :: alter_table_drop_column needs a snapshot that has a self transaction id\n");
		exit(-1);
	}

	mvcc_header new_row_mvcc_hdr = (mvcc_header){
		.is_xmin_NULL = 0,
		.xmin = {.is_committed = 0, .is_aborted = 0, .transaction_id = ss_p->self_transaction_id},
		.is_xmax_NULL = 1,
	};

	// retry the whole mini transaction for as long as it aborts, we return only on success or a logical failure
	while(1)
	{
		uint64_t new_partition_id = 0; // returned, 0 == failure
		rhendb_attribute* attribute_to_drop = NULL;
		tuple_pointer attribute_to_drop_tuple_pointer;

		int abort_error = 0;
		uint64_t page_latches_to_be_borrowed = 0;
		void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

		// (1) find the visible attribute at this rel_pos and make sure it is not already dropped (to == 0)
		attribute_to_drop = get_attribute_for_owner_at_rel_pos(catmgr_p, ss_p, table_id, rel_pos_in_owner_to_drop, &attribute_to_drop_tuple_pointer, 1, min_tx_id, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;
		if(attribute_to_drop == NULL || attribute_to_drop->table_part_id_to != 0) // not present, not visible, or already dropped
		{
			if(attribute_to_drop != NULL)
			{
				free(attribute_to_drop->derived_from_expr);
				free(attribute_to_drop);
			}
			engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
			return 0;
		}

		// (2) create the new partition, its storage and index fragments, this computes the new partition_id
		new_partition_id = create_new_partition_and_index_fragments(catmgr_p, ss_p, table_id, &new_row_mvcc_hdr, min_tx_id, &abort_error);
		if(abort_error)
			goto ABORT_ERROR;

		// (3) mark the old attribute version deleted by writing a non NULL xmax on its attributes row in place
		{
			persistent_page ppage = acquire_persistent_page_with_lock(engine->pam_p, min_tx_id, attribute_to_drop_tuple_pointer.page_id, WRITE_LOCK, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;

			const void* row = get_nth_tuple_on_persistent_page(&ppage, engine->pam_p->pas.page_size, &(catmgr_p->attributes_table.record_def.size_def), attribute_to_drop_tuple_pointer.tuple_index);
			mvcc_header hdr;
			catalog_read_mvcc_header(catmgr_p, row, &(catmgr_p->attributes_table.record_def), &hdr);
			hdr.is_xmax_NULL = 0;
			hdr.xmax = (transaction_id_with_hints){.is_committed = 0, .is_aborted = 0, .transaction_id = ss_p->self_transaction_id};

			char mvcc_hdr_serialized[get_maximum_tuple_size(&(catmgr_p->mvcc_header_tuple_def))];
			write_mvcc_header(mvcc_hdr_serialized, &(catmgr_p->mvcc_header_tuple_def), &hdr);
			set_element_in_tuple_in_place_on_persistent_page(engine->pmm_p, min_tx_id, &ppage, engine->pam_p->pas.page_size, &(catmgr_p->attributes_table.record_def), attribute_to_drop_tuple_pointer.tuple_index, STATIC_POSITION(0), &((datum){.tuple_value = mvcc_hdr_serialized}), &abort_error);

			release_lock_on_persistent_page(engine->pam_p, min_tx_id, &ppage, NONE_OPTION, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;
		}

		// (4) insert the new attribute version, same fields but now bounded : to = the new partition_id, plus its index entry
		tuple_pointer new_attribute_tuple_pointer;
		{
			rhendb_attribute new_attribute = (*attribute_to_drop);
			new_attribute.table_part_id_to = new_partition_id;
			void* attribute_tuple = serialize_rhendb_attribute(catmgr_p, &new_row_mvcc_hdr, &new_attribute, 1, min_tx_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;
			insert_in_catalog_heap_table(catmgr_p, &(catmgr_p->attributes_table), &new_attribute_tuple_pointer, (void const * const *)(&attribute_tuple), 1, min_tx_id, &abort_error);
			free(attribute_tuple);
			if(abort_error)
				goto ABORT_ERROR;
		}
		{
			rhendb_owner_to_attributes_idx_entry o2aidx_ent = (rhendb_owner_to_attributes_idx_entry){.owner_id = table_id, .rel_pos_in_owner = rel_pos_in_owner_to_drop, .attributes_tuple_pointer = new_attribute_tuple_pointer};
			void* o2aidx_key_tuple = serialize_rhendb_owner_to_attributes_idx_entry(catmgr_p, &o2aidx_ent);
			int inserted = insert_in_bplus_tree(catmgr_p->owner_to_attributes_idx.root_page_id, o2aidx_key_tuple, &(catmgr_p->owner_to_attributes_idx.index_defs), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
			free(o2aidx_key_tuple);
			if(abort_error)
				goto ABORT_ERROR;
			if(!inserted)
			{
				printf("BUG in (catalog_manager) :: a catalog b+tree insert failed for an already existing key, this must never happen\n");
				exit(-1);
			}
		}

		free(attribute_to_drop->derived_from_expr);
		free(attribute_to_drop);
		engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
		return new_partition_id;

		ABORT_ERROR:;
		if(attribute_to_drop != NULL)
		{
			free(attribute_to_drop->derived_from_expr);
			free(attribute_to_drop);
		}
		engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
	}
}


uint64_t create_type(catalog_manager* catmgr_p, const mvcc_snapshot* ss_p, char* name, const rhendb_attribute* attrs, uint32_t attrs_count)
{
	if(attrs_count == 0)
	{
		printf("BUG in (catalog_manager) :: create_type needs a non-zero attrs_count\n");
		exit(-1);
	}

	rage_engine* engine = catmgr_p->catmgr_engine;

	// we can only make persistent changes on behalf of a transaction that has a self transaction id
	if(!ss_p->has_self_transaction_id)
	{
		printf("ISSUE in (catalog_manager) :: create_type needs a snapshot that has a self transaction id\n");
		exit(-1);
	}

	// mvcc_header stamped on every new catalog row, born from our transaction and not yet deleted
	mvcc_header new_row_mvcc_hdr = (mvcc_header){
		.is_xmin_NULL = 0,
		.xmin = {.is_committed = 0, .is_aborted = 0, .transaction_id = ss_p->self_transaction_id},
		.is_xmax_NULL = 1,
	};

	// retry the whole mini transaction for as long as it aborts, we return only on success or a logical failure
	while(1)
	{
		uint64_t type_id = 0; // 0 == failure, real ids start at FIRST_SCHEMA_UNIQUE_ID
		tuple_pointer* attribute_tuple_pointers = NULL;

		int abort_error = 0;
		uint64_t page_latches_to_be_borrowed = 0;
		void* min_tx_id = engine->allot_new_sub_transaction_id(engine->context, page_latches_to_be_borrowed);

		// (1) ensure no visible type already has this name, the by-name getter walks the (RHENDB_TYPE, name) run,
		//     checks visibility and fixes hints for us within this same mini transaction, existing_type is a localized
		//     resource, so it is freed here itself before we bail
		{
			rhendb_type* existing_type = get_catalog_object_by_name(catmgr_p, ss_p, RHENDB_TYPE, name, 0, min_tx_id, &abort_error);
			if(abort_error)
				goto ABORT_ERROR;
			if(existing_type != NULL) // this is not abort, but we found a type with same name
			{
				free(existing_type);
				engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
				return 0;
			}
		}

		// (2) allocate a fresh globally unique id for the new type
		pthread_mutex_lock(&(catmgr_p->global_unique_schema_id_lock));
		if(catmgr_p->global_unique_schema_id == UINT64_MAX)
		{
			printf("ISSUE in (catalog_manager) :: create_type overflowed global_unique_schema_id\n");
			exit(-1);
		}
		type_id = catmgr_p->global_unique_schema_id++;
		pthread_mutex_unlock(&(catmgr_p->global_unique_schema_id_lock));

		// (3) insert the types_table entry, remember its tuple_pointer for the name and id indexes
		tuple_pointer type_tuple_pointer;
		{
			rhendb_type typ = (rhendb_type){.id = type_id};
			strncpy(typ.name, name, 64);
			void* type_tuple = serialize_rhendb_type(catmgr_p, &new_row_mvcc_hdr, &typ);
			insert_in_catalog_heap_table(catmgr_p, &(catmgr_p->types_table), &type_tuple_pointer, (void const * const *)(&type_tuple), 1, min_tx_id, &abort_error);
			free(type_tuple);
			if(abort_error)
				goto ABORT_ERROR;
		}

		// (4) all the attributes owned by this type, a type is not partitioned so from = 0 and to = 0, bulk inserted for locality
		//     attribute_tuples is a localized resource, so it is unwound here itself on abort
		attribute_tuple_pointers = malloc(sizeof(tuple_pointer) * attrs_count);
		{
			void** attribute_tuples = malloc(sizeof(void*) * attrs_count);
			for(uint32_t i = 0; i < attrs_count; i++)
			{
				rhendb_attribute a = attrs[i];
				a.owner_id = type_id;
				a.rel_pos_in_owner = i;
				a.table_part_id_from = 0;
				a.table_part_id_to = 0;
				attribute_tuples[i] = serialize_rhendb_attribute(catmgr_p, &new_row_mvcc_hdr, &a, 1, min_tx_id, &abort_error);
				if(abort_error)
				{
					for(uint32_t j = 0; j < i; j++)
						free(attribute_tuples[j]);
					free(attribute_tuples);
					goto ABORT_ERROR;
				}
			}
			insert_in_catalog_heap_table(catmgr_p, &(catmgr_p->attributes_table), attribute_tuple_pointers, (void const * const *)attribute_tuples, attrs_count, min_tx_id, &abort_error);
			for(uint32_t i = 0; i < attrs_count; i++)
				free(attribute_tuples[i]);
			free(attribute_tuples);
			if(abort_error)
				goto ABORT_ERROR;
		}

		// (5) name_idx and id_idx entries for the new type, pointing at its types_table row
		{
			rhendb_name_idx_entry name_idx_ent = (rhendb_name_idx_entry){.object_type = RHENDB_TYPE, .object_tuple_pointer = type_tuple_pointer};
			strncpy(name_idx_ent.name, name, 64);
			void* name_key_tuple = serialize_rhendb_name_idx_entry(catmgr_p, &name_idx_ent);
			int inserted = insert_in_bplus_tree(catmgr_p->name_idx.root_page_id, name_key_tuple, &(catmgr_p->name_idx.index_defs), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
			free(name_key_tuple);
			if(abort_error)
				goto ABORT_ERROR;
			if(!inserted)
			{
				printf("BUG in (catalog_manager) :: a catalog b+tree insert failed for an already existing key, this must never happen\n");
				exit(-1);
			}
		}
		{
			rhendb_id_idx_entry id_idx_ent = (rhendb_id_idx_entry){.object_type = RHENDB_TYPE, .id = type_id, .object_tuple_pointer = type_tuple_pointer};
			void* id_key_tuple = serialize_rhendb_id_idx_entry(catmgr_p, &id_idx_ent);
			int inserted = insert_in_bplus_tree(catmgr_p->id_idx.root_page_id, id_key_tuple, &(catmgr_p->id_idx.index_defs), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
			free(id_key_tuple);
			if(abort_error)
				goto ABORT_ERROR;
			if(!inserted)
			{
				printf("BUG in (catalog_manager) :: a catalog b+tree insert failed for an already existing key, this must never happen\n");
				exit(-1);
			}
		}

		// (6) owner_to_attributes_idx entries, one per attribute, pointing at each attribute's row
		for(uint32_t i = 0; i < attrs_count; i++)
		{
			rhendb_owner_to_attributes_idx_entry o2aidx_ent = (rhendb_owner_to_attributes_idx_entry){.owner_id = type_id, .rel_pos_in_owner = i, .attributes_tuple_pointer = attribute_tuple_pointers[i]};
			void* o2aidx_key_tuple = serialize_rhendb_owner_to_attributes_idx_entry(catmgr_p, &o2aidx_ent);
			int inserted = insert_in_bplus_tree(catmgr_p->owner_to_attributes_idx.root_page_id, o2aidx_key_tuple, &(catmgr_p->owner_to_attributes_idx.index_defs), engine->pam_p, engine->pmm_p, min_tx_id, &abort_error);
			free(o2aidx_key_tuple);
			if(abort_error)
				goto ABORT_ERROR;
			if(!inserted)
			{
				printf("BUG in (catalog_manager) :: a catalog b+tree insert failed for an already existing key, this must never happen\n");
				exit(-1);
			}
		}

		// commit the mini transaction and hand back the new type id
		free(attribute_tuple_pointers);
		engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
		return type_id;

		ABORT_ERROR:;
		if(attribute_tuple_pointers != NULL)
			free(attribute_tuple_pointers);
		engine->complete_sub_transaction(engine->context, min_tx_id, 1, NULL, 0, &page_latches_to_be_borrowed);
	}
}