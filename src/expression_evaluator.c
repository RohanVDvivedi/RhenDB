/*
	This file only contains callbacks made by SQLtoAST for evaluation and inference of the SQL expression
	It was very time consuming to write this, so it was made to be done by AI
	It must be correct, and leak free because the driver of this context/operators is well tested and lives in SQLtoAST
	ALL-IN-ALL this file has boring but important code, it is written by AI but thoroughly reviewed by a human.
*/

#include<rhendb/expression_evaluator.h>

#include<errno.h>

#include<rhendb/function_compare.h>
#include<rhendb/transaction.h>

#include<tuplelargetypes/common_extended.h>
#include<tuplelargetypes/binary_read_iterator.h>
#include<tuplelargetypes/binary_write_iterator.h>
#include<tuplelargetypes/digit_write_iterator.h>

#include<tupleindexer/blob_store/blob_store.h>
#include<tupleindexer/utils/heap_table_accumulative_notifier.h>

#include<lockking/rwlock.h>

#include<cutlery/cutlery_math.h>

/* Working precision (in significant decimal digits) used to round a NUMERIC division whose exact
 * quotient does not terminate (e.g. 1/3).  Add/sub/mul are always exact and use unbounded precision;
 * only non-terminating division needs a bound.  Kept generous (exceeds decimal128/decimal256 and the
 * DECIMAL maxima of common SQL engines) and value-dependent rather than representation-dependent.
 * Tune to taste for a stricter DECIMAL(p,s) policy. */
#ifndef RHENDB_EE_DIV_PREC
#define RHENDB_EE_DIV_PREC 100
#endif

static const expr_type_info rhendb_bool_type = {
	.type = RHENDB_BIT_FIELD,
	.dti_p = NULL,
	.should_free_dti_p = 0,
};

static const expr_value rhendb_true_bool = {
	.type_info = rhendb_bool_type,
	.value = (datum){.bit_field_value = 1},
};

static const expr_value rhendb_false_bool = {
	.type_info = rhendb_bool_type,
	.value = (datum){.bit_field_value = 0},
};

static const expr_value rhendb_unknown_bool = {
	.type_info = rhendb_bool_type,
	.value = (datum){.bit_field_value = 0},
};

static const expr_type_info rhendb_int_type = {
	.type = RHENDB_INT,
	.dti_p = NULL,
	.should_free_dti_p = 0,
};

static const expr_value rhendb_minus_one_number = {
	.type_info = rhendb_int_type,
	.value = (datum){.int_value = -1},
};

static const expr_value rhendb_zero_number = {
	.type_info = rhendb_int_type,
	.value = (datum){.int_value = 0},
};

static const expr_value rhendb_one_number = {
	.type_info = rhendb_int_type,
	.value = (datum){.int_value = 1},
};

static void* rhendb_get_bool(void* data, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_add(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_sub(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_mul(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_div(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_mod(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

// returns sign of data1 - data2
// uses compare_datum_rhendb() when possible, for simplyfying this function
// for numeric_value, convert them to materialized_numeric and then compare, this allows preserving RhenDB standard of NANs being equal and greater than even positive infinity
static int rhendb_compare(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_left_shift(void* data, void* shift_amt, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_right_shift(void* data, void* shift_amt, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_bit_and(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_bit_or(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_bit_xor(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_bit_not(void* data, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_cast(void* data, const void* to_type, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_create_number(const dstring* data_bytes, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_create_string(const dstring* data_bytes, const sql_expr_eval_context* ec_p, int* error_code);

static void rhendb_concat(void** data1_p, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_like(void* str_p, void* pattern_p, const sql_expr_eval_context* ec_p, int* error_code);

static void rhendb_delete_data(void* data, const sql_expr_eval_context* ec_p)
{
	expr_value* val_p = data;

	if(val_p->type_info.dti_p != NULL && val_p->type_info.should_free_dti_p)
		destroy_type_info_recursively(val_p->type_info.dti_p, NULL);

	if(val_p->type_info.type == RHENDB_NUMERIC)
		mpd_del(&(val_p->numeric_value));
	else
	{
		if(val_p->buffer)
			free(val_p->buffer);
	}

	/* recycle onto the context's free list instead of returning it to malloc (no lock : single thread) */
	rhendb_expr_eval_context* ctx = (ec_p != NULL) ? ec_p->context_p : NULL;
	if(ctx != NULL)
	{
		*((void**)val_p) = ctx->free_list_for_expr_value;   /* push : next ptr goes inside the dead block */
		ctx->free_list_for_expr_value = val_p;
	}
	else
		free(val_p);
}

static int rhendb_can_compare_types(void* typ1, void* typ2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_get_type_for_sql_type(const sql_type* type, const sql_expr_eval_context* ec_p, int* error_code);

static int rhendb_can_cast_types(const void* typ_from, const void* typ_to, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_get_return_type_for_op_exec_callback(void* op_exec_func, void* typ1, void* typ2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_get_type_for_data(void* data, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_unify_types(void* typ1, void* typ2, const sql_expr_eval_context* ec_p, int* error_code);

static void rhendb_delete_type(void* typ, const sql_expr_eval_context* ec_p)
{
	expr_type_info* e_type_p = typ;

	if(e_type_p->dti_p != NULL && e_type_p->should_free_dti_p)
		destroy_type_info_recursively(e_type_p->dti_p, NULL);
	free(e_type_p);
}


/* ------------------------------ helpers ------------------------------ */

static int et_is_int(expr_type t)
{
	return t == RHENDB_BIT_FIELD || t == RHENDB_UINT || t == RHENDB_INT || t == RHENDB_LARGE_UINT || t == RHENDB_LARGE_INT;
}
static int et_is_float(expr_type t)
{
	return t == RHENDB_FLOAT || t == RHENDB_DOUBLE;
}
static int et_is_num(expr_type t)
{
	return et_is_int(t) || et_is_float(t);
}
static int et_is_large(expr_type t)
{
	return t == RHENDB_LARGE_UINT || t == RHENDB_LARGE_INT;
}
static int et_is_signed(expr_type t)
{
	return t == RHENDB_INT || t == RHENDB_LARGE_INT;
}
static int et_is_num_or_numeric(expr_type t)
{
	return et_is_num(t) || t == RHENDB_NUMERIC;
}

/* result type for arithmetic on two operands. NUMERIC is the most general (contagious). */
/* forward declarations : the width helpers and effective_type() are defined further down */
static expr_type_info* new_type_sized(expr_type t, uint32_t width);
static uint32_t combined_width_bytes(const expr_type_info* a, const expr_type_info* b);
static expr_type effective_type(const expr_type_info* ti);

static expr_type num_result(expr_type a, expr_type b)
{
	if(a == RHENDB_NUMERIC || b == RHENDB_NUMERIC)
		return RHENDB_NUMERIC;

	/* float-ness wins. a 4-byte float result is only kept when BOTH sides are 4-byte floats; mixing a
	 * float with an integer (of any width) yields a double, since a 4-byte float cannot represent the
	 * wider integers exactly. */
	if(et_is_float(a) || et_is_float(b))
	{
		if(a == RHENDB_FLOAT && b == RHENDB_FLOAT) return RHENDB_FLOAT;
		return RHENDB_DOUBLE;
	}

	int signd = et_is_signed(a) || et_is_signed(b);

	if(et_is_large(a) || et_is_large(b))
		return signd ? RHENDB_LARGE_INT : RHENDB_LARGE_UINT;

	return signd ? RHENDB_INT : RHENDB_UINT;
}

/* the full width-preserving result of combining two operand TYPES. the result kind follows num_result();
 * the width is the wider of the two operands, expressed in the result kind's own unit. */
static expr_type_info* num_result_sized(const expr_type_info* ta, const expr_type_info* tb)
{
	expr_type a = effective_type(ta);
	expr_type b = (tb != NULL) ? effective_type(tb) : a;
	expr_type res = num_result(a, b);

	if(res == RHENDB_NUMERIC || et_is_float(res))
		return new_type_sized(res, 0);          /* numeric has no width; float width is implied by the kind */

	uint32_t w = combined_width_bytes(ta, tb);  /* 0 -> unspecified -> widest, i.e. the old behaviour */

	if(res == RHENDB_BIT_FIELD)
	{
		/* stays a bit-field only when both operands are bit-fields : take the wider bit count */
		uint32_t ba = (ta && ta->dti_p && ta->dti_p->type==BIT_FIELD) ? ta->dti_p->bit_field_size : 0;
		uint32_t bb = (tb && tb->dti_p && tb->dti_p->type==BIT_FIELD) ? tb->dti_p->bit_field_size : 0;
		uint32_t bits = (ba > bb) ? ba : bb;
		if(ba == 0 || (tb != NULL && bb == 0)) bits = 0;
		return new_type_sized(RHENDB_BIT_FIELD, bits);
	}

	/* a small int combined with a large one keeps the LARGE kind, but only needs the wider byte count */
	return new_type_sized(res, w);
}

/* pop a recycled expr_value off the context's free list, else calloc() a fresh one.
 * the free list is a plain LIFO stack whose "next" pointer lives in the first bytes of the DEAD block,
 * so being on the list costs an expr_value nothing. the block is zeroed before it is handed back, so it
 * is byte-identical to what calloc() would have returned.
 * NO LOCK : a context belongs to exactly one thread (see the header). */
static expr_value* new_val(expr_type t, const sql_expr_eval_context* ec_p)
{
	rhendb_expr_eval_context* ctx = (ec_p != NULL) ? ec_p->context_p : NULL;
	expr_value* v = NULL;

	if(ctx != NULL && ctx->free_list_for_expr_value != NULL)
	{
		v = ctx->free_list_for_expr_value;
		ctx->free_list_for_expr_value = *((void**)v);   /* pop : next ptr is stored inside the dead block */
	}
	else
		v = malloc(sizeof *v);

	if(v == NULL)
		return NULL;

	// DO NOT zero the whole expr_value. it is 88 bytes (the mpd_t in the union makes it fat), and an
	// int node only ever needs a couple of fields -- yet this runs once per AST node per row.
	// only these five are ever READ before the caller writes them:
	//   - type_info.type              : switched on everywhere, and by rhendb_delete_data()
	//   - type_info.dti_p             : rhendb_delete_data() tests it
	//   - type_info.should_free_dti_p : rhendb_delete_data() tests it
	//   - buffer                      : rhendb_delete_data() frees it if non-NULL
	//   - capacity                    : rhendb_concat() reads it alongside buffer
	// the `value` / `numeric_value` union is ALWAYS written by the caller before it is read.
	v->type_info.type = t;
	v->type_info.dti_p = NULL;
	v->type_info.should_free_dti_p = 0;
	v->buffer = NULL;
	v->capacity = 0;
	v->value.is_NULL = 0;      // datum carries a NULL flag -- garbage here makes values vanish

	return v;
}

/* release every expr_value still on the free list.
 * this is the ONLY place the free list gives memory back -- it is uncapped and never shrinks otherwise. */
static void drain_free_list_for_expr_value(rhendb_expr_eval_context* ctx)
{
	while(ctx->free_list_for_expr_value != NULL)
	{
		void* nxt = *((void**)(ctx->free_list_for_expr_value));
		free(ctx->free_list_for_expr_value);
		ctx->free_list_for_expr_value = nxt;
	}
}
static expr_type_info* new_type(expr_type t)
{
	expr_type_info* ti = calloc(1, sizeof *ti);
	ti->type = t;
	return ti;
}

/* ===================================================================================================
 * WIDTH-PRESERVING NATIVE TYPES
 *
 * a native scalar type may carry a dti_p that records its declared width, so that combining values does
 * not immediately widen everything to the maximum (8-byte int / 32-byte large / 64-bit bit-field).
 * uint(3 bytes) + uint(4 bytes) is a uint of 4 bytes, not of 8.
 *
 * every dti_p used here is either one of the static defaults or a type owned by an input tuple, so it is
 * never freed (should_free_dti_p stays 0) and can never leak.
 *
 * dti_p == NULL simply means "width unspecified" -- literals and booleans leave it NULL -- and is treated
 * as the widest form of that type, which is exactly the old behaviour.
 * =================================================================================================== */

/* declared width of a native type, in BITS for a bit-field and in BYTES otherwise; 0 when unspecified */
static uint32_t native_width(const expr_type_info* ti)
{
	if(ti == NULL || ti->dti_p == NULL) return 0;
	const data_type_info* d = ti->dti_p;
	switch(d->type)
	{
		case BIT_FIELD:  return d->bit_field_size;
		case UINT: case INT: case LARGE_UINT: case LARGE_INT: case FLOAT: return d->size;
		default: return 0;
	}
}

/* bit-field bits -> whole bytes, so a bit-field can be width-compared against the integer types */
static uint32_t bits_to_bytes(uint32_t bits){ return (bits + 7u) / 8u; }

/* the static default data_type_info of a given native type and width. width 0 (unspecified) yields the
 * widest form, matching the previous behaviour. always static, hence never freed. */
static data_type_info* static_dti_for(expr_type t, uint32_t width)
{
	switch(t)
	{
		case RHENDB_BIT_FIELD:
			if(width == 0 || width > 64) width = 64;
			return BIT_FIELD_NULLABLE[width];
		case RHENDB_UINT:
			if(width == 0 || width > 8) width = 8;
			return UINT_NULLABLE[width];
		case RHENDB_INT:
			if(width == 0 || width > 8) width = 8;
			return INT_NULLABLE[width];
		case RHENDB_LARGE_UINT:
			if(width == 0 || width > 32) width = 32;
			return LARGE_UINT_NULLABLE[width];
		case RHENDB_LARGE_INT:
			if(width == 0 || width > 32) width = 32;
			return LARGE_INT_NULLABLE[width];
		case RHENDB_FLOAT:  return FLOAT_float_NULLABLE;
		case RHENDB_DOUBLE: return FLOAT_double_NULLABLE;
		default: return NULL;
	}
}

/* a new type object carrying a width (via a borrowed static dti) */
static expr_type_info* new_type_sized(expr_type t, uint32_t width)
{
	expr_type_info* ti = new_type(t);
	ti->dti_p = static_dti_for(t, width);      /* static : never freed */
	ti->should_free_dti_p = 0;
	return ti;
}

/* a new type object that borrows an existing (static or tuple-owned) dti verbatim */
static expr_type_info* new_type_borrowing(expr_type t, data_type_info* dti)
{
	expr_type_info* ti = new_type(t);
	ti->dti_p = dti;
	ti->should_free_dti_p = 0;
	return ti;
}

/* width of a native type expressed in BYTES (bit-fields rounded up), 0 when unspecified */
static uint32_t native_width_bytes(const expr_type_info* ti)
{
	if(ti == NULL || ti->dti_p == NULL) return 0;
	if(ti->dti_p->type == BIT_FIELD) return bits_to_bytes(ti->dti_p->bit_field_size);
	return native_width(ti);
}

/* the resulting width for combining two native operands : the wider of the two, and unspecified as soon
 * as either side is unspecified (an unspecified operand may be arbitrarily wide) */
static uint32_t combined_width_bytes(const expr_type_info* a, const expr_type_info* b)
{
	uint32_t wa = native_width_bytes(a), wb = native_width_bytes(b);
	if(wa == 0 || wb == 0) return 0;
	return (wa > wb) ? wa : wb;
}

/* discriminators */
static int is_materialized_numeric(const expr_value* v)
{
	return v->type_info.type == RHENDB_NUMERIC;
}
static int is_tuple_numeric(const expr_value* v)
{
	return v->type_info.type == RHENDB_TUPLE && v->type_info.dti_p != NULL && is_numeric_type_info(v->type_info.dti_p);
}
static int is_numeric_operand(const expr_value* v)
{
	return is_materialized_numeric(v) || is_tuple_numeric(v);
}

/* is this value still in its on-disk container form (an extended text/blob/numeric, or a tuple/array)?
 *
 * this used to be written as simply "dti_p != NULL". native scalars now carry a dti_p as well -- purely to
 * record their declared width -- so the discriminator has to be the expr_type. RHENDB_TUPLE / RHENDB_ARRAY
 * are the only kinds ever used for a value that is not already a plain in-memory scalar. */
static int is_tuple_form(const expr_value* v)
{
	return v->type_info.type == RHENDB_TUPLE || v->type_info.type == RHENDB_ARRAY;
}

/* a string/binary operand: a native RHENDB_STRING/BINARY, or a tuple-form text/blob column */
static int is_sb_operand(const expr_value* v)
{
	if(is_tuple_form(v) && v->type_info.dti_p != NULL)
		return is_text_type_info(v->type_info.dti_p) || is_blob_type_info(v->type_info.dti_p);
	return v->type_info.type == RHENDB_STRING || v->type_info.type == RHENDB_BINARY;
}

/* the two float kinds are genuinely distinct in storage:
 *   RHENDB_FLOAT  -> datum.float_value  (4 bytes, dti FLOAT_float_NULLABLE)
 *   RHENDB_DOUBLE -> datum.double_value (8 bytes, dti FLOAT_double_NULLABLE)
 * read/write them only through these two helpers so no site can pick the wrong member. */
static double read_flt(const expr_value* v)
{
	return (v->type_info.type == RHENDB_FLOAT) ? (double)v->value.float_value : v->value.double_value;
}
static void write_flt(expr_value* v, expr_type kind, double d)
{
	if(kind == RHENDB_FLOAT) v->value.float_value = (float)d;
	else                     v->value.double_value = d;
}

static double to_dbl(const expr_value* v){
	switch(v->type_info.type){
		case RHENDB_BIT_FIELD:
		case RHENDB_UINT:
			return (double)v->value.uint_value;
		case RHENDB_INT:
			return (double)v->value.int_value;
		case RHENDB_FLOAT:
		case RHENDB_DOUBLE:
			return read_flt(v);
		case RHENDB_LARGE_UINT:
			return convert_to_double_uint256(v->value.large_uint_value);
		case RHENDB_LARGE_INT:
			return convert_to_double_int256(v->value.large_int_value);
		default:
			return 0;
	}
}
static uint64_t to_u64(const expr_value* v)
{
	if(et_is_float(v->type_info.type)) return (uint64_t)read_flt(v);   /* by value, never a bit reinterpret */
	return (v->type_info.type == RHENDB_INT) ? (uint64_t)v->value.int_value : v->value.uint_value;
}
static int64_t to_i64(const expr_value* v)
{
	if(et_is_float(v->type_info.type)) return (int64_t)read_flt(v);    /* by value, never a bit reinterpret */
	return (v->type_info.type == RHENDB_INT) ? v->value.int_value : (int64_t)v->value.uint_value;
}
static uint256  to_u256(const expr_value* v)
{
	switch(v->type_info.type){
		case RHENDB_BIT_FIELD:
		case RHENDB_UINT:
			return get_uint256(v->value.uint_value);
		case RHENDB_INT:
			return get_uint256((uint64_t)v->value.int_value);
		case RHENDB_LARGE_UINT:
			return v->value.large_uint_value;
		case RHENDB_LARGE_INT:
			return v->value.large_int_value.raw_uint_value;
		case RHENDB_FLOAT: case RHENDB_DOUBLE:
			return get_uint256((uint64_t)read_flt(v));   /* by value, from the member the kind uses */
		default:
			return get_uint256(0);
	}
}
static int256 to_i256(const expr_value* v)
{
	switch(v->type_info.type){
		case RHENDB_BIT_FIELD:
		case RHENDB_UINT:
			return (int256){ get_uint256(v->value.uint_value) };
		case RHENDB_INT:
			return get_int256(v->value.int_value);
		case RHENDB_LARGE_UINT:
			return (int256){ v->value.large_uint_value };
		case RHENDB_LARGE_INT:
			return v->value.large_int_value;
		case RHENDB_FLOAT: case RHENDB_DOUBLE:
			return get_int256((int64_t)read_flt(v));     /* by value, from the member the kind uses */
		default:
			return get_int256(0);
	}
}

/* the transaction backing this evaluation context (multi-blob-store refactor). */
static transaction* tx_from_ctx(const sql_expr_eval_context* ec_p)
{
	return ((rhendb_expr_eval_context*)(ec_p->context_p))->tx;
}

/* the engine + lock callback used to read one extended text/blob/numeric COLUMN out of a tuple.
 * The correct engine (persistent vs volatile temporary store) and, for the volatile stores, the
 * begin/end lock callback are chosen from the transaction by the column's extension sub-type.
 * `dti` is the column's own data_type_info; `cb_storage` supplies backing storage for the callback.
 *   returns the engine (NULL if not extended / no tx / unknown sub-type); *callback_out is the
 *   pass-through callback (NULL for the persistent store and for inline types). */
static rage_engine* engine_and_callback_from_ctx(const sql_expr_eval_context* ec_p, const data_type_info* dti,
	extension_reader_iterator_callback* cb_storage, extension_reader_iterator_callback** callback_out)
{
	transaction* tx = tx_from_ctx(ec_p);
	if(tx == NULL)
	{
		(*callback_out) = NULL;
		return NULL;
	}
	rage_engine* eng = NULL;
	(*callback_out) = get_callback_and_engine_for_extended_type(tx, dti, &eng, cb_storage);
	return eng;
}

/* True only if `dti` is an EXTENDED (blob-backed) text/blob/numeric type, i.e. one that actually needs an
 * engine + (for volatile) a lock callback to be read. An INLINE text/blob/numeric column is read through
 * the same iterator APIs but with bstd = pam_p = callback = NULL, so a NULL engine is NOT an error for it.
 * Only an extended type with no resolvable engine (e.g. no transaction, or an unknown sub-type) is. */
static int dti_needs_engine(const data_type_info* dti)
{
	return dti != NULL && is_extended_type_info(dti);
}

/* initialize a fresh, empty, usable mpd_t whose struct is static (kept inline in an expr_value)
 * and whose coefficient buffer is heap allocated -> release with mpd_del(). */
static int ee_mpd_new(mpd_t* m)
{
	m->flags = MPD_STATIC;
	m->exp = 0;
	m->digits = 0;
	m->len = 0;
	m->alloc = MPD_MINALLOC;
	m->data = mpd_alloc(MPD_MINALLOC, sizeof(mpd_uint_t));
	return m->data != NULL;
}

/* ---- text/blob materialization : ONLY used by concat and like ---- */
static int materialize_tb(expr_value* v, const sql_expr_eval_context* ec_p, int* error_code)
{
	const data_type_info* dti = v->type_info.dti_p;
	if(dti == NULL)
		return RHENDB_EE_OK;                    /* already native */
	int is_txt = is_text_type_info(dti);
	int is_bin = is_blob_type_info(dti);
	if(!is_txt && !is_bin)
		return RHENDB_EE_OK;             /* not text/blob */

	expr_type target = is_txt ? RHENDB_STRING : RHENDB_BINARY;

	extension_reader_iterator_callback cb_storage;
	extension_reader_iterator_callback* callback = NULL;
	rage_engine* eng = engine_and_callback_from_ctx(ec_p, dti, &cb_storage, &callback);
	/* inline text/blob is read with bstd = pam_p = callback = NULL; only an EXTENDED type must have an
	 * engine. so a NULL engine is an error only when the type actually needs one. */
	if(eng == NULL && dti_needs_engine(dti))
	{
		*error_code = RHENDB_EE_MISSING_ENGINE;
		return *error_code;
	}

	const void* transaction_id = NULL;
	int abort_error = 0;

	binary_read_iterator* bri = get_new_binary_read_iterator(&(v->value), dti, eng ? &(eng->bstd) : NULL, eng ? eng->pam_p : NULL, callback);
	if(bri == NULL)
	{
		*error_code = RHENDB_EE_MATERIALIZE_FAILED;
		return *error_code;
	}

	/* string_or_binary_size is a uint32_t, so a materialized text/blob can hold at most UINT32_MAX bytes.
	 * an EXTENDED value may stream more than that across many reads, so the buffer is grown with a strict
	 * uint32_t-safe doubling (cap*2 would itself overflow past 2 GiB) that saturates at UINT32_MAX, and the
	 * moment the source is found to exceed UINT32_MAX bytes materialization fails with RHENDB_EE_STRING_TOO_LONG
	 * instead of silently wrapping len / cap and corrupting the heap. */
	uint32_t cap = 64, len = 0;
	char* buf = malloc(cap);
	if(buf == NULL)
	{
		delete_binary_read_iterator(bri, transaction_id, &abort_error);
		*error_code = RHENDB_EE_OUT_OF_MEMORY;
		return *error_code;
	}
	while(1)
	{
		if(len == cap)
		{
			if(cap == UINT32_MAX)
			{
				/* the buffer already spans the whole representable size and the last read filled it, so the
				 * source is not necessarily exhausted. one probing byte distinguishes a value that is exactly
				 * UINT32_MAX bytes (representable) from one that is larger (which cannot fit the size field). */
				char probe;
				uint32_t extra = read_from_binary_read_iterator(bri, &probe, 1, transaction_id, &abort_error);
				if(abort_error)
				{
					free(buf);
					delete_binary_read_iterator(bri, transaction_id, &abort_error);
					*error_code = RHENDB_EE_MATERIALIZE_FAILED;
					return *error_code;
				}
				if(extra == 0)
					break;                          /* exactly UINT32_MAX bytes : representable, done */
				free(buf);                              /* strictly larger than UINT32_MAX bytes : refuse */
				delete_binary_read_iterator(bri, transaction_id, &abort_error);
				*error_code = RHENDB_EE_STRING_TOO_LONG;
				return *error_code;
			}
			uint32_t nc = (cap <= UINT32_MAX / 2) ? (cap * 2) : UINT32_MAX; /* double, saturating at UINT32_MAX */
			char* nb = realloc(buf, nc);
			if(nb == NULL)
			{
				free(buf);
				delete_binary_read_iterator(bri, transaction_id, &abort_error);
				*error_code = RHENDB_EE_OUT_OF_MEMORY;
				return *error_code;
			}
			buf = nb;
			cap = nc;
		}
		uint32_t got = read_from_binary_read_iterator(bri, buf + len, cap - len, transaction_id, &abort_error);
		if(abort_error)
		{
			free(buf);
			delete_binary_read_iterator(bri, transaction_id, &abort_error);
			*error_code = RHENDB_EE_MATERIALIZE_FAILED;
			return *error_code;
		}
		if(got == 0)
			break;
		len += got;   /* safe : got <= cap - len, so len stays <= cap <= UINT32_MAX */
	}
	delete_binary_read_iterator(bri, transaction_id, &abort_error);
	if(abort_error)
	{
		free(buf);
		*error_code = RHENDB_EE_MATERIALIZE_FAILED;
		return *error_code;
	}

	if(v->type_info.should_free_dti_p && v->type_info.dti_p)
		destroy_type_info_recursively(v->type_info.dti_p, NULL);
	if(v->buffer)
		free(v->buffer);
	v->buffer = buf;
	v->capacity = len;
	v->value = (datum){.string_or_binary_value = buf, .string_or_binary_size = len};
	v->type_info = (expr_type_info){.type = target, .dti_p = NULL, .should_free_dti_p = 0};
	return RHENDB_EE_OK;
}

/* ---- numeric materialization : tuple numeric -> materialized_numeric -> mpd_t (RHENDB_NUMERIC) ---- */
static int materialize_numeric(expr_value* v, const sql_expr_eval_context* ec_p, int* error_code)
{
	if(!is_tuple_form(v))
		return RHENDB_EE_OK;                 /* already native */
	if(!is_numeric_type_info(v->type_info.dti_p))
		return RHENDB_EE_OK;                 /* not a numeric */

	extension_reader_iterator_callback cb_storage;
	extension_reader_iterator_callback* callback = NULL;
	rage_engine* eng = engine_and_callback_from_ctx(ec_p, v->type_info.dti_p, &cb_storage, &callback);
	/* inline numeric is read with bstd = pam_p = callback = NULL; only an EXTENDED numeric needs an engine. */
	if(eng == NULL && dti_needs_engine(v->type_info.dti_p))
	{
		*error_code = RHENDB_EE_MISSING_ENGINE;
		return *error_code;
	}

	const void* transaction_id = NULL;
	int abort_error = 0;
	numeric_reader_interface nri = init_intuple_numeric_reader_interface(v->value, v->type_info.dti_p, eng ? &(eng->bstd) : NULL, eng ? eng->pam_p : NULL, callback, transaction_id, &abort_error);
	if(abort_error)
	{
		*error_code = RHENDB_EE_MATERIALIZE_FAILED;
		return *error_code;
	}

	numeric_sign_bits sb; int16_t exp;
	nri.extract_sign_bits_and_exponent(&nri, &sb, &exp);

	materialized_numeric mn;
	if(!initialize_materialized_numeric(&mn, 8))
	{
		nri.close_digits_stream(&nri);
		*error_code = RHENDB_EE_OUT_OF_MEMORY;
		return *error_code;
	}
	set_sign_bits_and_exponent_for_materialized_numeric(&mn, sb, exp);

	if(sb == POSITIVE_NUMERIC || sb == NEGATIVE_NUMERIC)   /* only finite non-zero values carry digits */
	{
		uint64_t buf[64];
		while(1)
		{
			int err = 0;
			uint32_t got = nri.read_digits_as_stream(&nri, buf, 64, &err);
			if(err)
			{
				nri.close_digits_stream(&nri);
				deinitialize_materialized_numeric(&mn);
				*error_code = RHENDB_EE_MATERIALIZE_FAILED;
				return *error_code;
			}
			if(got == 0)
				break;
			/* digits stream MSD-first; push_lsd appends, keeping the MSD at the front */
			for(uint32_t i = 0; i < got; i++)
				push_lsd_in_materialized_numeric(&mn, buf[i]);
		}
	}
	nri.close_digits_stream(&nri);

	mpd_t d = decimal_from_materialized_numeric(&mn);   /* inline mpd_t with heap coefficient, released via mpd_del */
	deinitialize_materialized_numeric(&mn);

	if(v->type_info.should_free_dti_p && v->type_info.dti_p)
		destroy_type_info_recursively(v->type_info.dti_p, NULL);
	if(v->buffer)
	{
		free(v->buffer);
		v->buffer = NULL;
		v->capacity = 0;
	}
	v->type_info = (expr_type_info){ .type = RHENDB_NUMERIC, .dti_p = NULL, .should_free_dti_p = 0};
	v->numeric_value = d;                                /* overwrites .value via the union */
	return RHENDB_EE_OK;
}

/* build an mpd_t from a native number. integers up to 64 bits are exact; 256-bit and floating
 * operands go through the decimal string of a double, so they are only double-precise. */
static int number_to_mpd(const expr_value* v, mpd_t* out)
{
	if(!ee_mpd_new(out))
		return 0;
	mpd_context_t ctx;
	get_mpd_context_for_materialized_numeric(&ctx);
	uint32_t st = 0;
	char b[256];
	switch(v->type_info.type){
		case RHENDB_BIT_FIELD:
		case RHENDB_UINT:
			mpd_qset_u64(out, v->value.uint_value, &ctx, &st); break;
		case RHENDB_INT:
			mpd_qset_i64(out, v->value.int_value, &ctx, &st); break;
		case RHENDB_FLOAT:
		case RHENDB_DOUBLE:
			snprintf(b, sizeof(b), "%.17g", read_flt(v));
			mpd_qset_string(out, b, &ctx, &st);
			break;
		case RHENDB_LARGE_UINT:
			b[serialize_to_decimal_uint256(b, v->value.large_uint_value)] = '\0';
			mpd_qset_string(out, b, &ctx, &st);
			break;
		case RHENDB_LARGE_INT:
			b[serialize_to_decimal_int256(b, v->value.large_int_value)] = '\0';
			mpd_qset_string(out, b, &ctx, &st);
			break;
		default:
			mpd_del(out);
			return 0;
	}
	return 1;
}

/* Resolve operand v to an mpd_t to use.
 *  - a native RHENDB_NUMERIC borrows its own mpd_t (owns=0),
 *  - a tuple-form numeric is materialized in place then borrowed (owns=0),
 *  - a native number is converted into *scratch (owns=1),
 *  - anything else is an error (err_for_bad). */
static mpd_t* operand_to_mpd(expr_value* v, mpd_t* scratch, int* owns, const sql_expr_eval_context* ec_p, int* error_code, int err_for_bad)
{
	*owns = 0;
	if(is_materialized_numeric(v))
		return &(v->numeric_value);
	if(is_tuple_numeric(v))
	{
		if(materialize_numeric(v, ec_p, error_code))
			return NULL;
		return &(v->numeric_value);
	}
	if(et_is_num(v->type_info.type) && !is_tuple_form(v))
	{
		if(!number_to_mpd(v, scratch))
		{
			*error_code = RHENDB_EE_OUT_OF_MEMORY;
			return NULL;
		}
		*owns = 1;
		return scratch;
	}
	*error_code = err_for_bad;
	return NULL;
}

/* ------------------------------ arithmetic ------------------------------ */

typedef enum {
	OP_ADD,
	OP_SUB,
	OP_MUL,
	OP_DIV,
	OP_MOD
} arith_op;

static void* do_arith(void* d1, void* d2, arith_op op, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* a = d1; expr_value* b = d2;

	/* NUMERIC path: either operand is a numeric (tuple-form or already an mpd_t) */
	if(is_numeric_operand(a) || is_numeric_operand(b))
	{
		mpd_t sa, sb; int oa = 0, ob = 0;
		mpd_t* pa = operand_to_mpd(a, &sa, &oa, ec_p, error_code, RHENDB_EE_NON_NUMERIC_OPERAND);
		if(pa == NULL) return NULL;
		mpd_t* pb = operand_to_mpd(b, &sb, &ob, ec_p, error_code, RHENDB_EE_NON_NUMERIC_OPERAND);
		if(pb == NULL){ if(oa) mpd_del(&sa); return NULL; }

		if((op == OP_DIV || op == OP_MOD) && mpd_iszero(pb))
		{
			if(oa)
				mpd_del(&sa);
			if(ob)
				mpd_del(&sb);
			*error_code = RHENDB_EE_DIVIDE_BY_ZERO;
			return NULL;
		}

		mpd_context_t ctx; get_mpd_context_for_materialized_numeric(&ctx); uint32_t st = 0;
		mpd_t result;
		if(!ee_mpd_new(&result)){ if(oa) mpd_del(&sa); if(ob) mpd_del(&sb); *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
		switch(op){
			case OP_ADD: mpd_qadd(&result, pa, pb, &ctx, &st); break;
			case OP_SUB: mpd_qsub(&result, pa, pb, &ctx, &st); break;
			case OP_MUL: mpd_qmul(&result, pa, pb, &ctx, &st); break;
			case OP_DIV:
				mpd_qdiv(&result, pa, pb, &ctx, &st);
				/* maxcontext (prec ~ MPD_MAX_PREC) attempts an EXACT quotient; a non-terminating
				 * division would need unbounded digits and fails (NaN, Malloc_error/Division_impossible).
				 * Fall back to a bounded precision so the quotient rounds to a finite value, the way a
				 * real DECIMAL engine does, instead of silently yielding NaN. */
				if(mpd_isnan(&result) && !mpd_isnan(pa) && !mpd_isnan(pb))
				{
					mpd_context_t dctx = ctx;
					dctx.prec = RHENDB_EE_DIV_PREC;   /* fixed working precision (value-, not representation-, dependent) */
					st = 0;
					mpd_qdiv(&result, pa, pb, &dctx, &st);
				}
				break;
			case OP_MOD: mpd_qrem(&result, pa, pb, &ctx, &st); break;
		}
		if(oa)
			mpd_del(&sa);
		if(ob)
			mpd_del(&sb);

		expr_value* v = new_val(RHENDB_NUMERIC, ec_p);
		v->numeric_value = result;
		return v;
	}

	/* non-numeric operands must be plain in-memory numbers */
	expr_type ta = a->type_info.type, tb = b->type_info.type;
	if(is_tuple_form(a) || is_tuple_form(b) || !et_is_num(ta) || !et_is_num(tb))
	{
		*error_code = RHENDB_EE_NON_NUMERIC_OPERAND;
		return NULL;
	}
	expr_type rt = num_result(ta, tb);

	if(et_is_float(rt)){
		double x = to_dbl(a), y = to_dbl(b), r = 0;
		switch(op){
			case OP_ADD: r = x + y; break;
			case OP_SUB: r = x - y; break;
			case OP_MUL: r = x * y; break;
			case OP_DIV:
				if(y == 0){ *error_code = RHENDB_EE_DIVIDE_BY_ZERO; return NULL; }
				r = x / y;
				break;
			case OP_MOD:
				if(y == 0){ *error_code = RHENDB_EE_DIVIDE_BY_ZERO; return NULL; }
				r = fmod(x, y);
				break;
		}
		expr_value* v = new_val(rt, ec_p);
		/* a FLOAT result is a genuine 4-byte float : rounding happens here, not only at storage time */
		write_flt(v, rt, r);
		return v;
	}
	if(rt == RHENDB_LARGE_INT){
		int256 x = to_i256(a), y = to_i256(b), r;
		switch(op){
			case OP_ADD: add_int256(&r, x, y); break;
			case OP_SUB: sub_int256(&r, x, y); break;
			case OP_MUL: mul_int256(&r, x, y); break;
			case OP_DIV:
				if(is_zero_int256(y)){ *error_code = RHENDB_EE_DIVIDE_BY_ZERO; return NULL; }
				div_int256(&r, x, y);
				break;
			case OP_MOD:
				if(is_zero_int256(y)){ *error_code = RHENDB_EE_DIVIDE_BY_ZERO; return NULL; }
				{ int256 q; r = div_int256(&q, x, y);}
				break;
		}
		expr_value* v = new_val(RHENDB_LARGE_INT, ec_p);
		v->value.large_int_value = r;
		return v;
	}
	if(rt == RHENDB_LARGE_UINT){
		uint256 x = to_u256(a), y = to_u256(b), r;
		switch(op){
			case OP_ADD:
				add_uint256(&r, x, y); break;
			case OP_SUB:
				sub_uint256(&r, x, y); break;
			case OP_MUL:
				mul_uint256(&r, x, y); break;
			case OP_DIV:
				if(is_zero_uint256(y)){ *error_code = RHENDB_EE_DIVIDE_BY_ZERO; return NULL; }
				div_uint256(&r, x, y);
				break;
			case OP_MOD:
				if(is_zero_uint256(y)){ *error_code = RHENDB_EE_DIVIDE_BY_ZERO; return NULL; }
				{ uint256 q; r = div_uint256(&q, x, y);}
				break;
		}
		expr_value* v = new_val(RHENDB_LARGE_UINT, ec_p);
		v->value.large_uint_value = r;
		return v;
	}
	if(rt == RHENDB_INT)
	{
		int64_t x = to_i64(a), y = to_i64(b), r = 0;
		switch(op){
			case OP_ADD:
				r = x + y; break;
			case OP_SUB:
				r = x - y; break;
			case OP_MUL:
				r = x * y; break;
			case OP_DIV:
				if(y == 0){ *error_code = RHENDB_EE_DIVIDE_BY_ZERO; return NULL; }
				r = (x == INT64_MIN && y == -1) ? INT64_MIN : (x / y);   /* guard 2's-complement overflow (INT64_MIN/-1 is UB -> SIGFPE) */
				break;
			case OP_MOD:
				if(y == 0){ *error_code = RHENDB_EE_DIVIDE_BY_ZERO; return NULL; }
				r = (x == INT64_MIN && y == -1) ? 0 : (x % y);           /* INT64_MIN % -1 is UB in C */
				break;
		}
		expr_value* v = new_val(RHENDB_INT, ec_p);
		v->value.int_value = r;
		return v;
	}
	{ /* RHENDB_UINT */
		uint64_t x = to_u64(a), y = to_u64(b), r = 0;
		switch(op){
			case OP_ADD:
				r = x + y; break;
			case OP_SUB:
				r = x - y; break;
			case OP_MUL:
				r = x * y; break;
			case OP_DIV:
				if(y == 0){ *error_code = RHENDB_EE_DIVIDE_BY_ZERO; return NULL; }
				r = x / y;
				break;
			case OP_MOD:
				if(y == 0){ *error_code = RHENDB_EE_DIVIDE_BY_ZERO; return NULL; }
				r = x % y;
				break;
		}
		expr_value* v = new_val(RHENDB_UINT, ec_p);
		v->value.uint_value = r;
		return v;
	}
}
static void* rhendb_add(void* d1, void* d2, const sql_expr_eval_context* ec_p, int* e){ return do_arith(d1, d2, OP_ADD, ec_p, e); }
static void* rhendb_sub(void* d1, void* d2, const sql_expr_eval_context* ec_p, int* e){ return do_arith(d1, d2, OP_SUB, ec_p, e); }
static void* rhendb_mul(void* d1, void* d2, const sql_expr_eval_context* ec_p, int* e){ return do_arith(d1, d2, OP_MUL, ec_p, e); }
static void* rhendb_div(void* d1, void* d2, const sql_expr_eval_context* ec_p, int* e){ return do_arith(d1, d2, OP_DIV, ec_p, e); }
static void* rhendb_mod(void* d1, void* d2, const sql_expr_eval_context* ec_p, int* e){ return do_arith(d1, d2, OP_MOD, ec_p, e); }

/* ------------------------------ truthiness ------------------------------ */

static int tuple_tb_is_empty(expr_value* v, const sql_expr_eval_context* ec_p, int* error_code)
{
	extension_reader_iterator_callback cb_storage;
	extension_reader_iterator_callback* callback = NULL;
	rage_engine* eng = engine_and_callback_from_ctx(ec_p, v->type_info.dti_p, &cb_storage, &callback);
	/* inline text/blob is read with bstd = pam_p = callback = NULL; only an EXTENDED type needs an engine. */
	if(eng == NULL && dti_needs_engine(v->type_info.dti_p))
	{
		*error_code = RHENDB_EE_MISSING_ENGINE;
		return 0;
	}
	const void* transaction_id = NULL; int abort_error = 0;
	binary_read_iterator* bri = get_new_binary_read_iterator(&(v->value), v->type_info.dti_p, eng ? &(eng->bstd) : NULL, eng ? eng->pam_p : NULL, callback);
	if(bri == NULL)
	{
		*error_code = RHENDB_EE_MATERIALIZE_FAILED;
		return 0;
	}
	char one; uint32_t got = read_from_binary_read_iterator(bri, &one, 1, transaction_id, &abort_error);
	delete_binary_read_iterator(bri, transaction_id, &abort_error);
	if(abort_error)
	{
		*error_code = RHENDB_EE_MATERIALIZE_FAILED;
		return 0;
	}
	return got == 0;
}

static void* rhendb_get_bool(void* data, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* v = data;

	if(is_numeric_operand(v))
	{
		if(materialize_numeric(v, ec_p, error_code))
			return NULL;
		return mpd_iszero(&(v->numeric_value)) ? ec_p->false_bool : ec_p->true_bool;   /* inf and nan are truthy */
	}

	/* tuple-form text/blob: emptiness without full materialization */
	if(v->type_info.dti_p != NULL && (is_text_type_info(v->type_info.dti_p) || is_blob_type_info(v->type_info.dti_p)))
	{
		int empty = tuple_tb_is_empty(v, ec_p, error_code);
		if(*error_code)
			return NULL;
		return empty ? ec_p->false_bool : ec_p->true_bool;
	}

	int truthy;
	switch(v->type_info.type){
		case RHENDB_BIT_FIELD:
		case RHENDB_UINT:
			truthy = (v->value.uint_value != 0);
			break;
		case RHENDB_INT:
			truthy = (v->value.int_value != 0);
			break;
		case RHENDB_FLOAT:
		case RHENDB_DOUBLE:
			truthy = (read_flt(v) != 0);   /* NaN != 0 -> true */
			break;
		case RHENDB_LARGE_UINT:
			truthy = !is_zero_uint256(v->value.large_uint_value);
			break;
		case RHENDB_LARGE_INT:
			truthy = !is_zero_int256(v->value.large_int_value);
			break;
		case RHENDB_STRING:
			truthy = (v->value.string_size != 0);
			break;
		case RHENDB_BINARY:
			truthy = (v->value.binary_size != 0);
			break;
		default:
			*error_code = RHENDB_EE_UNSUPPORTED_TYPE;
			return NULL;
	}
	return truthy ? ec_p->true_bool : ec_p->false_bool;
}

/* ------------------------------ compare ------------------------------ */

static const data_type_info* resolve_dti(const expr_value* v)
{
	/* a tuple-form value is compared through its own type. a native scalar always lives in memory at full
	 * width (uint_value / double_value / ...) no matter how narrow its DECLARED width is, so it must be
	 * compared with the full-width default below, not with its width-recording dti. */
	if(is_tuple_form(v))
		return v->type_info.dti_p;
	switch(v->type_info.type){
		case RHENDB_BIT_FIELD: return BIT_FIELD_NULLABLE[64];
		case RHENDB_UINT: return UINT_NULLABLE[8];
		case RHENDB_INT: return INT_NULLABLE[8];
		/* each float kind is compared through the layout it actually occupies */
		case RHENDB_FLOAT: return FLOAT_float_NULLABLE;
		case RHENDB_DOUBLE: return FLOAT_double_NULLABLE;
		case RHENDB_LARGE_UINT: return LARGE_UINT_NULLABLE[32];
		case RHENDB_LARGE_INT: return LARGE_INT_NULLABLE[32];
		default: return NULL;   /* string/binary handled by a dedicated byte compare */
	}
}

/* Wrap an in-memory string/binary operand in a plain variable-length dti so it can be fed to
 * compare_datum_rhendb (whose text/blob path reads every operand through a binary_read_iterator).
 * This lets us compare an on-disk extended text/blob against an in-memory one WITHOUT materializing
 * the extended side.  The scratch dti is caller-owned and lives only for the compare call. */
static const data_type_info* plain_sb_dti(const expr_value* v, data_type_info* scratch)
{
	uint32_t n = v->value.string_or_binary_size;
	if(v->type_info.type == RHENDB_STRING)
		*scratch = get_variable_length_string_type("s", n + 8);
	else
		*scratch = get_variable_length_binary_type("b", n + 8);
	finalize_type_info(scratch);
	return scratch;
}

static int rhendb_compare(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* a = data1; expr_value* b = data2;

	/* NUMERIC comparison, on the mpd_t: NaN == NaN and NaN > everything; otherwise mpd_qcmp
	 * (which already sorts -inf < finite < +inf). */
	if(is_numeric_operand(a) || is_numeric_operand(b))
	{
		/* both operands are on-disk extended numerics: stream-compare through the tuplestore
		 * (sign/exponent/inline first, overflow digits only if needed) with no mpd materialization.
		 * We fall back to materializing both to mpd_t only when at least one side is already an
		 * in-memory numeric/number. */
		if(is_tuple_numeric(a) && is_tuple_numeric(b))
		{
			if(!can_compare_datum_rhendb(a->type_info.dti_p, b->type_info.dti_p))
			{
				*error_code = RHENDB_EE_INCOMPARABLE_TYPES;
				return 0;
			}
			return compare_datum_rhendb(&a->value, a->type_info.dti_p, &b->value, b->type_info.dti_p, tx_from_ctx(ec_p));
		}

		mpd_t sa, sb; int oa = 0, ob = 0;
		mpd_t* pa = operand_to_mpd(a, &sa, &oa, ec_p, error_code, RHENDB_EE_INCOMPARABLE_TYPES);
		if(pa == NULL) return 0;
		mpd_t* pb = operand_to_mpd(b, &sb, &ob, ec_p, error_code, RHENDB_EE_INCOMPARABLE_TYPES);
		if(pb == NULL){ if(oa) mpd_del(&sa); return 0; }
		int an = mpd_isnan(pa), bn = mpd_isnan(pb), r;
		if(an || bn)
			r = (an && bn) ? 0 : (an ? 1 : -1);
		else
		{
			uint32_t st = 0;
			int c = mpd_qcmp(pa, pb, &st);
			r = (c > 0) ? 1 : (c < 0) ? -1 : 0;
		}
		if(oa) mpd_del(&sa);
		if(ob) mpd_del(&sb);
		return r;
	}

	/* string / blob comparison */
	int a_sb = is_sb_operand(a), b_sb = is_sb_operand(b);
	if(a_sb && b_sb){
		transaction* tx = tx_from_ctx(ec_p);
		int a_ext = is_tuple_form(a), b_ext = is_tuple_form(b);
		if(tx != NULL && (a_ext || b_ext))
		{
			/* at least one on-disk extended text/blob: stream-compare through read-iterators,
			 * never materializing the extended side.  An in-memory operand is wrapped in a plain
			 * variable-length dti; compare_datum_rhendb reads both via binary_read_iterators. */
			data_type_info scratch;
			const data_type_info* da = a_ext ? a->type_info.dti_p : plain_sb_dti(a, &scratch);
			const data_type_info* db = b_ext ? b->type_info.dti_p : plain_sb_dti(b, &scratch);
			if(!can_compare_datum_rhendb(da, db))
			{
				*error_code = RHENDB_EE_INCOMPARABLE_TYPES;
				return 0;
			}
			return compare_datum_rhendb(&a->value, da, &b->value, db, tx);
		}
		/* both already in memory (or no engine available): plain byte compare, no engine needed */
		if(materialize_tb(a, ec_p, error_code)) return 0;
		if(materialize_tb(b, ec_p, error_code)) return 0;
		uint32_t na = a->value.string_or_binary_size, nb = b->value.string_or_binary_size;
		uint32_t n = na < nb ? na : nb;
		int c = n ? memcmp(a->value.string_or_binary_value, b->value.string_or_binary_value, n) : 0;
		if(c)
			return c > 0 ? 1 : -1;
		return (na < nb) ? -1 : ((na > nb) ? 1 : 0);
	}
	if(a_sb || b_sb)
	{
		*error_code = RHENDB_EE_INCOMPARABLE_TYPES;
		return 0;
	}   /* string vs number */

	/* numbers (native or tuple-form primitive): compare_datum_rhendb needs no engine for primitives */
	const data_type_info* d1 = resolve_dti(a);
	const data_type_info* d2 = resolve_dti(b);
	if(d1 == NULL || d2 == NULL || !can_compare_datum_rhendb(d1, d2))
	{
		*error_code = RHENDB_EE_INCOMPARABLE_TYPES;
		return 0;
	}
	return compare_datum_rhendb(&a->value, d1, &b->value, d2, tx_from_ctx(ec_p)); // NAN ordering on floats already handled here
}

/* ------------------------------ bitwise / shifts ------------------------------ */

typedef enum
{
	B_AND,
	B_OR,
	B_XOR
} bit_op;

static void* do_bitlogic(void* d1, void* d2, bit_op op, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* a = d1;
	expr_value* b = d2;
	expr_type ta = a->type_info.type, tb = b->type_info.type;

	if(is_tuple_form(a) || is_tuple_form(b) || !et_is_int(ta) || !et_is_int(tb))
	{
		*error_code = RHENDB_EE_NON_INTEGER_OPERAND;
		return NULL;
	}

	expr_type rt = num_result(ta, tb);
	if(et_is_large(rt))
	{
		uint256 x = to_u256(a), y = to_u256(b), r;
		r = (op == B_AND) ? bitwise_and_uint256(x, y) : ((op == B_OR) ? bitwise_or_uint256(x, y) : bitwise_xor_uint256(x, y));
		expr_value* v = new_val(rt, ec_p);
		if(rt == RHENDB_LARGE_INT)
			v->value.large_int_value = (int256){r};
		else
			v->value.large_uint_value = r;
		return v;
	}

	uint64_t x = to_u64(a), y = to_u64(b), r;
	r = (op == B_AND) ? (x & y) : ((op == B_OR) ? (x | y) : (x ^ y));
	expr_value* v = new_val(rt, ec_p);
	if(rt == RHENDB_INT)
		v->value.int_value = (int64_t)r;
	else
		v->value.uint_value = r;
	return v;
}
static void* rhendb_bit_and(void* d1, void* d2, const sql_expr_eval_context* ec_p, int* e){ return do_bitlogic(d1,d2,B_AND,ec_p,e); }
static void* rhendb_bit_or (void* d1, void* d2, const sql_expr_eval_context* ec_p, int* e){ return do_bitlogic(d1,d2,B_OR ,ec_p,e); }
static void* rhendb_bit_xor(void* d1, void* d2, const sql_expr_eval_context* ec_p, int* e){ return do_bitlogic(d1,d2,B_XOR,ec_p,e); }
static void* rhendb_bit_not(void* data, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* a = data;
	expr_type t = a->type_info.type;
	if(is_tuple_form(a) || !et_is_int(t))
	{
		*error_code = RHENDB_EE_NON_INTEGER_OPERAND;
		return NULL;
	}

	expr_value* v = new_val(t, ec_p);
	switch(t)
	{
		case RHENDB_LARGE_UINT:
			v->value.large_uint_value = bitwise_not_uint256(a->value.large_uint_value);
			break;
		case RHENDB_LARGE_INT:
			v->value.large_int_value = (int256){ bitwise_not_uint256(a->value.large_int_value.raw_uint_value) };
			break;
		case RHENDB_INT:
			v->value.int_value = ~a->value.int_value;
			break;
		default:
			v->value.uint_value = ~to_u64(a);
			break;
	}
	return v;
}
static void* do_shift(void* data, void* shift_amt, int left, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* a = data;
	expr_value* s = shift_amt;
	expr_type t = a->type_info.type;
	if(is_tuple_form(a) || is_tuple_form(s) || !et_is_int(t) || !et_is_int(s->type_info.type))
	{
		*error_code = RHENDB_EE_NON_INTEGER_OPERAND;
		return NULL;
	}

	uint32_t amt = (uint32_t)to_u64(s);
	expr_value* v = new_val(t, ec_p);
	switch(t)
	{
		case RHENDB_LARGE_UINT:
			v->value.large_uint_value = left ? left_shift_uint256(a->value.large_uint_value, amt) : right_shift_uint256(a->value.large_uint_value, amt);
			break;
		case RHENDB_LARGE_INT:
			v->value.large_int_value = (int256){ left ? left_shift_uint256(a->value.large_int_value.raw_uint_value, amt) : right_shift_uint256(a->value.large_int_value.raw_uint_value, amt) };
			break;
		case RHENDB_INT:
			v->value.int_value = left ? (int64_t)(((uint64_t)a->value.int_value) << (amt & 63)) : (a->value.int_value >> (amt & 63));
			break;
		default:
			v->value.uint_value = left ? (to_u64(a) << (amt & 63)) : (to_u64(a) >> (amt & 63));
			break;
	}
	return v;
}
static void* rhendb_left_shift (void* d, void* s, const sql_expr_eval_context* ec_p, int* e){ return do_shift(d,s,1,ec_p,e); }
static void* rhendb_right_shift(void* d, void* s, const sql_expr_eval_context* ec_p, int* e){ return do_shift(d,s,0,ec_p,e); }

/* ------------------------------ literals ------------------------------ */

static void* rhendb_create_number(const dstring* data_bytes, const sql_expr_eval_context* ec_p, int* error_code)
{
	(void)ec_p;

	/* parse every byte of the literal (no truncation); a small stack buffer for the common case */
	uint32_t n = get_char_count_dstring(data_bytes);
	char stackbuf[64];
	char* buf = (n + 1 <= sizeof(stackbuf)) ? stackbuf : malloc(n + 1);
	if(buf == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
	memory_move(buf, get_byte_array_dstring(data_bytes), n);
	buf[n] = 0;

	expr_value* v = NULL;

	if(strpbrk(buf, "eE"))   /* an exponent -> approximate FLOAT (SQL approximate-numeric literal) */
	{
		v = new_val(RHENDB_DOUBLE, ec_p);
		v->value.double_value = strtod(buf, NULL);
	}
	else if(strchr(buf, '.'))   /* a fraction, no exponent -> exact NUMERIC (SQL exact-numeric literal) */
	{
		mpd_t d;
		if(!ee_mpd_new(&d)){ if(buf != stackbuf) free(buf); *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
		mpd_context_t ctx; get_mpd_context_for_materialized_numeric(&ctx); uint32_t st = 0;
		mpd_qset_string(&d, buf, &ctx, &st);
		v = new_val(RHENDB_NUMERIC, ec_p); v->numeric_value = d;
	}
	else   /* an integer literal : pick the smallest type it fits into */
	{
		int negative = (buf[0] == '-');
		char* end;

		errno = 0;
		long long sll = strtoll(buf, &end, 10);
		if(errno == 0 && *end == 0)   /* fits a signed 64-bit int */
		{
			v = new_val(RHENDB_INT, ec_p);
			v->value.int_value = (int64_t)sll;
		}
		else
		{
			int done = 0;
			if(!negative)   /* non-negative : try an unsigned 64-bit int next */
			{
				errno = 0;
				unsigned long long ull = strtoull(buf, &end, 10);
				if(errno == 0 && *end == 0)
				{
					v = new_val(RHENDB_UINT, ec_p);
					v->value.uint_value = (uint64_t)ull;
					done = 1;
				}
			}
			if(!done)   /* too big for 64 bits : accumulate the magnitude into a uint256 if it can fit */
			{
				const char* p = buf + (negative ? 1 : 0);
				uint32_t digits = 0;
				while(p[digits] >= '0' && p[digits] <= '9') digits++;
				if(digits >= 1 && digits <= 77 && p[digits] == 0)   /* <= 77 digits always fits 256 bits */
				{
					uint256 mag = get_uint256(0), ten = get_uint256(10);
					for(uint32_t k = 0; k < digits; k++)
					{
						mul_uint256(&mag, mag, ten);
						add_uint256(&mag, mag, get_uint256((uint64_t)(p[k] - '0')));
					}
					if(negative)
					{
						uint256 neg;
						sub_uint256(&neg, get_uint256(0), mag);   /* two's complement : 0 - mag */
						v = new_val(RHENDB_LARGE_INT, ec_p);
						v->value.large_int_value = (int256){ neg };
					}
					else
					{
						v = new_val(RHENDB_LARGE_UINT, ec_p);
						v->value.large_uint_value = mag;
					}
				}
				else   /* more digits than a 256-bit integer can hold -> approximate double */
				{
					v = new_val(RHENDB_DOUBLE, ec_p);
					v->value.double_value = strtod(buf, NULL);
				}
			}
		}
	}

	if(buf != stackbuf)
		free(buf);
	return v;
}
static void* rhendb_create_string(const dstring* data_bytes, const sql_expr_eval_context* ec_p, int* error_code)
{
	/* point straight at the bytes owned by the parse tree; no allocation, buffer stays NULL */
	expr_value* v = new_val(RHENDB_STRING, ec_p);
	v->value.string_value = get_byte_array_dstring(data_bytes);
	v->value.string_size = get_char_count_dstring(data_bytes);
	v->buffer = NULL;
	return v;
}

/* ------------------------------ concat / like ------------------------------ */

static void rhendb_concat(void** data1_p, void* data2, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* a = *data1_p; expr_value* b = data2;
	if(materialize_tb(a, ec_p, error_code)) return;
	if(materialize_tb(b, ec_p, error_code)) return;
	int a_ok = (a->type_info.type == RHENDB_STRING || a->type_info.type == RHENDB_BINARY);
	int b_ok = (b->type_info.type == RHENDB_STRING || b->type_info.type == RHENDB_BINARY);
	if(!a_ok || !b_ok){ *error_code = RHENDB_EE_NON_STRING_OPERAND; return; }

	/* concat modifies the first operand in place.  If a already owns a big enough buffer, b's bytes
	 * are appended with no allocation; if a owns a too-small buffer it is grown (with 0.5x spare);
	 * if a is only borrowing its bytes (buffer == NULL) a fresh buffer is allocated and a is cloned. */
	uint32_t na = a->value.string_or_binary_size, nb = b->value.string_or_binary_size;
	/* the result size is held in a uint32_t : refuse to build a string whose length would not fit */
	if(will_unsigned_sum_overflow(uint32_t, na, nb)){ *error_code = RHENDB_EE_STRING_TOO_LONG; return; }
	uint32_t need = na + nb;
	const char* a_bytes = a->value.string_or_binary_value;
	const char* b_bytes = b->value.string_or_binary_value;

	if(a->buffer != NULL && a->capacity >= need)
	{
		/* a owns a large enough buffer : append b's bytes in place */
		memory_move((char*)a->buffer + na, b_bytes, nb);
	}
	else if(a->buffer != NULL)
	{
		/* a owns a buffer but it is too small : grow it (keeping a's bytes) with 0.5x spare */
		uint64_t cap = min((((uint64_t)need) + (need / 2)), UINT32_MAX);
		char* nbuf = realloc(a->buffer, cap ? cap : 1);
		if(nbuf == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return; }
		memory_move(nbuf + na, b_bytes, nb);
		a->buffer = nbuf;
		a->capacity = cap;
	}
	else
	{
		/* a is borrowing its bytes (buffer == NULL) : allocate with 0.5x spare, clone a then append b */
		uint64_t cap = min((((uint64_t)need) + (need / 2)), UINT32_MAX);
		char* nbuf = malloc(cap ? cap : 1);
		if(nbuf == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return; }
		memory_move(nbuf, a_bytes, na);
		memory_move(nbuf + na, b_bytes, nb);
		a->buffer = nbuf;
		a->capacity = cap;
	}

	a->value.string_or_binary_value = a->buffer;
	a->value.string_or_binary_size = need;
	a->type_info = (expr_type_info){ .type = RHENDB_STRING, .dti_p = NULL, .should_free_dti_p = 0 };
	/* *data1_p is unchanged: a is modified in place */
}
static int like_match(const char* s, uint32_t sl, const char* p, uint32_t pl)
{
	uint32_t si = 0, pi = 0, star = ((uint32_t)(-1)), ss = 0;
	while(si < sl){
		if(pi < pl && (p[pi] == '_' || p[pi] == s[si])){ si++; pi++; }
		else if(pi < pl && p[pi] == '%'){ star = pi++; ss = si; }
		else if(star != (uint32_t)-1){ pi = star + 1; si = ++ss; }
		else return 0;
	}
	while(pi < pl && p[pi] == '%') pi++;
	return pi == pl;
}
static void* rhendb_like(void* str_p, void* pattern_p, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* s = str_p; expr_value* p = pattern_p;
	if(materialize_tb(s, ec_p, error_code)) return NULL;
	if(materialize_tb(p, ec_p, error_code)) return NULL;
	if(s->type_info.type != RHENDB_STRING || p->type_info.type != RHENDB_STRING)
	{
		*error_code = RHENDB_EE_NON_STRING_OPERAND;
		return NULL;
	}
	return like_match(s->value.string_value, s->value.string_size, p->value.string_value, p->value.string_size) ? ec_p->true_bool : ec_p->false_bool;
}

/* ------------------------------ cast ------------------------------ */
/* to_type is the implementer's own type object (an expr_type_info*), produced by get_type_for_sql_type */
/* ---- helpers for the numeric/large/string cast paths ---- */

/* mpd_t -> double, going through the shortest scientific string */
static double mpd_to_double(const mpd_t* m)
{
	char* sci = mpd_to_sci(m, 0);
	double d = sci ? strtod(sci, NULL) : 0.0;
	if(sci) mpd_free(sci);
	return d;
}

/* heap-allocated NUL-terminated copy of a materialized string/binary value's bytes */
static char* sb_to_cstring(const expr_value* a)
{
	uint32_t n = a->value.string_or_binary_size;
	char* s = malloc((size_t)n + 1);
	if(s == NULL) return NULL;
	if(n) memory_move(s, a->value.string_or_binary_value, n);
	s[n] = 0;
	return s;
}

/* fold a run of decimal digits into a 256-bit value (mod 2^256); non-digits are skipped */
static uint256 accumulate_decimal_uint256(const char* digits, uint32_t ndigits)
{
	uint256 acc = get_uint256(0);
	uint256 ten = get_uint256(10);
	for(uint32_t i = 0; i < ndigits; i++)
	{
		if(digits[i] < '0' || digits[i] > '9') continue;
		uint256 t; mul_uint256(&t, acc, ten);
		add_uint256(&acc, t, get_uint256((uint64_t)(digits[i] - '0')));
	}
	return acc;
}

/* a signed decimal integer string -> its 256-bit two's-complement bit pattern */
static uint256 decimal_str_to_int_bits(const char* str)
{
	const char* p = str;
	int neg = 0;
	while(*p == ' ' || *p == '\t') p++;
	if(*p == '+') p++;
	else if(*p == '-'){ neg = 1; p++; }
	uint256 bits = accumulate_decimal_uint256(p, (uint32_t)strlen(p));
	if(neg) sub_uint256(&bits, get_uint256(0), bits);   /* negate (mod 2^256) */
	return bits;
}

/* mpd_t -> 256-bit two's-complement bit pattern of its truncated (toward zero) integer part */
static int numeric_to_int_bits(const mpd_t* m, uint256* out)
{
	mpd_t t;
	if(!ee_mpd_new(&t)) return 0;
	mpd_context_t ctx; get_mpd_context_for_materialized_numeric(&ctx); uint32_t st = 0;
	mpd_qtrunc(&t, m, &ctx, &st);                  /* drop the fraction, toward zero */
	char* s = mpd_qformat(&t, "f", &ctx, &st);     /* plain integer digits, never an exponent */
	if(s == NULL){ mpd_del(&t); return 0; }
	*out = decimal_str_to_int_bits(s);
	mpd_free(s);
	mpd_del(&t);
	return 1;
}

/* trim ASCII whitespace and parse a decimal/scientific numeric string into an initialized mpd_t.
 * returns 1 on success (out set), 0 if the string is not a valid number (out untouched), -1 on OOM.
 * honours '.', 'e'/'E' and sign; "Infinity"/"NaN" parse to the mpd specials. an empty/whitespace-only
 * string, or any trailing/embedded garbage (mpd is whole-string strict), is rejected as not-a-number. */
static int parse_numeric_string(const char* str, mpd_t* out)
{
	const char* p = str;
	while(*p==' '||*p=='\t'||*p=='\n'||*p=='\r') p++;
	size_t len = strlen(p);
	while(len>0 && (p[len-1]==' '||p[len-1]=='\t'||p[len-1]=='\n'||p[len-1]=='\r')) len--;
	if(len == 0) return 0;

	char stackbuf[128];
	char* tmp = (len + 1 <= sizeof(stackbuf)) ? stackbuf : malloc(len + 1);
	if(tmp == NULL) return -1;
	memory_move(tmp, p, len); tmp[len] = 0;

	if(!ee_mpd_new(out)){ if(tmp != stackbuf) free(tmp); return -1; }
	mpd_context_t ctx; get_mpd_context_for_materialized_numeric(&ctx); uint32_t st = 0;
	mpd_qset_string(out, tmp, &ctx, &st);
	if(tmp != stackbuf) free(tmp);
	if(st & MPD_Conversion_syntax){ mpd_del(out); return 0; }   /* not a valid numeric literal */
	return 1;
}

/* store a 256-bit integer bit-pattern into an integer-typed value, narrowing to the target width */
static void store_int_bits(expr_value* v, expr_type target, uint256 bits)
{
	switch(target)
	{
		case RHENDB_BIT_FIELD:  v->value.bit_field_value  = bits.limbs[0];          break;
		case RHENDB_UINT:       v->value.uint_value       = bits.limbs[0];          break;
		case RHENDB_INT:        v->value.int_value        = (int64_t)bits.limbs[0]; break;
		case RHENDB_LARGE_UINT: v->value.large_uint_value = bits;                   break;
		case RHENDB_LARGE_INT:  v->value.large_int_value  = (int256){ bits };       break;
		default: break;
	}
}

/* narrow a cast integer result to the width its target type declares.
 *
 * without this the cast returns a full-width value while the type says (say) 2 bytes, and the
 * discrepancy is only resolved when the value is stored -- where the tuplestore silently wraps it
 * (70000 into an INT[2] reads back as 4464, with set_element_in_tuple still reporting success).
 * narrowing here makes the value the expression sees identical to the value that will be stored.
 * width 0 means "unspecified", which is the widest form and needs no narrowing. */
static void narrow_int_to_declared_width(expr_value* v, expr_type kind, uint32_t width)
{
	if(width == 0) return;
	switch(kind)
	{
		case RHENDB_BIT_FIELD:
			if(width < 64) v->value.bit_field_value &= ((1ULL << width) - 1ULL);
			return;
		case RHENDB_UINT:
			if(width < 8) v->value.uint_value &= ((1ULL << (width * 8)) - 1ULL);
			return;
		case RHENDB_INT:
			if(width < 8)
			{
				uint32_t sh = 64 - (width * 8);
				v->value.int_value = (int64_t)(((uint64_t)v->value.int_value) << sh) >> sh;  /* sign-extend */
			}
			return;
		case RHENDB_LARGE_UINT:
			if(width < 32)
				for(uint32_t b = width; b < 32; b++) set_byte_in_uint256(&(v->value.large_uint_value), b, 0);
			return;
		case RHENDB_LARGE_INT:
			if(width < 32)
			{
				uint8_t top = get_byte_from_uint256(v->value.large_int_value.raw_uint_value, width - 1);
				uint8_t fill = (top & 0x80) ? 0xFF : 0x00;                     /* sign-extend */
				for(uint32_t b = width; b < 32; b++)
					set_byte_in_uint256(&(v->value.large_int_value.raw_uint_value), b, fill);
			}
			return;
		default: return;
	}
}


/* plain decimal text of a number/numeric operand, as a malloc'd NUL-terminated string (caller frees with
 * free()). integers (native and 256-bit) are exact; floats and NUMERIC go through an mpd_t and the 'f'
 * (fixed-point) format, so the result is always ordinary decimal, never scientific. returns NULL on OOM. */
static char* number_to_decimal_cstring(const expr_value* a)
{
	char stack[512];
	switch(a->type_info.type)
	{
		case RHENDB_BIT_FIELD:
		case RHENDB_UINT:
		{
			int n = snprintf(stack, sizeof(stack), "%llu", (unsigned long long)a->value.uint_value);
			if(n < 0) return NULL;
			char* s = malloc((size_t)n + 1);
			if(s != NULL) memory_move(s, stack, (size_t)n + 1);
			return s;
		}
		case RHENDB_INT:
		{
			int n = snprintf(stack, sizeof(stack), "%lld", (long long)a->value.int_value);
			if(n < 0) return NULL;
			char* s = malloc((size_t)n + 1);
			if(s != NULL) memory_move(s, stack, (size_t)n + 1);
			return s;
		}
		case RHENDB_LARGE_UINT:
		{
			uint32_t n = serialize_to_decimal_uint256(stack, a->value.large_uint_value);
			stack[n] = 0;
			char* s = malloc((size_t)n + 1);
			if(s != NULL) memory_move(s, stack, (size_t)n + 1);
			return s;
		}
		case RHENDB_LARGE_INT:
		{
			uint32_t n = serialize_to_decimal_int256(stack, a->value.large_int_value);
			stack[n] = 0;
			char* s = malloc((size_t)n + 1);
			if(s != NULL) memory_move(s, stack, (size_t)n + 1);
			return s;
		}
		case RHENDB_NUMERIC:
		case RHENDB_FLOAT:
		case RHENDB_DOUBLE:
		{
			// route both the mpd_t and the two float kinds through one mpd_t, then format fixed-point.
			mpd_t scratch;
			const mpd_t* d;
			if(a->type_info.type == RHENDB_NUMERIC)
				d = &(a->numeric_value);
			else
			{
				if(!number_to_mpd(a, &scratch)) return NULL;
				d = &scratch;
			}
			mpd_context_t ctx; get_mpd_context_for_materialized_numeric(&ctx); uint32_t st = 0;
			char* mpd_str = mpd_qformat(d, "f", &ctx, &st);
			if(d == &scratch) mpd_del(&scratch);
			if(mpd_str == NULL) return NULL;
			// normalize to a free()-able buffer : mpd_qformat allocates with mpd_free, unlike the paths above.
			size_t len = strlen(mpd_str);
			char* s = malloc(len + 1);
			if(s != NULL) memory_move(s, mpd_str, len + 1);
			mpd_free(mpd_str);
			return s;
		}
		default:
			return NULL;
	}
}

static void* rhendb_cast(void* data, const void* to_type, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* a = data;
	expr_type target = ((const expr_type_info*)to_type)->type;

	/* bring an extended (tuple-form) source down to its scalar representation first */
	if(is_numeric_operand(a)){ if(materialize_numeric(a, ec_p, error_code)) return NULL; }
	else if(is_sb_operand(a)){ if(materialize_tb(a, ec_p, error_code)) return NULL; }

	int src_is_numeric = (a->type_info.type == RHENDB_NUMERIC);
	int src_is_number  = (!is_tuple_form(a) && et_is_num(a->type_info.type));   /* native bit/int/float */
	int src_is_sb      = (a->type_info.type == RHENDB_STRING || a->type_info.type == RHENDB_BINARY);

	/* integer targets : carry the value as a 256-bit two's-complement pattern, then narrow to width.
	 * this keeps full precision for LARGE_* and avoids the old double round-trip that lost int64 bits. */
	if(et_is_int(target))
	{
		uint256 bits;
		if(src_is_numeric)
		{
			if(!numeric_to_int_bits(&(a->numeric_value), &bits)){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
		}
		else if(src_is_number)
		{
			if(et_is_float(a->type_info.type))
			{
				mpd_t d;
				if(!number_to_mpd(a, &d)){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
				int ok = numeric_to_int_bits(&d, &bits);
				mpd_del(&d);
				if(!ok){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			}
			else
				bits = to_i256(a).raw_uint_value;   /* native integer, sign/zero-extended to 256 bits */
		}
		else if(src_is_sb)
		{
			char* s = sb_to_cstring(a);
			if(s == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			mpd_t d;
			int pr = parse_numeric_string(s, &d);   /* parses "1.939e5" as 193900 */
			free(s);
			if(pr < 0){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			if(pr == 0){ *error_code = RHENDB_EE_INVALID_CAST_VALUE; return NULL; }   /* not a number */
			if(!mpd_isfinite(&d)){ mpd_del(&d); *error_code = RHENDB_EE_INVALID_CAST_VALUE; return NULL; }   /* inf/NaN has no integer value */
			int ok = numeric_to_int_bits(&d, &bits);   /* truncate toward zero */
			mpd_del(&d);
			if(!ok){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
		}
		else { *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }

		expr_value* v = new_val(target, ec_p);
		store_int_bits(v, target, bits);
		/* the target type may declare a narrower width than the full-width bits we just stored
		 * (CAST(x AS SMALLINT) is a 2-byte INT). narrow now, so the expression's value is exactly the
		 * value that will be stored rather than one the tuplestore silently wraps later. */
		narrow_int_to_declared_width(v, target, native_width((const expr_type_info*)to_type));
		v->type_info.dti_p = ((const expr_type_info*)to_type)->dti_p;   /* carry the declared width */
		v->type_info.should_free_dti_p = 0;
		return v;
	}

	/* FLOAT / DOUBLE target */
	if(et_is_float(target))
	{
		double d;
		if(src_is_numeric)     d = mpd_to_double(&(a->numeric_value));
		else if(src_is_number) d = to_dbl(a);
		else if(src_is_sb)
		{
			char* s = sb_to_cstring(a);
			if(s == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			mpd_t m;
			int pr = parse_numeric_string(s, &m);
			free(s);
			if(pr < 0){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			if(pr == 0){ *error_code = RHENDB_EE_INVALID_CAST_VALUE; return NULL; }   /* not a number */
			d = mpd_to_double(&m);   /* inf/NaN are valid floating-point values */
			mpd_del(&m);
		}
		else { *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }
		expr_value* v = new_val(target, ec_p); write_flt(v, target, d); return v;
	}

	/* NUMERIC target */
	if(target == RHENDB_NUMERIC)
	{
		mpd_t d;
		if(src_is_numeric)
		{
			if(!ee_mpd_new(&d)){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			uint32_t st = 0; mpd_qcopy(&d, &(a->numeric_value), &st);
		}
		else if(src_is_number)
		{
			if(!number_to_mpd(a, &d)){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
		}
		else if(src_is_sb)
		{
			char* s = sb_to_cstring(a);
			if(s == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			int pr = parse_numeric_string(s, &d);
			free(s);
			if(pr < 0){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			if(pr == 0){ *error_code = RHENDB_EE_INVALID_CAST_VALUE; return NULL; }   /* not a number */
		}
		else { *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }
		expr_value* v = new_val(RHENDB_NUMERIC, ec_p); v->numeric_value = d; return v;
	}

	/* STRING / BINARY target */
	if(target == RHENDB_STRING || target == RHENDB_BINARY)
	{
		const char* bytes;   /* the result bytes and their length, sourced below */
		uint32_t n;
		char* owned;         /* a buffer we allocated that the result value must take ownership of */

		if(src_is_sb)
		{
			/* string/binary source (already materialized above) : reinterpret its bytes under the new label */
			n = a->value.string_or_binary_size;
			owned = malloc(n ? n : 1);
			if(owned == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			if(n) memory_move(owned, a->value.string_or_binary_value, n);
			bytes = owned;
		}
		else if(src_is_numeric || src_is_number)
		{
			/* number / numeric source : plain decimal text */
			owned = number_to_decimal_cstring(a);
			if(owned == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			n = (uint32_t)strlen(owned);
			bytes = owned;
		}
		else { *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }

		expr_value* v = new_val(target, ec_p);
		v->buffer = owned;               /* freed by rhendb_delete_data */
		v->capacity = n;
		v->value = (datum){ .string_or_binary_value = bytes, .string_or_binary_size = n };
		return v;
	}

	*error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL;   /* unknown target kind */
}

/* ------------------------------ type inference ------------------------------ */

static void* rhendb_get_type_for_data(void* data, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* v = data;
	expr_type_info* ti = new_type(v->type_info.type);
	/* forward the value's dti verbatim : containers need it, and native scalars carry their declared
	 * width in it. it is always static or owned by an input tuple, so it is borrowed, never freed. */
	ti->dti_p = v->type_info.dti_p;
	ti->should_free_dti_p = 0;
	return ti;
}
static void* rhendb_get_type_for_sql_type(const sql_type* type, const sql_expr_eval_context* ec_p, int* error_code)
{
	switch(type->type_name)
	{
		case SQL_BOOL: return (void*)(&rhendb_bool_type);

		/* the integer widths are part of the SQL type, and native types now carry their declared width,
		 * so give each its own rather than collapsing all three onto a full-width RHENDB_INT. a CAST to
		 * SMALLINT then really does produce a 2-byte result, and a projection of it stores 2 bytes. */
		case SQL_SMALLINT: return new_type_sized(RHENDB_INT, 2);
		case SQL_INT:      return new_type_sized(RHENDB_INT, 4);
		case SQL_BIGINT:   return new_type_sized(RHENDB_INT, 8);

		/* BIT(n) : a collection of n bits. spec[0] carries n when the type was written with a length. */
		case SQL_BIT:
		{
			uint32_t bits = 64;
			if(type->spec_size > 0 && type->spec[0] > 0 && type->spec[0] <= 64)
				bits = (uint32_t)(type->spec[0]);
			return new_type_sized(RHENDB_BIT_FIELD, bits);
		}
		/* REAL is the 4-byte approximate type; FLOAT and DOUBLE PRECISION are 8-byte, as in common SQL */
		case SQL_REAL: return new_type_sized(RHENDB_FLOAT, 0);
		case SQL_FLOAT: case SQL_DOUBLE: return new_type_sized(RHENDB_DOUBLE, 0);
		case SQL_DECIMAL: case SQL_NUMERIC: return new_type(RHENDB_NUMERIC);
		case SQL_TEXT: case SQL_CHAR: case SQL_VARCHAR: case SQL_STRING: case SQL_CLOB: return new_type(RHENDB_STRING);
		case SQL_BINARY: case SQL_BLOB: return new_type(RHENDB_BINARY);
		default: *error_code = RHENDB_EE_UNSUPPORTED_TYPE; return NULL;
	}
}
/* the scalar type an operator should see for a value/type : an unmaterialized extended numeric/
 * text/blob (RHENDB_TUPLE + a prefixed dti) acts as its scalar kind; everything else is its own type. */
static expr_type effective_type(const expr_type_info* ti)
{
	if(ti->type == RHENDB_TUPLE && ti->dti_p != NULL)
	{
		if(is_numeric_type_info(ti->dti_p)) return RHENDB_NUMERIC;
		if(is_text_type_info(ti->dti_p))    return RHENDB_STRING;
		if(is_blob_type_info(ti->dti_p))    return RHENDB_BINARY;
	}
	return ti->type;
}

/* an extended (blob-backed) text/blob/numeric type. these read through an engine; two of them are only
 * interchangeable-in-tuple-form when they are byte-for-byte the same type (same sub_type/name, same
 * max_size, same inline prefix, same containees) -- i.e. are_identical_type_info. */
static int is_extended_kind(const data_type_info* d)
{
	return d != NULL && is_extended_type_info(d);
}

/* two tuple-form types are compatible if they are the same extended kind, or (for plain tuples)
 * the same declared type_name and container type -- pointer equality is not required. */
static int same_tuple_kind(const data_type_info* d1, const data_type_info* d2)
{
	if(d1 == NULL || d2 == NULL) return d1 == d2;
	/* extended text/blob/numeric stay in (unmaterialized) tuple form through a binary op ONLY when the
	 * two sides are the exact same extended type. two extended types of the same scalar family but with
	 * a different sub_type, max_size, prefix or table-derived name are NOT interchangeable: the caller
	 * (rhendb_unify_types) then promotes them to the materialized scalar (RHENDB_STRING/BINARY/NUMERIC).
	 * this keeps all differing extended text types -- and a plain RHENDB_STRING -- behaving identically. */
	if(is_extended_kind(d1) || is_extended_kind(d2))
		return is_extended_kind(d1) && is_extended_kind(d2) && are_identical_type_info(d1, d2);
	/* plain (inline-container) tuples: same declared structure */
	return are_identical_type_info(d1, d2);
}

/* two ARRAY types unify if their CONTAINEE types are identical -- the arrays themselves need NOT be
 * identical: they may differ in element_count, in being fixed or variable element counted, in max_size
 * or in their declared type_name. it is the element type that has to line up. */
static int same_array_containee(const data_type_info* d1, const data_type_info* d2)
{
	if(d1 == NULL || d2 == NULL) return d1 == d2;
	if(d1 == d2) return 1;
	if(d1->containee == NULL || d2->containee == NULL) return d1->containee == d2->containee;
	if(d1->containee == d2->containee) return 1;
	return are_identical_type_info(d1->containee, d2->containee);
}

static int rhendb_can_compare_types(void* typ1, void* typ2, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_type a = effective_type((expr_type_info*)typ1), b = effective_type((expr_type_info*)typ2);
	if(et_is_num_or_numeric(a) && et_is_num_or_numeric(b)) return 1;   /* numerics compare with any number */
	if(a==RHENDB_STRING && b==RHENDB_STRING) return 1;
	if(a==RHENDB_BINARY && b==RHENDB_BINARY) return 1;
	return 0;
}
static int rhendb_can_cast_types(const void* typ_from, const void* typ_to, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_type from = effective_type((const expr_type_info*)typ_from), to = effective_type((const expr_type_info*)typ_to);
	/* aligned with rhendb_cast : any scalar among {numbers, NUMERIC, STRING, BINARY} casts to any other.
	 * numbers <-> numbers/NUMERIC, string/binary -> number (parsed), number/NUMERIC -> string (decimal text),
	 * and string/binary -> string/binary (byte reinterpretation). */
	int from_ok = et_is_num_or_numeric(from) || from == RHENDB_STRING || from == RHENDB_BINARY;
	int to_ok   = et_is_num_or_numeric(to)   || to   == RHENDB_STRING || to   == RHENDB_BINARY;
	return from_ok && to_ok;
}
static void* rhendb_get_return_type_for_op_exec_callback(void* op_exec_func, void* typ1, void* typ2, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_type a = effective_type((expr_type_info*)typ1);
	expr_type b = (typ2 != NULL) ? effective_type((expr_type_info*)typ2) : a;   /* typ2 is NULL for unary ops */
	const expr_type_info* ta = (const expr_type_info*)typ1;
	const expr_type_info* tb = (const expr_type_info*)typ2;
	if(op_exec_func==(void*)ec_p->add||op_exec_func==(void*)ec_p->sub||op_exec_func==(void*)ec_p->mul||op_exec_func==(void*)ec_p->div||op_exec_func==(void*)ec_p->mod){
		if(!et_is_num_or_numeric(a) || !et_is_num_or_numeric(b)){ *error_code = RHENDB_EE_NON_NUMERIC_OPERAND; return NULL; }
		return num_result_sized(ta, tb);
	}
	if(op_exec_func==(void*)ec_p->bit_and||op_exec_func==(void*)ec_p->bit_or||op_exec_func==(void*)ec_p->bit_xor){
		if(!et_is_int(a) || !et_is_int(b)){ *error_code = RHENDB_EE_NON_INTEGER_OPERAND; return NULL; }
		return num_result_sized(ta, tb);
	}
	if(op_exec_func==(void*)ec_p->left_shift||op_exec_func==(void*)ec_p->right_shift){
		/* a shift keeps the LEFT operand's kind and width : shifting by a wider count cannot widen it */
		if(!et_is_int(a) || !et_is_int(b)){ *error_code = RHENDB_EE_NON_INTEGER_OPERAND; return NULL; }
		return new_type_borrowing(a, ta ? ta->dti_p : NULL);
	}
	if(op_exec_func==(void*)ec_p->bit_not){
		if(!et_is_int(a)){ *error_code = RHENDB_EE_NON_INTEGER_OPERAND; return NULL; }
		return new_type_borrowing(a, ta ? ta->dti_p : NULL);
	}
	if(op_exec_func==(void*)ec_p->concat) return new_type(RHENDB_STRING);
	if(op_exec_func==(void*)ec_p->like)   return new_type(RHENDB_BIT_FIELD);
	*error_code = RHENDB_EE_UNSUPPORTED_TYPE; return NULL;
}
static void* rhendb_unify_types(void* typ1, void* typ2, const sql_expr_eval_context* ec_p, int* error_code)
{
	(void)ec_p;
	expr_type_info* t1 = typ1; expr_type_info* t2 = typ2;

	/* two tuple-form types (plain tuples, or unmaterialized extended numeric/text/blob) unify only
	 * if they are the same kind/structure -- not necessarily the same pointer -- and stay tuple form. */
	if(t1->type == RHENDB_TUPLE && t2->type == RHENDB_TUPLE)
	{
		/* identical extended types (or identical plain tuples) stay in tuple form -- the reader will
		 * pull them straight from their engine without a full materialization. */
		if(t1->dti_p == t2->dti_p || same_tuple_kind(t1->dti_p, t2->dti_p))
		{
			expr_type_info* r = new_type(RHENDB_TUPLE);
			r->dti_p = t1->dti_p;   /* borrowed */
			return r;
		}
		/* two DIFFERENT extended text/blob/numeric types (different engine, sub_type, max_size, name):
		 * fall through to reconcile their materialized scalar kinds below (RHENDB_STRING/BINARY/NUMERIC),
		 * so the expression result is a materialized value. only genuinely unrelated plain tuples error. */
		if(!(is_extended_kind(t1->dti_p) || is_extended_kind(t2->dti_p)))
		{
			*error_code = RHENDB_EE_INCOMPARABLE_TYPES;
			return NULL;
		}
	}
	if(t1->type == RHENDB_ARRAY && t2->type == RHENDB_ARRAY)
	{
		/* identical CONTAINEES, not identical arrays */
		if(t1->dti_p == t2->dti_p || same_array_containee(t1->dti_p, t2->dti_p))
		{
			expr_type_info* r = new_type(RHENDB_ARRAY);
			r->dti_p = t1->dti_p;   /* borrowed */
			return r;
		}
		*error_code = RHENDB_EE_INCOMPARABLE_TYPES;
		return NULL;
	}

	/* otherwise reconcile the effective scalar types (an unmaterialized extended value acts as its scalar) */
	expr_type a = effective_type(t1), b = effective_type(t2);
	if(a == b)
	{
		/* same kind : keep the wider of the two declared widths rather than jumping to the maximum */
		if(et_is_num(a) && !et_is_float(a))
			return new_type_sized(a, combined_width_bytes(t1, t2));
		if(et_is_float(a))
			return new_type_sized(a, 0);
		return new_type(a);
	}
	if(et_is_num_or_numeric(a) && et_is_num_or_numeric(b))
		return num_result_sized(t1, t2);
	*error_code = RHENDB_EE_INCOMPARABLE_TYPES;
	return NULL;
}

/* ------------------------------ variables (column references) ------------------------------ */

/* "a.b.c" is resolved once against the schema and cached; the cache lives for the life of the
 * context, so the same reference is only parsed and located once and then read every row.
 * A reference is resolved against every input tuple under both readings -- with a leading table
 * (type) name and without one -- and succeeds only if exactly one input column matches (see
 * resolve_into()); the table name is therefore optional wherever it is not needed to disambiguate.
 * Every component after any table name is a field name (a string) OR an integer (an array/tuple index). */
typedef struct var_cache_entry var_cache_entry;
struct var_cache_entry
{
	bstnode node;                      /* embedded cutlery hashmap node (red-black bst buckets) */

	/* the cache KEY is the identifier's bytes in memory : (address, length).
	 * the dstring handed to get_variable()/get_type_for_variable() points INTO the AST node, so these
	 * bytes are stable for the life of the expression. hashing/comparing an address+length is O(1) and
	 * costs no FNV pass and no memory_compare() over the identifier on every single evaluation.
	 * two AST nodes naming the same column at different addresses simply get two entries -- accepted. */
	const char* key_bytes;
	cy_uint key_length;

	dstring identifier;                /* owned clone of the variable name (diagnostics only) */
	uint32_t tuple_index;              /* which input tuple it lives in */
	positional_accessor pa;            /* owns pa.positions : the located element */
	const data_type_info* column_dti;  /* the located column's on-disk type */
};

/* hash the identifier's LOCATION (address + length), not its contents.
 * key_bytes is only ever treated as an integer here -- it is NEVER dereferenced -- so it is safe even if
 * the AST that owned those bytes has since been freed. this removes the FNV pass over the identifier
 * string from every single variable reference of every single evaluation. */
static cy_uint var_hash(const void* d)
{
	const var_cache_entry* e = d;
	uintptr_t p = (uintptr_t)(e->key_bytes);
	uintptr_t h = p ^ (p >> 17) ^ (((uintptr_t)(e->key_length)) << 7);
	h *= (uintptr_t)1099511628211ULL;
	return (cy_uint)(h ^ (h >> 29));
}
/* order by (address, length) first -- both cheap integers -- and only then verify the actual bytes.
 * the verification is ESSENTIAL and must not be dropped : a context may outlive the expression it cached
 * (the differential fuzzers do exactly this), the AST's identifier bytes are freed, and a later AST's
 * identifier can land at the SAME address with the SAME length while naming a DIFFERENT column. keying on
 * the address alone hands back that stale entry -- silently resolving the wrong column.
 * the compare reads each side's OWNED clone, so no freed memory is ever dereferenced. */
static int var_cmp(const void* d1, const void* d2)
{
	const var_cache_entry* a = d1;
	const var_cache_entry* b = d2;
	if(a->key_bytes != b->key_bytes)
		return (a->key_bytes > b->key_bytes) ? 1 : -1;
	if(a->key_length != b->key_length)
		return (a->key_length > b->key_length) ? 1 : -1;
	return 0;   /* identical bytes at an identical address -> the same identifier */
}

/* map a located column's data_type_info to the expr_type we would expose it as */
static expr_type expr_type_for_column(const data_type_info* dti)
{
	switch(dti->type)
	{
		case BIT_FIELD: return RHENDB_BIT_FIELD;
		case UINT: return RHENDB_UINT;
		case INT: return RHENDB_INT;
		case FLOAT: return (dti->size == sizeof(float)) ? RHENDB_FLOAT : RHENDB_DOUBLE;
		case LARGE_UINT: return RHENDB_LARGE_UINT;
		case LARGE_INT: return RHENDB_LARGE_INT;
		case STRING: return RHENDB_STRING;
		case BINARY: return RHENDB_BINARY;
		case ARRAY: return RHENDB_ARRAY;
		default:
			return RHENDB_TUPLE;
	}
}

/* a fully-numeric component is used as a direct index; returns 1 and sets *out if so */
static int component_as_index(const char* p, uint32_t len, uint32_t* out)
{
	if(len == 0)
		return 0;
	uint64_t v = 0;
	for(uint32_t i = 0; i < len; i++){ if(p[i] < '0' || p[i] > '9') return 0; v = v * 10 + (uint64_t)(p[i] - '0'); }
	*out = (uint32_t)v;
	return 1;
}

/* the deepest dotted path we will resolve. this is only a sanity bound against pathological input; the
 * component and position arrays are heap-allocated to the actual path length, so nesting is not limited to
 * any small fixed constant. */
#define EE_MAX_PATH 1000

/* walk a dotted field path [start, ncomp) starting at container `root`, writing the located element indices
 * into positions_out (which must have room for ncomp-start entries). each component is either an integer
 * (a direct array/tuple index) or a field name. returns 1 and sets *depth_out / *dti_out on success; returns
 * 0 (without touching *error_code) if any component does not name a reachable element. a path that locates
 * no field at all (start == ncomp) fails -- a bare table name is not a variable. */
static int walk_field_path(data_type_info* root, const char** cptr, const uint32_t* clen, uint32_t start, uint32_t ncomp, uint32_t* positions_out, uint32_t* depth_out, const data_type_info** dti_out)
{
	char name[65];
	data_type_info* cur = root;
	uint32_t depth = 0;
	for(uint32_t c = start; c < ncomp; c++)
	{
		if(!is_container_type_info(cur)) return 0;
		uint32_t idx;
		if(!component_as_index(cptr[c], clen[c], &idx))   /* a field name -> look it up */
		{
			uint32_t l = clen[c]; if(l > 64) return 0;
			memory_move(name, cptr[c], l); name[l] = 0;
			idx = find_containee_using_field_name_in_tuple_type_info(cur, name);
			if(idx == UINT32_MAX) return 0;               /* no such field */
		}
		positions_out[depth++] = idx;
		cur = get_data_type_info_for_containee_of_container_without_data(cur, idx);
		if(cur == NULL) return 0;                         /* index out of bounds / not a container */
	}
	if(depth == 0) return 0;
	*depth_out = depth;
	*dti_out = cur;
	return 1;
}

/* does input tuple `t` exist and carry a schema whose table (type) name equals `tname`? */
static int tuple_table_named(rhendb_expr_eval_context* ctx, uint32_t t, const char* tname)
{
	return ctx->input_tuple_defs[t] != NULL
	    && ctx->input_tuple_defs[t]->type_info != NULL
	    && strncmp(ctx->input_tuple_defs[t]->type_info->type_name, tname, 64) == 0;
}

/* Resolve a dotted variable reference "a.b.c" to a unique (input tuple, element path).
 *
 * The reference is tried under BOTH readings, against EVERY input tuple, regardless of how many tuples there
 * are (1 or many):
 *   - table-qualified : the first component is a table (type) name and the remainder is a field path inside a
 *                       tuple whose schema carries that name;
 *   - unqualified     : the whole path is a field path inside a tuple (no leading table name).
 * Every reading that successfully locates an element is a candidate. The reference resolves ONLY if there is
 * exactly one candidate. Zero candidates -> RHENDB_EE_UNKNOWN_VARIABLE. Two or more distinct candidates
 * (whether from different tuples, or from the table-qualified vs unqualified reading, or from two tuples that
 * share a table name) -> RHENDB_EE_AMBIGUOUS_VARIABLE. This makes the table name optional wherever it is not
 * needed to disambiguate, while guaranteeing a reference can never silently pick one of several columns. */
static int resolve_into(rhendb_expr_eval_context* ctx, const dstring* id, uint32_t* out_tuple_index, positional_accessor* out_pa, const data_type_info** out_dti, int* error_code)
{
	const char* key = get_byte_array_dstring(id);
	cy_uint klen = get_char_count_dstring(id);

	/* count components first, so the component arrays can be sized to the actual path length */
	uint32_t ncomp = 0;
	for(cy_uint i = 0; i < klen; )
	{
		while(i < klen && key[i] != '.') i++;
		ncomp++;
		if(i < klen && key[i] == '.') i++;
	}
	if(ncomp == 0 || ncomp > EE_MAX_PATH){ *error_code = RHENDB_EE_UNSUPPORTED_TYPE; return 0; }

	const char** comp_ptr = malloc(sizeof(char*) * ncomp);
	uint32_t*    comp_len = malloc(sizeof(uint32_t) * ncomp);
	uint32_t*    scratch  = malloc(sizeof(uint32_t) * ncomp);   /* positions of the candidate being tried */
	if(comp_ptr == NULL || comp_len == NULL || scratch == NULL){ free(comp_ptr); free(comp_len); free(scratch); *error_code = RHENDB_EE_OUT_OF_MEMORY; return 0; }

	{
		uint32_t n = 0; cy_uint i = 0;
		while(i < klen)
		{
			cy_uint start = i;
			while(i < klen && key[i] != '.') i++;
			comp_ptr[n] = key + start; comp_len[n] = (uint32_t)(i - start); n++;
			if(i < klen && key[i] == '.') i++;
		}
	}

	// the 0th component must not be an integral access
	{uint32_t temp_idx; if(component_as_index(comp_ptr[0], comp_len[0], &temp_idx)){
		free(comp_ptr); free(comp_len); free(scratch);
		*error_code = RHENDB_EE_OUT_OF_MEMORY;
		return 0;
	}}

	/* enumerate candidates; keep the first, and on a second distinct hit flag ambiguity */
	int found = 0, ambiguous = 0, oom = 0;
	uint32_t win_ti = 0, win_depth = 0; uint32_t* win_pos = NULL; const data_type_info* win_dti = NULL;

	#define TRY_CANDIDATE(TI, START) do {                                                                   \
		uint32_t _d; const data_type_info* _dd;                                                             \
		if(!ambiguous && !oom && walk_field_path(ctx->input_tuple_defs[(TI)]->type_info, comp_ptr, comp_len, (START), ncomp, scratch, &_d, &_dd)) { \
			if(found) { ambiguous = 1; }                                                                    \
			else {                                                                                          \
				win_pos = malloc(sizeof(uint32_t) * (_d ? _d : 1));                                         \
				if(win_pos == NULL) { oom = 1; }                                                            \
				else { memory_move(win_pos, scratch, sizeof(uint32_t) * _d); win_ti = (TI); win_depth = _d; win_dti = _dd; found = 1; } \
			}                                                                                               \
		}                                                                                                   \
	} while(0)

	/* table-qualified reading : first component names a table; try it against every tuple carrying that name */
	if(ncomp >= 2 && comp_len[0] <= 64)
	{
		char tname[65]; memory_move(tname, comp_ptr[0], comp_len[0]); tname[comp_len[0]] = 0;
		for(uint32_t t = 0; t < ctx->input_tuples_count && !ambiguous && !oom; t++)
			if(tuple_table_named(ctx, t, tname))
				TRY_CANDIDATE(t, 1);
	}
	/* unqualified reading : the whole path is a field path inside some tuple */
	for(uint32_t t = 0; t < ctx->input_tuples_count && !ambiguous && !oom; t++)
		if(ctx->input_tuple_defs[t] != NULL && ctx->input_tuple_defs[t]->type_info != NULL)
			TRY_CANDIDATE(t, 0);

	#undef TRY_CANDIDATE

	free(comp_ptr); free(comp_len); free(scratch);

	if(oom)       { free(win_pos); *error_code = RHENDB_EE_OUT_OF_MEMORY;      return 0; }
	if(ambiguous) { free(win_pos); *error_code = RHENDB_EE_AMBIGUOUS_VARIABLE; return 0; }
	if(!found)    {                *error_code = RHENDB_EE_UNKNOWN_VARIABLE;   return 0; }

	out_pa->positions = win_pos;
	out_pa->positions_length = win_depth;
	*out_tuple_index = win_ti;
	*out_dti = win_dti;
	return 1;
}

#undef EE_MAX_PATH

/* find the cached resolution for id, resolving and caching it on the first miss */
static const var_cache_entry* get_or_resolve_entry(rhendb_expr_eval_context* ctx, const dstring* id, int* error_code)
{
	var_cache_entry probe;
	probe.key_bytes  = get_byte_array_dstring(id);   /* the identifier's actual bytes in memory */
	probe.key_length = get_char_count_dstring(id);
	const var_cache_entry* hit = find_equals_in_hashmap(&ctx->var_cache, &probe);
	if(hit != NULL)
		return hit;

	var_cache_entry* ne = malloc(sizeof(*ne));
	if(ne == NULL)
	{
		*error_code = RHENDB_EE_OUT_OF_MEMORY;
		return NULL;
	}
	ne->key_bytes  = get_byte_array_dstring(id);
	ne->key_length = get_char_count_dstring(id);
	if(!init_dstring(&ne->identifier, get_byte_array_dstring(id), get_char_count_dstring(id)))
	{
		free(ne);
		*error_code = RHENDB_EE_OUT_OF_MEMORY;
		return NULL;
	}

	if(!resolve_into(ctx, id, &ne->tuple_index, &ne->pa, &ne->column_dti, error_code))
	{
		deinit_dstring(&ne->identifier);
		free(ne);
		return NULL;
	}

	initialize_bstnode(&ne->node);

	insert_in_hashmap(&ctx->var_cache, ne);

	/* buckets are red-black trees, so a long chain is not fatal, but keeping the load factor low still
	 * shortens every probe. expand by 1.3x once we average more than 4 entries per bucket.
	 * expand_hashmap() failing is harmless -- the map simply stays as it is. */
	{
		cy_uint buckets = get_bucket_count_hashmap(&ctx->var_cache);
		cy_uint elements = get_element_count_hashmap(&ctx->var_cache);
		if(buckets > 0 && elements > (buckets * 4))
			expand_hashmap(&ctx->var_cache, 1.3f);   /* no-op on failure */
	}

	return ne;
}

static void* rhendb_get_variable(const dstring* identifier_bytes, const sql_expr_eval_context* ec_p, int* error_code)
{
	rhendb_expr_eval_context* ctx = ec_p->context_p;

	const var_cache_entry* e = get_or_resolve_entry(ctx, identifier_bytes, error_code);
	if(e == NULL)
		return NULL;

	datum d;
	if(!get_value_from_element_from_tuple(&d, ctx->input_tuple_defs[e->tuple_index], e->pa, ctx->input_tuples[e->tuple_index]))
		return NULL;                       /* could not read : treat as SQL NULL */
	if(is_datum_NULL(&d))
		return NULL;             /* NULL column -> NULL pointer */

	const data_type_info* dti = e->column_dti;
	expr_value* v = new_val(RHENDB_INT, ec_p);
	v->value = d;
	v->buffer = NULL;
	v->capacity = 0;
	v->type_info.should_free_dti_p = 0;
	if(dti->type == TUPLE || dti->type == ARRAY)
	{
		/* keep it in tuple form so it is materialized only when needed */
		v->type_info.type = expr_type_for_column(dti);
		v->type_info.dti_p = (data_type_info*)dti;   /* borrowed from the schema */
	}
	else
	{
		/* a primitive (or inline string/binary) : the datum already holds the value */
		v->type_info.type = expr_type_for_column(dti);
		/* native scalars now keep the column's own data_type_info, purely to remember their declared
		 * width so results are not widened to the maximum. it is owned by the input tuple's schema, so
		 * it is borrowed and never freed. inline string/binary keep dti_p NULL as before. */
		if(et_is_num(v->type_info.type))
		{
			v->type_info.dti_p = (data_type_info*)dti;
			v->type_info.should_free_dti_p = 0;
		}
		else
			v->type_info.dti_p = NULL;
		/* a 4-byte FLOAT column is read into float_value and STAYS there : RHENDB_FLOAT means the value
		 * genuinely lives in float_value. an 8-byte column is RHENDB_DOUBLE in double_value. nothing is
		 * promoted here -- read_flt()/write_flt() pick the right member everywhere else. */
	}
	return v;
}

static void* rhendb_get_type_for_variable(const dstring* identifier_bytes, const sql_expr_eval_context* ec_p, int* error_code)
{
	rhendb_expr_eval_context* ctx = ec_p->context_p;

	const var_cache_entry* e = get_or_resolve_entry(ctx, identifier_bytes, error_code);
	if(e == NULL)
		return NULL;

	expr_type t = expr_type_for_column(e->column_dti);
	expr_type_info* ti = new_type(t);
	/* containers need their dti; native scalars carry it too, to remember the column's declared width so
	 * that inference does not widen every result to the maximum. it belongs to the input tuple's schema,
	 * so it is borrowed and never freed. */
	if(t == RHENDB_TUPLE || t == RHENDB_ARRAY || et_is_num(t))
	{
		ti->dti_p = (data_type_info*)e->column_dti;   /* borrowed */
		ti->should_free_dti_p = 0;
	}
	return ti;
}

static void notify_removal_for_cache_entry(void* resource_p, const void* data_p)
{
	var_cache_entry* e = (var_cache_entry*)data_p;
	free(e->pa.positions);
	deinit_dstring(&e->identifier);
	free(e);
}

/* ------------------------------ context ------------------------------ */

sql_expr_eval_context get_sql_expr_eval_context_for_rhendb(tuple_def** input_tuple_defs, uint32_t input_tuples_count, transaction* tx)
{
	sql_expr_eval_context eval_context = (sql_expr_eval_context){
		.context_p = malloc(sizeof(rhendb_expr_eval_context)),

		.get_bool = rhendb_get_bool,

		.true_bool = (void*)(&rhendb_true_bool),
		.false_bool = (void*)(&rhendb_false_bool),
		.unknown_bool = (void*)(&rhendb_unknown_bool),
		.one_number = (void*)(&rhendb_one_number),
		.zero_number = (void*)(&rhendb_zero_number),
		.minus_one_number = (void*)(&rhendb_minus_one_number),

		.add = rhendb_add,
		.sub = rhendb_sub,
		.mul = rhendb_mul,
		.div = rhendb_div,
		.mod = rhendb_mod,

		.compare = rhendb_compare,

		.left_shift = rhendb_left_shift,
		.right_shift = rhendb_right_shift,

		.bit_and = rhendb_bit_and,
		.bit_or = rhendb_bit_or,
		.bit_xor = rhendb_bit_xor,
		.bit_not = rhendb_bit_not,

		.cast = rhendb_cast,

		.create_number = rhendb_create_number,
		.create_string = rhendb_create_string,

		.concat = rhendb_concat,

		.like = rhendb_like,

		.delete_data = rhendb_delete_data,

		.get_sub_query = NULL,
		.next_data_from_sub_query = NULL,
		.delete_sub_query = NULL,

		.call_function = NULL,

		.get_variable = rhendb_get_variable,

		.bool_type = (void*)(&rhendb_bool_type),

		.can_compare_types = rhendb_can_compare_types,

		.get_type_for_sql_type = rhendb_get_type_for_sql_type,
		.can_cast_types = rhendb_can_cast_types,

		.get_return_type_for_op_exec_callback = rhendb_get_return_type_for_op_exec_callback,

		.get_type_for_data = rhendb_get_type_for_data,

		.unify_types = rhendb_unify_types,

		.get_type_for_sub_query = NULL,

		.get_return_type_for_function = NULL,

		.get_type_for_variable = rhendb_get_type_for_variable,

		.delete_type = rhendb_delete_type,
	};

	rhendb_expr_eval_context* context_p = eval_context.context_p;

	context_p->input_tuple_defs = malloc(sizeof(tuple_def*) * input_tuples_count);
	memory_move(context_p->input_tuple_defs, input_tuple_defs, sizeof(tuple_def*) * input_tuples_count);

	context_p->input_tuples = calloc(sizeof(void*), input_tuples_count);

	context_p->input_tuples_count = input_tuples_count;

	initialize_hashmap(&(context_p->var_cache), ELEMENTS_AS_RED_BLACK_BST, 64, &simple_hasher(var_hash), &simple_comparator(var_cmp), offsetof(var_cache_entry, node));

	context_p->free_list_for_expr_value = NULL;

	context_p->tx = tx;

	return eval_context;
}

int has_reference_to_persistent_extended_type_from_expression(const rhendb_expr_eval_context* context_p)
{
	int is_some_variable_extended = 0;
	for(const var_cache_entry* e = get_first_of_in_hashmap(&(context_p->var_cache), FIRST_OF_HASHMAP); e != NULL && (is_some_variable_extended == 0); e = get_next_of_in_hashmap(&(context_p->var_cache), e, ANY_IN_HASHMAP))
	{
		is_some_variable_extended = has_extended_type_info(e->column_dti, PERSISTENT_EXT_SUB_TYPE);
	}
	return is_some_variable_extended;
}

void delete_context_p_for_sql_expr_eval_context_for_rhendb(rhendb_expr_eval_context* context_p)
{
	remove_all_from_hashmap(&(context_p->var_cache), &((notifier_interface){NULL, notify_removal_for_cache_entry}));
	drain_free_list_for_expr_value(context_p);          /* the ONLY place the free list is released */

	deinitialize_hashmap(&(context_p->var_cache));
	free(context_p->input_tuple_defs);
	free(context_p->input_tuples);
	free(context_p);
}

void set_input_tuples_in_context_for_rhendb(sql_expr_eval_context* ec_p, void** input_tuples, uint32_t input_tuples_count)
{
	rhendb_expr_eval_context* ctx = ec_p->context_p;
	// only copy what the context can actually hold (mismatched counts are a caller error)
	uint32_t n = (input_tuples_count < ctx->input_tuples_count) ? input_tuples_count : ctx->input_tuples_count;
	for(uint32_t i = 0; i < n; i++)
		ctx->input_tuples[i] = input_tuples[i];
}

void set_input_tuples_in_context_for_rhendb_v(sql_expr_eval_context* ec_p, uint32_t input_tuples_count, ...)
{
	rhendb_expr_eval_context* ctx = ec_p->context_p;
	uint32_t n = (input_tuples_count < ctx->input_tuples_count) ? input_tuples_count : ctx->input_tuples_count;
	va_list args;
	va_start(args, input_tuples_count);
	for(uint32_t i = 0; i < n; i++)
		ctx->input_tuples[i] = va_arg(args, void*);
	va_end(args);
}

int select_using_evaluate_sql_expr_for_rhendb(sql_expression* expr, sql_expr_eval_context* ec_p, int* error_code)
{
	(*error_code) = 0;

	void* res = evaluate_sql_expr(expr, ec_p, error_code);
	if((*error_code))
		return 0;

	// collapse to a boolean; get_bool() returns one of the static singletons
	// (true_bool / false_bool / unknown_bool), which must NOT be freed.
	void* log_res = get_bool(res, ec_p, error_code);
	delete_data(res, ec_p);
	if((*error_code))
		return 0;

	return (log_res == ec_p->true_bool) ? 1 : 0;
}

// ===================================================================================================
// projection
// ===================================================================================================

int is_valid_using_infer_sql_expr_for_rhendb(sql_expression* expr, sql_expr_eval_context* ec_p, int* error_code)
{
	(*error_code) = 0;
	void* t = infer_type_sql_expr(expr, ec_p, error_code);
	if(t != NULL)
		delete_type(t, ec_p);
	return ((*error_code) == 0) ? 1 : 0;
}

// the inline prefix budget of a projected extended value, and the total inline max_size (leaving room after
// the prefix for the blob pointer (page id up to 8+4 bytes), a 4-byte offset, the inline array size header
// and a few spare bytes). for numeric the prefix holds floor(90 / BYTES_PER_NUMERIC_DIGIT) radix-10^12
// digit slots.
#define PROJECTION_PREFIX_BYTES   90
#define PROJECTION_MAX_SIZE       128

// build the data_type_info a scalar result of kind `scalar` is PROJECTED into, and report through
// *should_free whether the caller owns it.
//   native scalars -> the matching default type_info, BORROWED (*should_free = 0); they are is_static and
//                     must never be destroyed anyway.
//   string / binary / arbitrary-precision numeric -> a freshly built, FINALIZED volatile-store extended type
//                     (*should_free = 1). finalizing matters: an unfinalized type differs from the same type
//                     sitting in a tuple (is_finalized/size), which would defeat the "value already has the
//                     projected type" fast path in project_using_evaluate.
// returns NULL for tuple/array (the caller borrows the inferred type instead) or when there is no
// transaction to supply the volatile page specs.
static data_type_info* build_projection_type_for_scalar(expr_type scalar, const expr_type_info* src,
	const sql_expr_eval_context* ec_p, int* should_free)
{
	*should_free = 0;
	if(et_is_num(scalar))
	{
		/* store the result no wider than it was declared to be : a uint of 3 bytes projects into a 3-byte
		 * uint, not an 8-byte one. an unspecified width falls back to the widest form. these are all
		 * static defaults, so the projected type is borrowed and never freed.
		 * NOTE: BIT_FIELD_NULLABLE is indexed by BIT width (0..64); the others are byte-indexed. */
		uint32_t w = 0;
		if(src != NULL && src->dti_p != NULL)
			w = (scalar == RHENDB_BIT_FIELD) ? src->dti_p->bit_field_size : native_width(src);
		return static_dti_for(scalar, w);
	}

	transaction* tx = tx_from_ctx(ec_p);
	if(tx == NULL)
		return NULL;
	const page_access_specs* vpas = &(tx->rdb->volatile_rage_engine.pam_p->pas);

	data_type_info* ext = NULL;
	if(scalar == RHENDB_STRING)
	{
		data_type_info* inl = get_text_inline_type_info(PROJECTION_MAX_SIZE);
		ext = inl ? get_text_extended_type_info(VOLATILE_EXT_SUB_TYPE, PROJECTION_MAX_SIZE, inl, vpas) : NULL;
	}
	else if(scalar == RHENDB_BINARY)
	{
		data_type_info* inl = get_blob_inline_type_info(PROJECTION_MAX_SIZE);
		ext = inl ? get_blob_extended_type_info(VOLATILE_EXT_SUB_TYPE, PROJECTION_MAX_SIZE, inl, vpas) : NULL;
	}
	else if(scalar == RHENDB_NUMERIC)
	{
		data_type_info* inl = get_numeric_inline_type_info(PROJECTION_MAX_SIZE);
		ext = inl ? get_numeric_extended_type_info(VOLATILE_EXT_SUB_TYPE, PROJECTION_MAX_SIZE, inl, vpas) : NULL;
	}
	else
		return NULL;   // RHENDB_TUPLE / RHENDB_ARRAY

	if(ext != NULL && !finalize_type_info(ext))
	{
		destroy_type_info_recursively(ext, NULL);
		return NULL;
	}
	*should_free = (ext != NULL);
	return ext;
}

void destroy_projected_type_info(projected_type_info pti)
{
	if(pti.projected_type_info != NULL && pti.should_free_projected_type_info)
		destroy_type_info_recursively(pti.projected_type_info, NULL);
}

void destroy_projected_value(projected_value pv)
{
	if(pv.buffer_to_free != NULL)
		free(pv.buffer_to_free);
}

projected_type_info infer_projected_type_sql_expr_for_rhendb(sql_expression* expr, sql_expr_eval_context* ec_p, int* error_code)
{
	*error_code = 0;
	projected_type_info res = (projected_type_info){ .projected_type_info = NULL, .should_free_projected_type_info = 0 };

	expr_type_info* t = infer_type_sql_expr(expr, ec_p, error_code);
	if((*error_code) || t == NULL)
	{
		if(t != NULL) delete_type(t, ec_p);
		if((*error_code) == 0) *error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
		return res;
	}
	expr_type scalar = effective_type(t);    // an unmaterialized extended large-type acts as its scalar

	// a genuine tuple / array result (not an extended text/blob/numeric large type) projects AS-IS: BORROW
	// the inferred container type directly -- no clone. we take over the inferred type's own ownership bit,
	// clearing it there so delete_type() does not free a type we now hold.
	if((scalar == RHENDB_TUPLE || scalar == RHENDB_ARRAY) && t->dti_p != NULL)
	{
		res.projected_type_info = t->dti_p;
		res.should_free_projected_type_info = t->should_free_dti_p;
		t->should_free_dti_p = 0;
		delete_type(t, ec_p);
		return res;
	}
	/* build the projected type BEFORE releasing the inferred one : the builder reads its declared width */
	int should_free = 0;
	data_type_info* proj = build_projection_type_for_scalar(scalar, t, ec_p, &should_free);
	delete_type(t, ec_p);
	if(proj == NULL)
	{
		*error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
		return res;
	}
	res.projected_type_info = proj;
	res.should_free_projected_type_info = should_free;
	return res;
}

// pick the temporary extension store for an already-written prefix (in `holder`, of type `proj`).
static temporary_extension_store* project_pick_store(transaction* tx, void* holder, data_type_info* proj)
{
	datum prefix_c; const data_type_info* prefix_dti;
	datum whole = (datum){ .is_NULL = 0, .tuple_value = holder };
	if(!get_nested_containee_from_datum(&prefix_c, &prefix_dti, &whole, proj, EXTENDED_PREFIX_POS_ACC))
		prefix_c = (*NULL_DATUM);
	uint64_t h = hash_datum(&prefix_c, prefix_dti, FNV_64_TUPLE_HASHER) % TEMPORARY_EXTENSION_STORE_COUNT;
	return &(tx->temp_ext_stores[h]);
}

// write already-materialized text/blob bytes into a volatile extended value.
//
// the caller materializes the evaluated value first (materialize_tb), so by the time we get here the bytes
// are simply `data`/`data_size` -- no read iterator, no source lock held. that also means the destination
// store's write lock is never held while the source is being read, which would otherwise self-deadlock when
// the source is a volatile extended value hashing to the same store.
//
// two phases: fill the inline prefix against a dummy root (the write iterator only opens the blob store on
// its first OVERFLOW append, so a dummy root is safe while just the prefix is being filled), then hash the
// finished prefix to choose the store, take its write lock, point the iterator at the real root and that
// store's notifier, and append the remainder.
static int project_write_sb_to_volatile(transaction* tx, data_type_info* proj, const char* data, uint32_t data_size,
	datum* out_datum, void** out_buf, int* error_code)
{
	rage_engine* VE = &(tx->rdb->volatile_rage_engine);
	const page_access_specs* pas = &(VE->pam_p->pas);
	int abort_error = 0;

	/* the holder carries exactly one projected extended value, and that type's max_size IS
	 * PROJECTION_MAX_SIZE -- there is never a reason to reserve a whole page for it. */
	void* holder = malloc(PROJECTION_MAX_SIZE);
	if(holder == NULL) { *error_code = RHENDB_EE_OUT_OF_MEMORY; return 0; }
	tuple_def td; initialize_tuple_def(&td, proj);
	init_tuple(&td, holder);
	set_element_in_tuple(&td, SELF, holder, EMPTY_DATUM, PROJECTION_MAX_SIZE);

	binary_write_iterator* wr = get_new_binary_write_iterator(holder, &td, SELF, 0 /*dummy root*/,
		get_NULL_tuple_pointer(pas), PROJECTION_PREFIX_BYTES, &(VE->bstd), VE->pam_p, VE->pmm_p);

	uint32_t off = 0;
	// phase 1 : inline prefix only. never hand the writer more than the remaining prefix room, else it would
	// run past the prefix and open the blob store with the dummy root inside a single append call. the
	// iterator caps its own prefix capacity by the available inline space, so drive off its counters.
	while(off < data_size && wr->bytes_written_to_prefix < wr->bytes_to_be_written_to_prefix && !abort_error)
	{
		uint32_t room = wr->bytes_to_be_written_to_prefix - wr->bytes_written_to_prefix;
		uint32_t give = (data_size - off) < room ? (data_size - off) : room;
		uint32_t wrote = append_to_binary_write_iterator(wr, data + off, give, NULL, NULL, &abort_error);
		if(abort_error || wrote == 0) { if(!abort_error) abort_error = 1; break; }
		off += wrote;
	}

	// phase 2 : the remainder goes into the store selected by hashing the finished prefix
	temporary_extension_store* store = NULL;
	if(!abort_error && off < data_size)
	{
		store = project_pick_store(tx, holder, proj);
		write_lock(&(store->blob_store_lock), BLOCKING);
		wr->blob_store_root_page_id = store->blob_store_root_page_id;
		const heap_table_notifier* notify = &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(store->htan));
		while(off < data_size && !abort_error)
		{
			uint32_t wrote = append_to_binary_write_iterator(wr, data + off, data_size - off, notify, NULL, &abort_error);
			if(abort_error || wrote == 0) { if(!abort_error) abort_error = 1; break; }
			off += wrote;
			fix_unused_space_entries_in_store(tx, store);   // lazy : the transaction decides when
		}
	}

	delete_binary_write_iterator(wr, NULL, &abort_error);
	if(store != NULL)
	{
		fix_unused_space_entries_in_store(tx, store);   // lazy : below its threshold this is a no-op
		write_unlock(&(store->blob_store_lock));
	}

	int ok = !abort_error && (off == data_size);
	if(ok)
	{
		uint32_t sz = get_tuple_size(&td, holder);
		void* copy = malloc(sz ? sz : 1);
		if(copy == NULL) { ok = 0; *error_code = RHENDB_EE_OUT_OF_MEMORY; }
		else { memcpy(copy, holder, sz); *out_buf = copy; *out_datum = (datum){ .is_NULL = 0, .tuple_value = copy }; }
	}
	if(!ok && (*error_code) == 0) *error_code = RHENDB_EE_MATERIALIZE_FAILED;
	free(holder);
	return ok;
}

static int project_write_numeric_to_volatile(transaction* tx, data_type_info* proj, const mpd_t* d,
	datum* out_datum, void** out_buf, int* error_code)
{
	rage_engine* VE = &(tx->rdb->volatile_rage_engine);
	const page_access_specs* pas = &(VE->pam_p->pas);

	int exp_too_big = 0;
	materialized_numeric mn = decimal_to_materialized_numeric(d, &exp_too_big);
	if(exp_too_big) { *error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION; return 0; }
	numeric_sign_bits sb; int16_t ex; get_sign_bits_and_exponent_for_materialized_numeric(&mn, &sb, &ex);
	uint32_t nd = get_digits_count_for_materialized_numeric(&mn);

	// gather the digits into one contiguous array so they can be appended in bulk
	uint64_t* digits = malloc((nd ? nd : 1) * sizeof(uint64_t));
	if(digits == NULL) { deinitialize_materialized_numeric(&mn); *error_code = RHENDB_EE_OUT_OF_MEMORY; return 0; }
	for(uint32_t i = 0; i < nd; i++) digits[i] = get_nth_digit_from_materialized_numeric(&mn, i);

	int abort_error = 0;
	/* one projected extended numeric, max_size PROJECTION_MAX_SIZE : no page-sized buffer needed */
	void* holder = malloc(PROJECTION_MAX_SIZE);
	if(holder == NULL) { free(digits); deinitialize_materialized_numeric(&mn); *error_code = RHENDB_EE_OUT_OF_MEMORY; return 0; }
	tuple_def td; initialize_tuple_def(&td, proj);
	init_tuple(&td, holder);
	set_element_in_tuple(&td, SELF, holder, EMPTY_DATUM, PROJECTION_MAX_SIZE);
	set_sign_bits_and_exponent_for_numeric(sb, ex, holder, &td, SELF);

	digit_write_iterator* wr = get_new_digit_write_iterator(holder, &td, SELF, 0 /*dummy root*/,
		get_NULL_tuple_pointer(pas), PROJECTION_PREFIX_BYTES / BYTES_PER_NUMERIC_DIGIT, &(VE->bstd), VE->pam_p, VE->pmm_p);

	temporary_extension_store* store = NULL;
	uint32_t off = 0;

	// ---- phase 1 : write only the prefix digits (dummy root, no blob store touched). the iterator caps its
	// own prefix capacity by the available inline space, so drive the loop by digits_to_be_written_to_prefix
	// and never hand it more than the remaining prefix room in a single append. ----
	while(off < nd && wr->digits_written_to_prefix < wr->digits_to_be_written_to_prefix && !abort_error)
	{
		uint32_t room = wr->digits_to_be_written_to_prefix - wr->digits_written_to_prefix;
		uint32_t give = (nd - off) < room ? (nd - off) : room;
		uint32_t wrote = append_to_digit_write_iterator(wr, digits + off, give, NULL, NULL, &abort_error);
		if(abort_error || wrote == 0) { if(!abort_error) abort_error = 1; break; }
		off += wrote;
	}

	// ---- phase 2 : if digits remain, hash the finished prefix, pick + lock the store, repoint the iterator
	// at that store's real root, and stream the remaining digits in bulk under the write lock. ----
	if(!abort_error && off < nd)
	{
		store = project_pick_store(tx, holder, proj);
		write_lock(&(store->blob_store_lock), BLOCKING);
		wr->blob_store_root_page_id = store->blob_store_root_page_id;
		const heap_table_notifier* notify = &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(store->htan));
		while(off < nd && !abort_error)
		{
			uint32_t wrote = append_to_digit_write_iterator(wr, digits + off, nd - off, notify, NULL, &abort_error);
			if(abort_error || wrote == 0) { if(!abort_error) abort_error = 1; break; }
			off += wrote;
			fix_unused_space_entries_in_store(tx, store);   // lazy : the transaction decides when
		}
	}

	delete_digit_write_iterator(wr, NULL, &abort_error);
	if(store != NULL)
	{
		fix_unused_space_entries_in_store(tx, store);   // lazy : below its threshold this is a no-op
		write_unlock(&(store->blob_store_lock));
	}

	int ok = !abort_error;
	if(ok)
	{
		uint32_t sz = get_tuple_size(&td, holder);
		void* copy = malloc(sz ? sz : 1);
		if(copy == NULL) { ok = 0; *error_code = RHENDB_EE_OUT_OF_MEMORY; }
		else { memcpy(copy, holder, sz); *out_buf = copy; *out_datum = (datum){ .is_NULL = 0, .tuple_value = copy }; }
	}
	if(!ok && (*error_code) == 0) *error_code = RHENDB_EE_MATERIALIZE_FAILED;
	free(holder); free(digits);
	deinitialize_materialized_numeric(&mn);
	return ok;
}

projected_value project_using_evaluate_sql_expr_for_rhendb(sql_expression* expr, sql_expr_eval_context* ec_p, projected_type_info pti, int* error_code)
{
	*error_code = 0;
	projected_value res = (projected_value){ .value = (datum){ .is_NULL = 1 }, .buffer_to_free = NULL };
	data_type_info* projection_type_info = pti.projected_type_info;

	// projection only ever produces a VOLATILE extended type; a persistent target is rejected outright.
	if(has_extended_type_info(projection_type_info, PERSISTENT_EXT_SUB_TYPE))
	{
		*error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
		return res;
	}

	transaction* tx = tx_from_ctx(ec_p);

	// the target is the type inference handed us, so its kind already matches the result.
	int to_ext_num  = has_extended_type_info(projection_type_info, VOLATILE_EXT_SUB_TYPE) && is_numeric_type_info(projection_type_info);
	int to_ext_text = has_extended_type_info(projection_type_info, VOLATILE_EXT_SUB_TYPE) && is_text_type_info(projection_type_info);
	int to_ext_blob = has_extended_type_info(projection_type_info, VOLATILE_EXT_SUB_TYPE) && is_blob_type_info(projection_type_info);

	expr_value* v = evaluate_sql_expr(expr, ec_p, error_code);
	if(*error_code)
		return res;
	if(v == NULL)             // a NULL result with no error : the projected value is SQL NULL
	{
		res.value = (*NULL_DATUM);
		return res;
	}

	// SQL three-valued logic: a boolean expression may evaluate to UNKNOWN, which the standard treats as
	// the null value (SQL:2003 -- a boolean site holding UNKNOWN is null). the evaluator returns the static
	// unknown_bool singleton for it, whose datum carries bit_field_value 0 with is_NULL clear, so without
	// this it would project as plain FALSE. TRUE/FALSE fall through to the normal bit-field path.
	if((void*)v == ec_p->unknown_bool)
	{
		res.value = (*NULL_DATUM);
		delete_data(v, ec_p);     // no-op for the static singletons
		return res;
	}

	// ---- the value already HAS the projected type : hand its datum straight back, nothing to rewrite ----
	// (an already-projected volatile extended value, or any tuple/array, whose type we borrowed verbatim)
	if(v->type_info.dti_p != NULL && are_identical_type_info(v->type_info.dti_p, projection_type_info))
	{
		res.value = v->value;
		if(v->buffer != NULL)          // the value owned these bytes : take that ownership over
		{
			res.buffer_to_free = v->buffer;
			v->buffer = NULL;
			v->capacity = 0;
		}
		delete_data(v, ec_p);
		return res;
	}

	// ---- extended volatile numeric target ----
	if(to_ext_num)
	{
		mpd_t scratch = (mpd_t){0}; int owns = 0;
		mpd_t* dp = operand_to_mpd(v, &scratch, &owns, ec_p, error_code, RHENDB_EE_INCOMPATIBLE_PROJECTION);
		if(dp == NULL) { delete_data(v, ec_p); return res; }
		datum od; void* ob = NULL;
		int ok = project_write_numeric_to_volatile(tx, projection_type_info, dp, &od, &ob, error_code);
		if(owns) mpd_del(&scratch);
		delete_data(v, ec_p);
		if(!ok) return res;
		res.value = od; res.buffer_to_free = ob;
		return res;
	}

	// ---- extended volatile text / blob target ----
	// materialize first (a no-op for an already-native string/binary; for an extended value this reads it
	// through its own callback + engine), then the bytes are simply the datum's -- write those out.
	if(to_ext_text || to_ext_blob)
	{
		if(materialize_tb(v, ec_p, error_code) != RHENDB_EE_OK)
		{
			delete_data(v, ec_p);
			return res;
		}
		if(!is_sb_operand(v))
		{
			delete_data(v, ec_p);
			*error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
			return res;
		}
		datum od; void* ob = NULL;
		int ok = project_write_sb_to_volatile(tx, projection_type_info,
			v->value.string_or_binary_value, v->value.string_or_binary_size, &od, &ob, error_code);
		delete_data(v, ec_p);
		if(!ok) return res;
		res.value = od; res.buffer_to_free = ob;
		return res;
	}

	// ---- tuple / array target : an identical type was already returned as-is above, so anything reaching
	// here is a genuine mismatch ----
	if(projection_type_info != NULL && is_container_type_info(projection_type_info) && !is_extended_type_info(projection_type_info))
	{
		delete_data(v, ec_p);
		*error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
		return res;
	}

	// ---- native scalar target : hand back the value directly, preserving its full width ----
	if(projection_type_info != NULL && !is_extended_type_info(projection_type_info))
	{
		datum out = (datum){ .is_NULL = 0 };
		switch(projection_type_info->type)
		{
			case FLOAT:
				/* a 4-byte FLOAT target stores into float_value, an 8-byte one into double_value */
				if(projection_type_info->size == sizeof(float)) out.float_value = (float)to_dbl(v);
				else                                            out.double_value = to_dbl(v);
				break;
			case BIT_FIELD:
			case UINT:       out.uint_value       = to_u64(v);  break;
			case INT:        out.int_value        = to_i64(v);  break;
			// large uint/int are 256-bit (32 bytes) : never truncate through a 64-bit path
			case LARGE_UINT: out.large_uint_value = to_u256(v); break;
			case LARGE_INT:  out.large_int_value  = to_i256(v); break;
			default:
				delete_data(v, ec_p);
				*error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
				return res;
		}
		delete_data(v, ec_p);
		res.value = out;
		return res;
	}

	delete_data(v, ec_p);
	*error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
	return res;
}