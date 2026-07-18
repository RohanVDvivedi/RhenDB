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
static int et_is_num(expr_type t)
{
	return et_is_int(t) || t == RHENDB_FLOAT;
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
static expr_type num_result(expr_type a, expr_type b)
{
	if(a == RHENDB_NUMERIC || b == RHENDB_NUMERIC)
		return RHENDB_NUMERIC;

	if(a == RHENDB_FLOAT || b == RHENDB_FLOAT)
		return RHENDB_FLOAT;

	int signd = et_is_signed(a) || et_is_signed(b);

	if(et_is_large(a) || et_is_large(b))
		return signd ? RHENDB_LARGE_INT : RHENDB_LARGE_UINT;

	return signd ? RHENDB_INT : RHENDB_UINT;
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

/* discriminators */
static int is_materialized_numeric(const expr_value* v)
{
	return v->type_info.type == RHENDB_NUMERIC;
}
static int is_tuple_numeric(const expr_value* v)
{
	return v->type_info.dti_p != NULL && is_numeric_type_info(v->type_info.dti_p);
}
static int is_numeric_operand(const expr_value* v)
{
	return is_materialized_numeric(v) || is_tuple_numeric(v);
}

/* a string/binary operand: a native RHENDB_STRING/BINARY, or a tuple-form text/blob column */
static int is_sb_operand(const expr_value* v)
{
	if(v->type_info.dti_p != NULL)
		return is_text_type_info(v->type_info.dti_p) || is_blob_type_info(v->type_info.dti_p);
	return v->type_info.type == RHENDB_STRING || v->type_info.type == RHENDB_BINARY;
}

static double to_dbl(const expr_value* v){
	switch(v->type_info.type){
		case RHENDB_BIT_FIELD:
		case RHENDB_UINT:
			return (double)v->value.uint_value;
		case RHENDB_INT:
			return (double)v->value.int_value;
		case RHENDB_FLOAT:
			return v->value.double_value;
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
	return (v->type_info.type == RHENDB_INT) ? (uint64_t)v->value.int_value : v->value.uint_value;
}
static int64_t to_i64(const expr_value* v)
{
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
			uint32_t nc = cap * 2;
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
		len += got;
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
	if(v->type_info.dti_p == NULL)
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
	mpd_maxcontext(&ctx);
	uint32_t st = 0;
	char b[256];
	switch(v->type_info.type){
		case RHENDB_BIT_FIELD:
		case RHENDB_UINT:
			mpd_qset_u64(out, v->value.uint_value, &ctx, &st); break;
		case RHENDB_INT:
			mpd_qset_i64(out, v->value.int_value, &ctx, &st); break;
		case RHENDB_FLOAT:
			snprintf(b, sizeof(b), "%.17g", v->value.double_value);
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
	if(et_is_num(v->type_info.type) && v->type_info.dti_p == NULL)
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

		mpd_context_t ctx; mpd_maxcontext(&ctx); uint32_t st = 0;
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
	if(a->type_info.dti_p != NULL || b->type_info.dti_p != NULL || !et_is_num(ta) || !et_is_num(tb))
	{
		*error_code = RHENDB_EE_NON_NUMERIC_OPERAND;
		return NULL;
	}
	expr_type rt = num_result(ta, tb);

	if(rt == RHENDB_FLOAT){
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
		expr_value* v = new_val(RHENDB_FLOAT, ec_p);
		v->value.double_value = r;
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
			truthy = (v->value.double_value != 0);   /* NaN != 0 -> true */
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
	if(v->type_info.dti_p != NULL)
		return v->type_info.dti_p;
	switch(v->type_info.type){
		case RHENDB_BIT_FIELD: return BIT_FIELD_NULLABLE[64];
		case RHENDB_UINT: return UINT_NULLABLE[8];
		case RHENDB_INT: return INT_NULLABLE[8];
		case RHENDB_FLOAT: return FLOAT_double_NULLABLE;
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
		int a_ext = (a->type_info.dti_p != NULL), b_ext = (b->type_info.dti_p != NULL);
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

	if(a->type_info.dti_p != NULL || b->type_info.dti_p != NULL || !et_is_int(ta) || !et_is_int(tb))
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
	if(a->type_info.dti_p != NULL || !et_is_int(t))
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
	if(a->type_info.dti_p != NULL || s->type_info.dti_p != NULL || !et_is_int(t) || !et_is_int(s->type_info.type))
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
		v = new_val(RHENDB_FLOAT, ec_p);
		v->value.double_value = strtod(buf, NULL);
	}
	else if(strchr(buf, '.'))   /* a fraction, no exponent -> exact NUMERIC (SQL exact-numeric literal) */
	{
		mpd_t d;
		if(!ee_mpd_new(&d)){ if(buf != stackbuf) free(buf); *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
		mpd_context_t ctx; mpd_maxcontext(&ctx); uint32_t st = 0;
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
				else   /* more digits than a 256-bit integer can hold -> FLOAT */
				{
					v = new_val(RHENDB_FLOAT, ec_p);
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
	mpd_context_t ctx; mpd_maxcontext(&ctx); uint32_t st = 0;
	mpd_qtrunc(&t, m, &ctx, &st);                  /* drop the fraction, toward zero */
	char* s = mpd_qformat(&t, "f", &ctx, &st);     /* plain integer digits, never an exponent */
	if(s == NULL){ mpd_del(&t); return 0; }
	*out = decimal_str_to_int_bits(s);
	mpd_free(s);
	mpd_del(&t);
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

static void* rhendb_cast(void* data, const void* to_type, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* a = data;
	expr_type target = ((const expr_type_info*)to_type)->type;

	/* bring an extended (tuple-form) source down to its scalar representation first */
	if(is_numeric_operand(a)){ if(materialize_numeric(a, ec_p, error_code)) return NULL; }
	else if(is_sb_operand(a)){ if(materialize_tb(a, ec_p, error_code)) return NULL; }

	int src_is_numeric = (a->type_info.type == RHENDB_NUMERIC);
	int src_is_number  = (a->type_info.dti_p == NULL && et_is_num(a->type_info.type));   /* native bit/int/float */
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
			if(a->type_info.type == RHENDB_FLOAT)
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
			bits = decimal_str_to_int_bits(s);
			free(s);
		}
		else { *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }

		expr_value* v = new_val(target, ec_p);
		store_int_bits(v, target, bits);
		return v;
	}

	/* FLOAT target */
	if(target == RHENDB_FLOAT)
	{
		double d;
		if(src_is_numeric)     d = mpd_to_double(&(a->numeric_value));
		else if(src_is_number) d = to_dbl(a);
		else if(src_is_sb){ char* s = sb_to_cstring(a); if(s == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; } d = strtod(s, NULL); free(s); }
		else { *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }
		expr_value* v = new_val(RHENDB_FLOAT, ec_p); v->value.double_value = d; return v;
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
			if(!ee_mpd_new(&d)){ free(s); *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			mpd_context_t ctx; mpd_maxcontext(&ctx); uint32_t st = 0;
			mpd_qset_string(&d, s, &ctx, &st);
			free(s);
		}
		else { *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }
		expr_value* v = new_val(RHENDB_NUMERIC, ec_p); v->numeric_value = d; return v;
	}

	*error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL;   /* STRING/BINARY targets not implemented yet */
}

/* ------------------------------ type inference ------------------------------ */

static void* rhendb_get_type_for_data(void* data, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* v = data;
	expr_type_info* ti = new_type(v->type_info.type);
	if(ti->type == RHENDB_TUPLE || ti->type == RHENDB_ARRAY)
		ti->dti_p = v->type_info.dti_p;
	return ti;
}
static void* rhendb_get_type_for_sql_type(const sql_type* type, const sql_expr_eval_context* ec_p, int* error_code)
{
	switch(type->type_name)
	{
		case SQL_BOOL: return (void*)(&rhendb_bool_type);
		case SQL_SMALLINT: case SQL_INT: case SQL_BIGINT: return new_type(RHENDB_INT);
		case SQL_REAL: case SQL_DOUBLE: case SQL_FLOAT: return new_type(RHENDB_FLOAT);
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
	/* aligned with rhendb_cast : a number target is reachable from any number, or by parsing a
	 * string/binary; string/binary cast *targets* are not implemented yet. */
	if(et_is_num_or_numeric(to) && (et_is_num_or_numeric(from) || from == RHENDB_STRING || from == RHENDB_BINARY)) return 1;
	return 0;
}
static void* rhendb_get_return_type_for_op_exec_callback(void* op_exec_func, void* typ1, void* typ2, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_type a = effective_type((expr_type_info*)typ1);
	expr_type b = (typ2 != NULL) ? effective_type((expr_type_info*)typ2) : a;   /* typ2 is NULL for unary ops */
	if(op_exec_func==(void*)ec_p->add||op_exec_func==(void*)ec_p->sub||op_exec_func==(void*)ec_p->mul||op_exec_func==(void*)ec_p->div||op_exec_func==(void*)ec_p->mod){
		if(!et_is_num_or_numeric(a) || !et_is_num_or_numeric(b)){ *error_code = RHENDB_EE_NON_NUMERIC_OPERAND; return NULL; }
		return new_type(num_result(a, b));
	}
	if(op_exec_func==(void*)ec_p->bit_and||op_exec_func==(void*)ec_p->bit_or||op_exec_func==(void*)ec_p->bit_xor){
		if(!et_is_int(a) || !et_is_int(b)){ *error_code = RHENDB_EE_NON_INTEGER_OPERAND; return NULL; }
		return new_type(num_result(a, b));
	}
	if(op_exec_func==(void*)ec_p->left_shift||op_exec_func==(void*)ec_p->right_shift){
		if(!et_is_int(a) || !et_is_int(b)){ *error_code = RHENDB_EE_NON_INTEGER_OPERAND; return NULL; }
		return new_type(a);
	}
	if(op_exec_func==(void*)ec_p->bit_not){
		if(!et_is_int(a)){ *error_code = RHENDB_EE_NON_INTEGER_OPERAND; return NULL; }
		return new_type(a);
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
		return new_type(a);
	if(et_is_num_or_numeric(a) && et_is_num_or_numeric(b))
		return new_type(num_result(a, b));
	*error_code = RHENDB_EE_INCOMPARABLE_TYPES;
	return NULL;
}

/* ------------------------------ variables (column references) ------------------------------ */

/* "a.b.c" is resolved once against the schema and cached; the cache lives for the life of the
 * context, so the same reference is only parsed and located once and then read every row.
 * With two or more input tuples the first component is the table name (a string, matched against
 * a tuple_def's type_name); every remaining component is a field name (a string) OR an integer
 * (an array/tuple index). */
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
		case FLOAT: return RHENDB_FLOAT;
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

/* parse "a.b.c" (from a dstring), pick the tuple and field path, and build the positional accessor */
static int resolve_into(rhendb_expr_eval_context* ctx, const dstring* id, uint32_t* out_tuple_index, positional_accessor* out_pa, const data_type_info** out_dti, int* error_code)
{
	#define EE_MAX_PATH 16

	const char* key = get_byte_array_dstring(id);
	cy_uint klen = get_char_count_dstring(id);

	const char* comp_ptr[EE_MAX_PATH]; uint32_t comp_len[EE_MAX_PATH]; uint32_t ncomp = 0;
	cy_uint i = 0;
	while(i < klen && ncomp < EE_MAX_PATH){
		cy_uint start = i;
		while(i < klen && key[i] != '.') i++;
		uint32_t clen = (uint32_t)(i - start);
		if(clen > 64)   /* type/field names are null-terminated strings bounded to 64 bytes */
		{
			*error_code = RHENDB_EE_UNSUPPORTED_TYPE;
			return 0;
		}
		comp_ptr[ncomp] = key + start; comp_len[ncomp] = clen; ncomp++;
		if(i < klen && key[i] == '.') i++;
	}
	if(ncomp == 0)
	{
		*error_code = RHENDB_EE_UNSUPPORTED_TYPE;
		return 0;
	}

	char name[65];

	uint32_t tuple_index = 0, field_start = 0;
	if(ctx->input_tuples_count >= 2)
	{
		/* first component is the table name (always a string) */
		uint32_t l = comp_len[0]; memory_move(name, comp_ptr[0], l); name[l] = 0;
		int found = -1;
		for(uint32_t t = 0; t < ctx->input_tuples_count; t++){
			if(ctx->input_tuple_defs[t] != NULL && ctx->input_tuple_defs[t]->type_info != NULL
			   && strncmp(ctx->input_tuple_defs[t]->type_info->type_name, name, 64) == 0){ found = (int)t; break; }
		}
		if(found < 0)   /* unknown table */
		{
			*error_code = RHENDB_EE_UNSUPPORTED_TYPE;
			return 0;
		}
		tuple_index = (uint32_t)found;
		field_start = 1;
	}
	else
	{
		/* single tuple: whole path is a field path, but tolerate a leading table name */
		tuple_index = 0; field_start = 0;
		if(ncomp >= 2 && ctx->input_tuple_defs[0] != NULL && ctx->input_tuple_defs[0]->type_info != NULL)
		{
			uint32_t l = comp_len[0]; memory_move(name, comp_ptr[0], l); name[l] = 0;
			if(strncmp(ctx->input_tuple_defs[0]->type_info->type_name, name, 64) == 0) field_start = 1;
		}
	}
	if(field_start >= ncomp)    /* no field named */
	{
		*error_code = RHENDB_EE_UNSUPPORTED_TYPE;
		return 0;
	}

	uint32_t positions[EE_MAX_PATH]; uint32_t depth = 0;
	data_type_info* cur = ctx->input_tuple_defs[tuple_index]->type_info;
	for(uint32_t c = field_start; c < ncomp; c++){
		if(!is_container_type_info(cur)){ *error_code = RHENDB_EE_UNSUPPORTED_TYPE; return 0; }
		uint32_t idx;
		if(!component_as_index(comp_ptr[c], comp_len[c], &idx)){   /* a field name -> look it up */
			uint32_t l = comp_len[c]; memory_move(name, comp_ptr[c], l); name[l] = 0;
			idx = find_containee_using_field_name_in_tuple_type_info(cur, name);
			if(idx == UINT32_MAX){ *error_code = RHENDB_EE_UNSUPPORTED_TYPE; return 0; }   /* no such field */
		}
		positions[depth++] = idx;
		cur = get_data_type_info_for_containee_of_container_without_data(cur, idx);
		if(cur == NULL){ *error_code = RHENDB_EE_UNSUPPORTED_TYPE; return 0; }   /* index out of bounds / not a container */
	}

	out_pa->positions_length = depth;
	out_pa->positions = malloc((depth ? depth : 1) * sizeof(uint32_t));
	if(out_pa->positions == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return 0; }
	memory_move(out_pa->positions, positions, depth * sizeof(uint32_t));

	*out_tuple_index = tuple_index;
	*out_dti = cur;
	return 1;

	#undef EE_MAX_PATH
}

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
		v->type_info.dti_p = NULL;
		/* a native FLOAT column may be stored as a 4-byte float (the reader puts it in float_value);
		 * RHENDB_FLOAT is always a double, so promote it up to double_value. */
		if(dti->type == FLOAT && dti->size == sizeof(float))
			v->value.double_value = (double)v->value.float_value;
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
	if(t == RHENDB_TUPLE || t == RHENDB_ARRAY)
		ti->dti_p = (data_type_info*)e->column_dti;   /* borrowed */
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

int has_reference_to_extended_type_from_expression(const rhendb_expr_eval_context* context_p)
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

int is_valid_using_infer_sql_expr_for_rhendb(sql_expr_eval_context* ec_p, sql_expression* expr)
{
	int error_code = 0;
	void* t = infer_type_sql_expr(expr, ec_p, &error_code);
	if(t != NULL)
		delete_type(t, ec_p);
	return (error_code == 0) ? 1 : 0;
}

// the inline prefix budget of a projected extended value, and the total inline max_size (leaving room after
// the prefix for the blob pointer (page id up to 8+4 bytes), a 4-byte offset, the inline array size header
// and a few spare bytes). for numeric the prefix holds floor(90 / BYTES_PER_NUMERIC_DIGIT) radix-10^12
// digit slots.
#define PROJECTION_PREFIX_BYTES   90
#define PROJECTION_MAX_SIZE       128
// once this many wrong-unused-space notifications have accumulated in a store's htan, drain them
#define PROJECTION_HTAN_FIX_THRESHOLD  20

// build the data_type_info a scalar result of kind `scalar` is projected into. string/binary/numeric become
// volatile-store extended types (freshly allocated); native scalars map to their default type_info. the
// result is always safe for destroy_type_info_recursively() (defaults are is_static). NULL for tuple/array
// or a missing transaction.
static data_type_info* build_projection_type_for_scalar(expr_type scalar, const sql_expr_eval_context* ec_p)
{
	switch(scalar)
	{
		// native scalars : matching default (nullable, so a projected NULL fits too). these are is_static,
		// hence destroy_type_info_recursively() on them is a safe no-op.
		case RHENDB_BIT_FIELD:  return BIT_FIELD_NULLABLE[8];
		case RHENDB_UINT:       return UINT_NULLABLE[8];
		case RHENDB_INT:        return INT_NULLABLE[8];
		case RHENDB_FLOAT:      return FLOAT_double_NULLABLE;
		case RHENDB_LARGE_UINT: return LARGE_UINT_NULLABLE[32];
		case RHENDB_LARGE_INT:  return LARGE_INT_NULLABLE[32];
		default: break;
	}

	transaction* tx = tx_from_ctx(ec_p);
	if(tx == NULL)
		return NULL;
	const page_access_specs* vpas = &(tx->rdb->volatile_rage_engine.pam_p->pas);

	if(scalar == RHENDB_STRING)
	{
		data_type_info* inl = get_text_inline_type_info(PROJECTION_PREFIX_BYTES);
		return inl ? get_text_extended_type_info(VOLATILE_EXT_SUB_TYPE, PROJECTION_MAX_SIZE, inl, vpas) : NULL;
	}
	if(scalar == RHENDB_BINARY)
	{
		data_type_info* inl = get_blob_inline_type_info(PROJECTION_PREFIX_BYTES);
		return inl ? get_blob_extended_type_info(VOLATILE_EXT_SUB_TYPE, PROJECTION_MAX_SIZE, inl, vpas) : NULL;
	}
	if(scalar == RHENDB_NUMERIC)
	{
		data_type_info* inl = get_numeric_inline_type_info(PROJECTION_PREFIX_BYTES);
		return inl ? get_numeric_extended_type_info(VOLATILE_EXT_SUB_TYPE, PROJECTION_MAX_SIZE, inl, vpas) : NULL;
	}
	return NULL;   // RHENDB_TUPLE / RHENDB_ARRAY
}

data_type_info* infer_projected_type_sql_expr_for_rhendb(sql_expr_eval_context* ec_p, sql_expression* expr, int* error_code)
{
	*error_code = 0;
	expr_type_info* t = infer_type_sql_expr(expr, ec_p, error_code);
	if((*error_code) || t == NULL)
	{
		if(t != NULL) delete_type(t, ec_p);
		return NULL;
	}
	expr_type scalar = effective_type(t);    // an unmaterialized extended large-type acts as its scalar

	// a genuine tuple / array result (one that is NOT an extended text/blob/numeric large type) is projected
	// AS-IS: the projected type is that very data_type_info -- exactly as a tuple/array column would be
	// stored. clone it so the returned type is owned by the caller (safe to destroy) and does not alias the
	// inferred-type object we are about to delete.
	if((scalar == RHENDB_TUPLE || scalar == RHENDB_ARRAY) && t->dti_p != NULL)
	{
		int ae = 0;
		data_type_info* proj = clone_type_info_recursively(t->dti_p, &ae, NULL, NULL);
		delete_type(t, ec_p);
		if(ae || proj == NULL)
		{
			*error_code = RHENDB_EE_OUT_OF_MEMORY;
			return NULL;
		}
		return proj;
	}
	delete_type(t, ec_p);

	data_type_info* proj = build_projection_type_for_scalar(scalar, ec_p);
	if(proj == NULL)
		*error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
	return proj;
}

// drain a store's accumulated wrong-unused-space notifications back into the blob store, but only once they
// cross the threshold (or when forced at the end of a write).
static void project_fix_unused_space(transaction* tx, temporary_extension_store* store, int force)
{
	uint32_t pending = get_notification_count_for_heap_table_accumulative_notifier(&(store->htan));
	if(pending == 0 || (!force && pending < PROJECTION_HTAN_FIX_THRESHOLD))
		return;
	rage_engine* VE = &(tx->rdb->volatile_rage_engine);
	int ae = 0; uint64_t root, page; uint32_t unused;
	while(pop_from_heap_table_accumulative_notifier(&(store->htan), &root, &unused, &page))
		fix_unused_space_in_heap_table(root, unused, page, &(VE->bstd.httd), VE->pam_p, VE->pmm_p, NULL, &ae);
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

// project a RHENDB_STRING/BINARY value into a volatile extended text/blob:
//   read the evaluated bytes via a read iterator (peeking, no copy), streaming them into a write iterator.
//   the first PROJECTION_PREFIX_BYTES land in the inline prefix (dummy root); if any bytes remain we hash
//   the finished prefix, point the write iterator at the chosen store (root + its htan) and continue under
//   that store's write lock.
static int project_write_sb_to_volatile(transaction* tx, data_type_info* proj, expr_value* v,
	const sql_expr_eval_context* ec_p, datum* out_datum, void** out_buf, int* error_code)
{
	rage_engine* VE = &(tx->rdb->volatile_rage_engine);
	const page_access_specs* pas = &(VE->pam_p->pas);

	// a reader over the source value. a materialized RHENDB_STRING/BINARY is read as an inline datum with
	// dti = NULL; an unmaterialized extended value is read via its own dti + engine + lock callback.
	int src_extended = (v->type_info.dti_p != NULL);
	extension_reader_iterator_callback cbs; extension_reader_iterator_callback* cb = NULL;
	rage_engine* seng = NULL;
	const data_type_info* sdti = NULL;
	datum sval;
	if(src_extended)
	{
		sdti = v->type_info.dti_p;
		seng = engine_and_callback_from_ctx(ec_p, sdti, &cbs, &cb);
		sval = v->value;
	}
	else
	{
		// inline: reader treats {string_value,string_size} as the whole value (dti = NULL)
		sval = v->value;
	}

	int abort_error = 0;
	void* holder = malloc(pas->page_size);
	if(holder == NULL) { *error_code = RHENDB_EE_OUT_OF_MEMORY; return 0; }
	tuple_def td; initialize_tuple_def(&td, proj);
	init_tuple(&td, holder);
	set_element_in_tuple(&td, SELF, holder, EMPTY_DATUM, pas->page_size);

	binary_read_iterator* rd = get_new_binary_read_iterator(&sval, sdti,
		seng ? &(seng->bstd) : NULL, seng ? seng->pam_p : NULL, cb);
	// write iterator starts with a DUMMY root; we fill ONLY the inline prefix first (no blob store touched).
	binary_write_iterator* wr = get_new_binary_write_iterator(holder, &td, SELF, 0 /*dummy root*/,
		get_NULL_tuple_pointer(pas), PROJECTION_PREFIX_BYTES, &(VE->bstd), VE->pam_p, VE->pmm_p);

	temporary_extension_store* store = NULL;   // chosen (and locked) only once the prefix is full

	// ---- phase 1 : fill the inline prefix. give at most the remaining prefix budget per step so the writer
	// never overflows into the (still dummy-rooted) blob store. ----
	while(wr->bytes_written_to_prefix < wr->bytes_to_be_written_to_prefix)
	{
		uint32_t avail = 0;
		const char* chunk = peek_in_binary_read_iterator(rd, &avail, NULL, &abort_error);
		if(abort_error || avail == 0) break;   // source no longer than the prefix : no overflow needed
		uint32_t room = wr->bytes_to_be_written_to_prefix - wr->bytes_written_to_prefix;
		uint32_t give = avail < room ? avail : room;
		uint32_t wrote = append_to_binary_write_iterator(wr, chunk, give, NULL, NULL, &abort_error);
		if(abort_error || wrote == 0) break;
		char sink[128]; uint32_t left = wrote;
		while(left > 0 && !abort_error)
		{
			uint32_t take = left < sizeof(sink) ? left : (uint32_t)sizeof(sink);
			uint32_t got = read_from_binary_read_iterator(rd, sink, take, NULL, &abort_error);
			if(got == 0) break;
			left -= got;
		}
		if(abort_error) break;
	}

	// ---- phase 2 : if source bytes remain, hash the finished prefix, pick + lock the store, repoint the
	// write iterator at that store's real root, and stream the remainder into it under the write lock. ----
	if(!abort_error)
	{
		uint32_t avail = 0;
		const char* chunk = peek_in_binary_read_iterator(rd, &avail, NULL, &abort_error);
		if(!abort_error && avail > 0)
		{
			store = project_pick_store(tx, holder, proj);
			write_lock(&(store->blob_store_lock), BLOCKING);
			wr->blob_store_root_page_id = store->blob_store_root_page_id;
			const heap_table_notifier* notify = &HEAP_TABLE_ACCUMULATIVE_NOTIFIER(&(store->htan));

			while(!abort_error)
			{
				if(avail == 0)
				{
					chunk = peek_in_binary_read_iterator(rd, &avail, NULL, &abort_error);
					if(abort_error || avail == 0) break;
				}
				uint32_t wrote = append_to_binary_write_iterator(wr, chunk, avail, notify, NULL, &abort_error);
				if(abort_error || wrote == 0) { if(!abort_error) abort_error = 1; break; }
				char sink[512]; uint32_t left = wrote;
				while(left > 0 && !abort_error)
				{
					uint32_t take = left < sizeof(sink) ? left : (uint32_t)sizeof(sink);
					uint32_t got = read_from_binary_read_iterator(rd, sink, take, NULL, &abort_error);
					if(got == 0) break;
					left -= got;
				}
				avail = 0;
				project_fix_unused_space(tx, store, 0);   // opportunistic threshold-based drain while writing
			}
		}
	}

	delete_binary_write_iterator(wr, NULL, &abort_error);
	delete_binary_read_iterator(rd, NULL, &abort_error);

	if(store != NULL)
	{
		project_fix_unused_space(tx, store, 1);       // force-drain the remainder before releasing the lock
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
	free(holder);
	return ok;
}

// project a numeric value (as an mpd_t) into a volatile extended numeric. same prefix-then-hash-then-lock
// discipline; digits are written in one bulk append rather than one at a time.
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
	void* holder = malloc(pas->page_size);
	if(holder == NULL) { free(digits); deinitialize_materialized_numeric(&mn); *error_code = RHENDB_EE_OUT_OF_MEMORY; return 0; }
	tuple_def td; initialize_tuple_def(&td, proj);
	init_tuple(&td, holder);
	set_element_in_tuple(&td, SELF, holder, EMPTY_DATUM, pas->page_size);
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
			project_fix_unused_space(tx, store, 0);
		}
	}

	delete_digit_write_iterator(wr, NULL, &abort_error);
	if(store != NULL)
	{
		project_fix_unused_space(tx, store, 1);
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

datum project_using_evaluate_sql_expr_for_rhendb(sql_expression* expr, sql_expr_eval_context* ec_p, data_type_info* projection_type_info, void** buffer_to_free, int* error_code)
{
	*error_code = 0;
	*buffer_to_free = NULL;
	datum err_datum = (datum){ .is_NULL = 1 };   // returned (with *error_code set) on any failure

	// projection only ever produces a VOLATILE extended type; a persistent target is rejected outright.
	if(has_extended_type_info(projection_type_info, PERSISTENT_EXT_SUB_TYPE))
	{
		*error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
		return err_datum;
	}

	transaction* tx = tx_from_ctx(ec_p);

	// the target is a type we ourselves returned from inference, so its kind already matches the result;
	// classify it directly.
	int to_ext_num  = has_extended_type_info(projection_type_info, VOLATILE_EXT_SUB_TYPE) && is_numeric_type_info(projection_type_info);
	int to_ext_text = has_extended_type_info(projection_type_info, VOLATILE_EXT_SUB_TYPE) && is_text_type_info(projection_type_info);
	int to_ext_blob = has_extended_type_info(projection_type_info, VOLATILE_EXT_SUB_TYPE) && is_blob_type_info(projection_type_info);

	expr_value* v = evaluate_sql_expr(expr, ec_p, error_code);
	if(*error_code)
		return err_datum;
	if(v == NULL)             // a NULL result with no error : the projected value is SQL NULL
		return (*NULL_DATUM);

	// ---- extended volatile numeric target ----
	if(to_ext_num)
	{
		mpd_t scratch = (mpd_t){0}; int owns = 0;
		mpd_t* dp = operand_to_mpd(v, &scratch, &owns, ec_p, error_code, RHENDB_EE_INCOMPATIBLE_PROJECTION);
		if(dp == NULL) { rhendb_delete_data(v, ec_p); return err_datum; }
		datum od; void* ob = NULL;
		int ok = project_write_numeric_to_volatile(tx, projection_type_info, dp, &od, &ob, error_code);
		if(owns) mpd_del(&scratch);
		rhendb_delete_data(v, ec_p);
		if(!ok) return err_datum;
		*buffer_to_free = ob;
		return od;
	}

	// ---- extended volatile text / blob target ----
	if(to_ext_text || to_ext_blob)
	{
		datum od; void* ob = NULL;
		int ok = project_write_sb_to_volatile(tx, projection_type_info, v, ec_p, &od, &ob, error_code);
		rhendb_delete_data(v, ec_p);
		if(!ok) return err_datum;
		*buffer_to_free = ob;
		return od;
	}

	// ---- tuple / array target : project the container value AS-IS into a caller-owned copy ----
	if(projection_type_info != NULL && is_container_type_info(projection_type_info) && !is_extended_type_info(projection_type_info))
	{
		if(v->type_info.dti_p == NULL || !are_identical_type_info(v->type_info.dti_p, projection_type_info))
		{
			rhendb_delete_data(v, ec_p);
			*error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
			return err_datum;
		}
		const void* bytes = v->value.tuple_value;   // tuple_value / array_value are the same union member
		if(bytes == NULL)   // an empty / minimally-initialized container
		{
			datum out = (datum){ .is_NULL = v->value.is_NULL, .tuple_value = NULL };
			rhendb_delete_data(v, ec_p);
			return out;
		}
		uint32_t sz = get_size_for_type_info(projection_type_info, bytes);
		void* copy = malloc(sz ? sz : 1);
		if(copy == NULL) { rhendb_delete_data(v, ec_p); *error_code = RHENDB_EE_OUT_OF_MEMORY; return err_datum; }
		memcpy(copy, bytes, sz);
		rhendb_delete_data(v, ec_p);
		*buffer_to_free = copy;
		return (datum){ .is_NULL = 0, .tuple_value = copy };
	}

	// ---- native scalar target : hand back the value directly, preserving its full width ----
	if(projection_type_info != NULL && !is_extended_type_info(projection_type_info))
	{
		datum out = (datum){ .is_NULL = 0 };
		switch(projection_type_info->type)
		{
			case FLOAT:      out.double_value     = to_dbl(v);  break;
			case BIT_FIELD:
			case UINT:       out.uint_value       = to_u64(v);  break;
			case INT:        out.int_value        = to_i64(v);  break;
			// large uint/int are 256-bit (32 bytes) : never truncate through a 64-bit path
			case LARGE_UINT: out.large_uint_value = to_u256(v); break;
			case LARGE_INT:  out.large_int_value  = to_i256(v); break;
			default:
				rhendb_delete_data(v, ec_p);
				*error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
				return err_datum;
		}
		rhendb_delete_data(v, ec_p);
		return out;
	}

	rhendb_delete_data(v, ec_p);
	*error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION;
	return err_datum;
}