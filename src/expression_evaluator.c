/*
	This file only contains callbacks made by SQLtoAST for evaluation and inference of the SQL expression
	It was very time consuming to write this, so it was made to be done by AI
	It must be correct, and leak free because the driver of this context/operators is well tested and lives in SQLtoAST
	ALL-IN-ALL this file has boring but important code, it is written by AI but thoroughly reviewed by a human.
*/

#include<rhendb/expression_evaluator.h>

#include<rhendb/function_compare.h>

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
 
static expr_value* new_val(expr_type t)
{
	expr_value* v = calloc(1, sizeof *v);
	v->type_info.type = t;
	return v;
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
static int is_native_float(const expr_value* v, double* out)
{
	if(v->type_info.type == RHENDB_FLOAT)
	{
		*out = v->value.double_value;
		return 1;
	}
	return 0;
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
 
/* the engine used to read extended text/blob/numeric out of tuples */
static rage_engine* engine_from_ctx(const sql_expr_eval_context* ec_p)
{
	if(((rhendb_expr_eval_context*)(ec_p->context_p))->rdb)
		return &(((rhendb_expr_eval_context*)(ec_p->context_p))->rdb->persistent_acid_rage_engine);
	return NULL;
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
 
	rage_engine* eng = engine_from_ctx(ec_p);
	if(eng == NULL)
	{
		*error_code = RHENDB_EE_MISSING_ENGINE;
		return *error_code;
	}
 
	const void* transaction_id = NULL;
	int abort_error = 0;

	binary_read_iterator* bri = get_new_binary_read_iterator(&(v->value), dti, &(eng->bstd), eng->pam_p);
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
 
	rage_engine* eng = engine_from_ctx(ec_p);
	if(eng == NULL)
	{
		*error_code = RHENDB_EE_MISSING_ENGINE;
		return *error_code;
	}
 
	const void* transaction_id = NULL;
	int abort_error = 0;
	numeric_reader_interface nri = init_intuple_numeric_reader_interface(v->value, v->type_info.dti_p, &(eng->bstd), eng->pam_p, transaction_id, &abort_error);
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
			case OP_DIV: mpd_qdiv(&result, pa, pb, &ctx, &st); break;
			case OP_MOD: mpd_qrem(&result, pa, pb, &ctx, &st); break;
		}
		if(oa)
			mpd_del(&sa);
		if(ob)
			mpd_del(&sb);
 
		expr_value* v = new_val(RHENDB_NUMERIC);
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
		expr_value* v = new_val(RHENDB_FLOAT);
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
		expr_value* v = new_val(RHENDB_LARGE_INT);
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
		expr_value* v = new_val(RHENDB_LARGE_UINT);
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
				r = x / y;
				break;
			case OP_MOD:
				if(y == 0){ *error_code = RHENDB_EE_DIVIDE_BY_ZERO; return NULL; }
				r = x % y;
				break;
		}
		expr_value* v = new_val(RHENDB_INT);
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
		expr_value* v = new_val(RHENDB_UINT);
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
	rage_engine* eng = engine_from_ctx(ec_p);
	if(eng == NULL)
	{
		*error_code = RHENDB_EE_MISSING_ENGINE;
		return 0;
	}
	const void* transaction_id = NULL; int abort_error = 0;
	binary_read_iterator* bri = get_new_binary_read_iterator(&(v->value), v->type_info.dti_p, &(eng->bstd), eng->pam_p);
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
		case RHENDB_BIT_FIELD: return BIT_FIELD_NON_NULLABLE[64];
		case RHENDB_UINT: return UINT_NON_NULLABLE[8];
		case RHENDB_INT: return INT_NON_NULLABLE[8];
		case RHENDB_FLOAT: return FLOAT_double_NON_NULLABLE;
		case RHENDB_LARGE_UINT: return LARGE_UINT_NON_NULLABLE[32];
		case RHENDB_LARGE_INT: return LARGE_INT_NON_NULLABLE[32];
		default: return NULL;   /* string/binary handled by a dedicated byte compare */
	}
}
 
static int rhendb_compare(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* a = data1; expr_value* b = data2;
 
	/* NUMERIC comparison, on the mpd_t: NaN == NaN and NaN > everything; otherwise mpd_qcmp
	 * (which already sorts -inf < finite < +inf). */
	if(is_numeric_operand(a) || is_numeric_operand(b))
	{
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
		/* both on-disk text/blob columns: stream-compare, no materialization */
		if(a->type_info.dti_p != NULL && b->type_info.dti_p != NULL)
		{
			if(!can_compare_datum_rhendb(a->type_info.dti_p, b->type_info.dti_p))
			{
				*error_code = RHENDB_EE_INCOMPARABLE_TYPES;
				return 0;
			}
			return compare_datum_rhendb(&a->value, a->type_info.dti_p, &b->value, b->type_info.dti_p, engine_from_ctx(ec_p));
		}
		/* at least one is an in-memory string/binary: bring the other in-memory and compare bytes */
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
	return compare_datum_rhendb(&a->value, d1, &b->value, d2, engine_from_ctx(ec_p)); // NAN ordering on floats already handled here
}

/* ------------------------------ bitwise / shifts ------------------------------ */

typedef enum
{
	B_AND,
	B_OR,
	B_XOR
} bit_op;

static void* do_bitlogic(void* d1, void* d2, bit_op op, int* error_code)
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
		expr_value* v = new_val(rt);
		if(rt == RHENDB_LARGE_INT)
			v->value.large_int_value = (int256){r};
		else
			v->value.large_uint_value = r;
		return v;
	}

	uint64_t x = to_u64(a), y = to_u64(b), r;
	r = (op == B_AND) ? (x & y) : ((op == B_OR) ? (x | y) : (x ^ y));
	expr_value* v = new_val(rt);
	if(rt == RHENDB_INT)
		v->value.int_value = (int64_t)r;
	else
		v->value.uint_value = r;
	return v;
}
static void* rhendb_bit_and(void* d1, void* d2, const sql_expr_eval_context* ec_p, int* e){ (void)ec_p; return do_bitlogic(d1,d2,B_AND,e); }
static void* rhendb_bit_or (void* d1, void* d2, const sql_expr_eval_context* ec_p, int* e){ (void)ec_p; return do_bitlogic(d1,d2,B_OR ,e); }
static void* rhendb_bit_xor(void* d1, void* d2, const sql_expr_eval_context* ec_p, int* e){ (void)ec_p; return do_bitlogic(d1,d2,B_XOR,e); }
static void* rhendb_bit_not(void* data, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* a = data;
	expr_type t = a->type_info.type;
	if(a->type_info.dti_p != NULL || !et_is_int(t))
	{
		*error_code = RHENDB_EE_NON_INTEGER_OPERAND;
		return NULL;
	}

	expr_value* v = new_val(t);
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
static void* do_shift(void* data, void* shift_amt, int left, int* error_code)
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
	expr_value* v = new_val(t);
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
static void* rhendb_left_shift (void* d, void* s, const sql_expr_eval_context* ec_p, int* e){ (void)ec_p; return do_shift(d,s,1,e); }
static void* rhendb_right_shift(void* d, void* s, const sql_expr_eval_context* ec_p, int* e){ (void)ec_p; return do_shift(d,s,0,e); }

/* ------------------------------ literals ------------------------------ */

static void* rhendb_create_number(const dstring* data_bytes, const sql_expr_eval_context* ec_p, int* error_code)
{
	uint32_t n = get_char_count_dstring(data_bytes);
	char buf[128];
	if(n >= sizeof(buf))
		n = sizeof(buf) - 1;
	memory_move(buf, get_byte_array_dstring(data_bytes), n); buf[n] = 0;
	if(strpbrk(buf, ".eE"))
	{
		expr_value* v = new_val(RHENDB_FLOAT);
		v->value.double_value = strtod(buf, NULL);
		return v;
	}
	expr_value* v = new_val(RHENDB_INT);
	v->value.int_value = strtoll(buf, NULL, 10);
	return v;
}
static void* rhendb_create_string(const dstring* data_bytes, const sql_expr_eval_context* ec_p, int* error_code)
{
	/* point straight at the bytes owned by the parse tree; no allocation, buffer stays NULL */
	expr_value* v = new_val(RHENDB_STRING);
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

	/* concat modifies the first operand in place.  a may be borrowing its bytes (e.g. a string
	 * literal pointing into the parse tree), so allocate a fresh buffer, clone a's part into it
	 * first, then append b's part; only afterwards release a's own old buffer (if it had one). */
	uint32_t na = a->value.string_or_binary_size, nb = b->value.string_or_binary_size;
	char* nbuf = malloc((na + nb) ? (na + nb) : 1);
	if(nbuf == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return; }
	memory_move(nbuf, a->value.string_or_binary_value, na);
	memory_move(nbuf + na, b->value.string_or_binary_value, nb);

	if(a->buffer)
		free(a->buffer);

	a->buffer = nbuf;
	a->capacity = na + nb;
	a->value = (datum){.string_or_binary_value = nbuf, a->value.string_or_binary_size = na + nb};
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
static void* rhendb_cast(void* data, const void* to_type, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* a = data;
	expr_type target = ((const expr_type_info*)to_type)->type;
 
	/* numeric source : casts operate on the materialized mpd_t */
	if(is_numeric_operand(a)){
		if(materialize_numeric(a, ec_p, error_code)) return NULL;
		switch(target){
			case RHENDB_NUMERIC: {
				mpd_t d; if(!ee_mpd_new(&d)){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
				uint32_t st = 0; mpd_qcopy(&d, &(a->numeric_value), &st);
				expr_value* v = new_val(RHENDB_NUMERIC); v->numeric_value = d; return v;
			}
			case RHENDB_INT: case RHENDB_FLOAT: {
				char* sci = mpd_to_sci(&(a->numeric_value), 0);
				double dv = sci ? strtod(sci, NULL) : 0;
				if(sci) mpd_free(sci);
				if(target == RHENDB_FLOAT){ expr_value* v = new_val(RHENDB_FLOAT); v->value.double_value = dv; return v; }
				expr_value* v = new_val(RHENDB_INT); v->value.int_value = (int64_t)dv; return v;
			}
			case RHENDB_BIT_FIELD: {
				expr_value* v = new_val(RHENDB_BIT_FIELD); v->value.bit_field_value = !mpd_iszero(&(a->numeric_value)); return v;
			}
			default: *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL;   /* STRING/BINARY targets deferred */
		}
	}
 
	/* string / blob / text source -> a number type : materialize the bytes and parse them.
	 * (BIT_FIELD is left to the emptiness rule below.)  256-bit targets are parsed via the low
	 * 64 bits, so a value wider than 64 bits is truncated. */
	if(is_sb_operand(a) && et_is_num_or_numeric(target) && target != RHENDB_BIT_FIELD){
		if(materialize_tb(a, ec_p, error_code)) return NULL;
		uint32_t n = a->value.string_or_binary_size;
		char* tmp = malloc((size_t)n + 1);
		if(tmp == NULL){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
		if(n) memory_move(tmp, a->value.string_or_binary_value, n);
		tmp[n] = 0;
		expr_value* v = NULL;
		switch(target){
			case RHENDB_NUMERIC: {
				mpd_t d;
				if(!ee_mpd_new(&d)){ free(tmp); *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
				mpd_context_t ctx; mpd_maxcontext(&ctx); uint32_t st = 0;
				mpd_qset_string(&d, tmp, &ctx, &st);
				v = new_val(RHENDB_NUMERIC); v->numeric_value = d; break;
			}
			case RHENDB_FLOAT:      v = new_val(RHENDB_FLOAT);      v->value.double_value     = strtod(tmp, NULL); break;
			case RHENDB_INT:        v = new_val(RHENDB_INT);        v->value.int_value        = (int64_t)strtoll(tmp, NULL, 10); break;
			case RHENDB_UINT:       v = new_val(RHENDB_UINT);       v->value.uint_value       = (uint64_t)strtoull(tmp, NULL, 10); break;
			case RHENDB_LARGE_INT:  v = new_val(RHENDB_LARGE_INT);  v->value.large_int_value  = get_int256((int64_t)strtoll(tmp, NULL, 10)); break;
			case RHENDB_LARGE_UINT: v = new_val(RHENDB_LARGE_UINT); v->value.large_uint_value = get_uint256((uint64_t)strtoull(tmp, NULL, 10)); break;
			default: break;
		}
		free(tmp);
		if(v == NULL){ *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }
		return v;
	}
 
	switch(target){
		case RHENDB_BIT_FIELD: {
			int t = 0;
			if(et_is_num(a->type_info.type) && a->type_info.dti_p == NULL) t = (to_dbl(a) != 0);
			else if(a->type_info.type==RHENDB_STRING||a->type_info.type==RHENDB_BINARY) t = (a->value.string_or_binary_size != 0);
			else { *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }
			expr_value* v = new_val(RHENDB_BIT_FIELD); v->value.bit_field_value = t; return v;
		}
		case RHENDB_INT: {
			if(a->type_info.dti_p != NULL || !et_is_num(a->type_info.type)){ *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }
			expr_value* v = new_val(RHENDB_INT); v->value.int_value = (int64_t)to_dbl(a); return v;
		}
		case RHENDB_FLOAT: {
			if(a->type_info.dti_p != NULL || !et_is_num(a->type_info.type)){ *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }
			expr_value* v = new_val(RHENDB_FLOAT); v->value.double_value = to_dbl(a); return v;
		}
		case RHENDB_NUMERIC: {
			if(a->type_info.dti_p != NULL || !et_is_num(a->type_info.type)){ *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL; }
			mpd_t d; if(!number_to_mpd(a, &d)){ *error_code = RHENDB_EE_OUT_OF_MEMORY; return NULL; }
			expr_value* v = new_val(RHENDB_NUMERIC); v->numeric_value = d; return v;
		}
		default: *error_code = RHENDB_EE_UNSUPPORTED_CAST; return NULL;   /* STRING/BINARY targets deferred */
	}
}
 
/* ------------------------------ type inference ------------------------------ */
 
static expr_type type_of_value(const expr_value* v)
{
	if(v->type_info.dti_p != NULL){
		if(is_numeric_type_info(v->type_info.dti_p)) return RHENDB_NUMERIC;
		if(is_text_type_info(v->type_info.dti_p)) return RHENDB_STRING;
		if(is_blob_type_info(v->type_info.dti_p)) return RHENDB_BINARY;
	}
	return v->type_info.type;
}
static void* rhendb_get_type_for_data(void* data, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_value* v = data;
	expr_type t = type_of_value(v);
	expr_type_info* ti = new_type(t);
	if((t == RHENDB_TUPLE || t == RHENDB_ARRAY) && t == v->type_info.type)
		ti->dti_p = v->type_info.dti_p;
	return ti;
}
static void* rhendb_get_type_for_sql_type(const sql_type* type, const sql_expr_eval_context* ec_p, int* error_code)
{
	switch(type->type_name)
	{
		case SQL_BOOL: return new_type(RHENDB_BIT_FIELD);
		case SQL_SMALLINT: case SQL_INT: case SQL_BIGINT: return new_type(RHENDB_INT);
		case SQL_REAL: case SQL_DOUBLE: case SQL_FLOAT: return new_type(RHENDB_FLOAT);
		case SQL_DECIMAL: case SQL_NUMERIC: return new_type(RHENDB_NUMERIC);
		case SQL_TEXT: case SQL_CHAR: case SQL_VARCHAR: case SQL_STRING: case SQL_CLOB: return new_type(RHENDB_STRING);
		case SQL_BINARY: case SQL_BLOB: return new_type(RHENDB_BINARY);
		default: *error_code = RHENDB_EE_UNSUPPORTED_TYPE; return NULL;
	}
}
static int rhendb_can_compare_types(void* typ1, void* typ2, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_type a = ((expr_type_info*)typ1)->type, b = ((expr_type_info*)typ2)->type;
	if(et_is_num_or_numeric(a) && et_is_num_or_numeric(b)) return 1;   /* numerics compare with any number */
	if(a==RHENDB_STRING && b==RHENDB_STRING) return 1;
	if(a==RHENDB_BINARY && b==RHENDB_BINARY) return 1;
	return 0;
}
static int rhendb_can_cast_types(const void* typ_from, const void* typ_to, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_type from = ((const expr_type_info*)typ_from)->type, to = ((const expr_type_info*)typ_to)->type;
	if(et_is_num_or_numeric(to) && et_is_num_or_numeric(from)) return 1;             /* any number <-> any number */
	if(to==RHENDB_BIT_FIELD && (from==RHENDB_STRING || from==RHENDB_BINARY)) return 1;
	if(to<=RHENDB_BINARY && from<=RHENDB_BINARY) return 1;                            /* primitive/string/binary */
	return 0;
}
static void* rhendb_get_return_type_for_op_exec_callback(void* op_exec_func, void* typ1, void* typ2, const sql_expr_eval_context* ec_p, int* error_code)
{
	expr_type a = ((expr_type_info*)typ1)->type;
	expr_type b = (typ2 != NULL) ? ((expr_type_info*)typ2)->type : a;   /* typ2 is NULL for unary ops */
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
	expr_type a = ((expr_type_info*)typ1)->type, b = ((expr_type_info*)typ2)->type;
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
	dstring identifier;                /* owned clone of the variable name; the cache key */
	uint32_t tuple_index;              /* which input tuple it lives in */
	positional_accessor pa;            /* owns pa.positions : the located element */
	const data_type_info* column_dti;  /* the located column's on-disk type */
};
 
static cy_uint var_hash(const void* d)
{
	const var_cache_entry* e = d;
	const char* b = get_byte_array_dstring(&e->identifier);
	cy_uint n = get_char_count_dstring(&e->identifier);
	cy_uint h = (cy_uint)1469598103934665603ULL;   /* FNV-1a over the identifier bytes */
	for(cy_uint i = 0; i < n; i++){ h ^= (unsigned char)b[i]; h *= (cy_uint)1099511628211ULL; }
	return h;
}
static int var_cmp(const void* d1, const void* d2)
{
	return compare_dstring(&((const var_cache_entry*)d1)->identifier, &((const var_cache_entry*)d2)->identifier);
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
		comp_ptr[ncomp] = key + start; comp_len[ncomp] = (uint32_t)(i - start); ncomp++;
		if(i < klen && key[i] == '.') i++;
	}
	if(ncomp == 0)
	{
		*error_code = RHENDB_EE_UNSUPPORTED_TYPE;
		return 0;
	}
 
	char name[64];
 
	uint32_t tuple_index = 0, field_start = 0;
	if(ctx->input_tuples_count >= 2)
	{
		/* first component is the table name (always a string) */
		uint32_t l = comp_len[0]; if(l > 63) l = 63; memory_move(name, comp_ptr[0], l); name[l] = 0;
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
			uint32_t l = comp_len[0]; if(l > 63) l = 63; memory_move(name, comp_ptr[0], l); name[l] = 0;
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
			uint32_t l = comp_len[c]; if(l > 63) l = 63; memory_move(name, comp_ptr[c], l); name[l] = 0;
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
	probe.identifier = get_dstring_pointing_to(get_byte_array_dstring(id), get_char_count_dstring(id));   /* non-owning probe key */
	const var_cache_entry* hit = find_equals_in_hashmap(&ctx->var_cache, &probe);
	if(hit != NULL)
		return hit;
 
	var_cache_entry* ne = malloc(sizeof(*ne));
	if(ne == NULL)
	{
		*error_code = RHENDB_EE_OUT_OF_MEMORY;
		return NULL;
	}
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
	expr_value* v = new_val(RHENDB_INT);
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

sql_expr_eval_context get_sql_expr_eval_context_for_rhendb(tuple_def** input_tuple_defs, uint32_t input_tuples_count, rhendb* rdb, void* catalog_manager)
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

	context_p->rdb = rdb;

	context_p->catalog_manager = catalog_manager;

	return eval_context;
}

void delete_context_p_for_sql_expr_eval_context_for_rhendb(rhendb_expr_eval_context* context_p)
{
	remove_all_from_hashmap(&(context_p->var_cache), &((notifier_interface){NULL, notify_removal_for_cache_entry}));
	deinitialize_hashmap(&(context_p->var_cache));
	free(context_p->input_tuple_defs);
	free(context_p->input_tuples);
	free(context_p);
}