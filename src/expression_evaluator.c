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
static int rhendb_compare(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_left_shift(void* data, void* shift_amt, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_right_shift(void* data, void* shift_amt, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_bit_and(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_bit_or(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_bit_xor(void* data1, void* data2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_bit_not(void* data, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_cast(void* data, const sql_type* to_type, const sql_expr_eval_context* ec_p, int* error_code);

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
		deinitialize_materialized_numeric(&(val_p->number));
	else
	{
		if(val_p->buffer)
			free(val_p->buffer);
	}

	free(val_p);
}

static int rhendb_can_compare_types(void* typ1, void* typ2, const sql_expr_eval_context* ec_p, int* error_code);

static void* rhendb_get_type_for_sql_type(const sql_type* type, const sql_expr_eval_context* ec_p, int* error_code);

static int rhendb_can_cast_types(void* typ_to, void* typ_from, const sql_expr_eval_context* ec_p, int* error_code);

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

sql_expr_eval_context get_sql_expr_eval_context_for_rhendb(tuple_def** input_tuple_defs, void** input_tuples, uint32_t input_tuples_count, void* catalog_manager)
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

		.get_variable = NULL,

		.bool_type = (void*)(&rhendb_bool_type),

		.can_compare_types = rhendb_can_compare_types,

		.get_type_for_sql_type = rhendb_get_type_for_sql_type,
		.can_cast_types = rhendb_can_cast_types,

		.get_return_type_for_op_exec_callback = NULL,

		.get_type_for_data = rhendb_get_type_for_data,

		.unify_types = rhendb_unify_types,

		.get_type_for_sub_query = NULL,

		.get_return_type_for_function = NULL,

		.get_type_for_variable = NULL,

		.delete_type = rhendb_delete_type,
	};

	rhendb_expr_eval_context* context_p = eval_context.context_p;

	context_p->input_tuple_defs = malloc(sizeof(tuple_def*) * input_tuples_count);
	memory_move(context_p->input_tuple_defs, input_tuple_defs, sizeof(tuple_def*) * input_tuples_count);

	context_p->input_tuples = malloc(sizeof(void*) * input_tuples_count);
	memory_move(context_p->input_tuples, input_tuples, sizeof(void*) * input_tuples_count);

	context_p->input_tuples_count = input_tuples_count;

	return eval_context;
}

void delete_context_p_for_sql_expr_eval_context_for_rhendb(rhendb_expr_eval_context* context_p)
{
	free(context_p->input_tuple_defs);
	free(context_p->input_tuples);
	free(context_p);
}