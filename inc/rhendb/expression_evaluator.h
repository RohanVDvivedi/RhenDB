#ifndef EXPRESSION_EVALUATOR_H
#define EXPRESSION_EVALUATOR_H

#include<stdarg.h>

#include<sqltoast/sqltoast.h>
#include<sqltoast/sql_expression_eval.h>

#include<tuplestore/datum.h>
#include<tuplestore/data_type_info.h>

#include<tuplelargetypes/text_extended.h>
#include<tuplelargetypes/blob_extended.h>
#include<tuplelargetypes/numeric_extended.h>
#include<tuplelargetypes/jsonb_extended.h>
#include<tuplelargetypes/materialized_numeric.h>

#include<mpdecimal.h>

#include<cutlery/hashmap.h>
#include<cutlery/arraylist.h>
#include<cutlery/dstring.h>

#include<rhendb/transaction.h>

typedef enum expr_type expr_type;
enum expr_type
{
	RHENDB_EXPR_BIT_FIELD  = 0,

	RHENDB_EXPR_UINT       = 1,
	RHENDB_EXPR_INT        = 2,

	RHENDB_EXPR_LARGE_UINT = 3,
	RHENDB_EXPR_LARGE_INT  = 4,

	RHENDB_EXPR_FLOAT      = 5,
	RHENDB_EXPR_DOUBLE     = 6,

	RHENDB_EXPR_TUPLE      = 7,
	RHENDB_EXPR_ARRAY      = 8,

	// find all above types in expr_value.value
	// they will have valid dti_p

	RHENDB_EXPR_STRING     = 9,
	RHENDB_EXPR_BINARY     = 10,
	// find the above types in expr_value.string_value or expr_value.blob_value
	// they may optionally have a dti_p
	// if absent project them into volatile_rage_engine.text/blob_extended_type_info

	RHENDB_EXPR_NUMERIC    = 11,
	// find the above types in expr_value.numeric_value
	// they will not have a dti_p
	// project them into volatile_rage_engine.numeric_extended_type_info

	RHENDB_EXPR_JSONB      = 12,
	// find the above types in expr_value.jsonb_value
	// they will not have a dti_p
	// project them into volatile_rage_engine.jsonb_extended_type_info
};

typedef struct expr_type_info expr_type_info;
struct expr_type_info
{
	expr_type type;

	data_type_info* dti_p; // not valid for RHENDB_EXPR_STRING, RHENDB_EXPR_BINARY, RHENDB_EXPR_NUMERIC and RHENDB_EXPR_JSONB
	// dti_p will always be a borrowed type, or a statically created and returned type and will not need freeing
};

typedef struct expr_value expr_value;
struct expr_value
{
	expr_type_info type_info;

	union
	{
		datum value;
		dstring string_value;    // used only for RHENDB_EXPR_STRING
		dstring binary_value;    // used only for RHENDB_EXPR_BINARY
		mpd_t numeric_value;     // used only for RHENDB_EXPR_NUMERIC
		jsonb_node* jsonb_value; // used only for RHENDB_EXPR_JSONB
	};

	void* buffer_to_free; // temporary buffer always to be freed, on destroying the expr_value, may be used by user defined functions returning a complex RHENDB_EXPR_TUPLE or RHENDB_EXPR_ARRAY type
};

// this is what sits in sql_expr_eval_context.context_p
typedef struct rhendb_expr_eval_context rhendb_expr_eval_context;
struct rhendb_expr_eval_context
{
	tuple_def** input_tuple_defs;

	// populate this by index, to set tuples for current expression evaluation
	// these input_tuples_count number of pointers will be 0 initialized
	void** input_tuples;

	uint32_t input_tuples_count;

	// variable/identifier cache that stores variable identifier to tuple_index and positional_accessor in it
	// owned by the context; caches "a.b.c" -> (tuple index + positional accessor + type)
	hashmap var_cache;

	// for materializing the on-disk, and extended volatile store -> text, blob, numeric and jsonb columns
	// and to access the catalog_manager, for user defined types and functions
	transaction* tx;

	// CONSTANT FOLDING CACHE OWNERSHIP.
	// the folded values themselves live on the AST, in sql_expression.user_meta_value, but the AST
	// can not free them, it has no idea what an expr_value is.
	// All constant expression nodes are registered here in this list, so their corresponding expr_value,
	// is freed by traversing this list upon deleting the context.
	arraylist folded_expressions;

	// FREE LIST OF RECYCLED expr_value-s, owned by this context
	// every delete pushes to it, every allocate first tries to allocate by getting one from this list.
	void* free_list_for_expr_value;
};

// error codes written into *error_code by the rhendb expression-evaluation callbacks.
// (the sqltoast evaluator only distinguishes zero from non-zero; these names are for rhendb-side diagnostics.)
typedef enum rhendb_expr_eval_error rhendb_expr_eval_error;
enum rhendb_expr_eval_error
{
	RHENDB_EE_OK                  = 0,  // no error
	RHENDB_EE_UNSUPPORTED_TYPE    = 1,  // operand/result type not supported (TUPLE, ARRAY, unknown sql type)
	RHENDB_EE_NON_NUMERIC_OPERAND = 2,  // arithmetic on a non-numeric operand
	RHENDB_EE_NON_INTEGER_OPERAND = 3,  // bitwise/shift on a non-integer operand
	RHENDB_EE_DIVIDE_BY_ZERO      = 4,  // division or modulo by exact zero
	RHENDB_EE_INCOMPARABLE_TYPES  = 5,  // compare()/unify_types() on types that cannot be reconciled
	RHENDB_EE_UNSUPPORTED_CAST    = 6,  // cast() to an unsupported target type
	RHENDB_EE_NON_STRING_OPERAND  = 7,  // concat()/like() on a non string/binary operand
	RHENDB_EE_MATERIALIZE_FAILED  = 8,  // failed while reading/materializing an extended text/blob/numeric value
	RHENDB_EE_OUT_OF_MEMORY       = 10, // allocation failure
	RHENDB_EE_NULL_OPERAND        = 11, // a required operand pointer was NULL
	RHENDB_EE_STRING_TOO_LONG     = 12, // a text/blob value's byte length would not fit in the uint32_t size field (concat result, or an extended value being materialized)
	RHENDB_EE_INCOMPATIBLE_PROJECTION = 13, // evaluated value cannot be projected into the requested type
	RHENDB_EE_INVALID_CAST_VALUE  = 14, // a string being cast to a number is not a valid numeric value
	RHENDB_EE_UNKNOWN_VARIABLE    = 15, // a variable (column reference) does not resolve to any input column
	RHENDB_EE_AMBIGUOUS_VARIABLE  = 16, // a variable resolves to more than one input column (collision)
};

// ===================================================================================================
// (1) THREADING -- ONE CONTEXT PER THREAD. A CONTEXT MUST NOT BE SHARED BETWEEN THREADS.
//
// the context is deliberately unsynchronized: its expr_value free list and its variable cache are both
// plain, lock-free structures, and evaluate_sql_expr() writes into context_p->input_tuples[] before
// every evaluation. Two threads driving the same context would corrupt all three.
// If an operator runs its expression on N threads, it must build N contexts.
//
// (2) LIFETIME -- THE EXPRESSIONS MUST OUTLIVE THE CONTEXT.
//
//     THE INVARIANT, stated once and precisely:
//       * a context is always built FOR one or more already-existing expressions, and is only ever used
//         to evaluate/infer THOSE expressions;
//       * the context's lifespan must be fully CONTAINED WITHIN the lifespan of every expression it is
//         used with -- create the context strictly after the expressions exist, and destroy it strictly
//         before any of those expressions is destroyed.
//       equivalently: expressions outlive the context; the context never outlives its expressions.
//
//     build the sql_expression(s)                          // the expression(s) come first ...
//       -> get_sql_expr_eval_context_for_rhendb(...)       // ... then the context, built around them
//       -> infer_type_sql_expr() / evaluate_sql_expr(), as many times, on as many expressions, as you like
//       -> delete_context_p_for_sql_expr_eval_context_for_rhendb(...)   // context dies first ...
//       -> delete_sql_expr(...)                            // ... only then may the expression(s) die
//
// the variable cache keys its entries on the identifier's BYTES IN MEMORY (address + length), taken
// straight from the dstring inside the AST node -- that is what makes a variable lookup O(1) with no
// string hashing and no memcmp on every evaluation. So an expression must NOT be destroyed while a
// context that has evaluated (or type-inferred) it is still alive: freeing the AST frees those
// identifier bytes, and a later allocation landing on the same address would produce a stale cache hit
// for a DIFFERENT column -- a wrong answer, not merely a wasted entry.
//
// NOTE: flatten_similar_associative_operators_in_sql_expression() DESTROYS the tree it is handed and
// returns a new one, so it counts as destroying an expression: rebuild the context around it.
// ===================================================================================================

// intitialize this per instance for evaluation context of one stream of tuples of 1 type
sql_expr_eval_context get_sql_expr_eval_context_for_rhendb(tuple_def** input_tuple_defs, uint32_t input_tuples_count, transaction* tx);

// must be called, only after you infer the output_type of the corresponding expressions that you want to process with this context
// returns true only if any of the var_cache points to an extended type
int has_reference_to_persistent_extended_type_from_expression(const rhendb_expr_eval_context* context_p);

// frees shallow copied input_tuple_defs and input_tuples, and the pointer itself, and everything else held by the context_p, including the expr_value-s in free list, and the folded_expressions[i]->user_meta_value
// and the entries in var_cache
void delete_context_p_for_sql_expr_eval_context_for_rhendb(rhendb_expr_eval_context* context_p);

// set the input tuples used by the next evaluate/infer on this context.
// copies min(input_tuples_count, context's input_tuples_count) pointers into the input_tuples array
// the context does not take ownership of the tuples, you manage their memory
void set_input_tuples_in_context_for_rhendb(sql_expr_eval_context* ec_p, void** input_tuples, uint32_t input_tuples_count);

// variadic form : pass the tuples directly, in index order.
//   set_input_tuples_in_context_for_rhendb_v(&ec, 2, left_tuple, right_tuple);
void set_input_tuples_in_context_for_rhendb_v(sql_expr_eval_context* ec_p, uint32_t input_tuples_count, ...);

// evaluate `expr` as a WHERE/filter predicate under `ec` and return the boolean directly, so the caller
// no longer has to call get_bool() and then delete_data() on the intermediate result itself.
//   returns 1 if the expression evaluates to TRUE, 0 otherwise (FALSE, or UNKNOWN/NULL).
//   on an evaluation or bool-conversion error, *error_code is set non-zero and 0 is returned.
int select_using_evaluate_sql_expr_for_rhendb(sql_expression* expr, sql_expr_eval_context* ec_p, int* error_code);

// run infer_type_sql_expr() on `expr`; return 1 if inference produced no error. the inferred type object
// is deleted internally, so the caller may not need to free it.
int is_valid_using_infer_sql_expr_for_rhendb(sql_expression* expr, sql_expr_eval_context* ec_p, int* error_code);

// ===================================================================================================
// PROJECTION
//
// a projected type and a projected value each carry their own ownership, so both can BORROW from the
// expression evaluator instead of forcing a copy:
//
//   projected_type_info : the data_type_info to project into, plus the bit saying whether this struct owns
//                         it. a native scalar borrows a static default (bit 0); a tuple/array borrows the
//                         inferred container type directly -- no clone (bit copied from the inferred type);
//                         only a text/blob/numeric result allocates a fresh volatile extended type (bit 1).
//   projected_value     : the projected datum, plus the bulk buffer that backs it (NULL when the datum is
//                         self-contained or was borrowed as-is).
//
// lifetimes follow the operator: the projected_type_info is destroyed once, when the projection operator
// finishes; a projected_value is destroyed after each use of the expression (i.e. per row).
// ===================================================================================================

typedef struct projected_value projected_value;
struct projected_value
{
	// the projected value. is_NULL is set for a SQL NULL result and on any error.
	datum value;

	// the heap buffer backing `value` (the extended value's bytes, or bytes taken over from the evaluated
	// expr_value). NULL when the datum is self-contained (a native scalar) or borrowed from a live tuple.
	void* buffer_to_free;
};

// run infer_type_sql_expr() on `expr`; return 1 if inference produced no error. the inferred type object
// is deleted internally, so the caller may not need to free it.
int is_valid_using_infer_sql_expr_for_rhendb(sql_expression* expr, sql_expr_eval_context* ec_p, int* error_code);

// infer the result kind of `expr` and describe how it would be PROJECTED:
//   - native scalar (bit-field / uint / int / large uint / large int / float / double) -> the matching default type_info.
//   - string / binary / arbitrary-precision numeric / jsonb -> a freshly built, finalized volatile-store extended
//     type (sub_type = VOLATILE_EXT_SUB_TYPE, ~90-byte inline prefix, 128-byte max_size).
//   - tuple / array -> the inferred container type itself, borrowed or taken over exactly as the inferred type held it.
// on failure the returned struct has a NULL type and *error_code is set.
data_type_info* infer_projected_type_sql_expr_for_rhendb(sql_expression* expr, sql_expr_eval_context* ec_p, int* error_code);

// evaluate `expr` and PROJECT the result into `pti`, which must be what
// infer_projected_type_sql_expr_for_rhendb() returned for this expression (so no cast is needed -- the kinds
// already match).
//   - if the evaluated value ALREADY has the projected type, its datum is handed back untouched;
//   - a RHENDB_EXPR_STRING/BINARY result is materialized and written into the transaction's volatile blob store
//     as an extended value (prefix written, hashed to pick one of the 64 temporary_extension_stores, then
//     the remainder appended into that store under its WRITE lock);
//   - a numeric result is stored as an extended numeric the same way;
//   - a jsonb result is stored as an extended numeric the same way;
//   - a native scalar is returned directly at full width (including 256-bit large uint/int).
// a NULL (no-error) evaluation result yields a NULL datum. a PERSISTENT projection type, or a value/target
// mismatch, sets *error_code = RHENDB_EE_INCOMPATIBLE_PROJECTION.
// destroy the result with destroy_projected_value() once the value has been consumed.
projected_value project_using_evaluate_sql_expr_for_rhendb(sql_expression* expr, sql_expr_eval_context* ec_p, data_type_info* pti, int* error_code);

// release a projected_value : frees the backing buffer if there is one.
void destroy_projected_value(projected_value pv);

#endif