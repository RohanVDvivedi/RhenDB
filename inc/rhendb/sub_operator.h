#ifndef SUB_OPERATOR_H
#define SUB_OPERATOR_H

#include<tuplestore/data_type_info.h>
#include<tuplestore/user_value.h>
#include<tuplestore/tuple_def.h>
#include<tuplestore/tuple.h>

#include<cutlery/singlylist.h>

// transformer

typedef struct typed_user_value typed_user_value;
struct typed_user_value
{
	// type of the value
	data_type_info* type;

	// value itself
	user_value value;
};

typedef struct transformer transformer;
struct transformer
{
	const void* context;

	typed_user_value (*transform)(const void* context, uint32_t input_count, const typed_user_value** input);
};

typed_user_value transform(transformer* t, uint32_t input_count, const typed_user_value** input);

// selection

typedef enum selection_node_type selection_node_type;
enum selection_node_type
{
	SELECT_NOT,
	SELECT_AND,
	SELECT_OR,
	SELECT_XOR,

	SELECT_EQ,	// ==
	SELECT_NE,	// !=
	SELECT_GT,	// >
	SELECT_LT,	// <
	SELECT_GTE,	// >=
	SELECT_LTE,	// <=

	SELECT_INPUT,

	SELECT_CONSTANT,
};

typedef struct selection_tree selection_tree;
struct selection_tree
{
	// type of this node
	selection_node_type type;

	union
	{
		selection_tree* not_of; // NOT

		singlylist logi_of; // AND, OR, XOR

		// logi_of and not_of must never have SELECT_INPUT and SELECT_CONSTANT in them

		struct // ==, !=, <, >, <=, >=
		{ // both lhs and rhs must be either SELECT_INPUT or SELECT_CONSTANT
			selection_tree* lhs;
			selection_tree* rhs;
		};

		struct // INPUT
		{
			transformer* input_transformer;	// can be NULL

			positional_accessor input_position;
		};

		struct // CONSTANT
		{
			transformer* constant_transformer; // can be NULL

			typed_user_value constant;
		};
	};

	// to chain nodes using the logi_of singlylist
	slnode logi_of_embed_node;
};

selection_tree* initialize_selection_tree_node(selection_node_type type);

void insert_child_for_selection_tree_node(selection_tree* parent, selection_tree* child);

// things like
// AND/OR of just 1 node gets reduced
// XOR with false is reduced to NOT
// CONSTANT and CONSTANT comparison are evaluated upfront
// CONSTANT transformers get evaluated upfront before actual select
void optimize_selection_tree(selection_tree* tree);

void destroy_selection_tree(selection_tree* tree);

typedef struct selection_params selection_params;
struct selection_params
{
	selection_tree* tree;

	tuple_def* input_def;
};

// checks for logi_of and not_of to not hold input and constants
// checks for rhs and lhs to only hold input and constants
// checks to make sure that the accessed input_positions in the selection_params actually may exist in the input_tuple
int is_valid_selection_params(const selection_params* sp);

// below function works on the input_tuple, and return 1 if it passes the select clause, or 0 if it fails
// -1 if it can not be evaluated (accessing array indices out of bounds)
int do_select(const void* input_tuple, const selection_params* sp);

// projection

#endif