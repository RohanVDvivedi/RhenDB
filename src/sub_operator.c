#include<rhendb/sub_operator.h>

#include<stdlib.h>

typed_user_value transform(transformer* t, uint32_t input_count, const typed_user_value** input)
{
	return t->transform(t->context, input_count, input);
}

selection_tree* initialize_selection_tree_node(selection_node_type type)
{
	selection_tree* node = malloc(sizeof(selection_tree));

	node->type = type;
	initialize_slnode(&(node->logi_of_embed_node));

	switch(type)
	{
		case SELECT_NOT :
		{
			node->not_of = NULL;
			break;
		}

		case SELECT_AND :
		case SELECT_OR :
		case SELECT_XOR :
		{
			initialize_singlylist(&(node->logi_of), offsetof(selection_tree, logi_of_embed_node));
			break;
		}

		case SELECT_EQ :
		case SELECT_NE :
		case SELECT_GT :
		case SELECT_LT :
		case SELECT_GTE :
		case SELECT_LTE :
		{
			node->lhs = NULL;
			node->rhs = NULL;
			break;
		}

		case SELECT_INPUT :
		{
			node->input_transformer = NULL;
			node->input_position = SELF;
			break;
		}

		case SELECT_CONSTANT :
		{
			node->constant_transformer = NULL;
			node->constant = (typed_user_value){NULL, *NULL_USER_VALUE};
			break;
		}
	}

	return node;
}

void insert_child_for_selection_tree_node(selection_tree* parent, selection_tree* child);