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

int insert_child_for_selection_tree_node(selection_tree* parent, selection_tree* child)
{
	switch(parent->type)
	{
		case SELECT_NOT :
		{
			if(parent->not_of == NULL)
			{
				parent->not_of = child;
				return 1;
			}
			return 0;
		}

		case SELECT_AND :
		case SELECT_OR :
		case SELECT_XOR :
		{
			return insert_tail_in_singlylist(&(parent->logi_of), child);
		}

		case SELECT_EQ :
		case SELECT_NE :
		case SELECT_GT :
		case SELECT_LT :
		case SELECT_GTE :
		case SELECT_LTE :
		{
			if(parent->lhs == NULL)
			{
				parent->lhs = child;
				return 1;
			}
			if(parent->rhs == NULL)
			{
				parent->rhs = child;
				return 1;
			}
			return 0;
		}

		case SELECT_INPUT :
		case SELECT_CONSTANT :
		{
			return 0;
		}
	}

	return 0;
}

void destroy_selection_tree(selection_tree* tree)
{
	switch(tree->type)
	{
		case SELECT_NOT :
		{
			if(tree->not_of != NULL)
				destroy_selection_tree(tree->not_of);
			break;
		}

		case SELECT_AND :
		case SELECT_OR :
		case SELECT_XOR :
		{
			selection_tree* x = NULL;
			while((x = (selection_tree*) get_head_of_singlylist(&(tree->logi_of))) != NULL)
			{
				remove_head_from_singlylist(&(tree->logi_of));
				destroy_selection_tree(x);
			}
			break;
		}

		case SELECT_EQ :
		case SELECT_NE :
		case SELECT_GT :
		case SELECT_LT :
		case SELECT_GTE :
		case SELECT_LTE :
		{
			if(tree->lhs != NULL)
				free(tree->lhs);
			if(tree->rhs != NULL)
				free(tree->rhs);
			return;
		}

		case SELECT_INPUT :
		case SELECT_CONSTANT :
		{
			break;
		}
	}

	free(tree);
}