#include<rhendb/sub_operator.h>

#include<stdlib.h>

void destroy_typed_user_value(typed_user_value t)
{
	if(!(t.value_needs_to_be_freed))
		return;

	switch(t.type->type)
	{
		case STRING :
		{
			free((void*)(t.value.string_value));
			break;
		}
		case BLOB :
		{
			free((void*)(t.value.blob_value));
			break;
		}
		case TUPLE :
		{
			free((void*)(t.value.tuple_value));
			break;
		}
		case ARRAY :
		{
			free((void*)(t.value.array_value));
			break;
		}
		default :
		{
			break;
		}
	}
}

const data_type_info* get_transformed_type(const transformer* t, uint32_t input_count, const data_type_info** input_types)
{
	return t->transformed_type(t->context, input_count, input_types);
}

typed_user_value transform(const transformer* t, uint32_t input_count, const typed_user_value** input)
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

		case SELECT_TRUE :
		case SELECT_FALSE :
		{
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

		case SELECT_TRUE :
		case SELECT_FALSE :
		{
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
				destroy_selection_tree(tree->lhs);
			if(tree->rhs != NULL)
				destroy_selection_tree(tree->rhs);
			break;
		}

		case SELECT_TRUE :
		case SELECT_FALSE :
		{
			break;
		}

		case SELECT_INPUT :
		case SELECT_CONSTANT :
		{
			break;
		}
	}

	free(tree);
}

static const data_type_info* get_type_of_selection_tree_node(selection_tree* node, tuple_def* input_def)
{
	switch(node->type)
	{
		case SELECT_NOT :
		case SELECT_AND :
		case SELECT_OR :
		case SELECT_XOR :
		case SELECT_EQ :
		case SELECT_NE :
		case SELECT_GT :
		case SELECT_LT :
		case SELECT_GTE :
		case SELECT_LTE :
		case SELECT_TRUE :
		case SELECT_FALSE :
		{
			return NULL;
		}

		case SELECT_INPUT :
		{
			const data_type_info* type = get_type_info_for_element_from_tuple_def(input_def, node->input_position);
			if(node->input_transformer == NULL)
				return type;

			return get_transformed_type(node->input_transformer, 1, (data_type_info*[]){type});
		}

		case SELECT_CONSTANT :
		{
			const data_type_info* type = node->constant.type;
			if(node->constant_transformer == NULL)
				return type;

			return get_transformed_type(node->constant_transformer, 1, (data_type_info*[]){type});
		}
	}
	return NULL;
}

static const typed_user_value get_value_of_selection_tree_node(selection_tree* node, tuple_def* input_def, const void* input_tuple, int* error)
{
	(*error) = 0;

	switch(node->type)
	{
		case SELECT_NOT :
		case SELECT_AND :
		case SELECT_OR :
		case SELECT_XOR :
		case SELECT_EQ :
		case SELECT_NE :
		case SELECT_GT :
		case SELECT_LT :
		case SELECT_GTE :
		case SELECT_LTE :
		case SELECT_TRUE :
		case SELECT_FALSE :
		{
			break;
		}

		case SELECT_INPUT :
		{
			typed_user_value tvalue = {.value_needs_to_be_freed = 0};
			if(0 == get_value_from_element_from_tuple(&(tvalue.value), input_def, node->input_position, input_tuple))
				break;
			tvalue.type = get_type_of_selection_tree_node(node, input_def);

			return transform(node->input_transformer, 1, (const typed_user_value* []){&tvalue});
		}

		case SELECT_CONSTANT :
		{
			return transform(node->constant_transformer, 1, (const typed_user_value* []){&(node->constant)});
		}
	}

	(*error) = 1;
	return (typed_user_value){};
}

int is_valid_selection_params(const selection_params* sp)
{
	switch(sp->tree->type)
	{
		case SELECT_NOT :
		{
			if(sp->tree->not_of == NULL)
				return 0;

			if(sp->tree->not_of->type == SELECT_INPUT || sp->tree->not_of->type == SELECT_CONSTANT)
				return 0;

			if(!is_valid_selection_params(&((selection_params){sp->tree->not_of, sp->input_def})))
				return 0;

			return 1;
		}

		case SELECT_AND :
		case SELECT_OR :
		case SELECT_XOR :
		{
			if(is_empty_singlylist(&(sp->tree->logi_of)))
				return 0;

			for(selection_tree* x = (selection_tree*) get_head_of_singlylist(&(sp->tree->logi_of)); x != NULL; x = (selection_tree*) get_next_of_in_singlylist(&(sp->tree->logi_of), x))
			{
				if(x->type == SELECT_INPUT || x->type == SELECT_CONSTANT)
					return 0;

				if(!is_valid_selection_params(&((selection_params){x, sp->input_def})))
					return 0;
			}

			return 1;
		}

		case SELECT_EQ :
		case SELECT_NE :
		case SELECT_GT :
		case SELECT_LT :
		case SELECT_GTE :
		case SELECT_LTE :
		{
			if(sp->tree->lhs == NULL)
				return 0;
			if(sp->tree->lhs->type != SELECT_INPUT && sp->tree->lhs->type != SELECT_CONSTANT) // we can only compare input or a constant
				return 0;
			if(!is_valid_selection_params(&((selection_params){sp->tree->lhs, sp->input_def})))
				return 0;

			if(sp->tree->rhs == NULL)
				return 0;
			if(sp->tree->rhs->type != SELECT_INPUT && sp->tree->rhs->type != SELECT_CONSTANT)
				return 0;
			if(!is_valid_selection_params(&((selection_params){sp->tree->rhs, sp->input_def})))
				return 0;

			// TODO: ensure that the lhs and rhs are of comparable type, use a functions that also encompases types from TupleLargeTypes
			if(!can_compare_user_value(get_type_of_selection_tree_node(sp->tree->lhs, sp->input_def), get_type_of_selection_tree_node(sp->tree->rhs, sp->input_def)))
				return 0;

			return 1;
		}

		case SELECT_TRUE :
		case SELECT_FALSE :
		{
			return 1;
		}

		case SELECT_INPUT :
		{
			if(get_type_of_selection_tree_node(sp->tree, sp->input_def) == NULL)
				return 0;
			return 1;
		}

		case SELECT_CONSTANT :
		{
			return 1;
		}
	}

	return 0;
}

void optimize_selection_tree(selection_tree* tree);

int do_select(const void* input_tuple, const selection_params* sp)
{
	switch(sp->tree->type)
	{
		case SELECT_NOT :
		{
			int res = do_select(input_tuple, &((selection_params){sp->tree->not_of, sp->input_def}));
			if(res == -1)
				return -1;

			return !res;
		}

		case SELECT_AND :
		{
			int res = 1;

			for(selection_tree* x = (selection_tree*) get_head_of_singlylist(&(sp->tree->logi_of)); x != NULL && res != 0; x = (selection_tree*) get_next_of_in_singlylist(&(sp->tree->logi_of), x))
			{
				int temp_res = do_select(input_tuple, &((selection_params){x, sp->input_def}));
				if(temp_res == -1)
					return -1;

				res = res && temp_res;
			}

			return res;
		}

		case SELECT_OR :
		{
			int res = 0;

			for(selection_tree* x = (selection_tree*) get_head_of_singlylist(&(sp->tree->logi_of)); x != NULL && res != 1; x = (selection_tree*) get_next_of_in_singlylist(&(sp->tree->logi_of), x))
			{
				int temp_res = do_select(input_tuple, &((selection_params){x, sp->input_def}));
				if(temp_res == -1)
					return -1;

				res = res || temp_res;
			}

			return res;
		}

		case SELECT_XOR :
		{
			int res = 0;

			for(selection_tree* x = (selection_tree*) get_head_of_singlylist(&(sp->tree->logi_of)); x != NULL; x = (selection_tree*) get_next_of_in_singlylist(&(sp->tree->logi_of), x))
			{
				int temp_res = do_select(input_tuple, &((selection_params){x, sp->input_def}));
				if(temp_res == -1)
					return -1;

				res = res ^ temp_res;
			}

			return res;
		}

		case SELECT_EQ :
		case SELECT_NE :
		case SELECT_GT :
		case SELECT_LT :
		case SELECT_GTE :
		case SELECT_LTE :
		{
			int error = 0;

			typed_user_value lhs = get_value_of_selection_tree_node(sp->tree->lhs, sp->input_def, input_tuple, &error);
			if(error)
				return -1;

			typed_user_value rhs = get_value_of_selection_tree_node(sp->tree->rhs, sp->input_def, input_tuple, &error);
			if(error)
			{
				destroy_typed_user_value(lhs);
				return -1;
			}

			int res;

			// TODO : use a more elaborate compare function that also includes types from TupleLargeTypes
			int cmp = compare_user_value(&(lhs.value), lhs.type, &(rhs.value), rhs.type);

			switch(sp->tree->type)
			{
				case SELECT_EQ :
				{
					res = (cmp == 0);
					break;
				}
				case SELECT_NE :
				{
					res = (cmp != 0);
					break;
				}
				case SELECT_GT :
				{
					res = (cmp > 0);
					break;
				}
				case SELECT_LT :
				{
					res = (cmp < 0);
					break;
				}
				case SELECT_GTE :
				{
					res = (cmp >= 0);
					break;
				}
				case SELECT_LTE :
				{
					res = (cmp <= 0);
					break;
				}
				default :
				{
					res = -1;
					break;
				}
			}

			destroy_typed_user_value(lhs);
			destroy_typed_user_value(rhs);

			return res;
		}

		case SELECT_TRUE :
		{
			return 1;
		}
		case SELECT_FALSE :
		{
			return 0;
		}

		// never reaches here
		case SELECT_INPUT :
		case SELECT_CONSTANT :
		{
			return -1;
		}
	}

	return -1;
}