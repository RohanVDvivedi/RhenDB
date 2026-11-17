#include<rhendb/query_plan.h>

#include<rhendb/cash_table.h>
#include<rhendb/grash_table.h>

#include<rhendb/join_type.h>

#define INIT_BUCKET_COUNT 64
#define MAX_LOAD_FACTOR 1.3

typedef struct op_state op_state;
struct op_state
{
	operator* right_input_operator;
	operator* left_input_operator;

	const tuple_def* left_tuple_def;
	const tuple_def* right_tuple_def;

	// in_memory state

	cash_table* right_cash;

	// spilling state

	grash_table* right_grash;
	grash_table* left_grash;

	cash_table* right_grash_index;

	grash_table_node* right_curr_node; // current node being probed
	grash_table_node* left_curr_node; // current node from which keys are selected to be probed

	unsigned int curr_nodes_processed:1; // suggests if right_curr_node and left_curr_node should be advanced

	uint64_t left_curr_node_offset;
	uint64_t right_curr_node_offset;

	// probe key

	unsigned int is_valid_left_probe_key:1;
	cash_table_entry left_probe_key;
};

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	// build phase
	if(this_state->right_input_operator != NULL)
	{
		while(1)
		{
			// get right side tuple for building the hash table
			void* right_tuple = this_state->right_input_operator->next_tuple(this_state->right_input_operator, error_code);
			if(right_tuple == NULL)
			{
				if(*error_code)
				{
					o->destroy_state(o);
					return NULL;
				}
				this_state->right_input_operator->destroy_state(this_state->right_input_operator);
				this_state->right_input_operator = NULL;

				if(this_state->right_grash != NULL)
				{
					unmap_all_partition_mmaped_regions(this_state->right_grash->root);
					close_all_partition_files(this_state->right_grash->root);
				}
				break;
			}

			// build
			{
				if(this_state->right_cash != NULL)
				{
					int inserted = 0;
					cash_table_entry* e = get_new_cash_table_value_own(this_state->right_cash, right_tuple, 0);
					if(find_equals_in_cash_table(this_state->right_cash, e) == NULL)
						inserted = insert_in_cash_table(this_state->right_cash, e);
					else
						delete_cash_table_entry(e);

					if(inserted && o->desc->params.hash_semi_join.min_part_size < this_state->right_cash->memory_utilization)
					{
						// initialize probe side partition also, then create right_grash (that we will be actively inserting to)
						this_state->left_grash = get_new_grash_table(this_state->left_tuple_def, o->desc->params.hash_semi_join.left_key_element_ids, o->desc->params.hash_semi_join.key_element_count, o->desc->params.hash_semi_join.min_part_size, o->self_query_plan->desc->curr_tx);
						// insertions to the left will be done later, so close those files for now
						unmap_all_partition_mmaped_regions(this_state->left_grash->root);
						close_all_partition_files(this_state->left_grash->root);

						this_state->right_grash = get_new_grash_table(this_state->right_tuple_def, o->desc->params.hash_semi_join.right_key_element_ids, o->desc->params.hash_semi_join.key_element_count, o->desc->params.hash_semi_join.min_part_size, o->self_query_plan->desc->curr_tx);

						for(const cash_table_entry* e = find_all_in_cash_table(this_state->right_cash); e != NULL; e = next_all_in_cash_table(this_state->right_cash, e))
							insert_to_grash_table(this_state->right_grash, e->as_value.value, e->hash_value);

						delete_cash_table(this_state->right_cash);
						this_state->right_cash = NULL;
					}
				}
				else // else right grash must be present
				{
					insert_to_grash_table(this_state->right_grash, right_tuple, 0);

					free(right_tuple);
				}
			}
		}
	}

	// in_memory probe phase
	if(this_state->right_cash != NULL)
	{
		while(this_state->left_input_operator != NULL)
		{
			// get left side tuple for probing the hash table
			void* left_tuple = this_state->left_input_operator->next_tuple(this_state->left_input_operator, error_code);
			if(left_tuple == NULL)
			{
				if(*error_code)
				{
					o->destroy_state(o);
					return NULL;
				}
				this_state->left_input_operator->destroy_state(this_state->left_input_operator);
				this_state->left_input_operator = NULL;
				break;
			}

			if(this_state->is_valid_left_probe_key)
				reset_cash_table_key(&(this_state->left_probe_key), this_state->right_cash, left_tuple, this_state->left_tuple_def, o->desc->params.hash_semi_join.left_key_element_ids, 0);
			else
			{
				this_state->left_probe_key = get_new_cash_table_key(this_state->right_cash, left_tuple, this_state->left_tuple_def, o->desc->params.hash_semi_join.left_key_element_ids, 0);
				this_state->is_valid_left_probe_key = 1;
			}

			int left_key_exists_in_right = (NULL != find_equals_in_cash_table(this_state->right_cash, &(this_state->left_probe_key)));

			void* output_tuple = NULL;
			if(left_key_exists_in_right)
			{
				if(DOES_PRODUCE_MATCHED_LEFT_TUPLES(o->desc->params.hash_semi_join.stype))
					output_tuple = left_tuple;
			}
			else
			{
				if(DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(o->desc->params.hash_semi_join.stype))
					output_tuple = left_tuple;
			}

			if(output_tuple != NULL)
				return output_tuple;
			else
				free(left_tuple);
		}

		// completed
		o->destroy_state(o);
		return NULL;
	}

	// build side spilled to right_grash, spill left side too

	// we will need to partition the probe side just like the build side because we just spilled
	if(this_state->left_input_operator != NULL)
	{
		while(1)
		{
			// get left side tuple for building the probe partitions
			void* left_tuple = this_state->left_input_operator->next_tuple(this_state->left_input_operator, error_code);
			if(left_tuple == NULL)
			{
				if(*error_code)
				{
					o->destroy_state(o);
					return NULL;
				}
				this_state->left_input_operator->destroy_state(this_state->left_input_operator);
				this_state->left_input_operator = NULL;

				if(this_state->left_grash != NULL)
				{
					unmap_all_partition_mmaped_regions(this_state->left_grash->root);
					close_all_partition_files(this_state->left_grash->root);
				}
				break;
			}

			// build partition
			{
				insert_to_grash_table(this_state->left_grash, left_tuple, 0);

				free(left_tuple);
			}
		}
	}

	// now we need to recurse on both right and left side partitons
	// every time right side (the build side) is too big, then partition it, and do it similarly on the left side
	// once they are not partitionable, then index right side and probe left side tuples from it processing inputs

	uint64_t next_child_node = 0;

	if(this_state->right_curr_node == NULL)
	{
		this_state->curr_nodes_processed = 1;

		next_child_node = 0;

		this_state->left_curr_node = this_state->left_grash->root;
		this_state->right_curr_node = this_state->right_grash->root;
	}

	while(1)
	{
		// if the current nodes have been processed advance to the next node
		if(this_state->curr_nodes_processed)
		{
			// remove this particular partition from the current one to be iterated
			if(this_state->right_curr_node->type == PARTITION)
			{
				next_child_node = this_state->right_curr_node->index_in_parent + 1;

				this_state->right_curr_node = this_state->right_curr_node->parent;
				this_state->left_curr_node = this_state->left_curr_node->parent;
			}

			while(this_state->right_curr_node != NULL)
			{
				if(this_state->right_curr_node->type == COLLECTION)
				{
					if(next_child_node == PARTITIONS_PER_COLLECTION)
					{
						next_child_node = this_state->right_curr_node->index_in_parent + 1;

						this_state->right_curr_node = this_state->right_curr_node->parent;
						this_state->left_curr_node = this_state->left_curr_node->parent;
					}
					else
					{
						this_state->right_curr_node = this_state->right_curr_node->partitions[next_child_node];
						this_state->left_curr_node = this_state->left_curr_node->partitions[next_child_node];

						next_child_node = 0;
					}
				}
				else // this is a partition on both sides
				{
					// partition on build side is small enough, we are done
					if(get_total_bytes_in_interim_tuple_store(this_state->right_curr_node->partition) < o->desc->params.hash_semi_join.min_part_size)
						break;

					if(!partition_grash_table_node(this_state->right_grash, this_state->right_curr_node, 0))
						break;

					// partitioning was success, close the resources
					unmap_all_partition_mmaped_regions(this_state->right_curr_node);
					close_all_partition_files(this_state->right_curr_node);

					partition_grash_table_node(this_state->left_grash, this_state->left_curr_node, 1);

					// partitioning has to be a success, close the resources
					unmap_all_partition_mmaped_regions(this_state->left_curr_node);
					close_all_partition_files(this_state->left_curr_node);

					next_child_node = 0;

					continue;
				}
			}

			// both NULL is a terminal state, any 1 being NULL is an error case
			if(this_state->right_curr_node == NULL || this_state->left_curr_node == NULL)
			{
				o->destroy_state(o);
				return NULL;
			}

			this_state->curr_nodes_processed = 0;
		}

		// process both the this_state->right_curr_node and this_state->left_curr_node
		if(this_state->right_grash_index == NULL)
		{
			this_state->right_grash_index = index_grash_table_node(this_state->right_grash, this_state->right_curr_node, 1); // make sure we have a small unique index

			open_file_for_interim_tuple_store(this_state->left_curr_node->partition);

			this_state->left_curr_node_offset = 0;
			mmap_for_reading_tuple(this_state->left_curr_node->partition, &(this_state->left_curr_node->partition->embed_regions[0]), this_state->left_curr_node_offset, &(this_state->left_grash->hash_def.size_def), o->desc->params.hash_semi_join.min_part_size);
		}

		while(this_state->left_curr_node_offset < this_state->left_curr_node->partition->next_tuple_offset)
		{
			uint64_t hash_value = 0;
			{
				const data_type_info* dti_temp = NULL;
				datum uval;
				get_value_from_element_from_tuple(&uval, &dti_temp, &(this_state->left_grash->hash_def), SELF, this_state->left_curr_node->partition->embed_regions[0].tuple);
				hash_value = uval.uint_value;
			}

			this_state->left_curr_node_offset = next_tuple_offset_for_interim_tuple_region(&(this_state->left_curr_node->partition->embed_regions[0]));
			mmap_for_reading_tuple(this_state->left_curr_node->partition, &(this_state->left_curr_node->partition->embed_regions[0]), this_state->left_curr_node_offset, &(this_state->left_tuple_def->size_def), o->desc->params.hash_semi_join.min_part_size);
			void* left_tuple = this_state->left_curr_node->partition->embed_regions[0].tuple;

			if(this_state->is_valid_left_probe_key)
				reset_cash_table_key(&(this_state->left_probe_key), this_state->right_grash_index, left_tuple, this_state->left_tuple_def, o->desc->params.hash_semi_join.left_key_element_ids, hash_value);
			else
			{
				this_state->left_probe_key = get_new_cash_table_key(this_state->right_grash_index, left_tuple, this_state->left_tuple_def, o->desc->params.hash_semi_join.left_key_element_ids, hash_value);
				this_state->is_valid_left_probe_key = 1;
			}

			int left_key_exists_in_right = (NULL != find_equals_in_cash_table(this_state->right_grash_index, &(this_state->left_probe_key)));

			void* output_tuple = NULL;
			if(left_key_exists_in_right)
			{
				if(DOES_PRODUCE_MATCHED_LEFT_TUPLES(o->desc->params.hash_semi_join.stype))
				{
					uint32_t left_tuple_size = get_tuple_size(this_state->left_tuple_def, left_tuple);
					output_tuple = malloc(left_tuple_size);
					memory_move(output_tuple, left_tuple, left_tuple_size);
				}
			}
			else
			{
				if(DOES_PRODUCE_UN_MATCHED_LEFT_TUPLES(o->desc->params.hash_semi_join.stype))
				{
					uint32_t left_tuple_size = get_tuple_size(this_state->left_tuple_def, left_tuple);
					output_tuple = malloc(left_tuple_size);
					memory_move(output_tuple, left_tuple, left_tuple_size);
				}
			}

			this_state->left_curr_node_offset = next_tuple_offset_for_interim_tuple_region(&(this_state->left_curr_node->partition->embed_regions[0]));
			mmap_for_reading_tuple(this_state->left_curr_node->partition, &(this_state->left_curr_node->partition->embed_regions[0]), this_state->left_curr_node_offset, &(this_state->left_grash->hash_def.size_def), o->desc->params.hash_semi_join.min_part_size);
			left_tuple = NULL;

			if(output_tuple != NULL) // if output_tuple is set, clone it and return it
				return output_tuple;
		}

		delete_cash_table(this_state->right_grash_index);
		this_state->right_grash_index = NULL;

		unmap_all_partition_mmaped_regions(this_state->right_curr_node);
		close_all_partition_files(this_state->right_curr_node);

		unmap_all_partition_mmaped_regions(this_state->left_curr_node);
		close_all_partition_files(this_state->left_curr_node);

		this_state->curr_nodes_processed = 1;
	}

	o->destroy_state(o);
	return NULL;
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		if(this_state->left_input_operator != NULL)
			this_state->left_input_operator->destroy_state(this_state->left_input_operator);

		if(this_state->right_input_operator != NULL)
			this_state->right_input_operator->destroy_state(this_state->right_input_operator);

		if(this_state->right_cash != NULL)
			delete_cash_table(this_state->right_cash);

		if(this_state->right_grash_index != NULL)
			delete_cash_table(this_state->right_grash_index);

		if(this_state->right_grash != NULL)
			delete_grash_table(this_state->right_grash);

		if(this_state->left_grash != NULL)
			delete_grash_table(this_state->left_grash);

		if(this_state->is_valid_left_probe_key)
			delete_cash_table_entry(&(this_state->left_probe_key));

		free(this_state);
	}
	o->state = NULL;
}

void open_hash_semi_join_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->left_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.hash_semi_join.left_input_operator_index);
	this_state->right_input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.hash_semi_join.right_input_operator_index);

	this_state->left_tuple_def = this_state->left_input_operator->desc->output_tuple_def;
	this_state->right_tuple_def = this_state->right_input_operator->desc->output_tuple_def;

	this_state->right_cash = get_new_cash_table(100, this_state->right_tuple_def, o->desc->params.hash_semi_join.right_key_element_ids, o->desc->params.hash_semi_join.key_element_count, o->self_query_plan->desc->curr_tx);

	this_state->right_grash = NULL;
	this_state->left_grash = NULL;

	this_state->right_grash_index = NULL;

	this_state->right_curr_node = NULL;
	this_state->left_curr_node = NULL;

	this_state->curr_nodes_processed = 0;

	this_state->left_curr_node_offset = 0;
	this_state->right_curr_node_offset = 0;

	this_state->is_valid_left_probe_key = 0;

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}