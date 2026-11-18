#include<rhendb/query_plan.h>
#include<rhendb/operator_error_codes.h>

#include<rhendb/cash_table.h>
#include<rhendb/grash_table.h>

#include<rhendb/aggregate_functions.h>

typedef struct op_state op_state;
struct op_state
{
	operator* input_operator;

	const tuple_def* input_tuple_def;

	cash_table* cash;
	grash_table* grash;

	grash_table_node* curr_node;

	cash_table_entry* curr_entry;

	datum* input_datums; // enough datums to accomodate max inputs from any aggregate function 
};

static void revise_key_datums_from_tuple(datum* key_datums, const void* tuple, const tuple_def* tpl_d, const positional_accessor* key_element_ids, uint32_t key_element_count)
{
	for(uint32_t i = 0; i < key_element_count; i++)
	{
		const data_type_info* dti_temp;
		if(!get_value_from_element_from_tuple(&(key_datums[i]), &dti_temp, tpl_d, key_element_ids[i], tuple))
			key_datums[i] = (*NULL_DATUM);
	}
}

static void* next_tuple(operator* o, int* error_code)
{
	// already dead
	if(o->state == NULL)
		return NULL;

	op_state* this_state = o->state;

	if(this_state->input_operator != NULL) // consuming inputs is still pending
	{
		while(1)
		{
			void* tuple = this_state->input_operator->next_tuple(this_state->input_operator, error_code);
			if(tuple == NULL)
			{
				if(*error_code)
				{
					o->destroy_state(o);
					return NULL;
				}
				this_state->input_operator->destroy_state(this_state->input_operator);
				this_state->input_operator = NULL;

				if(this_state->grash != NULL) // insertions are done so close the files
				{
					unmap_all_partition_mmaped_regions(this_state->grash->root);
					close_all_partition_files(this_state->grash->root);
				}
				break;
			}

			cash_table_entry* e = get_new_cash_table_value_own(this_state->cash, tuple, 0);

			// find an equals entry
			cash_table_entry* f = find_equals_in_cash_table(this_state->cash, e);

			// if cash has this entry process_input it
			if(f != NULL)
			{
				// process the aggregate function calls for this tuple
				for(uint32_t i = 0; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++)
				{
					// generate input params to the i-th udaf in input_datums
					revise_key_datums_from_tuple(this_state->input_datums, tuple, this_state->input_tuple_def, o->desc->params.hash_aggregation.aggregate_input_element_ids[i], o->desc->params.hash_aggregation.aggregate_functions[i]->input_type_infos_count);

					// process_input for the udaf, if it fails kill the operator
					if(!o->desc->params.hash_aggregation.aggregate_functions[i]->process_input(o->desc->params.hash_aggregation.aggregate_functions[i], &(((void**)(f->as_value.state_p))[i]), this_state->input_datums))
					{
						delete_cash_table_entry(e);
						o->destroy_state(o);
						(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
						return NULL;
					}
				}

				// entry owns tuple, both get deleted, state was never inited
				delete_cash_table_entry(e);
			}
			else
			{
				if(this_state->grash == NULL) // if grash was not initialized, i.e. spilling not started insert into cash
				{
					// make it a new state_p and insert it
					e->as_value.state_p = calloc(o->desc->params.hash_aggregation.aggregate_functions_count, sizeof(void*));
					insert_in_cash_table(this_state->cash, e);

					// process the aggregate function calls for this tuple
					for(uint32_t i = 0; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++)
					{
						// generate input params to the i-th udaf in input_datums
						revise_key_datums_from_tuple(this_state->input_datums, tuple, this_state->input_tuple_def, o->desc->params.hash_aggregation.aggregate_input_element_ids[i], o->desc->params.hash_aggregation.aggregate_functions[i]->input_type_infos_count);

						// process_input for the udaf, if it fails kill the operator
						if(!o->desc->params.hash_aggregation.aggregate_functions[i]->process_input(o->desc->params.hash_aggregation.aggregate_functions[i], &(((void**)(e->as_value.state_p))[i]), this_state->input_datums))
						{
							o->destroy_state(o);
							(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
							return NULL;
						}
					}

					// see if there is enough tuples to start spilling
					if(o->desc->params.hash_aggregation.min_part_size < this_state->cash->memory_utilization)
						this_state->grash = get_new_grash_table(this_state->input_tuple_def, o->desc->params.hash_aggregation.key_element_ids, o->desc->params.hash_aggregation.key_element_count, o->desc->params.hash_aggregation.min_part_size, o->self_query_plan->desc->curr_tx);
				}
				else
				{
					insert_to_grash_table(this_state->grash, tuple, e->hash_value);

					delete_cash_table_entry(e);
				}
			}
		}

		// start iteration
		this_state->curr_entry = find_all_in_cash_table(this_state->cash);
	}

	while(1)
	{
		if(this_state->curr_entry != NULL)
		{
			// preserve curr_entry and go next 
			cash_table_entry* e = this_state->curr_entry;
			this_state->curr_entry = next_all_in_cash_table(this_state->cash, this_state->curr_entry);

			// create output tuple
			void* output_tuple = malloc(get_maximum_tuple_size(o->desc->output_tuple_def));
			init_tuple(o->desc->output_tuple_def, output_tuple);

			// set keys in output_tuple
			for(uint32_t i = 0; i < o->desc->params.hash_aggregation.key_element_count; i++)
			{
				const data_type_info* dti_temp;
				datum key_val;
				if(get_value_from_element_from_tuple(&key_val, &dti_temp, this_state->input_tuple_def, o->desc->params.hash_aggregation.key_element_ids[i], e->as_value.value))
					set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(i), output_tuple, &key_val, UINT32_MAX);
			}

			// set aggregation results in output_tuple
			for(uint32_t i = 0, j = o->desc->params.hash_aggregation.key_element_count; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++, j++)
			{
				// produce the output_uval the output of the i-th aggregate function
				datum output_uval;
				if(!o->desc->params.hash_aggregation.aggregate_functions[i]->produce_output(o->desc->params.hash_aggregation.aggregate_functions[i], &output_uval, &(((void**)(e->as_value.state_p))[i])))
				{
					free(output_tuple);
					o->destroy_state(o);
					(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
					return NULL;
				}

				set_element_in_tuple(o->desc->output_tuple_def, STATIC_POSITION(j), output_tuple, &output_uval, UINT32_MAX);
			}

			// destroy the corresponding cash_table entry we just processed
			remove_from_cash_table(this_state->cash, e);
			for(uint32_t i = 0; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++)
				o->desc->params.hash_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.hash_aggregation.aggregate_functions[i], &(((void**)(e->as_value.state_p))[i]));
			free(e->as_value.state_p);
			delete_cash_table_entry(e);

			return output_tuple;
		}

		// we never spilled
		if(this_state->grash == NULL)
			break;

		// if we just processed a partition fron grash, then unmap it's region and close it's file
		if(this_state->curr_node != NULL)
		{
			unmap_all_partition_mmaped_regions(this_state->curr_node);
			close_all_partition_files(this_state->curr_node);
		}

		// we come here only if cash is empty, we now need to populate it using the grash partition

		// find the next grash partition to process and store it in curr_node
		{
			uint64_t next_child_node = 0;

			if(this_state->curr_node == NULL) // we are just init-ing
			{
				next_child_node = 0;

				this_state->curr_node = this_state->grash->root;
			}
			else if(this_state->curr_node->type == PARTITION) // we are pointing to some active partition that was already indexed previously
			{
				next_child_node = this_state->curr_node->index_in_parent + 1;

				this_state->curr_node = this_state->curr_node->parent;
			}

			while(this_state->curr_node != NULL)
			{
				if(this_state->curr_node->type == COLLECTION)
				{
					if(next_child_node == PARTITIONS_PER_COLLECTION)
					{
						next_child_node = this_state->curr_node->index_in_parent + 1;

						this_state->curr_node = this_state->curr_node->parent;
					}
					else
					{
						this_state->curr_node = this_state->curr_node->partitions[next_child_node];

						next_child_node = 0;
					}
				}
				else // this is a partition on both sides
				{
					// partition is small enough, we are done
					if(get_total_bytes_in_interim_tuple_store(this_state->curr_node->partition) < o->desc->params.hash_aggregation.min_part_size)
						break;

					if(!partition_grash_table_node(this_state->grash, this_state->curr_node, 0))
						break;

					// partitioning was success, close the resources
					unmap_all_partition_mmaped_regions(this_state->curr_node);
					close_all_partition_files(this_state->curr_node);

					next_child_node = 0;

					continue;
				}
			}
		}

		// no grash node to iterate on next
		if(this_state->curr_node == NULL)
			break;

		// process the curr_node, and populate cash from it

		// open the file
		open_file_for_interim_tuple_store(this_state->curr_node->partition);

		uint64_t offset = 0;
		mmap_for_reading_tuple(this_state->curr_node->partition, &(this_state->curr_node->partition->embed_regions[0]), offset, &(this_state->grash->hash_def.size_def), get_total_bytes_in_interim_tuple_store(this_state->curr_node->partition));

		while(offset < this_state->curr_node->partition->next_tuple_offset)
		{
			mmap_for_reading_tuple(this_state->curr_node->partition, &(this_state->curr_node->partition->embed_regions[0]), offset, &(this_state->grash->hash_def.size_def), get_total_bytes_in_interim_tuple_store(this_state->curr_node->partition));
			uint64_t hash_value = 0;
			{
				const data_type_info* dti_temp = NULL;
				datum uval;
				get_value_from_element_from_tuple(&uval, &dti_temp, &(this_state->grash->hash_def), SELF, this_state->curr_node->partition->embed_regions[0].tuple);
				hash_value = uval.uint_value;
			}
			offset = next_tuple_offset_for_interim_tuple_region(&(this_state->curr_node->partition->embed_regions[0]));

			mmap_for_reading_tuple(this_state->curr_node->partition, &(this_state->curr_node->partition->embed_regions[0]), offset, &(this_state->input_tuple_def->size_def), get_total_bytes_in_interim_tuple_store(this_state->curr_node->partition));
			void* tuple = this_state->curr_node->partition->embed_regions[0].tuple;
			offset = next_tuple_offset_for_interim_tuple_region(&(this_state->curr_node->partition->embed_regions[0]));

			// insert this tuple or process_input on the partition
			cash_table_entry* e = get_new_cash_table_value_borrow(this_state->cash, tuple, hash_value);

			// find an equals entry
			cash_table_entry* f = find_equals_in_cash_table(this_state->cash, e);

			// if cash has this entry process_input it
			if(f != NULL)
			{
				// process the aggregate function calls for this tuple
				for(uint32_t i = 0; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++)
				{
					// generate input params to the i-th udaf in input_datums
					revise_key_datums_from_tuple(this_state->input_datums, tuple, this_state->input_tuple_def, o->desc->params.hash_aggregation.aggregate_input_element_ids[i], o->desc->params.hash_aggregation.aggregate_functions[i]->input_type_infos_count);

					// process_input for the udaf, if it fails kill the operator
					if(!o->desc->params.hash_aggregation.aggregate_functions[i]->process_input(o->desc->params.hash_aggregation.aggregate_functions[i], &(((void**)(f->as_value.state_p))[i]), this_state->input_datums))
					{
						delete_cash_table_entry(e);
						o->destroy_state(o);
						(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
						return NULL;
					}
				}

				// entry owns tuple, both get deleted, state was never inited
				delete_cash_table_entry(e);
			}
			else
			{
				// make it a new state_p and insert it
				e->as_value.state_p = calloc(o->desc->params.hash_aggregation.aggregate_functions_count, sizeof(void*));
				insert_in_cash_table(this_state->cash, e);

				// process the aggregate function calls for this tuple
				for(uint32_t i = 0; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++)
				{
					// generate input params to the i-th udaf in input_datums
					revise_key_datums_from_tuple(this_state->input_datums, tuple, this_state->input_tuple_def, o->desc->params.hash_aggregation.aggregate_input_element_ids[i], o->desc->params.hash_aggregation.aggregate_functions[i]->input_type_infos_count);

					// process_input for the udaf, if it fails kill the operator
					if(!o->desc->params.hash_aggregation.aggregate_functions[i]->process_input(o->desc->params.hash_aggregation.aggregate_functions[i], &(((void**)(e->as_value.state_p))[i]), this_state->input_datums))
					{
						o->destroy_state(o);
						(*error_code) = AGGREGATION_ERROR_IN_OPERATOR;
						return NULL;
					}
				}
			}
		}

		// start iteration
		this_state->curr_entry = find_all_in_cash_table(this_state->cash);
	}

	o->destroy_state(o);
	return NULL;
}

static void destroy_state(operator* o)
{
	if(o->state != NULL)
	{
		op_state* this_state = o->state;

		if(this_state->input_operator != NULL)
			this_state->input_operator->destroy_state(this_state->input_operator);

		if(this_state->cash != NULL)
		{
			for(cash_table_entry* e = find_all_in_cash_table(this_state->cash); e != NULL; e = next_all_in_cash_table(this_state->cash, e))
			{
				for(uint32_t i = 0; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++)
					o->desc->params.hash_aggregation.aggregate_functions[i]->destroy_state(o->desc->params.hash_aggregation.aggregate_functions[i], &(((void**)(e->as_value.state_p))[i]));
				free(e->as_value.state_p);
			}
			delete_cash_table(this_state->cash);
		}

		if(this_state->grash != NULL)
			delete_grash_table(this_state->grash);

		if(this_state->input_datums != NULL)
			free(this_state->input_datums);

		free(this_state);
	}
	o->state = NULL;
}

void open_hash_aggregation_operator(operator* o)
{
	o->state = malloc(sizeof(op_state));

	op_state* this_state = o->state;

	this_state->input_operator = (operator*) get_from_front_of_arraylist(&(o->self_query_plan->operators), o->desc->params.hash_aggregation.input_operator_index);

	this_state->input_tuple_def = this_state->input_operator->desc->output_tuple_def;

	this_state->cash = get_new_cash_table(100, this_state->input_tuple_def, o->desc->params.hash_aggregation.key_element_ids, o->desc->params.hash_aggregation.key_element_count, o->self_query_plan->desc->curr_tx);
	this_state->grash = NULL;

	this_state->curr_node = NULL;

	this_state->curr_entry = NULL;

	uint32_t max_input_datums_count = 0;
	for(uint32_t i = 0; i < o->desc->params.hash_aggregation.aggregate_functions_count; i++)
		max_input_datums_count = max(max_input_datums_count, o->desc->params.hash_aggregation.aggregate_functions[i]->input_type_infos_count);
	this_state->input_datums = malloc(sizeof(datum) * max_input_datums_count);

	o->next_tuple = next_tuple;
	o->destroy_state = destroy_state;
}