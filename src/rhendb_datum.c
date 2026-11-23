#include<rhendb/rhendb_datum.h>

static int get_containee_from_rhendb_datum(rhendb_datum* uval_c, const data_type_info** dti_c, const rhendb_datum* uval, const data_type_info* dti, uint32_t index)
{
	switch(uval->type)
	{
		case RAW_DATUM :
		{
			if(get_containee_from_datum(&(uval_c->as_raw_datum.value), dti_c, &(uval->as_raw_datum.value), dti, index))
			{
				uval_c->type = RAW_DATUM;
				return 1;
			}
			return 0;
		}
		case REF_DATUM :
		{
			rhendb_datum src_uval;
			const data_type_info* dti_temp;
			if(!get_nested_containee_from_rhendb_datum(&src_uval, &dti_temp, uval->as_ref_datum.src_datum, uval->as_ref_datum.src_dti, *(uval->as_ref_datum.src_pos)))
				return 0;

			return get_containee_from_rhendb_datum(uval_c, dti_c, &src_uval, dti, index);
		}
		case ARR_DATUM : // dti is expected to be tuple
		{
			(*dti_c) = get_data_type_info_for_containee_of_container_without_data(dti, index);
			(*uval_c) = (*(uval->as_arr_datum[index]));

			return 1;
		}
		default :
			return 0;
	}
}

// (*below macro is for internl use only)
// it initializes a new positional accessor that will make you point to the next position you need to extract next
#define NEXT_POSITION(pa) ((positional_accessor){.positions_length = pa.positions_length - 1, .positions = pa.positions + 1}) 	// build a positional accessor for the next nested object (* for internal use only)

int get_nested_containee_from_rhendb_datum(rhendb_datum* uval_c, const data_type_info** dti_c, const rhendb_datum* uval, const data_type_info* dti, positional_accessor pa)
{
	(*uval_c) = (*uval);
	(*dti_c) = dti;

	int result = 1;

	while(1)
	{
		// result is self
		if(IS_SELF(pa))
			break;

		{
			rhendb_datum temp_uval;
			const data_type_info* temp_dti;
			result = get_containee_from_rhendb_datum(&temp_uval, &temp_dti, uval_c, (*dti_c), pa.positions[0]);
			if(!result)
			{
				result = 0;
				(*dti_c) = NULL;
				break;
			}
			(*uval_c) = temp_uval;
			(*dti_c) = temp_dti;
		}

		pa = NEXT_POSITION(pa);
	}

	return result;
}

uint64_t hash_rhendb_datum(const rhendb_datum* uval, const data_type_info* dti, tuple_hasher* th, transaction* tx)
{
	switch(uval->type)
	{
		case RAW_DATUM :
		{
			return hash_datum_rhendb(&(uval->as_raw_datum.value), dti, th, tx);
		}
		case REF_DATUM :
		{
			rhendb_datum src_uval;
			const data_type_info* src_dti;
			if(!get_nested_containee_from_rhendb_datum(&src_uval, &src_dti, uval->as_ref_datum.src_datum, uval->as_ref_datum.src_dti, *(uval->as_ref_datum.src_pos)))
				return th->hash;

			return hash_rhendb_datum(&src_uval, src_dti, th, tx);
		}
		case ARR_DATUM : // dti is expected to be tuple
		{
			for(uint32_t index = 0; index < dti->element_count; index++)
				hash_rhendb_datum(uval->as_arr_datum[index], dti->containees[index].al.type_info, th, tx);

			return th->hash;
		}
		default :
			return th->hash;
	}
}

uint64_t hash_rhendb_tuple(const rhendb_datum* uval, const data_type_info* dti, const positional_accessor* element_ids, tuple_hasher* th, uint32_t element_count, transaction* tx)
{
	uint64_t hash_value = th->hash;

	for(uint32_t i = 0; i < element_count; i++)
	{
		rhendb_datum uval_c;
		const data_type_info* dti_c;
		if(!get_nested_containee_from_rhendb_datum(&uval_c, &dti_c, uval, dti, (element_ids != NULL) ? element_ids[i] : STATIC_POSITION(i)))
			uval_c = (rhendb_datum){.type = RAW_DATUM, .as_raw_datum.value = (*NULL_DATUM)};

		hash_value = hash_rhendb_datum(&uval_c, dti_c, th, tx);
	}

	return hash_value;
}

int compare_rhendb_datum(const rhendb_datum* uval1, const data_type_info* dti1, const rhendb_datum* uval2, const data_type_info* dti2, transaction* tx)
{
	if(uval1->type == RAW_DATUM && uval2->type == RAW_DATUM)
		return compare_datum_rhendb(&(uval1->as_raw_datum.value), dti1, &(uval2->as_raw_datum.value), dti2, tx);

	if(uval1->type == REF_DATUM)
	{
		rhendb_datum src_uval;
		const data_type_info* src_dti;
		if(!get_nested_containee_from_rhendb_datum(&src_uval, &src_dti, uval1->as_ref_datum.src_datum, uval1->as_ref_datum.src_dti, *(uval1->as_ref_datum.src_pos)))
			src_uval = (rhendb_datum){.type = RAW_DATUM, .as_raw_datum.value = (*NULL_DATUM)};

		return compare_rhendb_datum(&src_uval, src_dti, uval2, dti2, tx);
	}

	if(uval2->type == REF_DATUM)
	{
		return -compare_rhendb_datum(uval2, dti2, uval1, dti1, tx);
	}

	// one or both of them are ARR_DATUM
	// dtis are expected to be tuple
	{
		int cmp = 0;

		for(uint32_t i = 0; i < dti1->element_count && cmp == 0; i++)
		{
			rhendb_datum uval_c1;
			const data_type_info* dti_c1;
			if(!get_containee_from_rhendb_datum(&uval_c1, &dti_c1, uval1, dti1, i))
				 uval_c1 = (rhendb_datum){.type = RAW_DATUM, .as_raw_datum.value = (*NULL_DATUM)};

			rhendb_datum uval_c2;
			const data_type_info* dti_c2;
			if(!get_containee_from_rhendb_datum(&uval_c2, &dti_c2, uval2, dti2, i))
				 uval_c2 = (rhendb_datum){.type = RAW_DATUM, .as_raw_datum.value = (*NULL_DATUM)};

			cmp = compare_rhendb_datum(&uval_c1, dti_c1, &uval_c2, dti_c2, tx);
		}

		return cmp;
	}
}

int compare_rhendb_tuples(const rhendb_datum* uval1, const data_type_info* dti1, const positional_accessor* element_ids1, const rhendb_datum* uval2, const data_type_info* dti2, const positional_accessor* element_ids2, const compare_direction* cmp_dir, uint32_t element_count, transaction* tx)
{
	int cmp = 0;

	for(uint32_t i = 0; i < element_count && cmp == 0; i++)
	{
		rhendb_datum uval_c1;
		const data_type_info* dti_c1;
		if(!get_nested_containee_from_rhendb_datum(&uval_c1, &dti_c1, uval1, dti1, (element_ids1 != NULL) ? element_ids1[i] : STATIC_POSITION(i)))
			uval_c1 = (rhendb_datum){.type = RAW_DATUM, .as_raw_datum.value = (*NULL_DATUM)};

		rhendb_datum uval_c2;
		const data_type_info* dti_c2;
		if(!get_nested_containee_from_rhendb_datum(&uval_c2, &dti_c2, uval2, dti1, (element_ids2 != NULL) ? element_ids2[i] : STATIC_POSITION(i)))
			uval_c2 = (rhendb_datum){.type = RAW_DATUM, .as_raw_datum.value = (*NULL_DATUM)};

		cmp = compare_rhendb_datum(&uval_c1, dti_c1, &uval_c2, dti_c2, tx);

		if(cmp_dir)
			cmp = cmp * cmp_dir[i];
	}

	return cmp;
}

static void set_in_tuple_from_rhendb_datum(void* dest_tuple, const tuple_def* def, positional_accessor* pa, uint32_t* pa_capacity, const rhendb_datum* uval, const data_type_info* dti)
{
	switch(uval->type)
	{
		case RAW_DATUM :
		{
			set_element_in_tuple(def, *pa, dest_tuple, &(uval->as_raw_datum.value), UINT32_MAX);
			break;
		}
		case REF_DATUM :
		{
			rhendb_datum src_uval;
			const data_type_info* dti_temp;
			if(!get_nested_containee_from_rhendb_datum(&src_uval, &dti_temp, uval->as_ref_datum.src_datum, uval->as_ref_datum.src_dti, *(uval->as_ref_datum.src_pos)))
				return ;

			set_in_tuple_from_rhendb_datum(dest_tuple, def, pa, pa_capacity, &src_uval, dti);
			break;
		}
		case ARR_DATUM :
		{
			set_element_in_tuple(def, *pa, dest_tuple, EMPTY_DATUM, UINT32_MAX);

			if(pa->positions_length == (*pa_capacity))
			{
				(*pa_capacity) = 1.5 * (*pa_capacity);
				pa->positions = realloc(pa->positions, (*pa_capacity) * sizeof(uint32_t));
			}

			pa->positions_length++;

			for(uint32_t i = 0; i < dti->element_count; i++)
			{
				pa->positions[pa->positions_length-1] = i;
				const data_type_info* dti_c = get_data_type_info_for_containee_of_container_without_data(dti, i);

				set_in_tuple_from_rhendb_datum(dest_tuple, def, pa, pa_capacity, uval->as_arr_datum[i], dti_c);
			}

			pa->positions_length--;

			break;
		}
	}
}

void build_tuple_from_rhendb_datum(void* dest_tuple, const rhendb_datum* uval, const tuple_def* def)
{
	positional_accessor pa = SELF;
	uint32_t pa_capacity = 10;
	pa.positions = realloc(pa.positions, pa_capacity * sizeof(uint32_t));

	set_in_tuple_from_rhendb_datum(dest_tuple, def, &pa, &pa_capacity, uval, def->type_info);

	if(pa.positions != NULL)
		free(pa.positions);
}

void* make_tuple_from_rhendb_datum(const rhendb_datum* uval, const tuple_def* def)
{
	void* dest_tuple = malloc(get_maximum_tuple_size(def));

	build_tuple_from_rhendb_datum(dest_tuple, uval, def);

	return realloc(dest_tuple, get_tuple_size(def, dest_tuple));
}