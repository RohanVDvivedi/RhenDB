#include<rhendb/util_numeric_conversions.h>

#include<serint/large_uints.h>
#include<serint/large_ints.h>

#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<math.h>

// build a fresh mpd_t returned BY VALUE: static struct (copied out to caller), heap coefficient buffer.
// THE CALLER MUST RELEASE THE RETURNED VALUE WITH mpd_del() (frees .data, not the struct).
// this function ALWAYS returns an mpd_del-able value; on error it returns a quiet NaN.
static mpd_t new_returnable_mpd(void)
{
	mpd_t res;
	res.flags = MPD_STATIC;
	res.exp = 0; res.digits = 0; res.len = 0;
	res.alloc = MPD_MINALLOC;
	res.data = mpd_alloc(MPD_MINALLOC, sizeof(mpd_uint_t));
	if(res.data == NULL)
		exit(-1);
	return res;
}

mpd_t numeric_from_primitive_numeral(const data_type_info* dti, const datum* uval, int* error_code)
{
	(*error_code) = NUMERIC_CONVERSION_SUCCESSFULL;

	mpd_t numeric = new_returnable_mpd();

	mpd_context_t maxctx;
	mpd_maxcontext(&maxctx); // huge precision -> exact for every integer we can hold
	uint32_t status = 0;

	switch(dti->type)
	{
		case BIT_FIELD :
		{
			mpd_qset_u64(&numeric, uval->bit_field_value, &maxctx, &status);
			if(status & MPD_Malloc_error)
				exit(-1);
			break;
		}
		case UINT :
		{
			mpd_qset_u64(&numeric, uval->uint_value, &maxctx, &status);
			if(status & MPD_Malloc_error)
				exit(-1);
			break;
		}
		case INT :
		{
			mpd_qset_i64(&numeric, uval->int_value, &maxctx, &status);
			if(status & MPD_Malloc_error)
				exit(-1);
			break;
		}
		case LARGE_UINT :
		{
			char buf[80] = {};
			serialize_to_decimal_uint256(buf, uval->large_uint_value);
			mpd_qset_string(&numeric, buf, &maxctx, &status);
			if(status & MPD_Malloc_error)
				exit(-1);
			break;
		}
		case LARGE_INT :
		{
			char buf[80] = {};
			serialize_to_decimal_int256(buf, uval->large_int_value);
			mpd_qset_string(&numeric, buf, &maxctx, &status);
			if(status & MPD_Malloc_error)
				exit(-1);
			break;
		}
		case FLOAT :
		{
			double d; int is_flt;
			if(dti->size == sizeof(float))
			{
				d = (double)(uval->float_value);
				is_flt = 1;
			}
			else if(dti->size == sizeof(double))
			{
				d = uval->double_value;
				is_flt = 0;
			}
			else
			{
				(*error_code) = NUMERIC_CONVERSION_TYPE_FAILURE;
				mpd_del(&numeric);
				return numeric;
			}

			if(isnan(d))
				mpd_setspecial(&numeric, MPD_POS, MPD_NAN);
			else if(isinf(d))
				mpd_setspecial(&numeric, signbit(d) ? MPD_NEG : MPD_POS, MPD_INF);
			else
			{
				char buf[64];
				snprintf(buf, sizeof(buf), is_flt ? "%.9g" : "%.17g", d); // round-trip-exact digits
				mpd_qset_string(&numeric, buf, &maxctx, &status);
				if(status & MPD_Malloc_error)
					exit(-1);
			}
			break;
		}
		default :
		{
			(*error_code) = NUMERIC_CONVERSION_TYPE_FAILURE;
			mpd_del(&numeric);
			return numeric;
		}
	}

	return numeric;
}

// reconstruct a 256-bit magnitude (uint256) from an integer mpd into *out.
// returns 0 = ok, or NUMERIC_CONVERSION_UN_REPRESENTABLE (non-integer / > 256 bits).
static int mpd_integer_magnitude_to_uint256(const mpd_t* numeric, uint256* out)
{
	if(mpd_isspecial(numeric) || !mpd_isinteger(numeric))
		return NUMERIC_CONVERSION_UN_REPRESENTABLE;

	mpd_context_t maxctx;
	mpd_maxcontext(&maxctx);
	uint32_t status = 0;

	mpd_t* norm = mpd_qnew();
	if(norm == NULL)
		exit(-1);
	mpd_qrescale(norm, numeric, 0, &maxctx, &status); // exp -> 0 so exported coeff is the full integer
	if(status & MPD_Malloc_error)
		exit(-1);
	if(status & (MPD_Invalid_operation | MPD_Overflow))
	{
		mpd_del(norm);
		return NUMERIC_CONVERSION_UN_REPRESENTABLE;
	}

	uint16_t* words = NULL;
	uint32_t xstatus = 0;
	size_t nwords = mpd_qexport_u16(&words, 0, 65536, norm, &xstatus); // base 65536, little-endian magnitude
	mpd_del(norm);
	if(xstatus & MPD_Malloc_error)
		exit(-1);
	if(nwords == SIZE_MAX)
	{
		if(words)
			mpd_free(words);
		return NUMERIC_CONVERSION_UN_REPRESENTABLE;
	}

	if(nwords > 16)
	{
		if(words)
			mpd_free(words);
		return NUMERIC_CONVERSION_UN_REPRESENTABLE;
	}

	(*out) = get_0_uint256();
	for(size_t i = 0; i < nwords; i++)
	{
		set_byte_in_uint256(out, i * 2, words[i] & 0xff);
		set_byte_in_uint256(out, i * 2 + 1, (words[i] >> 8) & 0xff);
	}
	if(words)
		mpd_free(words);
	return NUMERIC_CONVERSION_SUCCESSFULL;
}

datum numeric_to_primitive_numeral(const data_type_info* dti, const mpd_t* numeric, int* error_code)
{
	(*error_code) = NUMERIC_CONVERSION_SUCCESSFULL;

	datum num = (*NULL_DATUM);

	uint32_t status = 0;

	switch(dti->type)
	{
		case BIT_FIELD :
		{
			uint64_t v = mpd_qget_u64(numeric, &status);
			if(status)
			{
				(*error_code) = NUMERIC_CONVERSION_UN_REPRESENTABLE;
				break;
			}
			if(dti->bit_field_size < 64 && v >= (UINT64_C(1) << dti->bit_field_size))
			{
				(*error_code) = NUMERIC_CONVERSION_UN_REPRESENTABLE;
				break;
			}
			num = (datum){.bit_field_value = v};
			break;
		}
		case UINT :
		{
			uint64_t v = mpd_qget_u64(numeric, &status);
			if(status)
			{
				(*error_code) = NUMERIC_CONVERSION_UN_REPRESENTABLE;
				break;
			}
			if(v >= get_UINT64_MAX(dti->size))
			{
				(*error_code) = NUMERIC_CONVERSION_UN_REPRESENTABLE;
				break;
			}
			num = (datum){.uint_value = v};
			break;
		}
		case INT :
		{
			int64_t v = mpd_qget_i64(numeric, &status);
			if(status)
			{
				(*error_code) = NUMERIC_CONVERSION_UN_REPRESENTABLE;
				break;
			}
			if(v < get_INT64_MIN(dti->size) || get_INT64_MAX(dti->size) < v)
			{
				(*error_code) = NUMERIC_CONVERSION_UN_REPRESENTABLE;
				break;
			}
			num = (datum){.int_value = v};
			break;
		}
		case LARGE_UINT :
		{
			if(mpd_isnegative(numeric) && !mpd_iszero(numeric))
			{
				(*error_code) = NUMERIC_CONVERSION_UN_REPRESENTABLE;
				break;
			}
			uint256 mag;
			int rc = mpd_integer_magnitude_to_uint256(numeric, &mag);
			if(rc)
			{
				(*error_code) = rc;
				break;
			}
			num = (datum){.large_uint_value = mag};
			break;
		}
		case LARGE_INT :
		{
			uint256 mag;
			int rc = mpd_integer_magnitude_to_uint256(numeric, &mag);
			if(rc)
			{
				(*error_code) = rc;
				break;
			}
			int neg = mpd_isnegative(numeric) && !mpd_iszero(numeric);
			int256 result;
			if(!neg)
			{
				if(get_bit_from_uint256(mag, 255))
				{
					(*error_code) = NUMERIC_CONVERSION_UN_REPRESENTABLE;
					break;
				} // >= 2^255 positive
				result = (int256){.raw_uint_value = mag};
			}
			else
			{
				uint256 raw;add_uint256(&raw, bitwise_not_uint256(mag), get_1_uint256()); // two's complement
				result = (int256){.raw_uint_value = raw};
				if(get_sign_bit_int256(result) != 1)
				{
					(*error_code) = NUMERIC_CONVERSION_UN_REPRESENTABLE;
					break;
				} // magnitude > 2^255
			}
			num = (datum){.large_int_value = result};
			break;
		}
		case FLOAT :
		{
			double d;
			if(mpd_isnan(numeric))
				d = NAN;
			else if(mpd_isinfinite(numeric))
				d = mpd_isnegative(numeric) ? -INFINITY : INFINITY;
			else
			{
				char* s = mpd_to_sci(numeric, 1);
				if(s == NULL)
					exit(-1);
				d = strtod(s, NULL);
				mpd_free(s);
			}
			if(dti->size == sizeof(float))
				num = (datum){.float_value = d};
			else if(dti->size == sizeof(double))
				num = (datum){.double_value = d};
			else
				(*error_code) = NUMERIC_CONVERSION_TYPE_FAILURE;
			break;
		}
		default :
			(*error_code) = NUMERIC_CONVERSION_TYPE_FAILURE;
			break;
	}

	return num;
}