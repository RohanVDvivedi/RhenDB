#ifndef TUPLES_DOWN_COUNTER_H
#define TUPLES_DOWN_COUNTER_H

typedef struct tuples_down_counter tuples_down_counter;
struct tuples_down_counter
{
	uint64_t counter;
};

/*
	Note:
		counter = 0          -> can not decrement any further
		counter = UINT64_MAX -> can always be decremented and still stays at the same value and greatr than every finite value
*/

#define TUPLES_DOWN_COUNTER_ZERO      ((tuples_down_counter){.counter = 0})
#define TUPLES_DOWN_COUNTER_FIN(c)    ((tuples_down_counter){.counter = c})
#define TUPLES_DOWN_COUNTER_INF       ((tuples_down_counter){.counter = UINT64_MAX})

static inline int is_zero_tuples_down_counter(const tuples_down_counter* tdc_p)
{
	return tdc_p->counter == 0;
}

static inline int is_inf_tuples_down_counter(const tuples_down_counter* tdc_p)
{
	return tdc_p->counter == UINT64_MAX;
}

static inline int can_decrement_tuples_down_counter(const tuples_down_counter* tdc_p)
{
	// only at 0, it can not further be decremented
	return tdc_p->counter != 0;
}

static inline int decrement_tuples_down_counter(tuples_down_counter* tdc_p)
{
	switch(tdc_p->counter)
	{
		case 0 : // can not further decrement
		{
			return 0;
		}
		default : // it is some other finite value, so decrement and return 1
		{
			tdc_p->counter--;
			return 1;
		}
		case UINT64_MAX : // this means infinity, just return 1
		{
			return 1;
		}
	}
	return 0;
}

#endif