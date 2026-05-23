#ifndef OPERATOR_RESOURCE_COUNTER_H
#define OPERATOR_RESOURCE_COUNTER_H

/*
	return value for the setup functions of the operators
	add these values (using add_resource_counters) to get to know the amount of resources needed by each of the operators
*/

typedef struct operator_resource_counter operator_resource_counter;
struct operator_resource_counter
{
	// resource required from the buffer_pool

	uint64_t buffer_counter;

	// resource required from the thread_pool

	uint64_t thread_counter;

	uint64_t job_counter;
};

/*
	after final numbers are calculated
	demand (buffer_counter + spare_buffers) number of resources from the rdb->bufferpool_usage_limiter
	and
	demand (thread_counter + ceil(job_counter / N)) number of resources from the rdb->operator_thread_pool_usage_limiter

	here spare_buffers is 2 to 3 buffers to allow keeping some of them in cache a bit longer
	here N (>= 1) is the leverage for threads, jobs come and go, even a single thread for all jobs of the query plan is sometimes fine
	lower N implies we aim for higher parallelism
*/

#define ZERO_OPERATOR_RESOURCE_COUNTER ((operator_resource_counter){})

static inline void add_resource_counters(operator_resource_counter* a, operator_resource_counter b)
{
	a->buffer_counter += b.buffer_counter;
	a->thread_counter += b.thread_counter;
	a->job_counter += b.job_counter;
}

#endif