#ifndef _TIME_UTIL_H_
#define _TIME_UTIL_H_

#include <time.h>

// Handle for timing.
typedef clock_t bench_timer_t;

/** Starts the clock for a benchmark. */
bench_timer_t time_start() {
  bench_timer_t t = clock();
  return t;
}

/** Stops the clock and returns time elapsed in seconds.
 * Throws an error if time__start() was not called first.
 * */
double time_stop(bench_timer_t start) {
  clock_t _end = clock();
  return ((double)(_end - start)) / CLOCKS_PER_SEC;
}

#endif
