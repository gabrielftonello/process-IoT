#define _GNU_SOURCE
#include "stats.h"
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

__attribute__((constructor))
static void stats_init(void) {
    printf("[libstats] library loaded\n");
    fflush(stdout);
}

#if defined(__APPLE__)
#include <sys/sysctl.h>
static int num_cpus(void) {
    int mib[2] = {CTL_HW, HW_AVAILCPU};
    uint32_t count;
    size_t len = sizeof(count);
    if (sysctl(mib, 2, &count, &len, NULL, 0) == -1 || count < 1) {
        mib[1] = HW_NCPU;
        sysctl(mib, 2, &count, &len, NULL, 0);
        if (count < 1) count = 1;
    }
    printf("[libstats] num_cpus => %d\n", (int)count);
    fflush(stdout);
    return (int)count;
}
#else
#include <unistd.h>
static int num_cpus(void) {
    long c = sysconf(_SC_NPROCESSORS_ONLN);
    int cpus = (int)(c > 0 ? c : 1);
    printf("[libstats] num_cpus => %d\n", cpus);
    fflush(stdout);
    return cpus;
}
#endif

typedef struct {
    SensorSeries *series;
    size_t begin;
    size_t end;
} ThreadArg;

static void *worker(void *arg_) {
    ThreadArg *arg = (ThreadArg *)arg_;
    printf("[libstats] worker begin=%zu end=%zu\n", arg->begin, arg->end);
    fflush(stdout);
    for (size_t i = arg->begin; i < arg->end; ++i) {
        SensorSeries *s = &arg->series[i];
        if (s->n == 0) {
            s->min = 0.0;
            s->max = 0.0;
            s->mean = 0.0;
            // printf("[libstats] series %zu empty\n", i);
            continue;
        }
        double min = s->values[0];
        double max = s->values[0];
        double sum = 0.0;
        for (uint32_t k = 0; k < s->n; ++k) {
            double v = s->values[k];
            if (v < min) min = v;
            if (v > max) max = v;
            sum += v;
        }
        s->min = min;
        s->max = max;
        s->mean = sum / (double)s->n;
        // printf("[libstats] series %zu min=%.3f max=%.3f mean=%.3f\n", i, s->min, s->max, s->mean);
        fflush(stdout);
    }
    return NULL;
}

void compute_stats_batch(SensorSeries *series, size_t nseries) {
    if (!series || nseries == 0) return;
    int ncpus = num_cpus();
    int nthreads = ncpus;
    if (nthreads > (int)nseries) nthreads = (int)nseries;
    printf("[libstats] compute_stats_batch nseries=%zu nthreads=%d\n", nseries, nthreads);
    fflush(stdout);
    pthread_t *threads = malloc(sizeof(*threads) * nthreads);
    ThreadArg *args = malloc(sizeof(*args) * nthreads);
    size_t per = nseries / nthreads;
    size_t extra = nseries % nthreads;
    size_t pos = 0;
    for (int t = 0; t < nthreads; ++t) {
        size_t begin = pos;
        size_t end = begin + per + (t < (int)extra);
        args[t].series = series;
        args[t].begin = begin;
        args[t].end = end;
        pthread_create(&threads[t], NULL, worker, &args[t]);
        pos = end;
    }
    for (int t = 0; t < nthreads; ++t) pthread_join(threads[t], NULL);
    free(threads);
    free(args);
    printf("[libstats] compute_stats_batch completed\n");
    fflush(stdout);
}
