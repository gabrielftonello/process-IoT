
#ifndef STATS_H
#define STATS_H
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    double *values;    
    uint32_t n;        
    double min;        
    double max;        
    double mean;       
} SensorSeries;

void compute_stats_batch(SensorSeries *series, size_t nseries);

#ifdef __cplusplus
}
#endif
#endif 
