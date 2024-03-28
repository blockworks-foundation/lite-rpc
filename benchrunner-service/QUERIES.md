
### Inspect the BenchRunner executions
```postgresql
SELECT * FROM benchrunner.bench_runs
ORDER BY ts DESC
```

### Bench1 (old bench)
```postgresql
SELECT * FROM benchrunner.bench_metrics_bench1
ORDER BY ts DESC
```

### Bench Confirmation Rate
```postgresql
SELECT * FROM benchrunner.bench_metrics_confirmation_rate
ORDER BY ts DESC
```

```postgresql
-- note: arrays are 1-based; histogram array has 21 elements
SELECT
    ts,
    average_confirmation_time_ms,
    histogram_confirmation_time_ms[1] as p_min,
    histogram_confirmation_time_ms[19] as p90,
    histogram_confirmation_time_ms[20] as p95,
    histogram_confirmation_time_ms[21] as p_max
FROM benchrunner.bench_metrics_confirmation_rate
ORDER BY ts DESC
```


