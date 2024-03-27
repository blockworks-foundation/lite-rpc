
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

