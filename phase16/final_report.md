# Phase 16 Final Analysis

- Total configurations: 64
- Log/summary mismatch rows: 0
- DAG restriction enforcement: 64/64 runs passed

## Best Configurations (Lowest Combined Miss)

|run_id|a1|a2|threads|dscale|dag1_miss|dag2_miss|combined_miss|
|---:|---:|---:|---:|---:|---:|---:|---:|
|31|3|5|8|1.1|0.951035|0.000181|0.475608|
|55|5|3|8|1.1|0.338358|0.613112|0.475735|
|43|4|4|8|1.1|0.952829|0.000092|0.476461|
|47|4|5|8|1.1|0.967315|0.000103|0.483709|
|11|2|4|8|1.1|0.978646|0.000420|0.489533|

## Worst Configurations (Highest Combined Miss)

|run_id|a1|a2|threads|dscale|dag1_miss|dag2_miss|combined_miss|
|---:|---:|---:|---:|---:|---:|---:|---:|
|61|5|5|4|0.8|0.983032|0.967868|0.975450|
|41|4|4|4|0.8|0.975143|0.943675|0.959409|
|45|4|5|4|0.8|0.967601|0.938437|0.953019|
|57|5|4|4|0.8|0.966468|0.934078|0.950273|
|21|3|3|4|0.8|0.958769|0.935674|0.947221|

## Artifacts

- `cross_configuration_table.csv`
- `best_worst_configs.csv`
- `log_extracted_metrics.csv`
- `dag_enforcement_check.csv`
- `threads_vs_avg_combined_miss.png`
- `max_active_vs_avg_miss.png`
- `deadline_scale_vs_miss_lateness.png`
- `lateness_hist_best.png`
- `lateness_hist_worst.png`
