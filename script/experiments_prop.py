# flink_rasp_hosts
rasp_hosts = ["flink_rasp1"]

stats_input_rates = [200, 400, 600, 700, 750, 800, 825, 850, 875, 900, 925, 950, 1000]
stats_nums_of_data = [48000, 96000, 144000, 168000, 180000, 192000, 198000, 204000, 210000, 216000, 222000, 228000, 240000]

# metrics_log_path
metrics_log_dir = "/mnt/d/Knowledge_Base/flink-query/metrics_logs"

# exp_metrics_log_save_dir
exp_results_archive_dir = "/mnt/d/Knowledge_Base/flink-query/results"

# exp_results_local_dir
exp_results_local_dir = "/mnt/d/Knowledge_Base/flink-query/results"

# exp_results_summ_file_name
exp_results_summ_file = "summary.csv"

exp_ids = ["job_name", "execution_time", "input_rate", "num_of_data"]
throughput_metrics = ["throughput"]
latency_metrics = ["latency_mean", "latency_min", "latency_p5", "latency_p10", "latency_p20", "latency_p25",
                   "latency_p30", "latency_p40", "latency_p50", "latency_p60", "latency_p70", "latency_p75",
                   "latency_p80", "latency_p90", "latency_p95", "latency_p98", "latency_p99", "latency_p999",
                   "latency_max"]

exp_metrics_head = exp_ids + throughput_metrics + latency_metrics