#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rclcpp/executors/multi_threaded_executor.hpp"
#include "rclcpp/executors/single_threaded_executor.hpp"
#include "rclcpp/rclcpp.hpp"

namespace baseline
{

int64_t steady_now_ns()
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
}

struct JobRecord
{
  size_t task_id;
  uint64_t job_id;
  int64_t release_time_ns;
  int64_t start_time_ns;
  int64_t finish_time_ns;
  int64_t response_time_us;
  int64_t lateness_us;
  bool missed;
};

struct TaskConfig
{
  size_t task_id;
  int64_t period_ns;
  double wcet_us;
  std::vector<size_t> predecessors;
};

struct ExperimentOptions
{
  std::string executor_type = "single";
  size_t threads = 2;
  size_t tasks = 8;
  std::string period_set = "harmonic";
  double utilization = 0.6;
  int dag_depth = 2;
  int duration_sec = 60;
  int warmup_sec = 5;
  double wcet_variation = 0.1;
  uint32_t seed = 42;
  std::string output_dir = "/tmp/default_executor_baseline";
  std::string run_id = "run";
};

std::string default_run_id()
{
  std::time_t t = std::time(nullptr);
  std::tm tm_buf{};
#if defined(_WIN32)
  localtime_s(&tm_buf, &t);
#else
  localtime_r(&t, &tm_buf);
#endif
  std::ostringstream oss;
  oss << "run_" << std::put_time(&tm_buf, "%Y%m%d_%H%M%S");
  return oss.str();
}

class BusyWaitCalibrator
{
public:
  BusyWaitCalibrator()
  {
    calibrate();
  }

  void busy_wait_for_us(double target_us) const
  {
    if (target_us <= 0.0) {
      return;
    }

    const int64_t target_ns = static_cast<int64_t>(target_us * 1000.0);
    const auto start = std::chrono::steady_clock::now();

    volatile uint64_t sink = 0;
    const double coarse_iters = static_cast<double>(target_ns) * iterations_per_ns_ * 0.85;
    const uint64_t iter_budget = coarse_iters > 0.0 ? static_cast<uint64_t>(coarse_iters) : 0U;
    for (uint64_t i = 0; i < iter_budget; ++i) {
      sink += i;
    }

    while (std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - start).count() < target_ns)
    {
      sink++;
    }
    (void)sink;
  }

private:
  void calibrate()
  {
    constexpr uint64_t kIterations = 3000000ULL;
    volatile uint64_t sink = 0;

    const auto start = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < kIterations; ++i) {
      sink += i;
    }
    const auto end = std::chrono::steady_clock::now();
    (void)sink;

    const int64_t elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    if (elapsed_ns <= 0) {
      iterations_per_ns_ = 1.0;
      return;
    }
    iterations_per_ns_ = static_cast<double>(kIterations) / static_cast<double>(elapsed_ns);
  }

  double iterations_per_ns_{1.0};
};

class TaskNode : public rclcpp::Node
{
public:
  TaskNode(
    const TaskConfig & config,
    std::vector<std::atomic<uint64_t>> * completion_counters,
    const std::shared_ptr<BusyWaitCalibrator> & calibrator,
    int64_t experiment_start_ns,
    int64_t analysis_start_ns,
    double wcet_variation,
    uint32_t seed)
  : Node("baseline_task_" + std::to_string(config.task_id)),
    config_(config),
    completion_counters_(completion_counters),
    calibrator_(calibrator),
    experiment_start_ns_(experiment_start_ns),
    analysis_start_ns_(analysis_start_ns),
    first_release_ns_(experiment_start_ns + config.period_ns),
    predecessor_seen_(config.predecessors.size(), 0),
    wcet_variation_(wcet_variation),
    rng_(seed + static_cast<uint32_t>(config.task_id)),
    jitter_dist_(-wcet_variation, wcet_variation)
  {
    timer_ = create_wall_timer(
      std::chrono::nanoseconds(config_.period_ns),
      std::bind(&TaskNode::on_timer, this));
  }

  size_t task_id() const
  {
    return config_.task_id;
  }

  uint64_t attempts() const
  {
    return attempts_;
  }

  uint64_t skipped_due_dependency() const
  {
    return skipped_due_dependency_;
  }

  const std::vector<JobRecord> & records() const
  {
    return records_;
  }

private:
  bool predecessors_ready()
  {
    for (size_t i = 0; i < config_.predecessors.size(); ++i) {
      const size_t predecessor = config_.predecessors[i];
      const uint64_t completed = completion_counters_->at(predecessor).load(std::memory_order_acquire);
      if (completed <= predecessor_seen_[i]) {
        return false;
      }
    }
    return true;
  }

  void mark_predecessors_consumed()
  {
    for (size_t i = 0; i < config_.predecessors.size(); ++i) {
      const size_t predecessor = config_.predecessors[i];
      predecessor_seen_[i] = completion_counters_->at(predecessor).load(std::memory_order_acquire);
    }
  }

  void on_timer()
  {
    attempts_++;

    const int64_t start_time_ns = steady_now_ns();
    const int64_t release_time_ns =
      first_release_ns_ + static_cast<int64_t>((attempts_ - 1) * config_.period_ns);

    if (!predecessors_ready()) {
      skipped_due_dependency_++;
      return;
    }
    mark_predecessors_consumed();

    successful_jobs_++;
    const uint64_t job_id = successful_jobs_;

    double scale = 1.0 + jitter_dist_(rng_);
    if (scale < 0.0) {
      scale = 0.0;
    }
    calibrator_->busy_wait_for_us(config_.wcet_us * scale);

    const int64_t finish_time_ns = steady_now_ns();
    completion_counters_->at(config_.task_id).store(job_id, std::memory_order_release);

    if (finish_time_ns < analysis_start_ns_) {
      return;
    }

    const int64_t response_time_us = (finish_time_ns - release_time_ns) / 1000;
    const int64_t deadline_ns = release_time_ns + config_.period_ns;
    const int64_t lateness_us = (finish_time_ns - deadline_ns) / 1000;

    records_.push_back(JobRecord{
        config_.task_id,
        job_id,
        release_time_ns,
        start_time_ns,
        finish_time_ns,
        response_time_us,
        lateness_us,
        lateness_us > 0
      });
  }

  TaskConfig config_;
  std::vector<std::atomic<uint64_t>> * completion_counters_;
  std::shared_ptr<BusyWaitCalibrator> calibrator_;
  int64_t experiment_start_ns_;
  int64_t analysis_start_ns_;
  int64_t first_release_ns_;
  std::vector<uint64_t> predecessor_seen_;
  double wcet_variation_;
  std::mt19937 rng_;
  std::uniform_real_distribution<double> jitter_dist_;
  rclcpp::TimerBase::SharedPtr timer_;
  uint64_t attempts_{0};
  uint64_t skipped_due_dependency_{0};
  uint64_t successful_jobs_{0};
  std::vector<JobRecord> records_;
};

std::vector<int64_t> make_periods_ns(const std::string & period_set, size_t tasks)
{
  const std::vector<int64_t> harmonic_ms{10, 20, 40, 80, 160, 320, 640, 1280};
  const std::vector<int64_t> nonharmonic_ms{11, 17, 23, 31, 43, 59, 71, 89, 113, 137};

  const auto & source = (period_set == "nonharmonic") ? nonharmonic_ms : harmonic_ms;
  std::vector<int64_t> periods_ns;
  periods_ns.reserve(tasks);
  for (size_t i = 0; i < tasks; ++i) {
    const int64_t period_ms = source[i % source.size()];
    periods_ns.push_back(period_ms * 1000000LL);
  }
  return periods_ns;
}

std::vector<std::vector<size_t>> make_dag_predecessors(size_t tasks, int dag_depth)
{
  std::vector<std::vector<size_t>> predecessors(tasks);
  if (tasks == 0 || dag_depth <= 1) {
    return predecessors;
  }

  const size_t layers = static_cast<size_t>(std::max(1, std::min(dag_depth, static_cast<int>(tasks))));
  std::vector<std::vector<size_t>> layer_tasks(layers);

  for (size_t i = 0; i < tasks; ++i) {
    const size_t layer = (i * layers) / tasks;
    layer_tasks[layer].push_back(i);
  }

  for (size_t layer = 1; layer < layers; ++layer) {
    const auto & prev = layer_tasks[layer - 1];
    auto & current = layer_tasks[layer];
    for (size_t i = 0; i < current.size(); ++i) {
      const size_t task_id = current[i];
      predecessors[task_id].push_back(prev[i % prev.size()]);
      if (prev.size() > 1 && (i % 3 == 0)) {
        const size_t second = prev[(i + 1) % prev.size()];
        if (second != predecessors[task_id][0]) {
          predecessors[task_id].push_back(second);
        }
      }
    }
  }
  return predecessors;
}

std::vector<TaskConfig> make_task_configs(
  size_t tasks,
  const std::string & period_set,
  double utilization,
  int dag_depth)
{
  std::vector<TaskConfig> configs;
  configs.reserve(tasks);

  const std::vector<int64_t> periods_ns = make_periods_ns(period_set, tasks);
  const auto predecessors = make_dag_predecessors(tasks, dag_depth);
  const double task_share = tasks == 0 ? 0.0 : utilization / static_cast<double>(tasks);

  for (size_t i = 0; i < tasks; ++i) {
    const double period_us = static_cast<double>(periods_ns[i]) / 1000.0;
    double wcet_us = task_share * period_us;
    if (wcet_us < 50.0) {
      wcet_us = 50.0;
    }
    configs.push_back(TaskConfig{i, periods_ns[i], wcet_us, predecessors[i]});
  }

  return configs;
}

void require_value(int argc, char ** argv, int i, const std::string & arg)
{
  if (i + 1 >= argc) {
    throw std::runtime_error("Missing value for argument: " + arg);
  }
  (void)argv;
}

ExperimentOptions parse_args(int argc, char ** argv)
{
  ExperimentOptions opts;
  opts.run_id = default_run_id();

  for (int i = 1; i < argc; ++i) {
    const std::string arg(argv[i]);
    if (arg == "--executor") {
      require_value(argc, argv, i, arg);
      opts.executor_type = argv[++i];
    } else if (arg == "--threads") {
      require_value(argc, argv, i, arg);
      opts.threads = static_cast<size_t>(std::stoul(argv[++i]));
    } else if (arg == "--tasks") {
      require_value(argc, argv, i, arg);
      opts.tasks = static_cast<size_t>(std::stoul(argv[++i]));
    } else if (arg == "--period-set") {
      require_value(argc, argv, i, arg);
      opts.period_set = argv[++i];
    } else if (arg == "--utilization") {
      require_value(argc, argv, i, arg);
      opts.utilization = std::stod(argv[++i]);
    } else if (arg == "--dag-depth") {
      require_value(argc, argv, i, arg);
      opts.dag_depth = std::stoi(argv[++i]);
    } else if (arg == "--duration-sec") {
      require_value(argc, argv, i, arg);
      opts.duration_sec = std::stoi(argv[++i]);
    } else if (arg == "--warmup-sec") {
      require_value(argc, argv, i, arg);
      opts.warmup_sec = std::stoi(argv[++i]);
    } else if (arg == "--wcet-variation") {
      require_value(argc, argv, i, arg);
      opts.wcet_variation = std::stod(argv[++i]);
    } else if (arg == "--seed") {
      require_value(argc, argv, i, arg);
      opts.seed = static_cast<uint32_t>(std::stoul(argv[++i]));
    } else if (arg == "--output-dir") {
      require_value(argc, argv, i, arg);
      opts.output_dir = argv[++i];
    } else if (arg == "--run-id") {
      require_value(argc, argv, i, arg);
      opts.run_id = argv[++i];
    } else if (arg == "--help") {
      std::cout <<
        "Usage: ros2 run default_executor_baseline default_executor_baseline [options]\n"
        "  --executor single|multi\n"
        "  --threads <n>                (multi-threaded only)\n"
        "  --tasks <n>\n"
        "  --period-set harmonic|nonharmonic\n"
        "  --utilization <0..1>\n"
        "  --dag-depth <n>\n"
        "  --duration-sec <seconds>\n"
        "  --warmup-sec <seconds>\n"
        "  --wcet-variation <fraction>\n"
        "  --seed <uint>\n"
        "  --output-dir <path>\n"
        "  --run-id <string>\n";
      std::exit(0);
    } else {
      throw std::runtime_error("Unknown argument: " + arg);
    }
  }

  if (opts.executor_type != "single" && opts.executor_type != "multi") {
    throw std::runtime_error("executor must be 'single' or 'multi'");
  }
  if (opts.tasks == 0) {
    throw std::runtime_error("tasks must be > 0");
  }
  if (opts.threads == 0) {
    throw std::runtime_error("threads must be > 0");
  }
  if (opts.duration_sec <= 0) {
    throw std::runtime_error("duration-sec must be > 0");
  }
  if (opts.warmup_sec < 0) {
    throw std::runtime_error("warmup-sec must be >= 0");
  }
  if (opts.wcet_variation < 0.0) {
    throw std::runtime_error("wcet-variation must be >= 0");
  }
  if (opts.utilization <= 0.0 || opts.utilization >= 1.0) {
    throw std::runtime_error("utilization must be within (0, 1)");
  }

  return opts;
}

struct TaskSummary
{
  size_t task_id;
  uint64_t total_jobs;
  uint64_t misses;
  double miss_rate;
  int64_t max_response_us;
  int64_t max_lateness_us;
  double mean_response_us;
  double stddev_response_us;
  double p99_response_us;
  uint64_t total_attempts;
  uint64_t skipped_due_dependency;
};

TaskSummary summarize_task(
  size_t task_id,
  const std::vector<JobRecord> & records,
  uint64_t attempts,
  uint64_t skipped)
{
  TaskSummary summary{};
  summary.task_id = task_id;
  summary.total_jobs = records.size();
  summary.total_attempts = attempts;
  summary.skipped_due_dependency = skipped;

  if (records.empty()) {
    summary.misses = 0;
    summary.miss_rate = 0.0;
    summary.max_response_us = 0;
    summary.max_lateness_us = 0;
    summary.mean_response_us = 0.0;
    summary.stddev_response_us = 0.0;
    summary.p99_response_us = 0.0;
    return summary;
  }

  std::vector<double> responses;
  responses.reserve(records.size());
  summary.max_response_us = std::numeric_limits<int64_t>::min();
  summary.max_lateness_us = std::numeric_limits<int64_t>::min();

  for (const auto & record : records) {
    responses.push_back(static_cast<double>(record.response_time_us));
    if (record.missed) {
      summary.misses++;
    }
    summary.max_response_us = std::max(summary.max_response_us, record.response_time_us);
    summary.max_lateness_us = std::max(summary.max_lateness_us, record.lateness_us);
  }

  summary.miss_rate = static_cast<double>(summary.misses) / static_cast<double>(summary.total_jobs);

  const double mean = std::accumulate(responses.begin(), responses.end(), 0.0) /
    static_cast<double>(responses.size());
  summary.mean_response_us = mean;

  if (responses.size() > 1) {
    double accum = 0.0;
    for (const double value : responses) {
      const double diff = value - mean;
      accum += diff * diff;
    }
    summary.stddev_response_us = std::sqrt(accum / static_cast<double>(responses.size() - 1));
  } else {
    summary.stddev_response_us = 0.0;
  }

  std::sort(responses.begin(), responses.end());
  const size_t index =
    std::min(
    responses.size() - 1,
    static_cast<size_t>(std::ceil(0.99 * static_cast<double>(responses.size())) - 1.0));
  summary.p99_response_us = responses[index];

  return summary;
}

std::string join_predecessors(const std::vector<size_t> & predecessors)
{
  if (predecessors.empty()) {
    return "-";
  }
  std::ostringstream oss;
  for (size_t i = 0; i < predecessors.size(); ++i) {
    if (i > 0) {
      oss << ";";
    }
    oss << predecessors[i];
  }
  return oss.str();
}

}  // namespace baseline

int main(int argc, char ** argv)
{
  using baseline::ExperimentOptions;
  using baseline::JobRecord;
  using baseline::TaskNode;

  ExperimentOptions opts;
  try {
    opts = baseline::parse_args(argc, argv);
  } catch (const std::exception & ex) {
    std::cerr << "Argument error: " << ex.what() << std::endl;
    return 1;
  }

  std::filesystem::create_directories(opts.output_dir);

  rclcpp::init(argc, argv);

  const int64_t experiment_start_ns = baseline::steady_now_ns();
  const int64_t analysis_start_ns =
    experiment_start_ns + static_cast<int64_t>(opts.warmup_sec) * 1000000000LL;
  const int total_runtime_sec = opts.warmup_sec + opts.duration_sec;

  auto calibrator = std::make_shared<baseline::BusyWaitCalibrator>();
  auto configs = baseline::make_task_configs(
    opts.tasks, opts.period_set, opts.utilization, opts.dag_depth);

  std::vector<std::atomic<uint64_t>> completion_counters(opts.tasks);
  for (auto & value : completion_counters) {
    value.store(0, std::memory_order_relaxed);
  }

  std::vector<std::shared_ptr<TaskNode>> task_nodes;
  task_nodes.reserve(opts.tasks);
  for (const auto & config : configs) {
    task_nodes.push_back(std::make_shared<TaskNode>(
        config,
        &completion_counters,
        calibrator,
        experiment_start_ns,
        analysis_start_ns,
        opts.wcet_variation,
        opts.seed));
  }

  std::unique_ptr<rclcpp::Executor> executor;
  if (opts.executor_type == "single") {
    executor = std::make_unique<rclcpp::executors::SingleThreadedExecutor>();
  } else {
    executor = std::make_unique<rclcpp::executors::MultiThreadedExecutor>(
      rclcpp::ExecutorOptions(), opts.threads);
  }

  for (const auto & task_node : task_nodes) {
    executor->add_node(task_node);
  }

  RCLCPP_INFO(
    rclcpp::get_logger("default_executor_baseline"),
    "Starting baseline run_id=%s executor=%s threads=%zu tasks=%zu period_set=%s "
    "utilization=%.3f dag_depth=%d duration_sec=%d warmup_sec=%d wcet_variation=%.3f",
    opts.run_id.c_str(),
    opts.executor_type.c_str(),
    opts.threads,
    opts.tasks,
    opts.period_set.c_str(),
    opts.utilization,
    opts.dag_depth,
    opts.duration_sec,
    opts.warmup_sec,
    opts.wcet_variation);

  std::thread shutdown_thread([total_runtime_sec]() {
      std::this_thread::sleep_for(std::chrono::seconds(total_runtime_sec));
      rclcpp::shutdown();
    });

  executor->spin();
  if (shutdown_thread.joinable()) {
    shutdown_thread.join();
  }

  std::vector<JobRecord> all_records;
  std::vector<baseline::TaskSummary> summaries;
  all_records.reserve(100000);
  summaries.reserve(task_nodes.size());

  for (const auto & task_node : task_nodes) {
    const auto & records = task_node->records();
    all_records.insert(all_records.end(), records.begin(), records.end());
    summaries.push_back(
      baseline::summarize_task(
        task_node->task_id(),
        records,
        task_node->attempts(),
        task_node->skipped_due_dependency()));
  }

  std::sort(
    all_records.begin(),
    all_records.end(),
    [](const JobRecord & lhs, const JobRecord & rhs) {
      if (lhs.start_time_ns == rhs.start_time_ns) {
        return lhs.task_id < rhs.task_id;
      }
      return lhs.start_time_ns < rhs.start_time_ns;
    });

  const std::filesystem::path output_dir(opts.output_dir);
  const auto jobs_path = output_dir / (opts.run_id + "_jobs.csv");
  const auto summary_path = output_dir / (opts.run_id + "_summary.csv");
  const auto config_path = output_dir / (opts.run_id + "_config.csv");
  const auto runinfo_path = output_dir / (opts.run_id + "_runinfo.txt");

  {
    std::ofstream job_file(jobs_path);
    job_file <<
      "run_id,executor_type,threads,num_tasks,period_set,utilization,dag_depth,"
      "task_id,job_id,release_time,start_time,finish_time,response_time_us,lateness_us,missed\n";
    for (const auto & record : all_records) {
      job_file
        << opts.run_id << ","
        << opts.executor_type << ","
        << opts.threads << ","
        << opts.tasks << ","
        << opts.period_set << ","
        << std::fixed << std::setprecision(3) << opts.utilization << ","
        << opts.dag_depth << ","
        << record.task_id << ","
        << record.job_id << ","
        << record.release_time_ns << ","
        << record.start_time_ns << ","
        << record.finish_time_ns << ","
        << record.response_time_us << ","
        << record.lateness_us << ","
        << (record.missed ? 1 : 0) << "\n";
    }
    job_file.flush();
  }

  {
    std::ofstream summary_file(summary_path);
    summary_file <<
      "run_id,executor_type,threads,num_tasks,period_set,utilization,dag_depth,wcet_variation,"
      "duration_sec,warmup_sec,task_id,total_jobs,misses,miss_rate,maximum_response_time_us,"
      "maximum_lateness_us,mean_response_time_us,stddev_response_time_us,p99_response_time_us,"
      "total_attempts,skipped_due_dependency\n";
    summary_file << std::fixed << std::setprecision(6);
    for (const auto & summary : summaries) {
      summary_file
        << opts.run_id << ","
        << opts.executor_type << ","
        << opts.threads << ","
        << opts.tasks << ","
        << opts.period_set << ","
        << opts.utilization << ","
        << opts.dag_depth << ","
        << opts.wcet_variation << ","
        << opts.duration_sec << ","
        << opts.warmup_sec << ","
        << summary.task_id << ","
        << summary.total_jobs << ","
        << summary.misses << ","
        << summary.miss_rate << ","
        << summary.max_response_us << ","
        << summary.max_lateness_us << ","
        << summary.mean_response_us << ","
        << summary.stddev_response_us << ","
        << summary.p99_response_us << ","
        << summary.total_attempts << ","
        << summary.skipped_due_dependency << "\n";
    }
    summary_file.flush();
  }

  {
    std::ofstream config_file(config_path);
    config_file << "task_id,period_ms,wcet_us,predecessors\n";
    config_file << std::fixed << std::setprecision(3);
    for (const auto & config : configs) {
      config_file
        << config.task_id << ","
        << (static_cast<double>(config.period_ns) / 1000000.0) << ","
        << config.wcet_us << ","
        << baseline::join_predecessors(config.predecessors) << "\n";
    }
    config_file.flush();
  }

  {
    std::ofstream runinfo_file(runinfo_path);
    runinfo_file
      << "run_id=" << opts.run_id << "\n"
      << "executor_type=" << opts.executor_type << "\n"
      << "threads=" << opts.threads << "\n"
      << "tasks=" << opts.tasks << "\n"
      << "period_set=" << opts.period_set << "\n"
      << "utilization=" << opts.utilization << "\n"
      << "dag_depth=" << opts.dag_depth << "\n"
      << "duration_sec=" << opts.duration_sec << "\n"
      << "warmup_sec=" << opts.warmup_sec << "\n"
      << "wcet_variation=" << opts.wcet_variation << "\n"
      << "output_dir=" << output_dir.string() << "\n"
      << "analysis_start_ns=" << analysis_start_ns << "\n"
      << "experiment_start_ns=" << experiment_start_ns << "\n";
    runinfo_file.flush();
  }

  RCLCPP_INFO(
    rclcpp::get_logger("default_executor_baseline"),
    "Finished run_id=%s -> jobs=%s summary=%s config=%s",
    opts.run_id.c_str(),
    jobs_path.string().c_str(),
    summary_path.string().c_str(),
    config_path.string().c_str());

  return 0;
}
