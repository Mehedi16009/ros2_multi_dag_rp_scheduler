#include <rclcpp/rclcpp.hpp>
#include "rp_executor/rp_executor.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using SteadyClock = std::chrono::steady_clock;

struct RunConfig
{
  std::string executor_type{"redag"};
  size_t threads{8};
  size_t tasks{8};
  std::string period_set{"harmonic"};
  double utilization{0.6};
  int dag_depth{2};
  int duration_sec{65};
  int warmup_sec{5};
  double wcet_variation{0.10};
  uint32_t seed{1};
  std::string deadline_model{"D_EQUALS_T"};
  std::string workload_file{};
  std::string workload_hash{};
  std::string output_dir{"./redag_results"};
  std::string run_id{};
};

struct TaskSpec
{
  uint32_t task_id{0};
  uint32_t dag_id{0};
  uint32_t depth_level{0};
  int64_t period_us{10000};
  int64_t wcet_us{1000};
  int64_t deadline_us{10000};
  std::vector<uint32_t> predecessors;
};

struct JobRecord
{
  uint32_t task_id{0};
  uint64_t job_id{0};
  int64_t release_time_ns{0};
  int64_t start_time_ns{-1};
  int64_t finish_time_ns{-1};
  int64_t deadline_time_ns{-1};
  double response_time_us{-1.0};
  double lateness_us{-1.0};
  int miss_flag{0};
  int executed{0};
  int in_warmup{0};
};

class BusyWaitCalibrator
{
public:
  void calibrate()
  {
    constexpr uint64_t kIterations = 4000000;
    constexpr int kRounds = 4;

    double ns_per_iter_sum = 0.0;
    for (int round = 0; round < kRounds; ++round) {
      volatile uint64_t sink = 0;
      const auto start = SteadyClock::now();
      for (uint64_t i = 0; i < kIterations; ++i) {
        sink += i;
        asm volatile("" : "+r"(sink) : : "memory");
      }
      const auto end = SteadyClock::now();
      const auto elapsed_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
      ns_per_iter_sum += static_cast<double>(elapsed_ns) / static_cast<double>(kIterations);
    }
    ns_per_iter_ = std::max(1e-6, ns_per_iter_sum / static_cast<double>(kRounds));
  }

  void busy_wait_for_us(int64_t target_us) const
  {
    if (target_us <= 0) {
      return;
    }

    const int64_t target_ns = target_us * 1000;
    const auto start = SteadyClock::now();
    const auto coarse_iters = static_cast<uint64_t>(
      (static_cast<double>(target_ns) / ns_per_iter_) * 0.85);

    volatile uint64_t sink = 0;
    for (uint64_t i = 0; i < coarse_iters; ++i) {
      sink += i;
      asm volatile("" : "+r"(sink) : : "memory");
    }
    while (std::chrono::duration_cast<std::chrono::nanoseconds>(
        SteadyClock::now() - start).count() < target_ns)
    {
      asm volatile("" : : : "memory");
    }
  }

  double ns_per_iter() const
  {
    return ns_per_iter_;
  }

private:
  double ns_per_iter_{1.0};
};

struct ExperimentContext
{
  SteadyClock::time_point run_start_tp;
  int64_t warmup_ns{0};
  BusyWaitCalibrator calibrator;
  std::vector<std::shared_ptr<std::atomic<uint64_t>>> completion_counts;
};

static int64_t ns_since_start(
  const ExperimentContext & context,
  const SteadyClock::time_point & tp)
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(tp - context.run_start_tp).count();
}

static std::string make_default_run_id()
{
  const auto now = std::chrono::system_clock::now();
  const std::time_t now_time_t = std::chrono::system_clock::to_time_t(now);
  std::tm now_tm{};
#if defined(_WIN32)
  localtime_s(&now_tm, &now_time_t);
#else
  localtime_r(&now_time_t, &now_tm);
#endif
  std::ostringstream oss;
  oss << "redag_" << std::put_time(&now_tm, "%Y%m%d_%H%M%S");
  return oss.str();
}

static void print_usage()
{
  std::cout
    << "Usage: ros2 run multi_dag_demo multi_dag_demo_main [options]\n"
    << "Options:\n"
    << "  --executor <redag>              Runtime mode (default: redag)\n"
    << "  --threads <N>                   RPExecutor thread count (default: 8)\n"
    << "  --tasks <N>                     Number of periodic tasks (default: 8)\n"
    << "  --period-set <harmonic|nonharmonic>\n"
    << "                                  Period set type (default: harmonic)\n"
    << "  --utilization <0-1>             Target total utilization (default: 0.6)\n"
    << "  --dag-depth <N>                 Dependency depth per DAG segment (default: 2)\n"
    << "  --duration-sec <N>              Total run time seconds (default: 65)\n"
    << "  --warmup-sec <N>                Warm-up seconds discarded from analysis (default: 5)\n"
    << "  --wcet-variation <0-1>          WCET variability fraction ±x (default: 0.10)\n"
    << "  --seed <N>                      RNG seed (default: 1)\n"
    << "  --deadline-model <D_EQUALS_T>   Deadline model for all tasks (default: D_EQUALS_T)\n"
    << "  --workload-file <path>          Shared workload specification file\n"
    << "  --workload-hash <sha256>        Expected workload hash (required)\n"
    << "  --output-dir <path>             Output directory (default: ./redag_results)\n"
    << "  --run-id <id>                   Optional run id\n";
}

static bool parse_arguments(
  const std::vector<std::string> & args,
  RunConfig & config)
{
  for (size_t i = 1; i < args.size(); ++i) {
    const auto & arg = args[i];

    auto require_value = [&](const std::string & key) -> std::string {
        if (i + 1 >= args.size()) {
          throw std::runtime_error("Missing value for " + key);
        }
        ++i;
        return args[i];
      };

    if (arg == "--executor") {
      config.executor_type = require_value(arg);
    } else if (arg == "--threads") {
      config.threads = static_cast<size_t>(std::stoul(require_value(arg)));
    } else if (arg == "--tasks") {
      config.tasks = static_cast<size_t>(std::stoul(require_value(arg)));
    } else if (arg == "--period-set") {
      config.period_set = require_value(arg);
    } else if (arg == "--utilization") {
      config.utilization = std::stod(require_value(arg));
    } else if (arg == "--dag-depth") {
      config.dag_depth = std::stoi(require_value(arg));
    } else if (arg == "--duration-sec") {
      config.duration_sec = std::stoi(require_value(arg));
    } else if (arg == "--warmup-sec") {
      config.warmup_sec = std::stoi(require_value(arg));
    } else if (arg == "--wcet-variation") {
      config.wcet_variation = std::stod(require_value(arg));
    } else if (arg == "--seed") {
      config.seed = static_cast<uint32_t>(std::stoul(require_value(arg)));
    } else if (arg == "--deadline-model") {
      config.deadline_model = require_value(arg);
    } else if (arg == "--workload-file") {
      config.workload_file = require_value(arg);
    } else if (arg == "--workload-hash") {
      config.workload_hash = require_value(arg);
    } else if (arg == "--output-dir") {
      config.output_dir = require_value(arg);
    } else if (arg == "--run-id") {
      config.run_id = require_value(arg);
    } else if (arg == "--help" || arg == "-h") {
      print_usage();
      return false;
    } else {
      throw std::runtime_error("Unknown argument: " + arg);
    }
  }

  if (config.run_id.empty()) {
    config.run_id = make_default_run_id();
  }

  if (config.executor_type != "redag") {
    throw std::runtime_error("Invalid --executor value: " + config.executor_type + ". Use redag");
  }
  if (config.period_set != "harmonic" && config.period_set != "nonharmonic") {
    throw std::runtime_error("Invalid --period-set value: " + config.period_set);
  }
  if (config.tasks == 0) {
    throw std::runtime_error("--tasks must be > 0");
  }
  if (config.utilization <= 0.0 || config.utilization >= 1.0) {
    throw std::runtime_error("--utilization must be in (0, 1)");
  }
  if (config.duration_sec <= 0 || config.warmup_sec < 0 || config.warmup_sec >= config.duration_sec) {
    throw std::runtime_error("Invalid duration/warmup configuration");
  }
  if (config.wcet_variation < 0.0 || config.wcet_variation > 1.0) {
    throw std::runtime_error("--wcet-variation must be in [0, 1]");
  }
  if (config.dag_depth < 1) {
    throw std::runtime_error("--dag-depth must be >= 1");
  }
  if (config.deadline_model != "D_EQUALS_T") {
    throw std::runtime_error("Unsupported --deadline-model. Use D_EQUALS_T");
  }
  if (config.workload_file.empty()) {
    throw std::runtime_error("--workload-file is required for ReDAG runtime convergence mode");
  }
  if (config.workload_hash.empty()) {
    throw std::runtime_error("--workload-hash is required for ReDAG runtime convergence mode");
  }

  return true;
}

static std::string trim_copy(const std::string & input)
{
  size_t start = 0;
  while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
    ++start;
  }
  size_t end = input.size();
  while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
    --end;
  }
  return input.substr(start, end - start);
}

static std::vector<std::string> split_copy(const std::string & input, char delim)
{
  std::vector<std::string> out;
  std::stringstream ss(input);
  std::string item;
  while (std::getline(ss, item, delim)) {
    out.push_back(item);
  }
  return out;
}

static int64_t parse_int64_strict(const std::string & value, const std::string & field_name)
{
  try {
    size_t idx = 0;
    const long long parsed = std::stoll(value, &idx, 10);
    if (idx != value.size()) {
      throw std::runtime_error("trailing characters");
    }
    return static_cast<int64_t>(parsed);
  } catch (const std::exception &) {
    throw std::runtime_error("Invalid integer for " + field_name + ": " + value);
  }
}

static uint32_t parse_uint32_strict(const std::string & value, const std::string & field_name)
{
  const int64_t parsed = parse_int64_strict(value, field_name);
  if (parsed < 0 || parsed > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
    throw std::runtime_error("Out-of-range value for " + field_name + ": " + value);
  }
  return static_cast<uint32_t>(parsed);
}

static std::vector<TaskSpec> load_task_specs_from_workload_file(const RunConfig & config)
{
  std::ifstream ifs(config.workload_file, std::ios::in);
  if (!ifs.is_open()) {
    throw std::runtime_error("Unable to open workload file: " + config.workload_file);
  }

  std::map<std::string, std::string> metadata;
  std::vector<TaskSpec> specs;
  bool in_task_rows = false;
  std::string line;
  while (std::getline(ifs, line)) {
    const std::string trimmed = trim_copy(line);
    if (trimmed.empty() || trimmed[0] == '#') {
      continue;
    }
    if (trimmed == "task_id,dag_id,depth_level,period_us,wcet_us,deadline_us,predecessors") {
      in_task_rows = true;
      continue;
    }

    if (!in_task_rows) {
      const auto pos = trimmed.find('=');
      if (pos == std::string::npos) {
        continue;
      }
      const std::string key = trim_copy(trimmed.substr(0, pos));
      const std::string value = trim_copy(trimmed.substr(pos + 1));
      metadata[key] = value;
      continue;
    }

    const auto cols = split_copy(trimmed, ',');
    if (cols.size() != 7) {
      throw std::runtime_error(
        "Malformed workload row (expected 7 comma-separated fields): " + trimmed);
    }

    TaskSpec spec;
    spec.task_id = parse_uint32_strict(trim_copy(cols[0]), "task_id");
    spec.dag_id = parse_uint32_strict(trim_copy(cols[1]), "dag_id");
    spec.depth_level = parse_uint32_strict(trim_copy(cols[2]), "depth_level");
    spec.period_us = parse_int64_strict(trim_copy(cols[3]), "period_us");
    spec.wcet_us = parse_int64_strict(trim_copy(cols[4]), "wcet_us");
    spec.deadline_us = parse_int64_strict(trim_copy(cols[5]), "deadline_us");
    if (spec.period_us <= 0 || spec.wcet_us <= 0 || spec.deadline_us <= 0) {
      throw std::runtime_error("Non-positive period/wcet/deadline in workload row: " + trimmed);
    }

    const std::string pred_text = trim_copy(cols[6]);
    if (!pred_text.empty()) {
      const auto pred_parts = split_copy(pred_text, ';');
      for (const auto & pred : pred_parts) {
        const auto pred_trimmed = trim_copy(pred);
        if (pred_trimmed.empty()) {
          continue;
        }
        spec.predecessors.push_back(parse_uint32_strict(pred_trimmed, "predecessor"));
      }
    }
    specs.push_back(spec);
  }

  if (specs.empty()) {
    throw std::runtime_error("No task rows found in workload file: " + config.workload_file);
  }

  std::sort(
    specs.begin(), specs.end(),
    [](const TaskSpec & a, const TaskSpec & b) { return a.task_id < b.task_id; });

  for (size_t i = 0; i < specs.size(); ++i) {
    if (specs[i].task_id != static_cast<uint32_t>(i)) {
      throw std::runtime_error("Task ids must be contiguous from 0 in workload file");
    }
  }

  if (config.tasks != specs.size()) {
    throw std::runtime_error(
      "Task count mismatch between --tasks and workload file: tasks=" +
      std::to_string(config.tasks) + " workload_tasks=" + std::to_string(specs.size()));
  }

  const auto it_deadline_model = metadata.find("deadline_model");
  if (it_deadline_model != metadata.end() && it_deadline_model->second != config.deadline_model) {
    throw std::runtime_error(
      "Deadline model mismatch: workload file has " + it_deadline_model->second +
      " but --deadline-model is " + config.deadline_model);
  }

  if (config.deadline_model == "D_EQUALS_T") {
    for (const auto & spec : specs) {
      if (spec.deadline_us != spec.period_us) {
        throw std::runtime_error(
          "D_EQUALS_T violation in workload file for task_id=" + std::to_string(spec.task_id));
      }
    }
  }

  const auto it_hash = metadata.find("workload_hash");
  if (it_hash == metadata.end()) {
    throw std::runtime_error("Workload metadata missing workload_hash entry");
  }
  if (config.workload_hash != it_hash->second) {
    throw std::runtime_error(
      "Workload hash mismatch: expected=" + config.workload_hash +
      " file=" + it_hash->second);
  }

  return specs;
}

static std::vector<TaskSpec> build_task_specs(const RunConfig & config)
{
  return load_task_specs_from_workload_file(config);
}

class TaskNode : public rclcpp::Node
{
public:
  TaskNode(
    const TaskSpec & spec,
    const RunConfig & config,
    ExperimentContext * context,
    size_t task_count)
  : Node("redag_task_d" + std::to_string(spec.dag_id + 1) + "_t" + std::to_string(spec.task_id) + "_p" + std::to_string(spec.period_us)),
    spec_(spec),
    context_(context),
    predecessor_consumed_(task_count, 0),
    rng_(config.seed + spec.task_id),
    jitter_dist_(std::max(0.0, 1.0 - config.wcet_variation), 1.0 + config.wcet_variation),
    next_release_ns_(spec.period_us * 1000)
  {
    timer_ = create_wall_timer(
      std::chrono::microseconds(spec_.period_us),
      std::bind(&TaskNode::on_timer, this));
  }

  const TaskSpec & spec() const
  {
    return spec_;
  }

  const std::vector<JobRecord> & records() const
  {
    return records_;
  }

private:
  bool dependencies_ready()
  {
    for (const auto pred : spec_.predecessors) {
      const auto completed = context_->completion_counts[pred]->load(std::memory_order_acquire);
      if (completed <= predecessor_consumed_[pred]) {
        return false;
      }
    }
    for (const auto pred : spec_.predecessors) {
      predecessor_consumed_[pred] =
        context_->completion_counts[pred]->load(std::memory_order_acquire);
    }
    return true;
  }

  void on_timer()
  {
    const uint64_t job_id = ++job_counter_;
    const int64_t release_ns = next_release_ns_;
    const int64_t deadline_ns = release_ns + spec_.deadline_us * 1000;
    next_release_ns_ += spec_.period_us * 1000;
    const bool in_warmup = release_ns < context_->warmup_ns;

    if (!dependencies_ready()) {
      records_.push_back(JobRecord{
        spec_.task_id,
        job_id,
        release_ns,
        -1,
        -1,
        deadline_ns,
        -1.0,
        -1.0,
        0,
        0,
        in_warmup ? 1 : 0
      });
      return;
    }

    const int64_t start_ns = ns_since_start(*context_, SteadyClock::now());
    const double jitter = jitter_dist_(rng_);
    const int64_t target_wcet_us = std::max<int64_t>(
      1,
      static_cast<int64_t>(std::llround(static_cast<double>(spec_.wcet_us) * jitter)));
    context_->calibrator.busy_wait_for_us(target_wcet_us);
    const int64_t finish_ns = ns_since_start(*context_, SteadyClock::now());

    context_->completion_counts[spec_.task_id]->fetch_add(1, std::memory_order_release);

    const double response_time_us = std::max(
      0.0,
      static_cast<double>(finish_ns - release_ns) / 1000.0);
    const double lateness_us = static_cast<double>(finish_ns - deadline_ns) / 1000.0;
    const int miss_flag = (lateness_us > 0.0) ? 1 : 0;

    records_.push_back(JobRecord{
      spec_.task_id,
      job_id,
      release_ns,
      start_ns,
      finish_ns,
      deadline_ns,
      response_time_us,
      lateness_us,
      miss_flag,
      1,
      in_warmup ? 1 : 0
    });
  }

  TaskSpec spec_;
  ExperimentContext * context_{nullptr};
  std::vector<uint64_t> predecessor_consumed_;
  std::mt19937 rng_;
  std::uniform_real_distribution<double> jitter_dist_;
  int64_t next_release_ns_{0};
  uint64_t job_counter_{0};
  rclcpp::TimerBase::SharedPtr timer_;
  std::vector<JobRecord> records_;
};

class StopNode : public rclcpp::Node
{
public:
  explicit StopNode(int duration_sec)
  : Node("redag_stopper")
  {
    timer_ = create_wall_timer(
      std::chrono::seconds(duration_sec),
      [this]() {
        RCLCPP_INFO(this->get_logger(), "Requested shutdown after configured duration");
        timer_->cancel();
        rclcpp::shutdown();
      });
  }

private:
  rclcpp::TimerBase::SharedPtr timer_;
};

struct TaskSummary
{
  uint64_t total_jobs{0};
  uint64_t misses{0};
  double miss_rate{0.0};
  double max_response_time_us{0.0};
  double max_lateness_us{0.0};
  double mean_response_time_us{0.0};
  double stddev_response_time_us{0.0};
  double p99_response_time_us{0.0};
  uint64_t total_attempts{0};
  uint64_t deferred_dependency{0};
};

static TaskSummary summarize_task_records(const std::vector<JobRecord> & records)
{
  TaskSummary summary;
  std::vector<double> responses;
  responses.reserve(records.size());

  for (const auto & record : records) {
    if (record.in_warmup == 1) {
      continue;
    }
    summary.total_attempts++;
    if (record.executed == 0) {
      summary.deferred_dependency++;
      continue;
    }
    summary.total_jobs++;
    summary.misses += static_cast<uint64_t>(record.miss_flag);
    summary.max_response_time_us = std::max(summary.max_response_time_us, record.response_time_us);
    summary.max_lateness_us = std::max(summary.max_lateness_us, record.lateness_us);
    responses.push_back(record.response_time_us);
  }

  if (summary.total_jobs == 0) {
    return summary;
  }

  const double n = static_cast<double>(responses.size());
  const double sum = std::accumulate(responses.begin(), responses.end(), 0.0);
  summary.mean_response_time_us = sum / n;
  summary.miss_rate = static_cast<double>(summary.misses) / static_cast<double>(summary.total_jobs);

  double sq_sum = 0.0;
  for (double r : responses) {
    const double diff = r - summary.mean_response_time_us;
    sq_sum += diff * diff;
  }
  summary.stddev_response_time_us = std::sqrt(sq_sum / n);

  std::sort(responses.begin(), responses.end());
  const size_t p99_idx = static_cast<size_t>(std::ceil(0.99 * n)) - 1;
  summary.p99_response_time_us = responses[std::min(p99_idx, responses.size() - 1)];

  return summary;
}

class RedagRunner
{
public:
  explicit RedagRunner(RunConfig config)
  : config_(std::move(config))
  {}

  int run()
  {
    task_specs_ = build_task_specs(config_);
    context_.run_start_tp = SteadyClock::now();
    context_.warmup_ns = static_cast<int64_t>(config_.warmup_sec) * 1000000000LL;
    context_.completion_counts.clear();
    context_.completion_counts.reserve(task_specs_.size());
    for (size_t i = 0; i < task_specs_.size(); ++i) {
      context_.completion_counts.emplace_back(std::make_shared<std::atomic<uint64_t>>(0));
    }
    context_.calibrator.calibrate();

    task_nodes_.reserve(task_specs_.size());
    for (const auto & spec : task_specs_) {
      task_nodes_.push_back(std::make_shared<TaskNode>(
          spec, config_, &context_, task_specs_.size()));
    }
    stop_node_ = std::make_shared<StopNode>(config_.duration_sec);

    RCLCPP_INFO(
      rclcpp::get_logger("redag_runtime"),
      "Starting ReDAG run_id=%s executor=%s threads=%zu tasks=%zu util=%.3f depth=%d duration=%d "
      "warmup=%d deadline_model=%s workload_file=%s workload_hash=%s",
      config_.run_id.c_str(), config_.executor_type.c_str(), config_.threads, config_.tasks,
      config_.utilization, config_.dag_depth, config_.duration_sec, config_.warmup_sec,
      config_.deadline_model.c_str(),
      config_.workload_file.empty() ? "<generated>" : config_.workload_file.c_str(),
      config_.workload_hash.empty() ? "<none>" : config_.workload_hash.c_str());

    rp_executor::RPExecutor executor(rclcpp::ExecutorOptions(), config_.threads, false);
    for (const auto & node : task_nodes_) {
      executor.add_node(node);
    }
    executor.add_node(stop_node_);
    executor.spin();

    write_outputs();
    return 0;
  }

private:
  void write_outputs() const
  {
    namespace fs = std::filesystem;
    const fs::path out_dir(config_.output_dir);
    fs::create_directories(out_dir);

    const fs::path job_csv = out_dir / (config_.run_id + "_jobs.csv");
    const fs::path summary_csv = out_dir / (config_.run_id + "_summary.csv");
    const fs::path config_csv = out_dir / (config_.run_id + "_config.csv");
    const fs::path runinfo_txt = out_dir / (config_.run_id + "_runinfo.txt");

    {
      std::ofstream ofs(config_csv.string(), std::ios::out | std::ios::trunc);
      ofs << "run_id,executor,threads,tasks,period_set,utilization,dag_depth,duration_sec,"
             "warmup_sec,wcet_variation,seed,deadline_model,workload_file,workload_hash,"
             "calibrated_ns_per_iter\n";
      ofs << config_.run_id << ","
          << config_.executor_type << ","
          << config_.threads << ","
          << config_.tasks << ","
          << config_.period_set << ","
          << std::fixed << std::setprecision(6) << config_.utilization << ","
          << config_.dag_depth << ","
          << config_.duration_sec << ","
          << config_.warmup_sec << ","
          << std::fixed << std::setprecision(6) << config_.wcet_variation << ","
          << config_.seed << ","
          << config_.deadline_model << ","
          << config_.workload_file << ","
          << config_.workload_hash << ","
          << std::fixed << std::setprecision(6) << context_.calibrator.ns_per_iter() << "\n";
      ofs.flush();
    }

    {
      std::ofstream ofs(job_csv.string(), std::ios::out | std::ios::trunc);
      const std::string job_header = "run_id,executor,threads,period_set,utilization,dag_depth,task_id,dag_id,depth_level,"
        "period_us,wcet_us,deadline_us,job_id,release_time_ns,start_time_ns,finish_time_ns,"
        "deadline_time_ns,response_time_us,lateness_us,miss_flag,missed,executed,in_warmup\n";
      const std::string expected_header = "run_id,executor,threads,period_set,utilization,dag_depth,task_id,dag_id,depth_level,"
        "period_us,wcet_us,deadline_us,job_id,release_time_ns,start_time_ns,finish_time_ns,"
        "deadline_time_ns,response_time_us,lateness_us,miss_flag,missed,executed,in_warmup\n";
      if (job_header != expected_header) {
        throw std::runtime_error("Job CSV schema mismatch with baseline");
      }
      ofs << job_header;
      for (const auto & node : task_nodes_) {
        const auto & spec = node->spec();
        for (const auto & record : node->records()) {
          ofs << config_.run_id << ","
              << config_.executor_type << ","
              << config_.threads << ","
              << config_.period_set << ","
              << std::fixed << std::setprecision(6) << config_.utilization << ","
              << config_.dag_depth << ","
              << record.task_id << ","
              << spec.dag_id << ","
              << spec.depth_level << ","
              << spec.period_us << ","
              << spec.wcet_us << ","
              << spec.deadline_us << ","
              << record.job_id << ","
              << record.release_time_ns << ","
              << record.start_time_ns << ","
              << record.finish_time_ns << ","
              << record.deadline_time_ns << ","
              << std::fixed << std::setprecision(3) << record.response_time_us << ","
              << std::fixed << std::setprecision(3) << record.lateness_us << ","
              << record.miss_flag << ","
              << record.miss_flag << ","
              << record.executed << ","
              << record.in_warmup << "\n";
        }
      }
      ofs.flush();
    }

    {
      std::ofstream ofs(summary_csv.string(), std::ios::out | std::ios::trunc);
      ofs << "run_id,executor,threads,period_set,utilization,dag_depth,task_id,dag_id,depth_level,"
             "period_us,wcet_us,deadline_us,total_jobs,misses,miss_rate,maximum_response_time_us,"
             "maximum_lateness_us,mean_response_time_us,stddev_response_time_us,"
             "p99_response_time_us,total_attempts,deferred_dependency\n";
      for (const auto & node : task_nodes_) {
        const auto & spec = node->spec();
        const auto summary = summarize_task_records(node->records());
        ofs << config_.run_id << ","
            << config_.executor_type << ","
            << config_.threads << ","
            << config_.period_set << ","
            << std::fixed << std::setprecision(6) << config_.utilization << ","
            << config_.dag_depth << ","
            << spec.task_id << ","
            << spec.dag_id << ","
            << spec.depth_level << ","
            << spec.period_us << ","
            << spec.wcet_us << ","
            << spec.deadline_us << ","
            << summary.total_jobs << ","
            << summary.misses << ","
            << std::fixed << std::setprecision(6) << summary.miss_rate << ","
            << std::fixed << std::setprecision(3) << summary.max_response_time_us << ","
            << std::fixed << std::setprecision(3) << summary.max_lateness_us << ","
            << std::fixed << std::setprecision(3) << summary.mean_response_time_us << ","
            << std::fixed << std::setprecision(3) << summary.stddev_response_time_us << ","
            << std::fixed << std::setprecision(3) << summary.p99_response_time_us << ","
            << summary.total_attempts << ","
            << summary.deferred_dependency << "\n";
      }
      ofs.flush();
    }

    {
      std::ofstream ofs(runinfo_txt.string(), std::ios::out | std::ios::trunc);
      ofs << "run_id=" << config_.run_id << "\n";
      ofs << "executor=" << config_.executor_type << "\n";
      ofs << "threads=" << config_.threads << "\n";
      ofs << "tasks=" << config_.tasks << "\n";
      ofs << "period_set=" << config_.period_set << "\n";
      ofs << "utilization=" << config_.utilization << "\n";
      ofs << "dag_depth=" << config_.dag_depth << "\n";
      ofs << "duration_sec=" << config_.duration_sec << "\n";
      ofs << "warmup_sec=" << config_.warmup_sec << "\n";
      ofs << "wcet_variation=" << config_.wcet_variation << "\n";
      ofs << "seed=" << config_.seed << "\n";
      ofs << "deadline_model=" << config_.deadline_model << "\n";
      ofs << "workload_file=" << config_.workload_file << "\n";
      ofs << "workload_hash=" << config_.workload_hash << "\n";
      ofs << "calibrated_ns_per_iter=" << context_.calibrator.ns_per_iter() << "\n";
      ofs << "jobs_csv=" << job_csv.string() << "\n";
      ofs << "summary_csv=" << summary_csv.string() << "\n";
      ofs.flush();
    }

    RCLCPP_INFO(
      rclcpp::get_logger("redag_runtime"),
      "Logs written: %s, %s, %s",
      job_csv.c_str(), summary_csv.c_str(), config_csv.c_str());
  }

  RunConfig config_;
  ExperimentContext context_;
  std::vector<TaskSpec> task_specs_;
  std::vector<std::shared_ptr<TaskNode>> task_nodes_;
  std::shared_ptr<StopNode> stop_node_;
};

int main(int argc, char ** argv)
{
  const auto args = rclcpp::init_and_remove_ros_arguments(argc, argv);

  RunConfig config;
  try {
    if (!parse_arguments(args, config)) {
      rclcpp::shutdown();
      return 0;
    }
  } catch (const std::exception & ex) {
    std::cerr << "Argument error: " << ex.what() << "\n";
    print_usage();
    rclcpp::shutdown();
    return 1;
  }

  try {
    RedagRunner runner(config);
    const int code = runner.run();
    rclcpp::shutdown();
    return code;
  } catch (const std::exception & ex) {
    std::cerr << "Runtime error: " << ex.what() << "\n";
    rclcpp::shutdown();
    return 1;
  }
}
