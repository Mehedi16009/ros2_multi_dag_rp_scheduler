#include "rp_executor/rp_executor.hpp"

#include <chrono>
#include <cctype>
#include <cstdlib>
#include <functional>
#include <limits>
#include <string>
#include <stdexcept>
#include <thread>
#include <vector>

#include "rcpputils/scope_exit.hpp"
#include "rclcpp/callback_group.hpp"
#include "rclcpp/logging.hpp"

namespace rp_executor
{

namespace
{

int64_t steady_now_ns()
{
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
}

bool env_flag_enabled(const char * env_name, bool default_value)
{
  const char * value = std::getenv(env_name);
  if (value == nullptr) {
    return default_value;
  }

  const std::string parsed(value);
  return parsed == "1" || parsed == "true" || parsed == "TRUE" ||
         parsed == "yes" || parsed == "YES" || parsed == "on" || parsed == "ON";
}

int env_int_or_default(const char * env_name, int default_value, int minimum)
{
  const char * value = std::getenv(env_name);
  if (value == nullptr) {
    return default_value;
  }

  try {
    const int parsed = std::stoi(value);
    return parsed < minimum ? minimum : parsed;
  } catch (const std::exception &) {
    return default_value;
  }
}

bool starts_with(const std::string & text, const std::string & prefix)
{
  return text.rfind(prefix, 0) == 0;
}

int64_t parse_period_us_from_node_name(const std::string & node_name)
{
  const std::size_t marker = node_name.rfind("_p");
  if (marker == std::string::npos || marker + 2 >= node_name.size()) {
    return -1;
  }

  const std::string suffix = node_name.substr(marker + 2);
  for (char c : suffix) {
    if (!std::isdigit(static_cast<unsigned char>(c))) {
      return -1;
    }
  }

  try {
    return std::stoll(suffix);
  } catch (const std::exception &) {
    return -1;
  }
}

}  // namespace

RPExecutor::RPExecutor(
  const rclcpp::ExecutorOptions & options,
  size_t number_of_threads,
  bool yield_before_execute,
  std::chrono::nanoseconds timeout)
: rclcpp::executors::MultiThreadedExecutor(
    options, number_of_threads, yield_before_execute, timeout),
  configured_number_of_threads_(get_number_of_threads()),
  yield_before_execute_(yield_before_execute),
  max_active_dag1_(env_int_or_default("RP_EXECUTOR_MAX_ACTIVE_DAG1", 3, 1)),
  max_active_dag2_(env_int_or_default("RP_EXECUTOR_MAX_ACTIVE_DAG2", 5, 1)),
  trace_callback_logs_(env_flag_enabled("RP_EXECUTOR_TRACE", false)),
  wait_timeout_(timeout),
  report_interval_(std::chrono::seconds(3)),
  dag1_active(0),
  dag2_active(0),
  total_executed_dag1(0),
  total_executed_dag2(0),
  total_deferred_dag1(0),
  total_deferred_dag2(0),
  deadline_miss_count_dag1(0),
  deadline_miss_count_dag2(0),
  deadline_miss_log_count(0),
  total_execution_time_dag1_ns(0),
  total_execution_time_dag2_ns(0),
  max_execution_time_dag1(0),
  max_execution_time_dag2(0),
  total_lateness_dag1_ns(0),
  total_lateness_dag2_ns(0),
  max_lateness_dag1_ns(0),
  max_lateness_dag2_ns(0),
  lateness_hist_0_1ms_dag1(0),
  lateness_hist_1_5ms_dag1(0),
  lateness_hist_5_10ms_dag1(0),
  lateness_hist_gt_10ms_dag1(0),
  lateness_hist_0_1ms_dag2(0),
  lateness_hist_1_5ms_dag2(0),
  lateness_hist_5_10ms_dag2(0),
  lateness_hist_gt_10ms_dag2(0),
  next_report_time_ns(0)
{
  const char * deadline_model = std::getenv("REDAG_DEADLINE_MODEL");
  if (deadline_model != nullptr && std::string(deadline_model) != "D_EQUALS_T") {
    throw std::runtime_error("REDAG_DEADLINE_MODEL must be D_EQUALS_T");
  }

  RCLCPP_INFO(
    rclcpp::get_logger("rp_executor"),
    "RPExecutor constructed (requested_threads=%zu, configured_threads=%zu, "
    "yield_before_execute=%s, wait_timeout_ns=%lld, max_active_dag1=%d, "
    "max_active_dag2=%d, trace_callback_logs=%s)",
    number_of_threads,
    configured_number_of_threads_,
    yield_before_execute ? "true" : "false",
    static_cast<long long>(wait_timeout_.count()),
    max_active_dag1_,
    max_active_dag2_,
    trace_callback_logs_ ? "true" : "false");
}

const char * RPExecutor::dag_name(DagId dag_id)
{
  switch (dag_id) {
    case DagId::Dag1:
      return "dag1";
    case DagId::Dag2:
      return "dag2";
    case DagId::None:
    default:
      return "none";
  }
}

RPExecutor::DagId RPExecutor::dag_from_topic(const std::string & topic_name)
{
  if (
    topic_name.find("/lidar") != std::string::npos ||
    topic_name.find("/perception") != std::string::npos ||
    topic_name.find("/planning") != std::string::npos)
  {
    return DagId::Dag1;
  }

  if (
    topic_name.find("/camera") != std::string::npos ||
    topic_name.find("/detection") != std::string::npos ||
    topic_name.find("/tracking") != std::string::npos)
  {
    return DagId::Dag2;
  }

  return DagId::None;
}

const char * RPExecutor::callback_type_from_executable(const rclcpp::AnyExecutable & any_exec)
{
  if (any_exec.timer) {
    return "timer";
  }
  if (any_exec.subscription) {
    return "subscription";
  }
  if (any_exec.service) {
    return "service";
  }
  if (any_exec.client) {
    return "client";
  }
  if (any_exec.waitable) {
    return "waitable";
  }
  return "unknown";
}

RPExecutor::DagId RPExecutor::classify_dag(
  const rclcpp::AnyExecutable & any_exec,
  std::string & callback_key) const
{
  const std::string node_name =
    any_exec.node_base ? std::string(any_exec.node_base->get_name()) : std::string();

  // Control callbacks are intentionally unrestricted.
  if (node_name == "control_node") {
    callback_key = "node=control_node";
    return DagId::None;
  }

  if (any_exec.subscription) {
    const std::string topic_name(any_exec.subscription->get_topic_name());
    callback_key = "topic=" + topic_name;
    return dag_from_topic(topic_name);
  }

  if (any_exec.timer) {
    callback_key = "timer_node=" + node_name;
    if (starts_with(node_name, "redag_task_d1_")) {
      return DagId::Dag1;
    }
    if (starts_with(node_name, "redag_task_d2_")) {
      return DagId::Dag2;
    }
    if (node_name == "lidar_node") {
      return DagId::Dag1;
    }
    if (node_name == "camera_node") {
      return DagId::Dag2;
    }
  }

  callback_key = node_name.empty() ? "callback=unknown" : "node=" + node_name;
  return DagId::None;
}

std::atomic<int> * RPExecutor::dag_counter(DagId dag_id)
{
  if (dag_id == DagId::Dag1) {
    return &dag1_active;
  }
  if (dag_id == DagId::Dag2) {
    return &dag2_active;
  }
  return nullptr;
}

int RPExecutor::dag_concurrency_limit(DagId dag_id) const
{
  if (dag_id == DagId::Dag1) {
    return max_active_dag1_;
  }
  if (dag_id == DagId::Dag2) {
    return max_active_dag2_;
  }
  return 0;
}

bool RPExecutor::try_acquire_dag_slot(std::atomic<int> * counter, int concurrency_limit)
{
  int observed = counter->load(std::memory_order_relaxed);
  while (observed < concurrency_limit) {
    if (counter->compare_exchange_weak(
        observed,
        observed + 1,
        std::memory_order_acq_rel,
        std::memory_order_relaxed))
    {
      return true;
    }
  }
  return false;
}

int64_t RPExecutor::relative_deadline_ns(const rclcpp::AnyExecutable & any_exec) const
{
  constexpr int64_t kNsPerUs = 1000;
  constexpr int64_t kFallbackDeadlineNs = 100000000;
  const std::string node_name =
    any_exec.node_base ? std::string(any_exec.node_base->get_name()) : std::string();

  if (any_exec.timer) {
    const int64_t period_us = parse_period_us_from_node_name(node_name);
    if (period_us > 0) {
      return period_us * kNsPerUs;
    }
  }

  return kFallbackDeadlineNs;
}

void RPExecutor::collect_ready_executables_locked()
{
  constexpr size_t kMaxCollectPerCycle = 64;
  constexpr size_t kMaxReadyQueueSize = 1024;
  size_t collected = 0;

  if (ready_queue_.size() >= kMaxReadyQueueSize) {
    return;
  }

  while (collected < kMaxCollectPerCycle && ready_queue_.size() < kMaxReadyQueueSize) {
    rclcpp::AnyExecutable any_exec;
    if (!get_next_executable(any_exec, std::chrono::nanoseconds(0))) {
      break;
    }

    ReadyExecutable ready_exec;
    ready_exec.any_exec = std::move(any_exec);
    ready_exec.ready_time_ns = steady_now_ns();
    ready_exec.absolute_deadline_ns =
      ready_exec.ready_time_ns + relative_deadline_ns(ready_exec.any_exec);
    ready_exec.dag_id = classify_dag(ready_exec.any_exec, ready_exec.callback_key);
    ready_exec.deferred_logged = false;
    ready_queue_.emplace_back(std::move(ready_exec));
    ++collected;
  }
}

bool RPExecutor::select_next_ready_executable_locked(
  ReadyExecutable & selected_exec,
  std::atomic<int> *& active_counter,
  bool & has_dag_slot)
{
  active_counter = nullptr;
  has_dag_slot = false;

  if (ready_queue_.empty()) {
    return false;
  }

  while (true) {
    size_t best_index = ready_queue_.size();
    int64_t earliest_deadline = std::numeric_limits<int64_t>::max();

    for (size_t i = 0; i < ready_queue_.size(); ++i) {
      auto & candidate = ready_queue_[i];
      const std::atomic<int> * counter = dag_counter(candidate.dag_id);
      const int concurrency_limit = dag_concurrency_limit(candidate.dag_id);
      if (
        counter != nullptr &&
        counter->load(std::memory_order_relaxed) >= concurrency_limit)
      {
        continue;
      }

      if (candidate.absolute_deadline_ns < earliest_deadline) {
        earliest_deadline = candidate.absolute_deadline_ns;
        best_index = i;
      }
    }

    if (best_index == ready_queue_.size()) {
      for (auto & candidate : ready_queue_) {
        if (candidate.dag_id == DagId::None || candidate.deferred_logged) {
          continue;
        }
        const std::atomic<int> * counter = dag_counter(candidate.dag_id);
        const int concurrency_limit = dag_concurrency_limit(candidate.dag_id);
        if (
          counter == nullptr ||
          counter->load(std::memory_order_relaxed) < concurrency_limit)
        {
          continue;
        }
        increment_deferred_metric(candidate.dag_id);
        candidate.deferred_logged = true;
      }
      return false;
    }

    ReadyExecutable & candidate = ready_queue_[best_index];
    active_counter = dag_counter(candidate.dag_id);
    if (active_counter != nullptr) {
      const int concurrency_limit = dag_concurrency_limit(candidate.dag_id);
      if (!try_acquire_dag_slot(active_counter, concurrency_limit))
      {
        if (!candidate.deferred_logged) {
          increment_deferred_metric(candidate.dag_id);
          candidate.deferred_logged = true;
        }
        continue;
      }
      has_dag_slot = true;
    }

    selected_exec = std::move(candidate);
    ready_queue_.erase(
      ready_queue_.begin() + static_cast<std::deque<ReadyExecutable>::difference_type>(best_index));
    return true;
  }
}

void RPExecutor::increment_deadline_miss_metric(DagId dag_id)
{
  if (dag_id == DagId::Dag1) {
    deadline_miss_count_dag1.fetch_add(1, std::memory_order_relaxed);
  } else if (dag_id == DagId::Dag2) {
    deadline_miss_count_dag2.fetch_add(1, std::memory_order_relaxed);
  }
}

void RPExecutor::update_lateness_metrics(DagId dag_id, int64_t lateness_ns)
{
  constexpr int64_t k1ms = 1000000;
  constexpr int64_t k5ms = 5000000;
  constexpr int64_t k10ms = 10000000;

  std::atomic<int64_t> * total_lateness_counter = nullptr;
  std::atomic<int64_t> * max_lateness_counter = nullptr;
  std::atomic<uint64_t> * hist_0_1 = nullptr;
  std::atomic<uint64_t> * hist_1_5 = nullptr;
  std::atomic<uint64_t> * hist_5_10 = nullptr;
  std::atomic<uint64_t> * hist_gt_10 = nullptr;

  if (dag_id == DagId::Dag1) {
    total_lateness_counter = &total_lateness_dag1_ns;
    max_lateness_counter = &max_lateness_dag1_ns;
    hist_0_1 = &lateness_hist_0_1ms_dag1;
    hist_1_5 = &lateness_hist_1_5ms_dag1;
    hist_5_10 = &lateness_hist_5_10ms_dag1;
    hist_gt_10 = &lateness_hist_gt_10ms_dag1;
  } else if (dag_id == DagId::Dag2) {
    total_lateness_counter = &total_lateness_dag2_ns;
    max_lateness_counter = &max_lateness_dag2_ns;
    hist_0_1 = &lateness_hist_0_1ms_dag2;
    hist_1_5 = &lateness_hist_1_5ms_dag2;
    hist_5_10 = &lateness_hist_5_10ms_dag2;
    hist_gt_10 = &lateness_hist_gt_10ms_dag2;
  } else {
    return;
  }

  total_lateness_counter->fetch_add(lateness_ns, std::memory_order_relaxed);

  int64_t observed_max = max_lateness_counter->load(std::memory_order_relaxed);
  while (observed_max < lateness_ns &&
    !max_lateness_counter->compare_exchange_weak(
      observed_max,
      lateness_ns,
      std::memory_order_relaxed,
      std::memory_order_relaxed))
  {
    // observed_max is updated by compare_exchange_weak on failure.
  }

  if (lateness_ns <= k1ms) {
    hist_0_1->fetch_add(1, std::memory_order_relaxed);
  } else if (lateness_ns <= k5ms) {
    hist_1_5->fetch_add(1, std::memory_order_relaxed);
  } else if (lateness_ns <= k10ms) {
    hist_5_10->fetch_add(1, std::memory_order_relaxed);
  } else {
    hist_gt_10->fetch_add(1, std::memory_order_relaxed);
  }
}

void RPExecutor::maybe_log_deadline_miss_detail(
  const ReadyExecutable & ready_exec,
  int64_t finish_time_ns,
  int64_t lateness_ns)
{
  uint64_t logged_count = deadline_miss_log_count.load(std::memory_order_relaxed);
  while (logged_count < 5) {
    if (deadline_miss_log_count.compare_exchange_weak(
        logged_count,
        logged_count + 1,
        std::memory_order_relaxed,
        std::memory_order_relaxed))
    {
      RCLCPP_WARN(
        rclcpp::get_logger("rp_executor"),
        "Deadline miss detail #%llu: topic=%s deadline_ns=%lld finish_time_ns=%lld "
        "lateness_ns=%lld dag=%s",
        static_cast<unsigned long long>(logged_count + 1),
        ready_exec.callback_key.c_str(),
        static_cast<long long>(ready_exec.absolute_deadline_ns),
        static_cast<long long>(finish_time_ns),
        static_cast<long long>(lateness_ns),
        dag_name(ready_exec.dag_id));
      break;
    }
  }
}

void RPExecutor::reset_metrics()
{
  dag1_active.store(0, std::memory_order_relaxed);
  dag2_active.store(0, std::memory_order_relaxed);
  total_executed_dag1.store(0, std::memory_order_relaxed);
  total_executed_dag2.store(0, std::memory_order_relaxed);
  total_deferred_dag1.store(0, std::memory_order_relaxed);
  total_deferred_dag2.store(0, std::memory_order_relaxed);
  deadline_miss_count_dag1.store(0, std::memory_order_relaxed);
  deadline_miss_count_dag2.store(0, std::memory_order_relaxed);
  deadline_miss_log_count.store(0, std::memory_order_relaxed);
  total_execution_time_dag1_ns.store(0, std::memory_order_relaxed);
  total_execution_time_dag2_ns.store(0, std::memory_order_relaxed);
  max_execution_time_dag1.store(0, std::memory_order_relaxed);
  max_execution_time_dag2.store(0, std::memory_order_relaxed);
  total_lateness_dag1_ns.store(0, std::memory_order_relaxed);
  total_lateness_dag2_ns.store(0, std::memory_order_relaxed);
  max_lateness_dag1_ns.store(0, std::memory_order_relaxed);
  max_lateness_dag2_ns.store(0, std::memory_order_relaxed);
  lateness_hist_0_1ms_dag1.store(0, std::memory_order_relaxed);
  lateness_hist_1_5ms_dag1.store(0, std::memory_order_relaxed);
  lateness_hist_5_10ms_dag1.store(0, std::memory_order_relaxed);
  lateness_hist_gt_10ms_dag1.store(0, std::memory_order_relaxed);
  lateness_hist_0_1ms_dag2.store(0, std::memory_order_relaxed);
  lateness_hist_1_5ms_dag2.store(0, std::memory_order_relaxed);
  lateness_hist_5_10ms_dag2.store(0, std::memory_order_relaxed);
  lateness_hist_gt_10ms_dag2.store(0, std::memory_order_relaxed);
  for (auto & ready_exec : ready_queue_) {
    release_callback_group(ready_exec.any_exec);
  }
  ready_queue_.clear();
  next_report_time_ns.store(
    steady_now_ns() + report_interval_.count(),
    std::memory_order_relaxed);
}

void RPExecutor::increment_deferred_metric(DagId dag_id)
{
  if (dag_id == DagId::Dag1) {
    total_deferred_dag1.fetch_add(1, std::memory_order_relaxed);
  } else if (dag_id == DagId::Dag2) {
    total_deferred_dag2.fetch_add(1, std::memory_order_relaxed);
  }
}

void RPExecutor::update_execution_metrics(DagId dag_id, int64_t execution_duration_ns)
{
  std::atomic<uint64_t> * executed_counter = nullptr;
  std::atomic<int64_t> * total_time_counter = nullptr;
  std::atomic<int64_t> * max_time_counter = nullptr;

  if (dag_id == DagId::Dag1) {
    executed_counter = &total_executed_dag1;
    total_time_counter = &total_execution_time_dag1_ns;
    max_time_counter = &max_execution_time_dag1;
  } else if (dag_id == DagId::Dag2) {
    executed_counter = &total_executed_dag2;
    total_time_counter = &total_execution_time_dag2_ns;
    max_time_counter = &max_execution_time_dag2;
  }

  if (executed_counter == nullptr) {
    return;
  }

  executed_counter->fetch_add(1, std::memory_order_relaxed);
  total_time_counter->fetch_add(execution_duration_ns, std::memory_order_relaxed);

  int64_t observed_max = max_time_counter->load(std::memory_order_relaxed);
  while (observed_max < execution_duration_ns &&
    !max_time_counter->compare_exchange_weak(
      observed_max,
      execution_duration_ns,
      std::memory_order_relaxed,
      std::memory_order_relaxed))
  {
    // observed_max is updated by compare_exchange_weak on failure.
  }
}

void RPExecutor::maybe_emit_scheduling_report()
{
  const int64_t now_ns = steady_now_ns();
  int64_t expected_report_ns = next_report_time_ns.load(std::memory_order_relaxed);
  if (now_ns < expected_report_ns) {
    return;
  }

  if (!next_report_time_ns.compare_exchange_strong(
      expected_report_ns,
      now_ns + report_interval_.count(),
      std::memory_order_relaxed,
      std::memory_order_relaxed))
  {
    return;
  }

  const uint64_t dag1_executed = total_executed_dag1.load(std::memory_order_relaxed);
  const uint64_t dag2_executed = total_executed_dag2.load(std::memory_order_relaxed);
  const uint64_t dag1_deferred = total_deferred_dag1.load(std::memory_order_relaxed);
  const uint64_t dag2_deferred = total_deferred_dag2.load(std::memory_order_relaxed);
  const uint64_t dag1_deadline_miss = deadline_miss_count_dag1.load(std::memory_order_relaxed);
  const uint64_t dag2_deadline_miss = deadline_miss_count_dag2.load(std::memory_order_relaxed);
  const int64_t dag1_total_ns = total_execution_time_dag1_ns.load(std::memory_order_relaxed);
  const int64_t dag2_total_ns = total_execution_time_dag2_ns.load(std::memory_order_relaxed);
  const int64_t dag1_max_ns = max_execution_time_dag1.load(std::memory_order_relaxed);
  const int64_t dag2_max_ns = max_execution_time_dag2.load(std::memory_order_relaxed);
  const int64_t dag1_total_lateness_ns = total_lateness_dag1_ns.load(std::memory_order_relaxed);
  const int64_t dag2_total_lateness_ns = total_lateness_dag2_ns.load(std::memory_order_relaxed);
  const int64_t dag1_max_lateness_ns = max_lateness_dag1_ns.load(std::memory_order_relaxed);
  const int64_t dag2_max_lateness_ns = max_lateness_dag2_ns.load(std::memory_order_relaxed);
  const uint64_t dag1_hist_0_1 = lateness_hist_0_1ms_dag1.load(std::memory_order_relaxed);
  const uint64_t dag1_hist_1_5 = lateness_hist_1_5ms_dag1.load(std::memory_order_relaxed);
  const uint64_t dag1_hist_5_10 = lateness_hist_5_10ms_dag1.load(std::memory_order_relaxed);
  const uint64_t dag1_hist_gt_10 = lateness_hist_gt_10ms_dag1.load(std::memory_order_relaxed);
  const uint64_t dag2_hist_0_1 = lateness_hist_0_1ms_dag2.load(std::memory_order_relaxed);
  const uint64_t dag2_hist_1_5 = lateness_hist_1_5ms_dag2.load(std::memory_order_relaxed);
  const uint64_t dag2_hist_5_10 = lateness_hist_5_10ms_dag2.load(std::memory_order_relaxed);
  const uint64_t dag2_hist_gt_10 = lateness_hist_gt_10ms_dag2.load(std::memory_order_relaxed);
  const int64_t dag1_avg_ns =
    dag1_executed == 0 ? 0 : dag1_total_ns / static_cast<int64_t>(dag1_executed);
  const int64_t dag2_avg_ns =
    dag2_executed == 0 ? 0 : dag2_total_ns / static_cast<int64_t>(dag2_executed);
  const int64_t dag1_avg_lateness_ns =
    dag1_deadline_miss == 0 ? 0 : dag1_total_lateness_ns / static_cast<int64_t>(dag1_deadline_miss);
  const int64_t dag2_avg_lateness_ns =
    dag2_deadline_miss == 0 ? 0 : dag2_total_lateness_ns / static_cast<int64_t>(dag2_deadline_miss);
  const double miss_rate_dag1 =
    dag1_executed == 0 ? 0.0 : static_cast<double>(dag1_deadline_miss) / static_cast<double>(dag1_executed);
  const double miss_rate_dag2 =
    dag2_executed == 0 ? 0.0 : static_cast<double>(dag2_deadline_miss) / static_cast<double>(dag2_executed);

  RCLCPP_INFO(
    rclcpp::get_logger("rp_executor"),
    "DAG1 executed=%llu deferred=%llu avg_exec_ns=%lld max_exec_ns=%lld "
    "deadline_miss_dag1=%llu miss_rate_dag1=%.6f avg_lateness_ns=%lld max_lateness_ns=%lld",
    static_cast<unsigned long long>(dag1_executed),
    static_cast<unsigned long long>(dag1_deferred),
    static_cast<long long>(dag1_avg_ns),
    static_cast<long long>(dag1_max_ns),
    static_cast<unsigned long long>(dag1_deadline_miss),
    miss_rate_dag1,
    static_cast<long long>(dag1_avg_lateness_ns),
    static_cast<long long>(dag1_max_lateness_ns));
  RCLCPP_INFO(
    rclcpp::get_logger("rp_executor"),
    "DAG2 executed=%llu deferred=%llu avg_exec_ns=%lld max_exec_ns=%lld "
    "deadline_miss_dag2=%llu miss_rate_dag2=%.6f avg_lateness_ns=%lld max_lateness_ns=%lld",
    static_cast<unsigned long long>(dag2_executed),
    static_cast<unsigned long long>(dag2_deferred),
    static_cast<long long>(dag2_avg_ns),
    static_cast<long long>(dag2_max_ns),
    static_cast<unsigned long long>(dag2_deadline_miss),
    miss_rate_dag2,
    static_cast<long long>(dag2_avg_lateness_ns),
    static_cast<long long>(dag2_max_lateness_ns));
  RCLCPP_INFO(
    rclcpp::get_logger("rp_executor"),
    "DAG1 lateness_hist 0-1ms=%llu 1-5ms=%llu 5-10ms=%llu >10ms=%llu",
    static_cast<unsigned long long>(dag1_hist_0_1),
    static_cast<unsigned long long>(dag1_hist_1_5),
    static_cast<unsigned long long>(dag1_hist_5_10),
    static_cast<unsigned long long>(dag1_hist_gt_10));
  RCLCPP_INFO(
    rclcpp::get_logger("rp_executor"),
    "DAG2 lateness_hist 0-1ms=%llu 1-5ms=%llu 5-10ms=%llu >10ms=%llu",
    static_cast<unsigned long long>(dag2_hist_0_1),
    static_cast<unsigned long long>(dag2_hist_1_5),
    static_cast<unsigned long long>(dag2_hist_5_10),
    static_cast<unsigned long long>(dag2_hist_gt_10));
}

void RPExecutor::release_callback_group(rclcpp::AnyExecutable & any_exec)
{
  if (any_exec.callback_group &&
    any_exec.callback_group->type() == rclcpp::CallbackGroupType::MutuallyExclusive)
  {
    any_exec.callback_group->can_be_taken_from().store(true);
    interrupt_guard_condition_.trigger();
  }
  any_exec.callback_group.reset();
}

void RPExecutor::run_execution_loop(std::mutex & wait_mutex)
{
  while (rclcpp::ok(context_) && spinning.load()) {
    ReadyExecutable ready_exec;
    std::atomic<int> * active_counter = nullptr;
    bool has_dag_slot = false;
    bool has_work = false;
    {
      std::lock_guard<std::mutex> wait_lock(wait_mutex);
      if (!rclcpp::ok(context_) || !spinning.load()) {
        return;
      }

      if (ready_queue_.empty()) {
        wait_for_work(wait_timeout_);
      }
      collect_ready_executables_locked();
      has_work = select_next_ready_executable_locked(ready_exec, active_counter, has_dag_slot);
      if (!has_work) {
        maybe_emit_scheduling_report();
      }
    }

    if (!has_work) {
      std::this_thread::yield();
      continue;
    }

    auto release_dag_slot = rcpputils::make_scope_exit([&has_dag_slot, &active_counter]() {
        if (has_dag_slot && active_counter != nullptr) {
          active_counter->fetch_sub(1, std::memory_order_acq_rel);
        }
      });
    auto release_exec = rcpputils::make_scope_exit([this, &ready_exec]() {
        release_callback_group(ready_exec.any_exec);
      });
    (void)release_dag_slot;
    (void)release_exec;

    if (yield_before_execute_) {
      std::this_thread::yield();
    }

    const int64_t execution_start_ns = steady_now_ns();
    execute_any_executable(ready_exec.any_exec);
    const int64_t execution_end_ns = steady_now_ns();
    update_execution_metrics(ready_exec.dag_id, execution_end_ns - execution_start_ns);
    if (
      (ready_exec.dag_id == DagId::Dag1 || ready_exec.dag_id == DagId::Dag2) &&
      execution_end_ns > ready_exec.absolute_deadline_ns)
    {
      const int64_t lateness_ns = execution_end_ns - ready_exec.absolute_deadline_ns;
      increment_deadline_miss_metric(ready_exec.dag_id);
      update_lateness_metrics(ready_exec.dag_id, lateness_ns);
      maybe_log_deadline_miss_detail(ready_exec, execution_end_ns, lateness_ns);
    }
    maybe_emit_scheduling_report();
  }
}

void RPExecutor::spin()
{
  if (spinning.exchange(true)) {
    throw std::runtime_error("RPExecutor::spin() called while already spinning");
  }
  RCPPUTILS_SCOPE_EXIT(this->spinning.store(false););

  reset_metrics();
  RCLCPP_INFO(rclcpp::get_logger("rp_executor"), "RPExecutor spin() started");

  std::mutex wait_mutex;
  const size_t thread_count = configured_number_of_threads_ == 0 ? 1 : configured_number_of_threads_;
  std::vector<std::thread> threads;
  if (thread_count > 1) {
    threads.reserve(thread_count - 1);
  }

  auto join_threads = [&threads]() {
      for (auto & thread : threads) {
        if (thread.joinable()) {
          thread.join();
        }
      }
    };

  try {
    for (size_t i = 0; i + 1 < thread_count; ++i) {
      threads.emplace_back([this, &wait_mutex]() { run_execution_loop(wait_mutex); });
    }

    run_execution_loop(wait_mutex);

    spinning.store(false);
    interrupt_guard_condition_.trigger();
    join_threads();
  } catch (...) {
    spinning.store(false);
    interrupt_guard_condition_.trigger();
    join_threads();
    throw;
  }

  {
    std::lock_guard<std::mutex> wait_lock(wait_mutex);
    for (auto & ready_exec : ready_queue_) {
      release_callback_group(ready_exec.any_exec);
    }
    ready_queue_.clear();
  }

  RCLCPP_INFO(rclcpp::get_logger("rp_executor"), "RPExecutor spin() stopped");
}

}  // namespace rp_executor
