#ifndef RP_EXECUTOR__RP_EXECUTOR_HPP_
#define RP_EXECUTOR__RP_EXECUTOR_HPP_

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>

#include "rclcpp/any_executable.hpp"
#include "rclcpp/executors/multi_threaded_executor.hpp"

namespace rp_executor
{

class RPExecutor : public rclcpp::executors::MultiThreadedExecutor
{
public:
  explicit RPExecutor(
    const rclcpp::ExecutorOptions & options = rclcpp::ExecutorOptions(),
    size_t number_of_threads = 0,
    bool yield_before_execute = false,
    std::chrono::nanoseconds timeout = std::chrono::nanoseconds(-1));

  ~RPExecutor() override = default;

  void spin() override;

private:
  enum class DagId
  {
    None = 0,
    Dag1 = 1,
    Dag2 = 2
  };

  struct ReadyExecutable
  {
    rclcpp::AnyExecutable any_exec;
    DagId dag_id;
    std::string callback_key;
    int64_t ready_time_ns;
    int64_t absolute_deadline_ns;
    bool deferred_logged;
  };

  void run_execution_loop(std::mutex & wait_mutex);

  static const char * callback_type_from_executable(const rclcpp::AnyExecutable & any_exec);

  static const char * dag_name(DagId dag_id);

  static DagId dag_from_topic(const std::string & topic_name);

  DagId classify_dag(const rclcpp::AnyExecutable & any_exec, std::string & callback_key) const;

  std::atomic<int> * dag_counter(DagId dag_id);

  int dag_concurrency_limit(DagId dag_id) const;

  bool try_acquire_dag_slot(std::atomic<int> * counter, int concurrency_limit);

  int64_t relative_deadline_ns(const rclcpp::AnyExecutable & any_exec) const;

  void collect_ready_executables_locked();

  bool select_next_ready_executable_locked(
    ReadyExecutable & selected_exec,
    std::atomic<int> *& active_counter,
    bool & has_dag_slot);

  void increment_deadline_miss_metric(DagId dag_id);

  void update_lateness_metrics(DagId dag_id, int64_t lateness_ns);

  void maybe_log_deadline_miss_detail(
    const ReadyExecutable & ready_exec,
    int64_t finish_time_ns,
    int64_t lateness_ns);

  void reset_metrics();

  void increment_deferred_metric(DagId dag_id);

  void update_execution_metrics(DagId dag_id, int64_t execution_duration_ns);

  void maybe_emit_scheduling_report();

  void release_callback_group(rclcpp::AnyExecutable & any_exec);

  size_t configured_number_of_threads_;
  bool yield_before_execute_;
  const int max_active_dag1_;
  const int max_active_dag2_;
  const bool trace_callback_logs_;
  std::chrono::nanoseconds wait_timeout_;
  const std::chrono::nanoseconds report_interval_;

  std::atomic<int> dag1_active;
  std::atomic<int> dag2_active;
  std::atomic<uint64_t> total_executed_dag1;
  std::atomic<uint64_t> total_executed_dag2;
  std::atomic<uint64_t> total_deferred_dag1;
  std::atomic<uint64_t> total_deferred_dag2;
  std::atomic<uint64_t> deadline_miss_count_dag1;
  std::atomic<uint64_t> deadline_miss_count_dag2;
  std::atomic<uint64_t> deadline_miss_log_count;
  std::atomic<int64_t> total_execution_time_dag1_ns;
  std::atomic<int64_t> total_execution_time_dag2_ns;
  std::atomic<int64_t> max_execution_time_dag1;
  std::atomic<int64_t> max_execution_time_dag2;
  std::atomic<int64_t> total_lateness_dag1_ns;
  std::atomic<int64_t> total_lateness_dag2_ns;
  std::atomic<int64_t> max_lateness_dag1_ns;
  std::atomic<int64_t> max_lateness_dag2_ns;
  std::atomic<uint64_t> lateness_hist_0_1ms_dag1;
  std::atomic<uint64_t> lateness_hist_1_5ms_dag1;
  std::atomic<uint64_t> lateness_hist_5_10ms_dag1;
  std::atomic<uint64_t> lateness_hist_gt_10ms_dag1;
  std::atomic<uint64_t> lateness_hist_0_1ms_dag2;
  std::atomic<uint64_t> lateness_hist_1_5ms_dag2;
  std::atomic<uint64_t> lateness_hist_5_10ms_dag2;
  std::atomic<uint64_t> lateness_hist_gt_10ms_dag2;
  std::atomic<int64_t> next_report_time_ns;
  std::deque<ReadyExecutable> ready_queue_;
};

}  // namespace rp_executor

#endif  // RP_EXECUTOR__RP_EXECUTOR_HPP_
