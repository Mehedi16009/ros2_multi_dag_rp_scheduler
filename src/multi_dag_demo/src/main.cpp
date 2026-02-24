#include <cstdlib>
#include <memory>
#include <vector>

#include "rclcpp/rclcpp.hpp"
#include "rp_executor/rp_executor.hpp"

namespace multi_dag_demo
{

std::shared_ptr<rclcpp::Node> create_lidar_node();
std::shared_ptr<rclcpp::Node> create_camera_node();
std::shared_ptr<rclcpp::Node> create_perception_node();
std::shared_ptr<rclcpp::Node> create_detection_node();
std::shared_ptr<rclcpp::Node> create_planning_node();
std::shared_ptr<rclcpp::Node> create_tracking_node();
std::shared_ptr<rclcpp::Node> create_control_node();

}  // namespace multi_dag_demo

int main(int argc, char ** argv)
{
  rclcpp::init(argc, argv);

  std::vector<std::shared_ptr<rclcpp::Node>> nodes;
  nodes.push_back(multi_dag_demo::create_lidar_node());
  nodes.push_back(multi_dag_demo::create_camera_node());
  nodes.push_back(multi_dag_demo::create_perception_node());
  nodes.push_back(multi_dag_demo::create_detection_node());
  nodes.push_back(multi_dag_demo::create_planning_node());
  nodes.push_back(multi_dag_demo::create_tracking_node());
  nodes.push_back(multi_dag_demo::create_control_node());

  size_t executor_thread_count = 8;
  if (const char * env_threads = std::getenv("RP_EXECUTOR_THREADS")) {
    char * end = nullptr;
    const unsigned long parsed_threads = std::strtoul(env_threads, &end, 10);
    if (end != env_threads && end != nullptr && *end == '\0' && parsed_threads > 0UL) {
      executor_thread_count = static_cast<size_t>(parsed_threads);
    }
  }

  rp_executor::RPExecutor executor(rclcpp::ExecutorOptions(), executor_thread_count, false);
  for (const auto & node : nodes) {
    executor.add_node(node);
  }

  RCLCPP_INFO(
    rclcpp::get_logger("multi_dag_demo_main"),
    "Starting timer-based multi-DAG demo with RPExecutor (threads=%zu)",
    executor_thread_count);

  executor.spin();
  rclcpp::shutdown();
  return 0;
}
