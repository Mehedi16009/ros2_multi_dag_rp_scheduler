#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/laser_scan.hpp"
#include "std_msgs/msg/string.hpp"

namespace multi_dag_demo
{

class PerceptionNode : public rclcpp::Node
{
public:
  PerceptionNode()
  : Node("perception_node"), instance_id_(0)
  {
    publisher_ = create_publisher<std_msgs::msg::String>("dag1/perception", 10);
    subscription_ = create_subscription<sensor_msgs::msg::LaserScan>(
      "dag1/lidar",
      10,
      std::bind(&PerceptionNode::on_lidar, this, std::placeholders::_1));
    RCLCPP_INFO(get_logger(), "Perception node started");
  }

private:
  void on_lidar(const sensor_msgs::msg::LaserScan::SharedPtr msg)
  {
    std_msgs::msg::String output;
    output.data =
      "perception_" + std::to_string(instance_id_) +
      "_ranges_" + std::to_string(msg->ranges.size());
    publisher_->publish(output);
    RCLCPP_INFO(get_logger(), "Published perception output %u", instance_id_);
    instance_id_++;
  }

  rclcpp::Publisher<std_msgs::msg::String>::SharedPtr publisher_;
  rclcpp::Subscription<sensor_msgs::msg::LaserScan>::SharedPtr subscription_;
  uint32_t instance_id_;
};

std::shared_ptr<rclcpp::Node> create_perception_node()
{
  return std::make_shared<PerceptionNode>();
}

}  // namespace multi_dag_demo
