#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "rclcpp/rclcpp.hpp"
#include "std_msgs/msg/string.hpp"

namespace multi_dag_demo
{

class TrackingNode : public rclcpp::Node
{
public:
  TrackingNode()
  : Node("tracking_node"), instance_id_(0)
  {
    publisher_ = create_publisher<std_msgs::msg::String>("dag2/tracking", 10);
    subscription_ = create_subscription<std_msgs::msg::String>(
      "dag2/detection",
      10,
      std::bind(&TrackingNode::on_detection, this, std::placeholders::_1));
    RCLCPP_INFO(get_logger(), "Tracking node started");
  }

private:
  void on_detection(const std_msgs::msg::String::SharedPtr msg)
  {
    std_msgs::msg::String output;
    output.data =
      "tracking_" + std::to_string(instance_id_) + "_from_" + msg->data;
    publisher_->publish(output);
    RCLCPP_INFO(get_logger(), "Published tracking output %u", instance_id_);
    instance_id_++;
  }

  rclcpp::Publisher<std_msgs::msg::String>::SharedPtr publisher_;
  rclcpp::Subscription<std_msgs::msg::String>::SharedPtr subscription_;
  uint32_t instance_id_;
};

std::shared_ptr<rclcpp::Node> create_tracking_node()
{
  return std::make_shared<TrackingNode>();
}

}  // namespace multi_dag_demo
