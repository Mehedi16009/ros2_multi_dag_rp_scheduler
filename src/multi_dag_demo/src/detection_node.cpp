#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/image.hpp"
#include "std_msgs/msg/string.hpp"

namespace multi_dag_demo
{

class DetectionNode : public rclcpp::Node
{
public:
  DetectionNode()
  : Node("detection_node"), instance_id_(0)
  {
    publisher_ = create_publisher<std_msgs::msg::String>("dag2/detection", 10);
    subscription_ = create_subscription<sensor_msgs::msg::Image>(
      "dag2/camera",
      10,
      std::bind(&DetectionNode::on_camera, this, std::placeholders::_1));
    RCLCPP_INFO(get_logger(), "Detection node started");
  }

private:
  void on_camera(const sensor_msgs::msg::Image::SharedPtr msg)
  {
    std_msgs::msg::String output;
    output.data =
      "detection_" + std::to_string(instance_id_) +
      "_bytes_" + std::to_string(msg->data.size());
    publisher_->publish(output);
    RCLCPP_INFO(get_logger(), "Published detection output %u", instance_id_);
    instance_id_++;
  }

  rclcpp::Publisher<std_msgs::msg::String>::SharedPtr publisher_;
  rclcpp::Subscription<sensor_msgs::msg::Image>::SharedPtr subscription_;
  uint32_t instance_id_;
};

std::shared_ptr<rclcpp::Node> create_detection_node()
{
  return std::make_shared<DetectionNode>();
}

}  // namespace multi_dag_demo
