#include <functional>
#include <memory>
#include <string>

#include "rclcpp/rclcpp.hpp"
#include "std_msgs/msg/string.hpp"

namespace multi_dag_demo
{

class ControlNode : public rclcpp::Node
{
public:
  ControlNode()
  : Node("control_node")
  {
    planning_subscription_ = create_subscription<std_msgs::msg::String>(
      "dag1/planning",
      10,
      std::bind(&ControlNode::on_planning, this, std::placeholders::_1));

    tracking_subscription_ = create_subscription<std_msgs::msg::String>(
      "dag2/tracking",
      10,
      std::bind(&ControlNode::on_tracking, this, std::placeholders::_1));

    RCLCPP_INFO(get_logger(), "Control node started");
  }

private:
  void on_planning(const std_msgs::msg::String::SharedPtr msg)
  {
    latest_planning_ = msg->data;
    RCLCPP_INFO(
      get_logger(),
      "Control received planning: %s",
      latest_planning_.c_str());
  }

  void on_tracking(const std_msgs::msg::String::SharedPtr msg)
  {
    latest_tracking_ = msg->data;
    RCLCPP_INFO(
      get_logger(),
      "Control received tracking: %s",
      latest_tracking_.c_str());
  }

  rclcpp::Subscription<std_msgs::msg::String>::SharedPtr planning_subscription_;
  rclcpp::Subscription<std_msgs::msg::String>::SharedPtr tracking_subscription_;
  std::string latest_planning_;
  std::string latest_tracking_;
};

std::shared_ptr<rclcpp::Node> create_control_node()
{
  return std::make_shared<ControlNode>();
}

}  // namespace multi_dag_demo
