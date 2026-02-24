#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/image.hpp"

using namespace std::chrono_literals;

namespace multi_dag_demo
{

class CameraNode : public rclcpp::Node
{
public:
  CameraNode()
  : Node("camera_node"), sequence_(0)
  {
    constexpr auto kCameraPeriod = 30ms;
    publisher_ = create_publisher<sensor_msgs::msg::Image>("dag2/camera", 10);
    timer_ = create_wall_timer(kCameraPeriod, std::bind(&CameraNode::on_timer, this));
    RCLCPP_INFO(
      get_logger(),
      "Camera node started (period_ms=%lld)",
      static_cast<long long>(
        std::chrono::duration_cast<std::chrono::milliseconds>(kCameraPeriod).count()));
  }

private:
  void on_timer()
  {
    sensor_msgs::msg::Image msg;
    msg.header.stamp = now();
    msg.header.frame_id = "camera_frame";
    msg.height = 1;
    msg.width = 1;
    msg.encoding = "mono8";
    msg.is_bigendian = false;
    msg.step = 1;
    msg.data = {static_cast<uint8_t>(sequence_ % 255)};

    publisher_->publish(msg);
    sequence_++;
    RCLCPP_INFO(get_logger(), "Published Camera frame %u", sequence_);
  }

  rclcpp::Publisher<sensor_msgs::msg::Image>::SharedPtr publisher_;
  rclcpp::TimerBase::SharedPtr timer_;
  uint32_t sequence_;
};

std::shared_ptr<rclcpp::Node> create_camera_node()
{
  return std::make_shared<CameraNode>();
}

}  // namespace multi_dag_demo
