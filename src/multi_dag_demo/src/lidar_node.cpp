#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

#include "rclcpp/rclcpp.hpp"
#include "sensor_msgs/msg/laser_scan.hpp"

using namespace std::chrono_literals;

namespace multi_dag_demo
{

class LidarNode : public rclcpp::Node
{
public:
  LidarNode()
  : Node("lidar_node"), sequence_(0)
  {
    constexpr auto kLidarPeriod = 20ms;
    publisher_ = create_publisher<sensor_msgs::msg::LaserScan>("dag1/lidar", 10);
    timer_ = create_wall_timer(kLidarPeriod, std::bind(&LidarNode::on_timer, this));
    RCLCPP_INFO(
      get_logger(),
      "LiDAR node started (period_ms=%lld)",
      static_cast<long long>(
        std::chrono::duration_cast<std::chrono::milliseconds>(kLidarPeriod).count()));
  }

private:
  void on_timer()
  {
    sensor_msgs::msg::LaserScan msg;
    msg.header.stamp = now();
    msg.header.frame_id = "lidar_frame";
    msg.angle_min = -1.57f;
    msg.angle_max = 1.57f;
    msg.angle_increment = 0.01f;
    msg.range_min = 0.1f;
    msg.range_max = 30.0f;
    msg.ranges.resize(314, 5.0f + static_cast<float>(sequence_ % 5));

    publisher_->publish(msg);
    sequence_++;
    RCLCPP_INFO(get_logger(), "Published LiDAR scan %u", sequence_);
  }

  rclcpp::Publisher<sensor_msgs::msg::LaserScan>::SharedPtr publisher_;
  rclcpp::TimerBase::SharedPtr timer_;
  uint32_t sequence_;
};

std::shared_ptr<rclcpp::Node> create_lidar_node()
{
  return std::make_shared<LidarNode>();
}

}  // namespace multi_dag_demo
