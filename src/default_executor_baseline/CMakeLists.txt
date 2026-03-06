cmake_minimum_required(VERSION 3.8)
project(default_executor_baseline)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)

add_executable(default_executor_baseline src/default_executor_baseline.cpp)
target_compile_features(default_executor_baseline PUBLIC cxx_std_17)
ament_target_dependencies(default_executor_baseline rclcpp)

install(TARGETS
  default_executor_baseline
  DESTINATION lib/${PROJECT_NAME}
)

install(FILES
  scripts/run_default_executor_matrix.sh
  scripts/plot_default_executor_results.py
  scripts/configure_cpu_environment.sh
  DESTINATION share/${PROJECT_NAME}/scripts
)

ament_package()
