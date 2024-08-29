cmake_minimum_required(VERSION 3.10)

project(MyStorageProject)

# 设置 C++ 标准为 20
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE Release)
find_package (Seastar REQUIRED)

include_directories(${PROJECT_SOURCE_DIR}/src/include)

# 添加可执行目标，并指定 Release 模式
add_executable(my_storage main.cpp)
target_link_libraries(my_storage Seastar::seastar)