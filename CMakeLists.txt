cmake_minimum_required(VERSION 3.10)
project(LoadTester C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra -Werror -O3)

include_directories(include)

add_executable(surge src/main.c)

target_link_libraries(surge pthread)