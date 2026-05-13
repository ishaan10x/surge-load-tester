cmake_minimum_required(VERSION 3.10)
project(LoadTester C)

set(CMAKE_C_STANDARD 11)

add_compile_options(
    -Wall
    -Wextra
    -Werror
    -O3
    -march=native
)

add_executable(surge src/main.c)

target_include_directories(surge PRIVATE include)
target_link_libraries(surge pthread)

set_target_properties(surge PROPERTIES
    INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE
)

add_custom_command(TARGET surge POST_BUILD
    COMMAND strip $<TARGET_FILE:surge>
)