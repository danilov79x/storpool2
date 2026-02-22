cmake_minimum_required(VERSION 3.10)

project(model_count C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

add_executable(model_count model_count.c)

if (CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(model_count PRIVATE -O2 -Wall -Wextra -Wpedantic)
endif()

enable_testing()

add_executable(model_count_tests tests/test_model_count.c)
if (CMAKE_C_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(model_count_tests PRIVATE -O2 -Wall -Wextra -Wpedantic)
endif()

add_test(NAME model_count_tests COMMAND model_count_tests)
