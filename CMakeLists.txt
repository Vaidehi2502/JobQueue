cmake_minimum_required(VERSION 3.16)
project(cpp_job_queue LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(JOBQ_BUILD_TESTS "Build unit/integration tests" ON)
option(JOBQ_BUILD_BENCHMARKS "Build the benchmark executable" ON)

find_package(Threads REQUIRED)

# --- SQLite3 -----------------------------------------------------------------
# Prefer the CMake-provided module; fall back to pkg-config.
find_package(SQLite3)
if(NOT SQLite3_FOUND)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(SQLite3 REQUIRED IMPORTED_TARGET sqlite3)
  add_library(SQLite::SQLite3 ALIAS PkgConfig::SQLite3)
endif()

add_compile_options(-Wall -Wextra)

# --- Core library ------------------------------------------------------------
add_library(jobqueue
  src/job.cpp
  src/job_store.cpp
  src/job_queue.cpp
)
target_include_directories(jobqueue PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(jobqueue PUBLIC Threads::Threads SQLite::SQLite3)

# --- CLI demo ----------------------------------------------------------------
add_executable(job_queue src/main.cpp)
target_link_libraries(job_queue PRIVATE jobqueue)

# --- Tests -------------------------------------------------------------------
if(JOBQ_BUILD_TESTS)
  enable_testing()
  include(FetchContent)
  # Try an installed GoogleTest first; otherwise fetch a pinned release.
  find_package(GTest QUIET)
  if(NOT GTest_FOUND)
    FetchContent_Declare(
      googletest
      URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.tar.gz
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
  endif()

  add_executable(job_queue_tests
    tests/job_queue_tests.cpp
    tests/concurrency_tests.cpp
    tests/recovery_tests.cpp
  )
  target_link_libraries(job_queue_tests PRIVATE jobqueue GTest::gtest_main)

  include(GoogleTest)
  gtest_discover_tests(job_queue_tests)
endif()

# --- Benchmarks --------------------------------------------------------------
if(JOBQ_BUILD_BENCHMARKS)
  add_executable(benchmark benchmarks/benchmark.cpp)
  target_link_libraries(benchmark PRIVATE jobqueue)
endif()
