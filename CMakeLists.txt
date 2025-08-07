# ~~~
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ~~~

cmake_minimum_required(VERSION 3.13...3.24)

# Define the project name and where to report bugs.
set(PACKAGE_BUGREPORT "https://github.com/googleapis/google-cloud-cpp/issues")

project(
    bigtable-emulator
    VERSION 1.0.0
    LANGUAGES CXX)
set(PROJECT_VERSION_PRE_RELEASE "rc")

if (NOT "${PROJECT_VERSION_PRE_RELEASE}" STREQUAL "")
    set(PROJECT_VERSION "${PROJECT_VERSION}-${PROJECT_VERSION_PRE_RELEASE}")
endif ()

# Automatically update the version(s) in the `MODULE.bazel` file.
file(READ "MODULE.bazel" module_contents)
string(
    REGEX
    REPLACE "version = \".*\",  # Updated by CMake"
            "version = \"${PROJECT_VERSION}\",  # Updated by CMake"
            updated_contents "${module_contents}")
string(
    REGEX
    REPLACE
        "compatibility_level = .*,  # Updated by CMake"
        "compatibility_level = ${PROJECT_VERSION_MAJOR},  # Updated by CMake"
        updated_contents "${updated_contents}")
if (NOT ("${updated_contents}" STREQUAL "${module_contents}"))
    # Rewrite the file *only* if it was updated.
    file(WRITE "MODULE.bazel" "${updated_contents}")
endif ()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.5)
        message(
            WARNING "The `google-cloud-cpp` library is tested with GCC >= 7.5."
                    " We will consider patches for older versions.")
    endif ()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 6.0)
        message(
            WARNING
                "The `google-cloud-cpp` library is tested with Clang >= 6.0."
                " We will consider patches for older versions.")
    endif ()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    if (CMAKE_VERSION VERSION_LESS 3.15)
        message(
            FATAL_ERROR
                "MSVC builds require CMake >= 3.15."
                " Previous versions of CMake lack a standard mechanism to"
                " select the runtime C++ library.")
    endif ()
endif ()

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

include(FetchContent)

FetchContent_Declare(
    google-cloud-cpp
    URL https://github.com/googleapis/google-cloud-cpp/archive/refs/tags/v2.39.0.tar.gz
)

FetchContent_MakeAvailable(google-cloud-cpp)

find_package(Protobuf REQUIRED)

add_library(
    bigtable_emulator_common # cmake-format: sort
    cell_view.h
    cluster.cc
    cluster.h
    column_family.cc
    column_family.h
    filter.cc
    filter.h
    filtered_map.h
    bigtable_limits.h
    range_set.cc
    range_set.h
    row_streamer.cc
    row_streamer.h
    server.cc
    server.h
    table.cc
    table.h
    test_util.cc
    test_util.h
    to_grpc_status.cc
    to_grpc_status.h)

target_link_libraries(
    bigtable_emulator_common
    google-cloud-cpp::bigtable
    google-cloud-cpp::bigtable_protos
    google-cloud-cpp::common
    google-cloud-cpp::grpc_utils
    ${PROTOBUF_LIBRARIES})

google_cloud_cpp_add_common_options(bigtable_emulator_common)

include(CreateBazelConfig)
create_bazel_config(bigtable_emulator_common YEAR 2024)

include(CTest)

if (BUILD_TESTING)
    # List the unit tests, then setup the targets and dependencies.
    set(bigtable_emulator_unit_tests
        # cmake-format: sort
        column_family_test.cc
        conditional_mutations_test.cc
        drop_row_range_test.cc
        filter_test.cc
        filtered_map_test.cc
        gc_test.cc
        mutations_test.cc
        range_set_test.cc
        server_test.cc
        table_test.cc)
    export_list_to_bazel("bigtable_emulator_unit_tests.bzl"
                         "bigtable_emulator_unit_tests" YEAR "2024")

    foreach (fname ${bigtable_emulator_unit_tests})
        google_cloud_cpp_add_executable(target "bigtable_emulator" "${fname}")
        target_link_libraries(
            ${target}
            PRIVATE bigtable_emulator_common
                    bigtable_client_testing
                    google_cloud_cpp_testing
                    google_cloud_cpp_testing_grpc
                    google-cloud-cpp::bigtable
                    google-cloud-cpp::bigtable_protos
                    google-cloud-cpp::common
                    google-cloud-cpp::grpc_utils)
        google_cloud_cpp_add_common_options(${target})
        add_test(NAME ${target} COMMAND ${target})
    endforeach ()
endif ()

find_package(absl CONFIG REQUIRED)

set(bigtable_emulator_programs # cmake-format: sort
                               emulator.cc)
export_list_to_bazel("bigtable_emulator_programs.bzl"
                     "bigtable_emulator_programs" YEAR "2024")

foreach (fname ${bigtable_emulator_programs})
    google_cloud_cpp_add_executable(target "bigtable" "${fname}")
    target_link_libraries(
        ${target}
        PRIVATE bigtable_emulator_common
                absl::flags
                absl::flags_parse
                google-cloud-cpp::bigtable
                google-cloud-cpp::bigtable_protos
                google-cloud-cpp::common
                google-cloud-cpp::grpc_utils
                google_cloud_cpp_testing)
    google_cloud_cpp_add_common_options(${target})
endforeach ()
