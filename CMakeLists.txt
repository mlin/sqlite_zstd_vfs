cmake_minimum_required(VERSION 3.11...3.13)
if(${CMAKE_VERSION} VERSION_LESS 3.12)
    cmake_policy(VERSION ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION})
endif()

include(FetchContent)

if(${CMAKE_VERSION} VERSION_LESS 3.14)
    macro(FetchContent_MakeAvailable NAME)
        FetchContent_GetProperties(${NAME})
        if(NOT ${NAME}_POPULATED)
            FetchContent_Populate(${NAME})
            add_subdirectory(${${NAME}_SOURCE_DIR} ${${NAME}_BINARY_DIR})
        endif()
    endmacro()
endif()

set(SQLITECPP_INTERNAL_SQLITE OFF CACHE INTERNAL "")
set(SQLITE_ENABLE_COLUMN_METADATA OFF CACHE INTERNAL "")
FetchContent_Declare(
    sqlitecpp
    GIT_REPOSITORY  https://github.com/SRombauts/SQLiteCpp.git
    GIT_TAG         3.1.1
)
FetchContent_MakeAvailable(sqlitecpp)
include_directories(${sqlitecpp_SOURCE_DIR}/include)

FetchContent_Declare(
    sqlite_web_vfs
    GIT_REPOSITORY  https://github.com/mlin/sqlite_web_vfs.git
    GIT_TAG         70de42e
)
FetchContent_MakeAvailable(sqlite_web_vfs)
FetchContent_MakeAvailable(concurrentqueue)
include_directories(${concurrentqueue_SOURCE_DIR})
include_directories(${sqlite_web_vfs_SOURCE_DIR}/src)
add_definitions(-DHTTP_LAZYCURL)

project(sqlite_nested_vfs VERSION 1.0
        DESCRIPTION "SQLite VFS extension storing database pages in...a SQLite database"
        LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(nested_vfs SHARED src/nested_vfs.cc src/SQLiteNestedVFS.h)
SET_TARGET_PROPERTIES(nested_vfs PROPERTIES PREFIX "")
target_link_libraries(nested_vfs PRIVATE SQLiteCpp)

add_library(zstd_vfs SHARED src/zstd_vfs.cc src/SQLiteNestedVFS.h src/zstd_vfs.h)
SET_TARGET_PROPERTIES(zstd_vfs PROPERTIES PREFIX "")
target_link_libraries(zstd_vfs PRIVATE SQLiteCpp zstd)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    FetchContent_Declare(
        catch
        GIT_REPOSITORY  https://github.com/catchorg/Catch2.git
        GIT_TAG         v2.12.1
    )
    FetchContent_MakeAvailable(catch)
    include_directories(${catch_SOURCE_DIR}/single_include)

    add_executable(test_exe test/test.cc test/test_vfstrace.c src/SQLiteNestedVFS.h)
    target_link_libraries(test_exe PRIVATE SQLiteCpp)

    include(CTest)
    enable_testing()
    add_test(NAME test_exe COMMAND ./test_exe -d yes)
    add_test(NAME pytest COMMAND env SQLITE_WEB_LOG=99 python3 -m pytest -sv ${CMAKE_CURRENT_SOURCE_DIR}/test/test.py)
endif()
