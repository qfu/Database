cmake_minimum_required(VERSION 3.7)
project(Database)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

set(SOURCE_FILES
    database.cpp
    database.h
    main.cpp)
add_executable(Database ${SOURCE_FILES})