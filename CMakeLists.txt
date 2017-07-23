cmake_minimum_required(VERSION 3.7)
project(Database)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
    Database.cpp
    Database.h
    main.cpp)
add_executable(Database ${SOURCE_FILES})