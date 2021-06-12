cmake_minimum_required(VERSION 3.19)
project(palgs)

set(CMAKE_CXX_STANDARD 20)

# Find the packages we need.
#find_package(OpenGL REQUIRED)
#find_package(GLUT REQUIRED)
#find_package(MPI REQUIRED)
#set(INCLUDE_DIRS ${OPENGL_INCLUDE_DIR} ${GLUT_INCLUDE_DIR} ${MPI_INCLUDE_PATH})
#set(LIBRARIES ${OPENGL_LIBRARIES} ${GLUT_LIBRARIES} ${MPI_LIBRARIES})
#include_directories(${INCLUDE_DIRS})
file(GLOB SOURCE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
file(GLOB INCLUDE_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.h)

add_executable(palgs main.cpp ${SOURCE_FILES} ${INCLUDE_FILES})

#add_definitions(-DMY_DEBUG)
#add_definitions(-DMY_NOT_MOVABLE)
#add_definitions(-DMY_TEST)
#add_definitions(-DCOUNT_COPIES)
#add_definitions(-DMY_VECTOR_VERSION)
#profiling
#set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")

#target_link_libraries(palgs ${LIBRARIES})

