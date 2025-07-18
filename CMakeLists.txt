cmake_minimum_required(VERSION 3.10)
project(rbus_elements)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED YES)
set(CMAKE_C_FLAGS
    "${CMAKE_C_FLAGS} -Wall -Wno-typedef-redefinition -Wno-unused-value -fno-asynchronous-unwind-tables -ffunction-sections -I ${CMAKE_SOURCE_DIR}"
)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lm -ldl")

if(APPLE)
  set(CMAKE_C_FLAGS
      "${CMAKE_C_FLAGS} -framework IOKit -framework CoreFoundation -Wno-unused-command-line-argument"
  )
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -Og -g3 -ggdb3")
  endif()
endif()

find_library(RBUS_LIBRARY NAMES rbus)
find_library(RBUS_CORE_LIBRARY NAMES rbuscore)
find_path(
  RBUS_INCLUDE_DIR
  NAMES rbus.h
  PATH_SUFFIXES rbus)
find_path(
  RTMSG_INCLUDE_DIR
  NAMES rtMessage.h
  PATH_SUFFIXES rtmessage)
if(NOT RBUS_LIBRARY
   OR NOT RBUS_CORE_LIBRARY
   OR NOT RBUS_INCLUDE_DIR
   OR NOT RTMSG_INCLUDE_DIR)
  if(APPLE)
    message(
      FATAL_ERROR
        "rbus not found, run:\nbrew tap stepherg/tap\nbrew install rbus")
  else()
    message(FATAL_ERROR "rbus not found")
  endif()
endif()

find_library(CJSON_LIBRARY NAMES cjson)
find_path(
  CJSON_INCLUDE_DIR
  NAMES cJSON.h
  PATH_SUFFIXES cjson)
if(NOT CJSON_LIBRARY OR NOT CJSON_INCLUDE_DIR)
  message(FATAL_ERROR "cjson library not found")
endif()

add_executable(rbus_elements ${CMAKE_SOURCE_DIR}/rbus_elements.c)
target_include_directories(
  rbus_elements PRIVATE ${RBUS_INCLUDE_DIR} ${RTMSG_INCLUDE_DIR}
                        ${CJSON_INCLUDE_DIR})
target_link_libraries(
  rbus_elements PRIVATE ${RBUS_LIBRARY} ${RBUS_CORE_LIBRARY} ${CJSON_LIBRARY})
file(COPY ${CMAKE_SOURCE_DIR}/elements.json DESTINATION ${CMAKE_BINARY_DIR})

install(TARGETS rbus_elements DESTINATION bin)
install(FILES ${CMAKE_SOURCE_DIR}/elements.json DESTINATION share/rbus_elements)
