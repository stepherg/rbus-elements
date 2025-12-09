cmake_minimum_required(VERSION 3.10)
project(rbus_elements)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED YES)
set(CMAKE_C_FLAGS
   "${CMAKE_C_FLAGS} -Wall -Wextra -Wpedantic -Wno-unused-value -fno-asynchronous-unwind-tables -Wno-format-truncation -ffunction-sections -I ${CMAKE_SOURCE_DIR}"
)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lm -ldl")

if(APPLE)
   set(CMAKE_C_FLAGS
      "${CMAKE_C_FLAGS} -framework IOKit -framework CoreFoundation -Wno-unused-command-line-argument -Wno-typedef-redefinition"
   )

   if(CMAKE_BUILD_TYPE STREQUAL "Debug")
      set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -Og -g3 -ggdb3")
   endif()
endif()

find_library(JANSSON_LIBRARY NAMES jansson)
find_path(JANSSON_INCLUDE_DIR NAMES jansson.h)

if(NOT JANSSON_LIBRARY OR NOT JANSSON_INCLUDE_DIR)
   message(FATAL_ERROR "jansson library not found")
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

add_executable(rbus_elements
   ${CMAKE_SOURCE_DIR}/rbus_elements.c
   ${CMAKE_SOURCE_DIR}/device_info.c
   ${CMAKE_SOURCE_DIR}/methods.c
   ${CMAKE_SOURCE_DIR}/handlers.c
)

target_include_directories(
   rbus_elements PRIVATE ${RBUS_INCLUDE_DIR} ${RTMSG_INCLUDE_DIR}
   ${CJSON_INCLUDE_DIR} ${JANSSON_INCLUDE_DIR})
target_link_libraries(
   rbus_elements PRIVATE ${RBUS_LIBRARY} ${RBUS_CORE_LIBRARY} ${CJSON_LIBRARY} ${JANSSON_LIBRARY})
file(COPY ${CMAKE_SOURCE_DIR}/elements.json DESTINATION ${CMAKE_BINARY_DIR})

install(TARGETS rbus_elements DESTINATION bin COMPONENT runtime)
install(FILES ${CMAKE_SOURCE_DIR}/elements.json DESTINATION share/rbus_elements COMPONENT runtime)
install(FILES ${CMAKE_SOURCE_DIR}/conf/rbus-elements.service DESTINATION /lib/systemd/system/ COMPONENT runtime)

# CPack configuration for Debian packages
set(CPACK_GENERATOR "DEB")
set(CPACK_PACKAGE_NAME "rbus-elements")
set(CPACK_PACKAGE_VERSION "1.0.0")
set(CPACK_PACKAGE_CONTACT "RBus Team")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "RBus Elements Provider")
set(CPACK_PACKAGE_DESCRIPTION "RBus elements provider for device information")

# Architecture mapping for Debian packages
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*|arm64.*|ARM64.*)")
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "arm64")
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64.*|X86_64.*|AMD64.*|amd64.*)")
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
else()
    set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE ${CMAKE_SYSTEM_PROCESSOR})
endif()

# Enable component-based packaging
set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_COMPONENTS_ALL runtime)

# Strip binaries in runtime package
set(CPACK_STRIP_FILES TRUE)

# Runtime package configuration
set(CPACK_DEBIAN_RUNTIME_PACKAGE_NAME "rbus-elements")
set(CPACK_DEBIAN_RUNTIME_FILE_NAME "rbus-elements_${CPACK_PACKAGE_VERSION}_${CPACK_DEBIAN_PACKAGE_ARCHITECTURE}.deb")
set(CPACK_COMPONENT_RUNTIME_DESCRIPTION "RBus elements provider for device information")
set(CPACK_DEBIAN_RUNTIME_PACKAGE_DEPENDS "rbus (>= 2.7.0), libcjson1, libjansson4, libmsgpackc2")
set(CPACK_DEBIAN_RUNTIME_PACKAGE_SECTION "net")
set(CPACK_DEBIAN_RUNTIME_PACKAGE_PRIORITY "optional")
set(CPACK_DEBIAN_RUNTIME_PACKAGE_MAINTAINER "RBus Team")
set(CPACK_DEBIAN_RUNTIME_PACKAGE_SHLIBDEPS ON)

# Generate postinst script for ldconfig and service management
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/postinst" "#!/bin/sh
set -e
if [ \"$1\" = \"configure\" ]; then
    ldconfig
    if [ -d /run/systemd/system ]; then
        systemctl daemon-reload >/dev/null 2>&1 || true
        systemctl enable rbus-elements.service >/dev/null 2>&1 || true
        systemctl start rbus-elements.service >/dev/null 2>&1 || true
    fi
fi
")

# Generate prerm script to stop service on removal/upgrade
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/prerm" "#!/bin/sh
set -e
if [ \"$1\" = \"remove\" ] || [ \"$1\" = \"upgrade\" ]; then
    if [ -d /run/systemd/system ]; then
        systemctl stop rbus-elements.service >/dev/null 2>&1 || true
    fi
fi
")

# Generate postrm script to disable service on purge
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/postrm" "#!/bin/sh
set -e
if [ \"$1\" = \"purge\" ] || [ \"$1\" = \"remove\" ]; then
    if [ -d /run/systemd/system ]; then
        systemctl disable rbus-elements.service >/dev/null 2>&1 || true
        systemctl daemon-reload >/dev/null 2>&1 || true
    fi
fi
")

set(CPACK_DEBIAN_RUNTIME_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_BINARY_DIR}/postinst;${CMAKE_CURRENT_BINARY_DIR}/prerm;${CMAKE_CURRENT_BINARY_DIR}/postrm")

include(CPack)
