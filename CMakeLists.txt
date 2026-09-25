cmake_minimum_required(VERSION 3.10)
project(rbus_elements)

include(CTest)

set(RBUS_ELEMENTS_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")

option(RBUS_ELEMENTS_ASAN_UBSAN "Enable AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(RBUS_ELEMENTS_TSAN "Enable ThreadSanitizer" OFF)

# Installs to absolute paths (/var/lib, /lib/systemd) only make sense for a
# staged distro package install (DESTDIR); disable for prefix installs.
if(APPLE)
   set(RBUS_ELEMENTS_INSTALL_SYSTEM_FILES_DEFAULT OFF)
else()
   set(RBUS_ELEMENTS_INSTALL_SYSTEM_FILES_DEFAULT ON)
endif()
option(RBUS_ELEMENTS_INSTALL_SYSTEM_FILES
   "Install the systemd unit and /var/lib state directory"
   ${RBUS_ELEMENTS_INSTALL_SYSTEM_FILES_DEFAULT})

# Compiled-in fallbacks, used when the RBUS_ELEMENTS_PSM_*_PATH env vars are unset.
set(RBUS_ELEMENTS_PSM_SEED_PATH "${CMAKE_INSTALL_PREFIX}/share/rbus_elements/psm-seed.json"
   CACHE STRING "Default PSM seed file location")
set(RBUS_ELEMENTS_PSM_STATE_PATH "/var/lib/rbus-elements/psm.json"
   CACHE STRING "Default PSM state file location")

if(RBUS_ELEMENTS_ASAN_UBSAN AND RBUS_ELEMENTS_TSAN)
   message(FATAL_ERROR "ASan/UBSan and TSan cannot be enabled in the same build")
endif()

if(RBUS_ELEMENTS_ASAN_UBSAN)
   set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -fsanitize=address,undefined")
   set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address,undefined")
elseif(RBUS_ELEMENTS_TSAN)
   set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-omit-frame-pointer -fsanitize=thread")
   set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
endif()

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED YES)
set(CMAKE_C_FLAGS
   "${CMAKE_C_FLAGS} -Wall -Wextra -Wpedantic -Wno-unused-value -fno-asynchronous-unwind-tables -Wno-format-truncation -ffunction-sections"
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
find_package(Threads REQUIRED)

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

# Tests include <rbus/rbus.h>, so they need the parent of the rbus header dir.
get_filename_component(RBUS_INCLUDE_PARENT_DIR "${RBUS_INCLUDE_DIR}" DIRECTORY)

find_library(CJSON_LIBRARY NAMES cjson)
find_path(
   CJSON_INCLUDE_DIR
   NAMES cJSON.h
   PATH_SUFFIXES cjson)

if(NOT CJSON_LIBRARY OR NOT CJSON_INCLUDE_DIR)
   message(FATAL_ERROR "cjson library not found")
endif()

add_executable(rbus_elements
   ${RBUS_ELEMENTS_SOURCE_DIR}/rbus_elements.c
   ${RBUS_ELEMENTS_SOURCE_DIR}/model_alloc.c
   ${RBUS_ELEMENTS_SOURCE_DIR}/model_value.c
   ${RBUS_ELEMENTS_SOURCE_DIR}/provider_state.c
   ${RBUS_ELEMENTS_SOURCE_DIR}/psm_store.c
   ${RBUS_ELEMENTS_SOURCE_DIR}/device_info.c
   ${RBUS_ELEMENTS_SOURCE_DIR}/methods.c
   ${RBUS_ELEMENTS_SOURCE_DIR}/handlers.c
)

target_include_directories(
   rbus_elements PRIVATE ${RBUS_ELEMENTS_SOURCE_DIR} ${RBUS_INCLUDE_DIR} ${RTMSG_INCLUDE_DIR}
   ${CJSON_INCLUDE_DIR} ${JANSSON_INCLUDE_DIR})
target_compile_definitions(
   rbus_elements PRIVATE
   RBUS_ELEMENTS_PSM_SEED_PATH="${RBUS_ELEMENTS_PSM_SEED_PATH}"
   RBUS_ELEMENTS_PSM_STATE_PATH="${RBUS_ELEMENTS_PSM_STATE_PATH}")
target_link_libraries(
   rbus_elements PRIVATE ${RBUS_LIBRARY} ${RBUS_CORE_LIBRARY} ${CJSON_LIBRARY} ${JANSSON_LIBRARY} Threads::Threads m)
file(COPY ${CMAKE_SOURCE_DIR}/elements.json DESTINATION ${CMAKE_BINARY_DIR})

if(BUILD_TESTING)
   add_executable(rbus_elements_tests
      ${CMAKE_SOURCE_DIR}/tests/test_smoke.c
      ${CMAKE_SOURCE_DIR}/tests/test_support.c
      ${CMAKE_SOURCE_DIR}/tests/test_model_value.c
      ${CMAKE_SOURCE_DIR}/tests/test_provider_state.c
      ${CMAKE_SOURCE_DIR}/tests/test_psm_store.c
      ${RBUS_ELEMENTS_SOURCE_DIR}/model_alloc.c
      ${RBUS_ELEMENTS_SOURCE_DIR}/model_value.c
      ${RBUS_ELEMENTS_SOURCE_DIR}/provider_state.c
      ${RBUS_ELEMENTS_SOURCE_DIR}/psm_store.c
   )
   target_include_directories(rbus_elements_tests PRIVATE ${RBUS_ELEMENTS_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/tests ${RBUS_INCLUDE_DIR} ${RBUS_INCLUDE_PARENT_DIR} ${RTMSG_INCLUDE_DIR} ${CJSON_INCLUDE_DIR})
   target_link_libraries(rbus_elements_tests PRIVATE ${RBUS_LIBRARY} ${RBUS_CORE_LIBRARY} ${CJSON_LIBRARY} Threads::Threads m)
   target_compile_definitions(rbus_elements_tests PRIVATE RBUS_ELEMENTS_TEST_SEED="${CMAKE_SOURCE_DIR}/psm-seed.json")
   if(RBUS_ELEMENTS_TSAN)
      target_compile_definitions(rbus_elements_tests PRIVATE RBUS_ELEMENTS_TSAN_BUILD=1)
   endif()
   add_test(NAME rbus-elements-unit COMMAND rbus_elements_tests)

   if(NOT RBUS_ELEMENTS_TSAN)
      add_executable(rbus_elements_startup_tests
         ${CMAKE_SOURCE_DIR}/tests/test_startup.c
         ${CMAKE_SOURCE_DIR}/tests/test_support.c)
      target_include_directories(rbus_elements_startup_tests PRIVATE ${RBUS_ELEMENTS_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/tests ${RBUS_INCLUDE_DIR} ${RBUS_INCLUDE_PARENT_DIR} ${RTMSG_INCLUDE_DIR})
      target_link_libraries(rbus_elements_startup_tests PRIVATE ${RBUS_LIBRARY} ${RBUS_CORE_LIBRARY} Threads::Threads)
      add_test(NAME rbus-elements-method-conflict
         COMMAND rbus_elements_startup_tests $<TARGET_FILE:rbus_elements> ${CMAKE_SOURCE_DIR}/tests/fixtures/valid-startup.json ${CMAKE_SOURCE_DIR}/psm-seed.json)
      add_executable(rbus_elements_integration_tests
         ${CMAKE_SOURCE_DIR}/tests/test_integration.c
         ${CMAKE_SOURCE_DIR}/tests/test_support.c)
      target_include_directories(rbus_elements_integration_tests PRIVATE ${CMAKE_SOURCE_DIR}/tests ${RBUS_INCLUDE_DIR} ${RBUS_INCLUDE_PARENT_DIR} ${RTMSG_INCLUDE_DIR})
      target_link_libraries(rbus_elements_integration_tests PRIVATE ${RBUS_LIBRARY} ${RBUS_CORE_LIBRARY} Threads::Threads)
      add_test(NAME rbus-elements-packaged-rbus
         COMMAND rbus_elements_integration_tests $<TARGET_FILE:rbus_elements> ${CMAKE_SOURCE_DIR}/elements.json ${CMAKE_SOURCE_DIR}/psm-seed.json ${CMAKE_SOURCE_DIR}/tests/fixtures/valid-sparse.json)
      set_tests_properties(rbus-elements-packaged-rbus PROPERTIES TIMEOUT 30)
   endif()
   add_test(NAME rbus-elements-model-packaged COMMAND rbus_elements --validate ${CMAKE_SOURCE_DIR}/elements.json)

   function(add_invalid_model_test test_name fixture pattern)
      add_test(NAME ${test_name}
         COMMAND ${CMAKE_COMMAND}
            -DPROGRAM=$<TARGET_FILE:rbus_elements>
            -DMODEL=${CMAKE_SOURCE_DIR}/tests/fixtures/${fixture}
            -DPATTERN=${pattern}
            -P ${CMAKE_SOURCE_DIR}/tests/check_invalid_model.cmake)
   endfunction()

   add_invalid_model_test(rbus-elements-model-missing-value invalid-missing-value.json "item 0.*Device.Test.Value")
   add_invalid_model_test(rbus-elements-model-fractional invalid-fractional.json "item 0.*Device.Test.Fractional")
   add_invalid_model_test(rbus-elements-model-range invalid-range.json "item 0.*Device.Test.Range")
   add_invalid_model_test(rbus-elements-model-empty-name invalid-empty-name.json "item 0")
   add_invalid_model_test(rbus-elements-model-overlong-name invalid-overlong-name.json "item 0")
   add_invalid_model_test(rbus-elements-model-duplicate invalid-duplicate.json "item 1.*Device.Test.Duplicate")
   add_invalid_model_test(rbus-elements-model-synthesized-conflict invalid-synthesized-conflict.json "item 1.*Device.Test.\\{i\\}.Value")
   add_invalid_model_test(rbus-elements-model-unsupported-method invalid-unsupported-method.json "item 0.*Device.Test.Unsupported")
   add_test(NAME rbus-elements-model-allocation-failures
      COMMAND ${CMAKE_COMMAND}
         -DPROGRAM=$<TARGET_FILE:rbus_elements>
         -DMODEL=${CMAKE_SOURCE_DIR}/tests/fixtures/valid-allocation.json
         -P ${CMAKE_SOURCE_DIR}/tests/check_allocation_failures.cmake)
endif()

install(TARGETS rbus_elements DESTINATION bin COMPONENT runtime)
install(FILES ${CMAKE_SOURCE_DIR}/elements.json DESTINATION share/rbus_elements COMPONENT runtime)
install(FILES ${CMAKE_SOURCE_DIR}/psm-seed.json DESTINATION share/rbus_elements COMPONENT runtime)
if(RBUS_ELEMENTS_INSTALL_SYSTEM_FILES)
   install(DIRECTORY DESTINATION /var/lib/rbus-elements COMPONENT runtime
      DIRECTORY_PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)
   install(FILES ${CMAKE_SOURCE_DIR}/conf/rbus-elements.service DESTINATION /lib/systemd/system/ COMPONENT runtime)
endif()

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
   install -d -m 0700 -o root -g root /var/lib/rbus-elements
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
