execute_process(
   COMMAND "${PROGRAM}" --validate "${MODEL}"
   RESULT_VARIABLE result
   OUTPUT_VARIABLE output
   ERROR_VARIABLE error)

if(result EQUAL 0)
   message(FATAL_ERROR "Invalid model unexpectedly passed: ${MODEL}")
endif()

set(diagnostic "${output}${error}")
if(NOT diagnostic MATCHES "${PATTERN}")
   message(FATAL_ERROR "Diagnostic did not match '${PATTERN}':\n${diagnostic}")
endif()