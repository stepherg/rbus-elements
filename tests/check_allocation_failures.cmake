set(reached_success FALSE)
foreach(fail_at RANGE 0 100)
   execute_process(
      COMMAND ${CMAKE_COMMAND} -E env RBUS_ELEMENTS_ALLOC_FAIL_AT=${fail_at} "${PROGRAM}" --validate "${MODEL}"
      RESULT_VARIABLE result
      OUTPUT_QUIET
      ERROR_QUIET)
   if(result EQUAL 0)
      set(reached_success TRUE)
      break()
   endif()
   if(NOT result EQUAL 1)
      message(FATAL_ERROR "Allocation failure ${fail_at} terminated with unexpected status ${result}")
   endif()
endforeach()

if(NOT reached_success)
   message(FATAL_ERROR "Model did not succeed after all allocation sites were exhausted")
endif()