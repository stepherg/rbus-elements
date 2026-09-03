#include "test_support.h"

#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

#define RFC_NAME "eRT.com.cisco.spvtg.ccsp.webpa.WebConfigRfcEnable"

static bool wait_for_readiness(pid_t child, const char* readiness) {
   for (int attempt = 0; attempt < 300; attempt++) {
      if (access(readiness, F_OK) == 0) return true;
      int status;
      if (waitpid(child, &status, WNOHANG) == child) return false;
      usleep(50000);
   }
   return false;
}

static pid_t start_provider(const char* program, const char* model, const char* seed,
   const char* state, const char* readiness) {
   unlink(readiness);
   pid_t child = fork();
   if (child == 0) {
      setenv("RBUS_ELEMENTS_READY_PATH", readiness, 1);
      setenv("RBUS_ELEMENTS_PSM_STATE_PATH", state, 1);
      setenv("RBUS_ELEMENTS_PSM_SEED_PATH", seed, 1);
      execl(program, program, model, NULL);
      _exit(127);
   }
   return child;
}

static bool stop_provider(pid_t child) {
   if (kill(child, SIGTERM) != 0) return false;
   int status;
   return waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

static bool invoke_psm_get(rbusHandle_t handle, rbusValue_t* result) {
   rbusObject_t input;
   rbusObject_Init(&input, NULL);
   rbusValue_t requested;
   rbusValue_Init(&requested);
   rbusValue_SetString(requested, "");
   rbusObject_SetValue(input, RFC_NAME, requested);
   rbusValue_Release(requested);
   rbusObject_t output = NULL;
   rbusError_t error = rbusMethod_Invoke(handle, "GetPSMRecordValue()", input, &output);
   if (error == RBUS_ERROR_SUCCESS) {
      rbusValue_t returned = rbusObject_GetValue(output, RFC_NAME);
      if (returned) {
         rbusValue_Init(result);
         rbusValue_Copy(*result, returned);
      } else {
         error = RBUS_ERROR_BUS_ERROR;
      }
   }
   rbusObject_Release(output);
   rbusObject_Release(input);
   return error == RBUS_ERROR_SUCCESS;
}

static bool invoke_psm_set_false(rbusHandle_t handle) {
   rbusObject_t input;
   rbusObject_Init(&input, NULL);
   rbusValue_t value;
   rbusValue_Init(&value);
   rbusValue_SetBoolean(value, false);
   rbusObject_SetValue(input, RFC_NAME, value);
   rbusValue_Release(value);
   rbusObject_t output = NULL;
   rbusError_t error = rbusMethod_Invoke(handle, "SetPSMRecordValue()", input, &output);
   rbusObject_Release(output);
   rbusObject_Release(input);
   return error == RBUS_ERROR_SUCCESS;
}

int main(int argc, char** argv) {
   TEST_ASSERT(argc == 5);
   const char* readiness = "/tmp/rbus-elements-integration-ready";
   const char* state = "/tmp/rbus-elements-integration-psm.json";
   unlink(readiness);
   unlink(state);

   pid_t child = start_provider(argv[1], argv[2], argv[3], state, readiness);
   TEST_ASSERT(child >= 0);
   TEST_ASSERT(wait_for_readiness(child, readiness));

   TestRbusFixture fixture;
   TEST_ASSERT(test_rbus_open(&fixture, "packaged"));
   rbusValue_t value = NULL;
   TEST_ASSERT(rbus_get(fixture.handle, "Device.Bridging.Bridge.1.Port.1.PVID", &value) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusValue_GetType(value) == RBUS_INT32 && rbusValue_GetInt32(value) == 100);
   rbusValue_Release(value);
   value = NULL;
   TEST_ASSERT(rbus_get(fixture.handle, "Device.Bridging.Bridge.1.Enable", &value) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusValue_GetType(value) == RBUS_BOOLEAN && rbusValue_GetBoolean(value));
   rbusValue_Release(value);
   value = NULL;
   TEST_ASSERT(rbus_get(fixture.handle, "Device.Bridging.Bridge.1.PortNumberOfEntries", &value) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusValue_GetType(value) == RBUS_UINT32 && rbusValue_GetUInt32(value) == 13);
   rbusValue_Release(value);

   rbusObject_t method_input;
   rbusObject_Init(&method_input, NULL);
   rbusObject_t method_output = NULL;
   TEST_ASSERT(rbusMethod_Invoke(fixture.handle, "Device.GetSystemInfo()", method_input, &method_output) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusObject_GetValue(method_output, "SerialNumber") != NULL);
   rbusObject_Release(method_output);
   rbusObject_Release(method_input);

   value = NULL;
   TEST_ASSERT(invoke_psm_get(fixture.handle, &value));
   TEST_ASSERT(rbusValue_GetType(value) == RBUS_STRING && strcmp(rbusValue_GetString(value, NULL), "true") == 0);
   rbusValue_Release(value);
   value = NULL;
   TEST_ASSERT(rbus_get(fixture.handle, "Device.X_RDK_WebConfig.RfcEnable", &value) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusValue_GetType(value) == RBUS_BOOLEAN && rbusValue_GetBoolean(value));
   rbusValue_Release(value);
   TEST_ASSERT(invoke_psm_set_false(fixture.handle));
   TEST_ASSERT(test_rbus_close(&fixture));

   TEST_ASSERT(stop_provider(child));
   child = start_provider(argv[1], argv[2], argv[3], state, readiness);
   TEST_ASSERT(child >= 0 && wait_for_readiness(child, readiness));
   TEST_ASSERT(test_rbus_open(&fixture, "persistence"));
   value = NULL;
   TEST_ASSERT(invoke_psm_get(fixture.handle, &value));
   TEST_ASSERT(rbusValue_GetType(value) == RBUS_BOOLEAN && !rbusValue_GetBoolean(value));
   rbusValue_Release(value);
   TEST_ASSERT(test_rbus_close(&fixture));
   TEST_ASSERT(stop_provider(child));

   unlink(state);
   child = start_provider(argv[1], argv[4], argv[3], state, readiness);
   TEST_ASSERT(child >= 0 && wait_for_readiness(child, readiness));
   TEST_ASSERT(test_rbus_open(&fixture, "sparse"));
   value = NULL;
   TEST_ASSERT(rbus_get(fixture.handle, "Device.Test.TableNumberOfEntries", &value) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusValue_GetType(value) == RBUS_UINT32 && rbusValue_GetUInt32(value) == 2);
   rbusValue_Release(value);
   value = NULL;
   TEST_ASSERT(rbus_get(fixture.handle, "Device.Test.Table.1.Value", &value) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusValue_GetInt32(value) == 1);
   rbusValue_Release(value);
   value = NULL;
   TEST_ASSERT(rbus_get(fixture.handle, "Device.Test.Table.10.Value", &value) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(rbusValue_GetInt32(value) == 10);
   rbusValue_Release(value);
   value = NULL;
   TEST_ASSERT(rbus_get(fixture.handle, "Device.Test.Table.2.Value", &value) != RBUS_ERROR_SUCCESS);
   TEST_ASSERT(test_rbus_close(&fixture));
   TEST_ASSERT(stop_provider(child));
   unlink(readiness);
   unlink(state);
   return 0;
}