#include "test_support.h"

#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>

static rbusError_t conflict_handler(rbusHandle_t handle, const char* method_name,
   rbusObject_t input, rbusObject_t output, rbusMethodAsyncHandle_t async_handle) {
   (void)handle;
   (void)method_name;
   (void)input;
   (void)output;
   (void)async_handle;
   return RBUS_ERROR_SUCCESS;
}

static bool run_failure(const char* program, const char* model, const char* seed,
   const char* readiness, const char* expected) {
   int diagnostics[2];
   if (pipe(diagnostics) != 0) return false;
   pid_t child = fork();
   if (child < 0) return false;
   if (child == 0) {
      close(diagnostics[0]);
      dup2(diagnostics[1], STDERR_FILENO);
      close(diagnostics[1]);
      setenv("RBUS_ELEMENTS_PSM_STATE_PATH", "/tmp/rbus-elements-startup-psm.json", 1);
      setenv("RBUS_ELEMENTS_PSM_SEED_PATH", seed, 1);
      setenv("RBUS_ELEMENTS_READY_PATH", readiness, 1);
      execl(program, program, model, NULL);
      _exit(127);
   }
   close(diagnostics[1]);
   char output[8192] = {0};
   ssize_t used = read(diagnostics[0], output, sizeof(output) - 1);
   close(diagnostics[0]);
   int status;
   bool waited = waitpid(child, &status, 0) == child;
   return used >= 0 && waited && WIFEXITED(status) && WEXITSTATUS(status) != 0 &&
      strstr(output, expected) != NULL;
}

int main(int argc, char** argv) {
   TEST_ASSERT(argc == 4);
   TestRbusFixture fixture;
   TEST_ASSERT(test_rbus_open(&fixture, "method-conflict"));
   rbusDataElement_t method = {.name = "Device.Reboot()", .type = RBUS_ELEMENT_TYPE_METHOD, .cbTable = {0}};
   rbusMethodHandler_t handler = conflict_handler;
   TEST_ASSERT(sizeof(method.cbTable.methodHandler) == sizeof(handler));
   memcpy(&method.cbTable.methodHandler, &handler, sizeof(handler));
   TEST_ASSERT(rbus_regDataElements(fixture.handle, 1, &method) == RBUS_ERROR_SUCCESS);

   int diagnostics[2];
   TEST_ASSERT(pipe(diagnostics) == 0);
   pid_t child = fork();
   TEST_ASSERT(child >= 0);
   if (child == 0) {
      close(diagnostics[0]);
      dup2(diagnostics[1], STDERR_FILENO);
      close(diagnostics[1]);
      setenv("RBUS_ELEMENTS_PSM_STATE_PATH", "/tmp/rbus-elements-conflict-psm.json", 1);
      setenv("RBUS_ELEMENTS_PSM_SEED_PATH", argv[3], 1);
      setenv("RBUS_ELEMENTS_READY_PATH", "/tmp/rbus-elements-conflict-ready", 1);
      execl(argv[1], argv[1], argv[2], NULL);
      _exit(127);
   }
   close(diagnostics[1]);
   char output[4096] = {0};
   ssize_t used = read(diagnostics[0], output, sizeof(output) - 1);
   close(diagnostics[0]);
   TEST_ASSERT(used >= 0);
   int status;
   TEST_ASSERT(waitpid(child, &status, 0) == child);
   TEST_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) != 0);
   TEST_ASSERT(strstr(output, "Device.Reboot()") != NULL);
   TEST_ASSERT(access("/tmp/rbus-elements-conflict-ready", F_OK) != 0);
   unlink("/tmp/rbus-elements-conflict-psm.json");

   TEST_ASSERT(rbus_unregDataElements(fixture.handle, 1, &method) == RBUS_ERROR_SUCCESS);
   TEST_ASSERT(test_rbus_close(&fixture));

   TEST_ASSERT(run_failure(argv[1], argv[2], "/tmp/rbus-elements-missing-seed.json",
      "/tmp/rbus-elements-psm-failure-ready", "Failed to initialize PSM"));
   TEST_ASSERT(access("/tmp/rbus-elements-psm-failure-ready", F_OK) != 0);
   TEST_ASSERT(run_failure(argv[1], argv[2], argv[3], "/tmp", "Failed to publish readiness marker"));
   unlink("/tmp/rbus-elements-startup-psm.json");
   return 0;
}