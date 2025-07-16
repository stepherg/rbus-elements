#include "rbus_elements.h"

rbusError_t system_reboot_method(rbusHandle_t handle, const char *methodName, rbusObject_t inParams, rbusObject_t outParams, rbusMethodAsyncHandle_t asyncHandle) {
   rbusValue_t delayVal = rbusObject_GetValue(inParams, "delay");
   int32_t delay = delayVal ? rbusValue_GetInt32(delayVal) : 0;

   if (delay < 0) {
      rbusObject_SetValue(outParams, "error", rbusValue_InitString("Invalid delay value"));
      return RBUS_ERROR_INVALID_INPUT;
   }

   rbusValue_t resultVal;
   rbusValue_Init(&resultVal);
   rbusValue_SetString(resultVal, "Reboot scheduled");
   rbusObject_SetValue(outParams, "status", resultVal);
   rbusValue_Release(resultVal);

   // Simulate reboot (in a real system, this would call system("reboot"))
   fprintf(stderr, "System reboot would be initiated after %d seconds\n", delay);

   return RBUS_ERROR_SUCCESS;
}

rbusError_t get_system_info_method(rbusHandle_t handle, const char *methodName, rbusObject_t inParams, rbusObject_t outParams, rbusMethodAsyncHandle_t asyncHandle) {
   rbusValue_t serialVal, timeVal, uptimeVal;
   rbusValue_Init(&serialVal);
   rbusValue_Init(&timeVal);
   rbusValue_Init(&uptimeVal);

   rbusProperty_t serialProp = rbusProperty_Init(NULL, "Device.DeviceInfo.SerialNumber", NULL);
   rbusProperty_t timeProp = rbusProperty_Init(NULL, "Device.DeviceInfo.X_RDKCENTRAL-COM_SystemTime", NULL);
   rbusProperty_t uptimeProp = rbusProperty_Init(NULL, "Device.DeviceInfo.UpTime", NULL);

   rbusGetHandlerOptions_t opts = {.context = NULL, .requestingComponent = NULL};
   get_system_serial_number(handle, serialProp, &opts);
   get_system_time(handle, timeProp, &opts);
   get_system_uptime(handle, uptimeProp, &opts);

   rbusObject_SetValue(outParams, "SerialNumber", rbusProperty_GetValue(serialProp));
   rbusObject_SetValue(outParams, "SystemTime", rbusProperty_GetValue(timeProp));
   rbusObject_SetValue(outParams, "UpTime", rbusProperty_GetValue(uptimeProp));

   rbusProperty_Release(serialProp);
   rbusProperty_Release(timeProp);
   rbusProperty_Release(uptimeProp);
   rbusValue_Release(serialVal);
   rbusValue_Release(timeVal);
   rbusValue_Release(uptimeVal);

   return RBUS_ERROR_SUCCESS;
}

rbusError_t device_x_rdk_xmidt_send_data(rbusHandle_t handle, const char *methodName, rbusObject_t inParams, rbusObject_t outParams, rbusMethodAsyncHandle_t asyncHandle) {
   // Validate required parameters
   rbusValue_t msgTypeVal = rbusObject_GetValue(inParams, "msg_type");
   rbusValue_t sourceVal = rbusObject_GetValue(inParams, "source");
   rbusValue_t destVal = rbusObject_GetValue(inParams, "dest");

   // Set default msg_type to 4 if not provided
   const char *msg_type_str = "4";
   if (msgTypeVal) {
      if (rbusValue_GetType(msgTypeVal) == RBUS_INT32) {
         if (rbusValue_GetInt32(msgTypeVal) != 4) {
            rbusValue_t errorVal;
            rbusValue_Init(&errorVal);
            rbusValue_SetString(errorVal, "msg_type must be integer 4 or string 'event' (Simple Event)");
            rbusObject_SetValue(outParams, "error", errorVal);
            rbusValue_Release(errorVal);
            return RBUS_ERROR_INVALID_INPUT;
         }
      } else if (rbusValue_GetType(msgTypeVal) == RBUS_STRING) {
         if (strcmp(rbusValue_GetString(msgTypeVal, NULL), "event") != 0) {
            rbusValue_t errorVal;
            rbusValue_Init(&errorVal);
            rbusValue_SetString(errorVal, "msg_type must be integer 4 or string 'event' (Simple Event)");
            rbusObject_SetValue(outParams, "error", errorVal);
            rbusValue_Release(errorVal);
            return RBUS_ERROR_INVALID_INPUT;
         }
         msg_type_str = "event";
      } else {
         rbusValue_t errorVal;
         rbusValue_Init(&errorVal);
         rbusValue_SetString(errorVal, "msg_type must be integer 4 or string 'event' (Simple Event)");
         rbusObject_SetValue(outParams, "error", errorVal);
         rbusValue_Release(errorVal);
         return RBUS_ERROR_INVALID_INPUT;
      }
   }

   if (!sourceVal || rbusValue_GetType(sourceVal) != RBUS_STRING || !rbusValue_GetString(sourceVal, NULL)) {
      rbusValue_t errorVal;
      rbusValue_Init(&errorVal);
      rbusValue_SetString(errorVal, "source must be a non-empty string");
      rbusObject_SetValue(outParams, "error", errorVal);
      rbusValue_Release(errorVal);
      return RBUS_ERROR_INVALID_INPUT;
   }

   if (!destVal || rbusValue_GetType(destVal) != RBUS_STRING || !rbusValue_GetString(destVal, NULL)) {
      rbusValue_t errorVal;
      rbusValue_Init(&errorVal);
      rbusValue_SetString(errorVal, "dest must be a non-empty string");
      rbusObject_SetValue(outParams, "error", errorVal);
      rbusValue_Release(errorVal);
      return RBUS_ERROR_INVALID_INPUT;
   }

   // Extract optional parameters
   const char *source = rbusValue_GetString(sourceVal, NULL);
   const char *dest = rbusValue_GetString(destVal, NULL);
   const char *content_type = NULL;
   rbusValue_t contentTypeVal = rbusObject_GetValue(inParams, "content_type");
   if (contentTypeVal && rbusValue_GetType(contentTypeVal) == RBUS_STRING) {
      content_type = rbusValue_GetString(contentTypeVal, NULL);
   }

   rbusValue_t partnerIdsVal = rbusObject_GetValue(inParams, "partner_ids");
   rbusValue_t headersVal = rbusObject_GetValue(inParams, "headers");
   rbusValue_t metadataVal = rbusObject_GetValue(inParams, "metadata");
   rbusValue_t payloadVal = rbusObject_GetValue(inParams, "payload");
   rbusValue_t sessionIdVal = rbusObject_GetValue(inParams, "session_id");
   rbusValue_t transactionUuidVal = rbusObject_GetValue(inParams, "transaction_uuid");
   rbusValue_t qosVal = rbusObject_GetValue(inParams, "qos");
   rbusValue_t rdrVal = rbusObject_GetValue(inParams, "rdr");

   // Log the event (in a real implementation, this would forward to Xmidt)
   fprintf(stderr, "Simple Event Received:\n");
   fprintf(stderr, "  Method: %s\n", methodName);
   fprintf(stderr, "  msg_type: %s\n", msg_type_str);
   fprintf(stderr, "  source: %s\n", source);
   fprintf(stderr, "  dest: %s\n", dest);
   if (content_type) fprintf(stderr, "  content_type: %s\n", content_type);

   if (partnerIdsVal && rbusValue_GetType(partnerIdsVal) == RBUS_OBJECT) {
      rbusObject_t partnerIdsObj = rbusValue_GetObject(partnerIdsVal);
      if (partnerIdsObj) {
         fprintf(stderr, "  partner_ids: [");
         rbusProperty_t prop = rbusObject_GetProperties(partnerIdsObj);
         bool first = true;
         while (prop) {
            rbusValue_t val = rbusProperty_GetValue(prop);
            if (val && rbusValue_GetType(val) == RBUS_STRING) {
               fprintf(stderr, "%s%s", first ? "" : ", ", rbusValue_GetString(val, NULL));
               first = false;
            }
            prop = rbusProperty_GetNext(prop);
         }
         fprintf(stderr, "]\n");
      }
   }

   if (headersVal && rbusValue_GetType(headersVal) == RBUS_OBJECT) {
      rbusObject_t headersObj = rbusValue_GetObject(headersVal);
      if (headersObj) {
         fprintf(stderr, "  headers: [");
         rbusProperty_t prop = rbusObject_GetProperties(headersObj);
         bool first = true;
         while (prop) {
            rbusValue_t val = rbusProperty_GetValue(prop);
            if (val && rbusValue_GetType(val) == RBUS_STRING) {
               fprintf(stderr, "%s%s", first ? "" : ", ", rbusValue_GetString(val, NULL));
               first = false;
            }
            prop = rbusProperty_GetNext(prop);
         }
         fprintf(stderr, "]\n");
      }
   }

   if (metadataVal && rbusValue_GetType(metadataVal) == RBUS_OBJECT) {
      rbusObject_t metadataObj = rbusValue_GetObject(metadataVal);
      if (metadataObj) {
         fprintf(stderr, "  metadata: {");
         rbusProperty_t prop = rbusObject_GetProperties(metadataObj);
         bool first = true;
         while (prop) {
            const char *key = rbusProperty_GetName(prop);
            rbusValue_t val = rbusProperty_GetValue(prop);
            if (key && val && rbusValue_GetType(val) == RBUS_STRING) {
               fprintf(stderr, "%s%s: %s", first ? "" : ", ", key, rbusValue_GetString(val, NULL));
               first = false;
            }
            prop = rbusProperty_GetNext(prop);
         }
         fprintf(stderr, "}\n");
      }
   }

   if (payloadVal && rbusValue_GetType(payloadVal) == RBUS_STRING) {
      fprintf(stderr, "  payload: %s\n", rbusValue_GetString(payloadVal, NULL));
   }
   if (sessionIdVal && rbusValue_GetType(sessionIdVal) == RBUS_STRING) {
      fprintf(stderr, "  session_id: %s\n", rbusValue_GetString(sessionIdVal, NULL));
   }
   if (transactionUuidVal && rbusValue_GetType(transactionUuidVal) == RBUS_STRING) {
      fprintf(stderr, "  transaction_uuid: %s\n", rbusValue_GetString(transactionUuidVal, NULL));
   }
   if (qosVal && rbusValue_GetType(qosVal) == RBUS_INT32) {
      int32_t qos = rbusValue_GetInt32(qosVal);
      if (qos >= 0 && qos <= 99) {
         fprintf(stderr, "  qos: %d\n", qos);
      } else {
         fprintf(stderr, "  qos: %d (invalid, must be 0-99)\n", qos);
      }
   }
   if (rdrVal && rbusValue_GetType(rdrVal) == RBUS_INT32) {
      fprintf(stderr, "  rdr: %d\n", rbusValue_GetInt32(rdrVal));
   }

   // Set response
   rbusValue_t resultVal;
   rbusValue_Init(&resultVal);
   rbusValue_SetString(resultVal, "Event received");
   rbusObject_SetValue(outParams, "status", resultVal);
   rbusValue_Release(resultVal);

   // Publish an RBUS event to simulate Xmidt event forwarding
   rbusEvent_t event = {.name = dest, .type = RBUS_EVENT_GENERAL, .data = inParams};
   rbusError_t rc = rbusEvent_Publish(handle, &event);
   if (rc != RBUS_ERROR_SUCCESS) {
      fprintf(stderr, "Failed to publish event to %s: %d\n", dest, rc);
   }

   return RBUS_ERROR_SUCCESS;
}
