/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include <string>

#define ROOT_NAME "ThinClientRegex3"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

CacheHelper* cacheHelper = NULL;
bool isLocalServer = false;

static bool isLocator = false;
static int numberOfLocators = 0;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);
#include "LocatorHelper.hpp"
void initClient(const bool isthinClient) {
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}
void cleanProc() {
  if (cacheHelper != NULL) {
    delete cacheHelper;
    cacheHelper = NULL;
  }
}

CacheHelper* getHelper() {
  ASSERT(cacheHelper != NULL, "No cacheHelper initialized.");
  return cacheHelper;
}

void _verifyEntry(const char* name, const char* key, const char* val,
                  bool noKey, bool isCreated = false) {
  // Verify key and value exist in this region, in this process.
  const char* value = (val == 0) ? "" : val;
  char* buf =
      reinterpret_cast<char*>(malloc(1024 + strlen(key) + strlen(value)));
  ASSERT(buf, "Unable to malloc buffer for logging.");
  if (!isCreated) {
    if (noKey) {
      sprintf(buf, "Verify key %s does not exist in region %s", key, name);
    } else if (val == 0) {
      sprintf(buf, "Verify value for key %s does not exist in region %s", key,
              name);
    } else {
      sprintf(buf, "Verify value for key %s is: %s in region %s", key, value,
              name);
    }
    LOG(buf);
  }
  free(buf);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  CacheableKeyPtr keyPtr = createKey(key);

  // if the region is no ack, then we may need to wait...
  if (!isCreated) {
    if (noKey == false) {  // need to find the key!
      ASSERT(regPtr->containsKey(keyPtr), "Key not found in region.");
    }
    if (val != NULL) {  // need to have a value!
      ASSERT(regPtr->containsValueForKey(keyPtr), "Value not found in region.");
    }
  }

  // loop up to MAX times, testing condition
  uint32_t MAX = 100;
  //  changed sleep from 10 ms
  uint32_t SLEEP = 1000;  // milliseconds
  uint32_t containsKeyCnt = 0;
  uint32_t containsValueCnt = 0;
  uint32_t testValueCnt = 0;

  for (int i = MAX; i >= 0; i--) {
    if (isCreated) {
      if (!regPtr->containsKey(keyPtr)) {
        containsKeyCnt++;
      } else {
        break;
      }
      ASSERT(containsKeyCnt < MAX, "Key has not been created in region.");
    } else {
      if (noKey) {
        if (regPtr->containsKey(keyPtr)) {
          containsKeyCnt++;
        } else {
          break;
        }
        ASSERT(containsKeyCnt < MAX, "Key found in region.");
      }
      if (val == NULL) {
        if (regPtr->containsValueForKey(keyPtr)) {
          containsValueCnt++;
        } else {
          break;
        }
        ASSERT(containsValueCnt < MAX, "Value found in region.");
      }

      if (val != NULL) {
        CacheableStringPtr checkPtr =
            dynCast<CacheableStringPtr>(regPtr->get(keyPtr));

        ASSERT(checkPtr != NULLPTR, "Value Ptr should not be null.");
        char buf[1024];
        sprintf(buf, "In verify loop, get returned %s for key %s",
                checkPtr->asChar(), key);
        LOG(buf);
        if (strcmp(checkPtr->asChar(), value) != 0) {
          testValueCnt++;
        } else {
          break;
        }
        ASSERT(testValueCnt < MAX, "Incorrect value found.");
      }
    }
    dunit::sleep(SLEEP);
  }
}

#define verifyInvalid(x, y) _verifyInvalid(x, y, __LINE__)

void _verifyInvalid(const char* name, const char* key, int line) {
  char logmsg[1024];
  sprintf(logmsg, "verifyInvalid() called from %d.\n", line);
  LOG(logmsg);
  _verifyEntry(name, key, 0, false);
  LOG("Entry invalidated.");
}

#define verifyDestroyed(x, y) _verifyDestroyed(x, y, __LINE__)

void _verifyDestroyed(const char* name, const char* key, int line) {
  char logmsg[1024];
  sprintf(logmsg, "verifyDestroyed() called from %d.\n", line);
  LOG(logmsg);
  _verifyEntry(name, key, 0, true);
  LOG("Entry destroyed.");
}

#define verifyEntry(x, y, z) _verifyEntry(x, y, z, __LINE__)

void _verifyEntry(const char* name, const char* key, const char* val,
                  int line) {
  char logmsg[1024];
  sprintf(logmsg, "verifyEntry() called from %d.\n", line);
  LOG(logmsg);
  _verifyEntry(name, key, val, false);
  LOG("Entry verified.");
}

#define verifyCreated(x, y) _verifyCreated(x, y, __LINE__)

void _verifyCreated(const char* name, const char* key, int line) {
  char logmsg[1024];
  sprintf(logmsg, "verifyCreated() called from %d.\n", line);
  LOG(logmsg);
  _verifyEntry(name, key, NULL, false, true);
  LOG("Entry created.");
}

void createRegion(const char* name, bool ackMode, const char* endpoints,
                  bool clientNotificationEnabled = false) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr = getHelper()->createRegion(
      name, ackMode, true, NULLPTR, endpoints, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}
void createPooledRegion(const char* name, bool ackMode, const char* locators,
                        const char* poolname,
                        bool clientNotificationEnabled = false,
                        bool cachingEnable = true) {
  LOG("createRegion_Pool() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr =
      getHelper()->createPooledRegion(name, ackMode, locators, poolname,
                                      cachingEnable, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Pooled Region created.");
}
void createEntry(const char* name, const char* key, const char* value = NULL) {
  LOG("createEntry() entered.");
  fprintf(stdout, "Creating entry -- key: %s  value: %s in region %s\n", key,
          value, name);
  fflush(stdout);
  // Create entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  if (value == NULL) {
    value = "";
  }
  CacheableStringPtr valPtr = CacheableString::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT(!regPtr->containsKey(keyPtr),
         "Key should not have been found in region.");
  ASSERT(!regPtr->containsValueForKey(keyPtr),
         "Value should not have been found in region.");

  regPtr->create(keyPtr, valPtr);
  // regPtr->put( keyPtr, valPtr );
  LOG("Created entry.");

  verifyEntry(name, key, value);
  LOG("Entry created.");
}

void updateEntry(const char* name, const char* key, const char* value) {
  LOG("updateEntry() entered.");
  fprintf(stdout, "Updating entry -- key: %s  value: %s in region %s\n", key,
          value, name);
  fflush(stdout);
  // Update entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableStringPtr valPtr = CacheableString::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT(regPtr->containsKey(keyPtr), "Key should have been found in region.");
  ASSERT(regPtr->containsValueForKey(keyPtr),
         "Value should have been found in region.");

  regPtr->put(keyPtr, valPtr);
  LOG("Put entry.");

  verifyEntry(name, key, value);
  LOG("Entry updated.");
}

void doNetsearch(const char* name, const char* key, const char* value) {
  LOG("doNetsearch() entered.");
  fprintf(
      stdout,
      "Netsearching for entry -- key: %s  expecting value: %s in region %s\n",
      key, value, name);
  fflush(stdout);
  // Get entry created in Process A, verify entry is correct
  CacheableKeyPtr keyPtr = CacheableKey::create(key);

  RegionPtr regPtr = getHelper()->getRegion(name);
  fprintf(stdout, "netsearch  region %s\n", regPtr->getName());
  fflush(stdout);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT(!regPtr->containsKey(keyPtr),
         "Key should not have been found in region.");
  ASSERT(!regPtr->containsValueForKey(keyPtr),
         "Value should not have been found in region.");

  CacheableStringPtr checkPtr =
      dynCast<CacheableStringPtr>(regPtr->get(keyPtr));  // force a netsearch

  if (checkPtr != NULLPTR) {
    LOG("checkPtr is not null");
    char buf[1024];
    sprintf(buf, "In net search, get returned %s for key %s",
            checkPtr->asChar(), key);
    LOG(buf);
  } else {
    LOG("checkPtr is NULL");
  }
  verifyEntry(name, key, value);
  LOG("Netsearch complete.");
}

// const char * testregex[] = { "Key-*1", "Key-*2", "Key-*3", "Key-*4" };
const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};
const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck"};
const char* vals[] = {"Value-1", "Value-2", "Value-3", "Value-4"};
const char* nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                       "New Value-4"};

const bool USE_ACK = true;
const bool NO_ACK = false;

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1Regions)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_",
                       true);
    createPooledRegion(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_", true);
    LOG("CreateClient1Regions complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2Regions)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_",
                       true);
    createPooledRegion(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_", true);
    LOG("CreateClient2Regions complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, RegisterMalformedRegex)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

    try {
      LOG("Registering regex: a*");
      regPtr0->registerRegex("a*");

      LOG("Registering regex: *[*2-[");
      regPtr1->registerRegex("*[*2-[");

      FAIL("Did not get expected exception!");
    } catch (Exception& excp) {
      LOG(excp.getMessage());
    }

    LOG("RegisterMalformedRegex complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, RegisterEmptyNullAndNonExistentRegex)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

    try {
      LOG("Registering empty regex");
      regPtr0->registerRegex("");
      FAIL("Did not get expected exception!");
    } catch (Exception& excp) {
      LOG(excp.getMessage());
    }

    try {
      LOG("Registering null regex");
      regPtr1->registerRegex(NULL);
      FAIL("Did not get expected exception!");
    } catch (Exception& excp) {
      LOG(excp.getMessage());
    }

    try {
      LOG("Unregistering non-existent regex");
      regPtr1->unregisterRegex("Non*Existent*Regex");
      FAIL("Did not get expected exception!");
    } catch (Exception& excp) {
      LOG(excp.getMessage());
    }

    LOG("RegisterEmptyNullAndNonExistentRegex complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
    LOG("cleanProc 1...");
    cleanProc();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  {
    LOG("cleanProc 2...");
    cleanProc();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    LOG("closing Server1...");
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION
