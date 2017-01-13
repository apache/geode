/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/High_Res_Timer.h>

#include <ace/OS.h>
#include <string>

#define ROOT_NAME "testThinClientHAMixedRedundancy"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

bool isLocalServer = false;

CacheHelper* cacheHelper = NULL;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define CLIENT3 s2p1
#define SERVERS s2p2
#define SERVER1 s2p2
static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
bool g_poolConfig = false;
bool g_poolLocators = false;
void initClient(int redundancyLevel) {
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(redundancyLevel);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}
void initClient() {
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(true);
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
      // ASSERT( regPtr->containsValueForKey( keyPtr ), "Value not found in
      // region." );
    }
  }

  // loop up to MAX times, testing condition
  uint32_t MAX = 100;
  //  changed sleep from 10 ms
  uint32_t SLEEP = 10;  // milliseconds
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

void createRegion(const char* name, bool ackMode,
                  bool clientNotificationEnabled = true) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  char* endpoints = NULL;
  // ack, caching
  RegionPtr regPtr = getHelper()->createRegion(
      name, ackMode, true, NULLPTR, endpoints, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}

void createEntry(const char* name, const char* key, const char* value) {
  LOG("createEntry() entered.");
  fprintf(stdout, "Creating entry -- key: %s  value: %s in region %s\n", key,
          value, name);
  fflush(stdout);
  // Create entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableStringPtr valPtr = CacheableString::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT(!regPtr->containsKey(keyPtr),
         "Key should not have been found in region.");
  ASSERT(!regPtr->containsValueForKey(keyPtr),
         "Value should not have been found in region.");

  // regPtr->create( keyPtr, valPtr );
  regPtr->put(keyPtr, valPtr);
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
  // ASSERT( regPtr->containsValueForKey( keyPtr ), "Value should have been
  // found in region." );

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

  // ASSERT( !regPtr->containsKey( keyPtr ), "Key should not have been found in
  // region." );
  // ASSERT( !regPtr->containsValueForKey( keyPtr ), "Value should not have been
  // found in region." );

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

const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};
const char* vals[] = {"Value-1", "Value-2", "Value-3", "Value-4"};
const char* nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                       "New Value-4"};

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck"};

const bool USE_ACK = true;
const bool NO_ACK = false;
#include "ThinClientTasks_C2S2.hpp"
void createCommRegions(int redundancy) {
  PropertiesPtr pp = Properties::create();
  getHelper()->createPoolWithLocators("__TESTPOOL1_", locatorsG, true,
                                      redundancy);
  getHelper()->createRegionAndAttachPool(regionNames[0], USE_ACK,
                                         "__TESTPOOL1_", true);
  getHelper()->createRegionAndAttachPool(regionNames[1], USE_ACK,
                                         "__TESTPOOL1_", true);
}

DUNIT_TASK_DEFINITION(SERVERS, CreateServers)
  {
    if (isLocalServer) {
      if (g_poolConfig && g_poolLocators) {
        if (isLocator) {
          CacheHelper::initLocator(1);
          LOG("Locator1 started");
        }
        CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                                locatorsG);
        LOG("SERVER1 started");
        CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml",
                                locatorsG);
        LOG("SERVER2 started");
        CacheHelper::initServer(3, "cacheserver_notify_subscription3.xml",
                                locatorsG);
        LOG("SERVER3 started");
      } else {
        CacheHelper::initServer(1, "cacheserver_notify_subscription.xml");
        LOG("SERVER1 started");
        CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml");
        LOG("SERVER2 started");
        CacheHelper::initServer(3, "cacheserver_notify_subscription3.xml");
        LOG("SERVER3 started");
      }
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
  {
    if (!g_poolConfig) {
      initClient(0);
    } else {
      initClient();
    }
    createCommRegions(0);
    createEntry(regionNames[0], keys[0], vals[0]);
    createEntry(regionNames[0], keys[1], vals[1]);
    createEntry(regionNames[1], keys[2], vals[2]);
    //  Sleep to allow update on no-ack region to propagate to all servers.
    // Otherwise a server can skip update for keys[2] (over p2p) if it first
    // receives update for keys[3].
    SLEEP(1000);
    createEntry(regionNames[1], keys[3], vals[3]);

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

    CacheableKeyPtr keyPtr0 = CacheableKey::create(keys[0]);
    CacheableKeyPtr keyPtr2 = CacheableKey::create(keys[2]);

    VectorOfCacheableKey keys0, keys1;
    keys0.push_back(keyPtr0);
    keys1.push_back(keyPtr2);
    regPtr0->registerKeys(keys0);
    regPtr1->registerKeys(keys1);

    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo)
  {
    if (!g_poolConfig) {
      initClient(1);
    } else {
      initClient();
    }
    createCommRegions(1);

    doNetsearch(regionNames[0], keys[0], vals[0]);
    doNetsearch(regionNames[0], keys[1], vals[1]);
    doNetsearch(regionNames[1], keys[2], vals[2]);
    doNetsearch(regionNames[1], keys[3], vals[3]);

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

    CacheableKeyPtr keyPtr1 = CacheableKey::create(keys[1]);
    CacheableKeyPtr keyPtr3 = CacheableKey::create(keys[3]);

    VectorOfCacheableKey keys0, keys1;
    keys0.push_back(keyPtr1);
    keys1.push_back(keyPtr3);
    regPtr0->registerKeys(keys0);
    regPtr1->registerKeys(keys1);

    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, StepThree)
  {
    if (!g_poolConfig) {
      initClient(2);
    } else {
      initClient();
    }
    createCommRegions(2);

    doNetsearch(regionNames[0], keys[0], vals[0]);
    doNetsearch(regionNames[0], keys[1], vals[1]);
    doNetsearch(regionNames[1], keys[2], vals[2]);
    doNetsearch(regionNames[1], keys[3], vals[3]);

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

    CacheableKeyPtr keyPtr0 = CacheableKey::create(keys[0]);
    CacheableKeyPtr keyPtr3 = CacheableKey::create(keys[3]);

    VectorOfCacheableKey keys0, keys1;
    keys0.push_back(keyPtr0);
    keys1.push_back(keyPtr3);
    regPtr0->registerKeys(keys0);
    regPtr1->registerKeys(keys1);

    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVERS, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFour)
  {
    updateEntry(regionNames[0], keys[0], nvals[0]);
    updateEntry(regionNames[0], keys[1], nvals[1]);
    updateEntry(regionNames[1], keys[2], nvals[2]);
    updateEntry(regionNames[1], keys[3], nvals[3]);
    SLEEP(1000);
    LOG("StepFour complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepFive)
  {
    verifyEntry(regionNames[0], keys[0], vals[0]);
    verifyEntry(regionNames[0], keys[1], nvals[1]);
    verifyEntry(regionNames[1], keys[2], vals[2]);
    verifyEntry(regionNames[1], keys[3], nvals[3]);
    LOG("StepFive complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, StepSix)
  {
    verifyEntry(regionNames[0], keys[0], nvals[0]);
    verifyEntry(regionNames[0], keys[1], vals[1]);
    verifyEntry(regionNames[1], keys[2], vals[2]);
    verifyEntry(regionNames[1], keys[3], nvals[3]);
    LOG("StepSix complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepSeven)
  {
    updateEntry(regionNames[0], keys[0], vals[0]);
    updateEntry(regionNames[0], keys[1], vals[1]);
    updateEntry(regionNames[1], keys[2], vals[2]);
    updateEntry(regionNames[1], keys[3], vals[3]);
    LOG("StepSeven complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepEight)
  {
    verifyEntry(regionNames[0], keys[0], vals[0]);
    verifyEntry(regionNames[0], keys[1], nvals[1]);
    verifyEntry(regionNames[1], keys[2], vals[2]);
    verifyEntry(regionNames[1], keys[3], nvals[3]);
    LOG("StepEight complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, StepNine)
  {
    verifyEntry(regionNames[0], keys[0], vals[0]);
    verifyEntry(regionNames[0], keys[1], vals[1]);
    verifyEntry(regionNames[1], keys[2], vals[2]);
    verifyEntry(regionNames[1], keys[3], vals[3]);
    LOG("StepNine complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, StepTen)
  {
    updateEntry(regionNames[0], keys[0], nvals[0]);
    updateEntry(regionNames[0], keys[1], nvals[1]);
    updateEntry(regionNames[1], keys[2], nvals[2]);
    updateEntry(regionNames[1], keys[3], nvals[3]);
    LOG("StepTen complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepEleven)
  {
    verifyEntry(regionNames[0], keys[0], nvals[0]);
    verifyEntry(regionNames[0], keys[1], nvals[1]);
    verifyEntry(regionNames[1], keys[2], nvals[2]);
    verifyEntry(regionNames[1], keys[3], nvals[3]);
    LOG("StepEleven complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwelve)
  {
    verifyEntry(regionNames[0], keys[0], vals[0]);
    verifyEntry(regionNames[0], keys[1], nvals[1]);
    verifyEntry(regionNames[1], keys[2], vals[2]);
    verifyEntry(regionNames[1], keys[3], nvals[3]);
    LOG("StepTwelve complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVERS, CloseServers)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
      CacheHelper::closeServer(3);
      LOG("SERVER3 stopped");
      CacheHelper::closeServer(4);
      LOG("SERVER4 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, CloseCache3)
  { cleanProc(); }
END_TASK_DEFINITION

void runThinClientMixedRedundancy() {
  CALL_TASK(CreateServers)
  CALL_TASK(StepOne);
  CALL_TASK(StepTwo);
  CALL_TASK(StepThree);
  CALL_TASK(CloseServer1);
  CALL_TASK(StepFour);
  CALL_TASK(StepFive);
  CALL_TASK(StepSix);
  CALL_TASK(StepSeven);
  CALL_TASK(StepEight);
  CALL_TASK(StepNine);
  CALL_TASK(StepTen);
  CALL_TASK(StepEleven);
  CALL_TASK(StepTwelve);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseCache3);
  CALL_TASK(CloseServers);
  closeLocator();
}
DUNIT_MAIN
  {
    runThinClientMixedRedundancy();
  }
END_MAIN
