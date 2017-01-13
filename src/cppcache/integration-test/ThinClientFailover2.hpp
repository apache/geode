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

#define ThinClientFailover2 "DistOps"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;
/*
 * This test for client failover with client notification.
 */
bool isLocalServer = false;

CacheHelper* cacheHelper = NULL;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2
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
                  bool noKey) {
  // Verify key and value exist in this region, in this process.
  const char* value = (val == 0) ? "" : val;
  char* buf = (char*)malloc(1024 + strlen(key) + strlen(value));
  ASSERT(buf, "Unable to malloc buffer for logging.");
  if (noKey)
    sprintf(buf, "Verify key %s does not exist in region %s", key, name);
  else if (val == 0)
    sprintf(buf, "Verify value for key %s does not exist in region %s", key,
            name);
  else
    sprintf(buf, "Verify value for key %s is: %s in region %s", key, value,
            name);
  LOG(buf);
  free(buf);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  CacheableKeyPtr keyPtr = createKey(key);

  if (noKey == false) {  // need to find the key!
    ASSERT(regPtr->containsKey(keyPtr), "Key not found in region.");
  }

  // loop up to MAX times, testing condition
  uint32_t MAX = 100;
  uint32_t SLEEP = 10;  // milliseconds
  uint32_t containsKeyCnt = 0;
  uint32_t containsValueCnt = 0;
  uint32_t testValueCnt = 0;

  for (int i = MAX; i >= 0; i--) {
    if (noKey) {
      if (regPtr->containsKey(keyPtr))
        containsKeyCnt++;
      else
        break;
      ASSERT(containsKeyCnt < MAX, "Key found in region.");
    }
    if (val == NULL) {
      if (regPtr->containsValueForKey(keyPtr)) {
        containsValueCnt++;
      } else
        break;
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

void createRegion(const char* name, bool ackMode, const char* endpoints,
                  bool clientNotificationEnabled = false) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  // ack, caching
  RegionPtr regPtr = getHelper()->createRegion(
      name, ackMode, true, NULLPTR, endpoints, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}
void createPooledRegion(const char* name, bool ackMode, 
                        const char* locators, const char* poolname,
                        bool clientNotificationEnabled = false,
                        bool cachingEnable = true) {
  LOG("createRegion_Pool() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr = getHelper()->createPooledRegion(
      name, ackMode, locators, poolname, cachingEnable,
      clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Pooled Region created.");
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

  /*NIL: Changed the asserion due to the change in invalidate.
    Now we create new entery for every invalidate event received or
    localInvalidate call
    so expect  containsKey to returns true insted of false earlier. */
  ASSERT(regPtr->containsKey(keyPtr), "Key should have been found in region.");
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

void invalidateEntry(const char* name, const char* key) {
  LOG("invalidateEntry() entered.");
  fprintf(stdout, "Invalidating entry -- key: %s  in region %s\n", key, name);
  fflush(stdout);
  // Invalidate entry, verify entry is invalidated
  CacheableKeyPtr keyPtr = CacheableKey::create(key);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT(regPtr->containsKey(keyPtr), "Key should have been found in region.");
  // ASSERT( regPtr->containsValueForKey( keyPtr ), "Value should have been
  // found in region." );

  regPtr->invalidate(keyPtr);
  LOG("Invalidate entry.");

  verifyInvalid(name, key);
  LOG("Entry invalidated.");
}

void destroyEntry(const char* name, const char* key) {
  LOG("destroyEntry() entered.");
  fprintf(stdout, "Destroying entry -- key: %s  in region %s\n", key, name);
  fflush(stdout);
  // Destroy entry, verify entry is destroyed
  CacheableKeyPtr keyPtr = CacheableKey::create(key);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT(regPtr->containsKey(keyPtr), "Key should have been found in region.");

  regPtr->destroy(keyPtr);
  LOG("Destroy entry.");

  verifyDestroyed(name, key);
  LOG("Entry destroyed.");
}

void destroyRegion(const char* name) {
  LOG("destroyRegion() entered.");
  RegionPtr regPtr = getHelper()->getRegion(name);
  regPtr->destroyRegion();
  LOG("Region destroyed.");
}

const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4", "Key-5"};
const char* vals[] = {"Value-1", "Value-2", "Value-3", "Value-4", "Value-5"};
const char* nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                       "New Value-4", "New Value-5"};

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck"};

const bool USE_ACK = true;
const bool NO_ACK = false;

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) CacheHelper::initServer(1);
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pool_Locator)
  {
    char buff[2048];
    char* buf = getcwd(buff, 2048);
    LOG(buf);
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG,
                       "__TEST_POOL1__", true);
    createPooledRegion(regionNames[1], NO_ACK,  locatorsG,
                       "__TEST_POOL1__", true);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->registerAllKeys(false, NULLPTR, false, false);
    regPtr = getHelper()->getRegion(regionNames[1]);
    regPtr->registerAllKeys(false, NULLPTR, false, false);
    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pool_Locator)
  {
    char buff[2048];
    char* buf = getcwd(buff, 2048);
    LOG(buf);
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG,
                       "__TEST_POOL1__", true);
    createPooledRegion(regionNames[1], NO_ACK,  locatorsG,
                       "__TEST_POOL1__", true);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->registerAllKeys(false, NULLPTR, false, false);
    regPtr = getHelper()->getRegion(regionNames[1]);
    regPtr->registerAllKeys(false, NULLPTR, false, false);
    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    createEntry(regionNames[0], keys[0], vals[0]);
    createEntry(regionNames[1], keys[2], vals[2]);
    createEntry(regionNames[0], keys[4], vals[4]);
    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepFour)
  {
    doNetsearch(regionNames[0], keys[0], vals[0]);
    doNetsearch(regionNames[1], keys[2], vals[2]);
    createEntry(regionNames[0], keys[1], vals[1]);
    createEntry(regionNames[1], keys[3], vals[3]);
    LOG("StepFour complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
  {
    if (isLocalServer) CacheHelper::initServer(2);
    LOG("SERVER2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFive)
  {
    VectorOfCacheableKey vec0, vec1;
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);
    reg0->serverKeys(vec0);
    reg1->serverKeys(vec1);
    ASSERT(vec0.size() == 3, "Should have 3 keys in first region.");
    ASSERT(vec1.size() == 2, "Should have 2 keys in second region.");

    std::string key0, key1, key2;
    key0 = vec0[0]->toString()->asChar();
    key1 = vec0[1]->toString()->asChar();
    key2 = vec0[2]->toString()->asChar();
    ASSERT(key0 != key1, "The two keys should be different in first region.");
    ASSERT(key0 != key2, "The two keys should be different in first region.");
    ASSERT(key1 != key2, "The two keys should be different in first region.");
    ASSERT(key0 == keys[0] || key0 == keys[1] || key0 == keys[4],
           "Unexpected key in first region.");
    ASSERT(key1 == keys[0] || key1 == keys[1] || key1 == keys[4],
           "Unexpected key in first region.");
    ASSERT(key2 == keys[0] || key2 == keys[1] || key2 == keys[4],
           "Unexpected key in first region.");

    key0 = vec1[0]->toString()->asChar();
    key1 = vec1[1]->toString()->asChar();
    ASSERT(key0 != key1, "The two keys should be different in second region.");
    ASSERT(key0 == keys[2] || key0 == keys[3],
           "Unexpected key in second region.");
    ASSERT(key1 == keys[2] || key1 == keys[3],
           "Unexpected key in second region.");

    doNetsearch(regionNames[0], keys[1], vals[1]);
    doNetsearch(regionNames[1], keys[3], vals[3]);
    updateEntry(regionNames[0], keys[0], nvals[0]);
    updateEntry(regionNames[1], keys[2], nvals[2]);
    verifyEntry(regionNames[0], keys[0], nvals[0]);
    verifyEntry(regionNames[1], keys[2], nvals[2]);
    LOG("StepFive complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, StepSix)
  {
    // verifyInvalid( regionNames[0], keys[0] );
    // verifyInvalid( regionNames[1], keys[2] );
    // This step get the value from region, if key is inavalidate in region,
    // value
    // will get from server.
    // doNetsearch( regionNames[0], keys[4], vals[4] );
    verifyEntry(regionNames[0], keys[0], nvals[0]);
    verifyEntry(regionNames[1], keys[2], nvals[2]);
    updateEntry(regionNames[0], keys[1], nvals[1]);
    updateEntry(regionNames[1], keys[3], nvals[3]);
    LOG("StepSix complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSeven)
  {
    verifyEntry(regionNames[0], keys[1], nvals[1]);
    verifyEntry(regionNames[1], keys[3], nvals[3]);
    LOG("StepSeven complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, StepEight)
  {
    destroyEntry(regionNames[0], keys[0]);
    destroyEntry(regionNames[1], keys[2]);
    LOG("StepEight complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepNine)
  {
    verifyDestroyed(regionNames[0], keys[0]);
    verifyDestroyed(regionNames[1], keys[2]);
    destroyEntry(regionNames[0], keys[1]);
    destroyEntry(regionNames[1], keys[3]);
    LOG("StepNine complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, StepTen)
  {
    verifyDestroyed(regionNames[0], keys[1]);
    verifyDestroyed(regionNames[1], keys[3]);
    LOG("StepTen complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(SERVER2, CloseServer2)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK_DEFINITION

void runThinClientFailover2() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator)

  CALL_TASK(StepOne_Pool_Locator);
  CALL_TASK(StepTwo_Pool_Locator);

  CALL_TASK(StepThree);
  CALL_TASK(StepFour);

  CALL_TASK(CreateServer2_With_Locator);

  CALL_TASK(CloseServer1);  // FailOver
  CALL_TASK(StepFive);
  CALL_TASK(StepSix);
  CALL_TASK(StepSeven);
  CALL_TASK(StepEight);
  CALL_TASK(StepNine);
  CALL_TASK(StepTen);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer2);

  CALL_TASK(CloseLocator1);
}
