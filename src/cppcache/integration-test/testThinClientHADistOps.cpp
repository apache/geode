/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>

#include <string>

#define ROOT_NAME "testThinClientHADistOps"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

CacheHelper* cacheHelper = NULL;
bool isLocalServer = false;

static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
#include "LocatorHelper.hpp"
static int clientWithRedundancy = 0;
void initClient(int redundancyLevel) {
  if (cacheHelper == NULL) {
    PropertiesPtr config = Properties::create();
    if (clientWithRedundancy > 0) config->insert("grid-client", "true");
    clientWithRedundancy += 1;
    cacheHelper = new CacheHelper(redundancyLevel, config);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

static int clientWithNothing = 0;
void initClient() {
  if (cacheHelper == NULL) {
    PropertiesPtr config = Properties::create();
    if (clientWithNothing > 1) config->insert("grid-client", "true");
    clientWithNothing += 1;
    cacheHelper = new CacheHelper(true, config);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

static int clientWithXml = 0;
void initClient(const char* clientXmlFile) {
  if (cacheHelper == NULL) {
    PropertiesPtr config = Properties::create();
    if (clientWithXml > 2) config->insert("grid-client", "true");
    clientWithXml += 1;
    cacheHelper = new CacheHelper(NULL, clientXmlFile, config);
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
  char* buf =
      reinterpret_cast<char*>(malloc(1024 + strlen(key) + strlen(value)));
  ASSERT(buf, "Unable to malloc buffer for logging.");
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
  free(buf);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  CacheableKeyPtr keyPtr = createKey(key);

  // if the region is no ack, then we may need to wait...
  if (noKey == false) {  // need to find the key!
    ASSERT(regPtr->containsKey(keyPtr), "Key not found in region.");
  }
  if (val != NULL) {  // need to have a value!
    ASSERT(regPtr->containsValueForKey(keyPtr), "Value not found in region.");
  }

  // loop up to MAX times, testing condition
  uint32_t MAX = 100;
  uint32_t SLEEP = 10;  // milliseconds
  uint32_t containsKeyCnt = 0;
  uint32_t containsValueCnt = 0;
  uint32_t testValueCnt = 0;

  for (int i = MAX; i >= 0; i--) {
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

void createRegion(const char* name, bool ackMode,
                  bool clientNotificationEnabled = false) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  char* endpoints = NULL;
  RegionPtr regPtr = getHelper()->createRegion(
      name, ackMode, true, NULLPTR, endpoints, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}

void createPooledRegion(const char* name, bool ackMode,
                        const char* locators, const char* poolname,
                        int reduendency = 1,
                        bool clientNotificationEnabled = false,
                        bool cachingEnable = true) {
  LOG("createRegion_Pool() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  PoolPtr poolPtr =
      getHelper()->createPool(poolname, locators, NULL, reduendency,
                              clientNotificationEnabled);
  RegionPtr regPtr = getHelper()->createRegionAndAttachPool(
      name, ackMode, poolname, cachingEnable);
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
  ASSERT(regPtr->containsValueForKey(keyPtr),
         "Value should have been found in region.");

  regPtr->put(keyPtr, valPtr);
  LOG("Put entry.");

  verifyEntry(name, key, value);
  LOG("Entry updated.");
}

void doGetAgain(const char* name, const char* key, const char* value) {
  LOG("doGetAgain() entered.");
  fprintf(stdout,
          "get for entry -- key: %s  expecting value: %s in region %s\n", key,
          value, name);
  fflush(stdout);
  // Get entry created in Process A, verify entry is correct
  CacheableKeyPtr keyPtr = CacheableKey::create(key);

  RegionPtr regPtr = getHelper()->getRegion(name);
  fprintf(stdout, "get  region name%s\n", regPtr->getName());
  fflush(stdout);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  CacheableStringPtr checkPtr =
      dynCast<CacheableStringPtr>(regPtr->get(keyPtr));  // force a netsearch

  if (checkPtr != NULLPTR) {
    LOG("checkPtr is not null");
    char buf[1024];
    sprintf(buf, "In doGetAgain, get returned %s for key %s",
            checkPtr->asChar(), key);
    LOG(buf);
  } else {
    LOG("checkPtr is NULL");
  }
  verifyEntry(name, key, value);
  LOG("GetAgain complete.");
}

void doNetsearch(const char* name, const char* key, const char* value) {
  LOG("doNetsearch() entered.");
  fprintf(
      stdout,
      "Netsearching for entry -- key: %s  expecting value: %s in region %s\n",
      key, value, name);
  fflush(stdout);
  static int count = 0;
  // Get entry created in Process A, verify entry is correct
  CacheableKeyPtr keyPtr = CacheableKey::create(key);

  RegionPtr regPtr = getHelper()->getRegion(name);
  fprintf(stdout, "netsearch  region %s\n", regPtr->getName());
  fflush(stdout);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  if (count == 0) {
    ASSERT(!regPtr->containsKey(keyPtr),
           "Key should not have been found in region.");
    ASSERT(!regPtr->containsValueForKey(keyPtr),
           "Value should not have been found in region.");
    count++;
  }
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

DUNIT_TASK_DEFINITION(CLIENT1, InitClient1)
  { initClient(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, InitClient2)
  { initClient(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateClient1PoolAndRegions)
  {
    getHelper()->createPoolWithLocators("__TESTPOOL1_", locatorsG, true, 1);

    getHelper()->createRegionAndAttachPool(regionNames[0], USE_ACK,
                                           "__TESTPOOL1_", true);
    getHelper()->createRegionAndAttachPool(regionNames[1], NO_ACK,
                                           "__TESTPOOL1_", true);
    /* createPooledRegion( regionNames[0], USE_ACK, locatorsG,
     "__TESTPOOL1_" );
     createPooledRegion( regionNames[1], NO_ACK, locatorsG, "__TESTPOOL2_"
     );*/

    createEntry(regionNames[0], keys[0], vals[0]);
    createEntry(regionNames[1], keys[2], vals[2]);

    LOG("CreateClient1PoolAndRegions complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2PoolAndRegions)
  {
    getHelper()->createPoolWithLocators("__TESTPOOL1_", locatorsG, true, 1);

    getHelper()->createRegionAndAttachPool(regionNames[0], USE_ACK,
                                           "__TESTPOOL1_", true);
    getHelper()->createRegionAndAttachPool(regionNames[1], NO_ACK,
                                           "__TESTPOOL1_", true);

    /* createPooledRegion( regionNames[0], USE_ACK,  locatorsG,
     "__TESTPOOL1_" );
     createPooledRegion( regionNames[1], NO_ACK,  locatorsG, "__TESTPOOL2_"
     );*/

    createEntry(regionNames[0], keys[1], vals[1]);
    createEntry(regionNames[1], keys[3], vals[3]);

    LOG("CreateClient2PoolAndRegions complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, RegisterClient1Keys)
  {
    VectorOfCacheableKey vec0, vec1;
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);
    reg0->serverKeys(vec0);
    reg1->serverKeys(vec1);
    ASSERT(vec0.size() == 2, "Should have 2 keys in first region.");
    ASSERT(vec1.size() == 2, "Should have 2 keys in second region.");
    std::string key0, key1;
    key0 = vec0[0]->toString()->asChar();
    key1 = vec0[1]->toString()->asChar();
    ASSERT(key0 != key1, "The two keys should be different in first region.");
    ASSERT(key0 == keys[0] || key0 == keys[1],
           "Unexpected key in first region.");
    ASSERT(key1 == keys[0] || key1 == keys[1],
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

    CacheableKeyPtr keyPtr1 = CacheableKey::create(keys[1]);
    CacheableKeyPtr keyPtr3 = CacheableKey::create(keys[3]);

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

    VectorOfCacheableKey keys0, keys1;
    keys0.push_back(keyPtr1);
    keys1.push_back(keyPtr3);
    regPtr0->registerKeys(keys0);
    regPtr1->registerKeys(keys1);
    LOG("RegisterClient1Keys complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, RegisterClient2Keys)
  {
    doNetsearch(regionNames[0], keys[0], vals[0]);
    doNetsearch(regionNames[1], keys[2], vals[2]);

    CacheableKeyPtr keyPtr0 = CacheableKey::create(keys[0]);
    CacheableKeyPtr keyPtr2 = CacheableKey::create(keys[2]);

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

    VectorOfCacheableKey keys0, keys1;
    keys0.push_back(keyPtr0);
    keys1.push_back(keyPtr2);
    regPtr0->registerKeys(keys0);
    regPtr1->registerKeys(keys1);
    LOG("RegisterClient2Keys complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, UpdateClient1Entries)
  {
    updateEntry(regionNames[0], keys[0], nvals[0]);
    updateEntry(regionNames[1], keys[2], nvals[2]);
    LOG("UpdateClient1Entries complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyAndUpdateClient2Entries)
  {
    verifyEntry(regionNames[0], keys[0], nvals[0]);
    verifyEntry(regionNames[1], keys[2], nvals[2]);

    updateEntry(regionNames[0], keys[1], nvals[1]);
    updateEntry(regionNames[1], keys[3], nvals[3]);
    LOG("VerifyAndUpdateClient2Entries complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyClient1Entries)
  {
    verifyEntry(regionNames[0], keys[1], nvals[1]);
    verifyEntry(regionNames[1], keys[3], nvals[3]);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, DoAbsolutelyNothing)
  {}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, DestroyAllKeys)
  {
    destroyEntry(regionNames[0], keys[0]);
    destroyEntry(regionNames[0], keys[1]);
    destroyEntry(regionNames[1], keys[2]);
    destroyEntry(regionNames[1], keys[3]);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CloseServer2)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    /* Starting Server*/
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML);
    CALL_TASK(CreateServer2_With_Locator_XML);

    /* Client init*/
    CALL_TASK(InitClient1);
    CALL_TASK(InitClient2);

    CALL_TASK(CreateClient1PoolAndRegions);
    CALL_TASK(CreateClient2PoolAndRegions);

    CALL_TASK(RegisterClient1Keys);
    CALL_TASK(RegisterClient2Keys);
    CALL_TASK(UpdateClient1Entries);
    CALL_TASK(VerifyAndUpdateClient2Entries);
    CALL_TASK(VerifyClient1Entries);
    CALL_TASK(DoAbsolutelyNothing);
    CALL_TASK(DestroyAllKeys);

    CALL_TASK(CloseServer1);
    CALL_TASK(CloseServer2);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
