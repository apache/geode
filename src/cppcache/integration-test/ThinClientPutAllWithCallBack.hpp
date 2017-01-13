/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef ThinClientPutAllWITHCALLBACK_HPP_
#define ThinClientPutAllWITHCALLBACK_HPP_

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include "testobject/PdxType.hpp"
#include "testobject/VariousPdxTypes.hpp"
#include <string>

#define ROOT_NAME "ThinClientPutAllWithCallBack"
#define ROOT_SCOPE DISTRIBUTED_ACK
#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

CacheHelper* cacheHelper = NULL;
static bool isLocalServer = false;
static bool isLocator = false;
static int numberOfLocators = 0;

const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);
const char* poolName = "__TESTPOOL1_";

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
  char* buf = (char*)malloc(1024 + strlen(key) + strlen(value));
  ASSERT(buf, "Unable to malloc buffer for logging.");
  if (!isCreated) {
    if (noKey)
      sprintf(buf, "Verify key %s does not exist in region %s", key, name);
    else if (val == 0)
      sprintf(buf, "Verify value for key %s does not exist in region %s", key,
              name);
    else
      sprintf(buf, "Verify value for key %s is: %s in region %s", key, value,
              name);
    LOG(buf);
  }

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
      if (!regPtr->containsKey(keyPtr))
        containsKeyCnt++;
      else
        break;
      ASSERT(containsKeyCnt < MAX, "Key has not been created in region.");
    } else {
      if (noKey) {
        if (regPtr->containsKey(keyPtr))
          containsKeyCnt++;
        else
          break;
        ASSERT(containsKeyCnt < MAX, "Key found in region.");
      }
      if (val == NULL) {
        if (regPtr->containsValueForKey(keyPtr))
          containsValueCnt++;
        else
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
void createRegion(const char* name, bool ackMode, bool isCacheEnabled,
                  bool clientNotificationEnabled = false) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr = getHelper()->createRegion(
      name, ackMode, isCacheEnabled, NULLPTR, clientNotificationEnabled);
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

void createPooledRegionConcurrencyCheckDisabled(
    const char* name, bool ackMode, const char* locators, const char* poolname,
    bool clientNotificationEnabled = false, bool cachingEnable = true,
    bool concurrencyCheckEnabled = true) {
  LOG("createRegion_Pool() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr = getHelper()->createPooledRegionConcurrencyCheckDisabled(
      name, ackMode, locators, poolname, cachingEnable,
      clientNotificationEnabled, concurrencyCheckEnabled);
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
  if (value == NULL) value = "";
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

const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};
const char* vals[] = {"Value-1", "Value-2", "Value-3", "Value-4"};
const char* nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                       "New Value-4"};

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck"};

const bool USE_ACK = true;
const bool NO_ACK = false;

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml");
      LOG("SERVER1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
  {
    if (isLocalServer) {
      CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml");
      LOG("SERVER2 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,
                      CreateClient1RegionsWithCachingWithConcurrencyCheck)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, poolName, true,
                       true);
    createPooledRegion(regionNames[1], NO_ACK, locatorsG, poolName, true, true);
    LOG("CreateClient1RegionsWithCachingWithConcurrencyCheck complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,
                      CreateClient1RegionsWithoutCachingWithConcurrencyCheck)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, poolName, false,
                       false);
    LOG("CreateClient1RegionsWithoutCachingWithConcurrencyCheck complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,
                      CreateClient1RegionsWithCachingWithoutConcurrencyCheck)
  {
    initClient(true);
    createPooledRegionConcurrencyCheckDisabled(
        regionNames[0], USE_ACK, locatorsG, poolName, true, true, false);
    LOG("CreateClient1RegionsWithCachingWithoutConcurrencyCheck complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CreateClient2Regions)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, poolName, true,
                       true);
    createPooledRegion(regionNames[1], NO_ACK, locatorsG, poolName, true, true);
    LOG("CreateClient2Regions complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, RegisterClient1Keys)
  {
    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
      Serializable::registerPdxType(PdxTests::PdxTypes1::createDeserializable);
      Serializable::registerPdxType(PdxTests::PdxTypes2::createDeserializable);
      Serializable::registerPdxType(PdxTests::Address::createDeserializable);
      Serializable::registerPdxType(PdxTests::PdxTypes3::createDeserializable);
    } catch (Exception e) {
      LOG("Got expected Exception for Serialization, already registered");
    }
    // the client1 will register k0 and k1
    // createEntry( regionNames[0], keys[0], vals[0] );
    // createEntry( regionNames[0], keys[1], vals[1] );
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

    CacheableKeyPtr keyPtr0 = CacheableKey::create(keys[0]);
    CacheableKeyPtr keyPtr1 = CacheableKey::create(keys[1]);

    VectorOfCacheableKey keys1;
    keys1.push_back(keyPtr0);
    keys1.push_back(keyPtr1);
    regPtr0->registerKeys(keys1, NULLPTR);

    CacheableKeyPtr keyPtr2 = CacheableKey::create(keys[2]);
    CacheableKeyPtr keyPtr3 = CacheableKey::create(keys[3]);

    VectorOfCacheableKey keys2;
    keys2.push_back(keyPtr2);
    keys2.push_back(keyPtr3);
    regPtr1->registerKeys(keys2, NULLPTR);

    LOG("RegisterClient1Keys complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, RegisterClient2Keys)
  {
    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
      Serializable::registerPdxType(PdxTests::PdxTypes1::createDeserializable);
      Serializable::registerPdxType(PdxTests::PdxTypes2::createDeserializable);
      Serializable::registerPdxType(PdxTests::Address::createDeserializable);
      Serializable::registerPdxType(PdxTests::PdxTypes3::createDeserializable);
    } catch (Exception e) {
      LOG("Got expected Exception for Serialization, already registered");
    }
    HashMapOfCacheable map0;
    HashMapOfCacheable map1;
    map0.clear();
    map1.clear();
    for (int i = 0; i < 2; i++) {
      map0.insert(CacheableKey::create(keys[i]),
                  CacheableString::create(vals[i]));
    }
    for (int i = 2; i < 4; i++) {
      map1.insert(CacheableKey::create(keys[i]),
                  CacheableString::create(vals[i]));
    }
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
    regPtr0->putAll(map0, 15, CacheableInt32::create(1000));
    regPtr1->putAll(map1, 15, CacheableInt32::create(1000));
    LOG("RegisterClient2Keys complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAllOps)
  {
    HashMapOfCacheable entryMap;
    entryMap.clear();
    char key[2048];
    char value[2048];
    for (int32_t item = 0; item < 500; item++) {
      sprintf(key, "key-%d", item);
      sprintf(value, "%d", item);
      entryMap.insert(CacheableKey::create(key),
                      CacheableString::create(value));
    }

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    regPtr0->putAll(entryMap, 15, CacheableInt32::create(1000));

    LOG("putAll1 complete");

    VectorOfCacheableKey getAllkeys;
    for (int32_t item = 0; item < 500; item++) {
      sprintf(key, "key-%d", item);
      getAllkeys.push_back(CacheableKey::create(key));
    }

    HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
    valuesMap->clear();
    regPtr0->getAll(getAllkeys, valuesMap, NULLPTR, false);
    ASSERT(valuesMap->size() == 500, "GetAll should return 500 entries.");

    LOG("PutAllOps complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyInitialEntries)
  {
    verifyCreated(regionNames[0], keys[0]);
    verifyCreated(regionNames[0], keys[1]);

    verifyEntry(regionNames[0], keys[0], vals[0]);
    verifyEntry(regionNames[0], keys[1], vals[1]);

    // doNetsearch( regionNames[1], keys[2], vals[2] );
    // doNetsearch( regionNames[1], keys[3], vals[3] );

    verifyCreated(regionNames[1], keys[2]);
    verifyCreated(regionNames[1], keys[3]);

    verifyEntry(regionNames[1], keys[2], vals[2]);
    verifyEntry(regionNames[1], keys[3], vals[3]);

    LOG("VerifyInitialEntries complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, TriggerAfterUpdateEvents)
  {
    LOG("Trigger afterUpdate events.");
    HashMapOfCacheable map0;
    HashMapOfCacheable map1;
    map0.clear();
    map1.clear();
    for (int i = 0; i < 2; i++) {
      map0.insert(CacheableKey::create(keys[i]),
                  CacheableString::create(nvals[i]));
    }
    for (int i = 2; i < 4; i++) {
      map1.insert(CacheableKey::create(keys[i]),
                  CacheableString::create(nvals[i]));
    }
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
    regPtr0->putAll(map0, 15, CacheableInt32::create(1000));
    regPtr1->putAll(map1, 15, CacheableInt32::create(1000));

    // register all keys for the large putall test case
    regPtr0->registerAllKeys();

    LOG("TriggerAfterUpdateEvents complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyUpdatedEntries)
  {
    verifyEntry(regionNames[0], keys[0], nvals[0]);
    verifyEntry(regionNames[0], keys[1], nvals[1]);
    // region1 is not changed at client
    verifyEntry(regionNames[1], keys[2], nvals[2]);
    verifyEntry(regionNames[1], keys[3], nvals[3]);
    LOG("VerifyUpdatedEntries complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ExecuteLargePutAll)
  {
    LOG("Do large PutAll");
    HashMapOfCacheable map0;
    map0.clear();
    for (int i = 0; i < 100000; i++) {
      char key0[50] = {0};
      char val0[2500] = {0};
      sprintf(key0, "key-%d", i);
      sprintf(val0, "%1000d", i);
      map0.insert(CacheableKey::create(key0), CacheableString::create(val0));
    }
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);

    regPtr0->putAll(map0, 40000, CacheableInt32::create(1000));

    LOG("ExecuteLargePutAll complete.");
    dunit::sleep(10000);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyLargePutAll)
  {
    LOG("Verify large PutAll");
    for (int i = 0; i < 100000; i++) {
      char key0[50] = {0};
      sprintf(key0, "key-%d", i);
      verifyCreated(regionNames[0], key0);
    }
    LOG("VerifyLargePutAll complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyRegionService)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionServicePtr rsp = regPtr0->getRegionService();
    RegionPtr regPtr = rsp->getRegion(regionNames[0]);
    ASSERT(regPtr != NULLPTR, "Failed to get region.");

    RegionServicePtr rsp1 = regPtr0->getRegionService();
    RegionPtr regPtr1 = rsp1->getRegion("NOT_CREATED_REGION");
    ASSERT(regPtr1 == NULLPTR, "Unknown Region Returned");

    LOG("VerifyRegionService complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, InvalidateKeys)
  {
    CacheableKeyPtr keyPtr0 = CacheableInt64::create(100L);
    CacheableKeyPtr valPtr0 = CacheableInt64::create(200L);

    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);

    regPtr0->put(keyPtr0, valPtr0);
    CacheableInt64Ptr checkPtr =
        dynCast<CacheableInt64Ptr>(regPtr0->get(keyPtr0));
    ASSERT(checkPtr != NULLPTR, "checkPtr should not be null.");

    regPtr0->invalidate(keyPtr0);
    checkPtr = dynCast<CacheableInt64Ptr>(regPtr0->get(keyPtr0));
    ASSERT(checkPtr == NULLPTR, "checkPtr should be null.");

    try {
      CacheableKeyPtr key;
      regPtr0->invalidate(key);
      FAIL("Invalidate on NULLPTR should throw exception");
    } catch (IllegalArgumentException e) {
      LOG(" Got an expected exception invalidate on NULLPTR should be throwing "
          "exception ");
    }

    LOG("InvalidateKeys complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyPutAllWithLongKeyAndStringValue)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    HashMapOfCacheable map0;
    for (int i = 0; i < 2; i++) {
      map0.insert(CacheableInt64::create(i), CacheableInt64::create(i));
    }
    regPtr0->putAll(map0, 15, CacheableInt32::create(1000));
    for (int i = 0; i < 2; i++) {
      CacheableInt64Ptr checkPtr =
          dynCast<CacheableInt64Ptr>(regPtr0->get(CacheableInt64::create(i)));
      ASSERT(checkPtr->value() == i,
             "putAll entry with long key and long value Mismatch.");
    }
    map0.clear();
    const char* vals[] = {"Value-100", "Value-200"};

    for (int i = 80; i < 82; i++) {
      map0.insert(CacheableInt64::create(i),
                  CacheableString::create(vals[i - 80]));
    }
    regPtr0->putAll(map0, 15, CacheableInt32::create(1000));
    for (int i = 80; i < 82; i++) {
      CacheableStringPtr checkPtr =
          dynCast<CacheableStringPtr>(regPtr0->get(CacheableInt64::create(i)));
      ASSERT(strcmp(checkPtr->asChar(), vals[i - 80]) == 0,
             "putAll entry with long key and string value  Mismatch");
    }

    LOG("InvalidateKeys complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyPutAllWithLongKeyAndLongValue)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    HashMapOfCacheable map0;
    try {
      map0.insert(CacheableInt64::create(345), CacheableInt64::create(3465987));
      regPtr0->putAll(map0, -1, CacheableInt32::create(1000));
      CacheableInt64Ptr checkPtr =
          dynCast<CacheableInt64Ptr>(regPtr0->get(CacheableInt64::create(345)));
      ASSERT(checkPtr->value() == 3465987,
             "putAll entry with long key and long value Mismatch.");
    } catch (Exception& excp) {
      std::string logmsg = "";
      logmsg += "expected exception ";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
    }
    map0.clear();

    try {
      map0.insert(CacheableInt64::create(3451),
                  CacheableInt64::create(3465987));
      regPtr0->putAll(map0, 2147500, CacheableInt32::create(1000));
      CacheableInt64Ptr checkPtr = dynCast<CacheableInt64Ptr>(
          regPtr0->get(CacheableInt64::create(3451)));
      ASSERT(checkPtr->value() == 3465987,
             "putAll entry with long key and long value Mismatch.");
    } catch (Exception& excp) {
      std::string logmsg = "";
      logmsg += "expected exception ";
      logmsg += excp.getName();
      logmsg += ": ";
      logmsg += excp.getMessage();
      LOG(logmsg.c_str());
    }

    LOG("VerifyPutAllWithLongKeyAndLongValue complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyPutAllWithObjectKey)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    HashMapOfCacheable map0;
    PdxTests::PdxTypes1Ptr val111(new PdxTests::PdxTypes1());
    map0.insert(CacheableInt32::create(1211), val111);
    regPtr0->putAll(map0, 15, CacheableInt32::create(1000));
    PdxTests::PdxTypes1Ptr retObj = dynCast<PdxTests::PdxTypes1Ptr>(
        regPtr0->get(CacheableInt32::create(1211)));
    ASSERT(val111->equals(retObj) == true, "val111 and retObj should match.");
    map0.clear();

    CacheableKeyPtr keyObject(new PdxTests::PdxType());
    map0.insert(keyObject, CacheableInt32::create(111));
    regPtr0->putAll(map0, 15, CacheableInt32::create(1000));
    CacheableInt32Ptr checkPtr = regPtr0->get(keyObject);
    ASSERT(checkPtr->value() == 111,
           "putAll with entry as object key and value as int  Mismatch");
    map0.clear();
    CacheableKeyPtr keyObject6(new PdxTests::PdxTypes3());
    map0.insert(keyObject6, CacheableString::create("testString"));
    regPtr0->putAll(map0, 15, CacheableInt32::create(1000));
    CacheablePtr checkPtr1 = regPtr0->get(keyObject6);
    ASSERT(strcmp(checkPtr1->toString()->asChar(), "testString") == 0,
           "strVal should be testString.");
    map0.clear();

    CacheableKeyPtr keyObject7(new PdxTests::PdxTypes2());
    PdxTests::PdxTypes1Ptr valObject(new PdxTests::PdxTypes1());
    CacheableKeyPtr keyObject8(new PdxTests::PdxTypes2());
    PdxTests::PdxTypes1Ptr valObject2(new PdxTests::PdxTypes1());
    map0.insert(keyObject7, valObject);
    map0.insert(keyObject8, valObject2);
    regPtr0->putAll(map0, 15, CacheableInt32::create(1000));
    PdxTests::PdxTypes1Ptr objVal =
        dynCast<PdxTests::PdxTypes1Ptr>(regPtr0->get(keyObject7));
    ASSERT(valObject == objVal, "valObject and objVal should match.");

    regPtr0->localInvalidateRegion();

    VectorOfCacheableKey keys1;
    keys1.push_back(keyObject7);
    keys1.push_back(keyObject8);
    HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
    valuesMap->clear();
    regPtr0->getAll(keys1, valuesMap, NULLPTR, true);
    if (valuesMap->size() == keys1.size()) {
      char buf[2048];
      for (HashMapOfCacheable::Iterator iter = valuesMap->begin();
           iter != valuesMap->end(); iter++) {
        CacheableKeyPtr key = dynCast<CacheableKeyPtr>(iter.first());
        CacheablePtr mVal = iter.second();
        if (mVal != NULLPTR) {
          PdxTests::PdxTypes1Ptr val1 = dynCast<PdxTests::PdxTypes1Ptr>(mVal);
          sprintf(buf, "value from map %d , expected value %d ",
                  val1->getm_i1(), 34324);
          LOG(buf);
          ASSERT(val1->getm_i1() == 34324, "value not matched");
        }
      }
    }

    LOG("VerifyPutAllWithObjectKey complete.");
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

#endif /* ThinClientPutAllWITHCALLBACK_HPP_ */
