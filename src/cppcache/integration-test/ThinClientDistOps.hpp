/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientDistOps.hpp
 *
 *  Created on: Oct 22, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTDISTOPS_HPP_
#define THINCLIENTDISTOPS_HPP_

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include "testobject/PdxType.hpp"
#include <string>
#include <limits>

#define ROOT_NAME "ThinClientDistOps"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2
#define CREATE_TWICE_KEY "__create_twice_key"
#define CREATE_TWICE_VALUE "__create_twice_value"
CacheHelper* cacheHelper = NULL;
static bool isLocalServer = false;
static bool isLocator = false;
static int numberOfLocators = 0;

const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);

bool g_isGridClient = false;

DUNIT_TASK_DEFINITION(CLIENT1, Alter_Client_Grid_Property_1)
  { g_isGridClient = !g_isGridClient; }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Alter_Client_Grid_Property_2)
  { g_isGridClient = !g_isGridClient; }
END_TASK_DEFINITION

void initClient(const bool isthinClient) {
  if (cacheHelper == NULL) {
    PropertiesPtr config = Properties::create();
    if (g_isGridClient) {
      config->insert("grid-client", "true");
    }
    config->insert("log-level", "finer");
    cacheHelper = new CacheHelper(isthinClient, config);
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
                  bool clientNotificationEnabled = false,
                  bool cachingEnable = true) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr =
      getHelper()->createRegion(name, ackMode, cachingEnable, NULLPTR,
                                endpoints, clientNotificationEnabled);
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

void createPooledRegionSticky(const char* name, bool ackMode,
                              const char* locators, const char* poolname,
                              bool clientNotificationEnabled = false,
                              bool cachingEnable = true) {
  LOG("createRegion_Pool() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr = getHelper()->createPooledRegionSticky(
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

void createAndVerifyEntry(const char* name) {
  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  /*1. create new entry with long key and long value */
  int64_t int64Key = 9223372036854775807LL;   // INT64_MAX
  int64_t in64Value = 9223372036854775807LL;  // INT64_MAX
  regPtr->create(int64Key, in64Value);
  CacheableInt64Ptr longRetValue =
      dynCast<CacheableInt64Ptr>(regPtr->get((int64_t)int64Key));
  ASSERT(in64Value == longRetValue->value(),
         "longRetValue and longvalue should match");

  int64_t int64Key1 = 9223372036854775807LL;
  try {
    regPtr->create(int64Key1, in64Value);
    CacheableInt64Ptr longRetValue =
        dynCast<CacheableInt64Ptr>(regPtr->get((int64_t)int64Key));
    FAIL("Expected EntryExistException here");
  } catch (EntryExistsException e) {
    LOG(" Expected EntryExistsException exception thrown by localCreate");
  }

  /*2.create new entry with long key and string value*/
  int64_t int64KeyMin = -9223372036854775807LL - 1LL;  // INT64_MIN
  regPtr->create(int64KeyMin, "testvalue");
  CacheableStringPtr strRetValue = regPtr->get(int64KeyMin);
  ASSERT(strcmp(strRetValue->asChar(), "testvalue") == 0,
         "strRetValue and 'testvalue' should match");

  /*3.create new with entry nullptr key and nullptr value.
   * IllegalArgumentException thrown*/
  CacheableInt32Ptr x;
  try {
    regPtr->create(x, 1);
    LOG("Entry with null key and value created successfully");
    FAIL("Expected IllegalArgumentException here");
  } catch (IllegalArgumentException ex) {
    LOGINFO("Expected IllegalArgumentException : %s", ex.getMessage());
  }

  /*4.create new with entry nullptr key and string value*/
  try {
    regPtr->create(x, "testvalue");
    FAIL("Expected IllegalArgumentException here");
  } catch (IllegalArgumentException ex) {
    LOGINFO("Expected IllegalArgumentException : %s", ex.getMessage());
  }

  /*5.create new with entry userobject cantain all cacheable type ( like
   * cacheableInt,CacheableDouble, CacheableString,CacheableHashMap etc) key and
   * null value*/
  // Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
  CacheableKeyPtr keyObject1(new PdxTests::PdxType());
  regPtr->create(keyObject1, x);
  CacheablePtr retVal = regPtr->get(keyObject1);
  ASSERT(retVal == x, "retVal and x should match.");

  /*6.create new with entry userobject cantain all cacheable type ( like
   * cacheableInt,CacheableDouble, CacheableString,CacheableHashMap etc) key and
   * int value*/
  CacheableKeyPtr keyObject2(new PdxTests::PdxType());
  regPtr->create(keyObject2, 1);
  CacheableInt32Ptr intVal = regPtr->get(keyObject2);
  ASSERT(intVal->value() == 1, "intVal should be 1.");
  regPtr->invalidate(keyObject2);
  intVal = dynCast<CacheableInt32Ptr>(regPtr->get(keyObject2));
  ASSERT(intVal == NULLPTR, "intVal should be null.");

  try {
    if (regPtr->containsKey(keyObject2)) {
      regPtr->create(keyObject2, in64Value);
      FAIL("Expected EntryExistException here");
    }
  } catch (EntryExistsException e) {
    LOG(" Expected EntryExistsException exception thrown by localCreate");
  }

  /*7.create new with entry userobject cantain all cacheable type ( like
   * cacheableInt,CacheableDouble, CacheableString,CacheableHashMap etc) key and
   * string value*/
  CacheableKeyPtr keyObject3(new PdxTests::PdxType());
  regPtr->create(keyObject3, "testString");
  CacheablePtr strVal = regPtr->get(keyObject3);
  ASSERT(strcmp(strVal->toString()->asChar(), "testString") == 0,
         "strVal should be testString.");

  /*8.create new with entry userobject cantain all cacheable type ( like
   * cacheableInt,CacheableDouble, CacheableString,CacheableHashMap etc) key and
   * userobject
   * cantain all cacheable type ( like cacheableInt,CacheableDouble,
   * CacheableString,CacheableHashMap etc)  value*/
  CacheableKeyPtr keyObject4(new PdxTests::PdxType());
  PdxTests::PdxTypePtr valObject(new PdxTests::PdxType());
  regPtr->create(keyObject4, valObject);
  PdxTests::PdxTypePtr objVal =
      dynCast<PdxTests::PdxTypePtr>(regPtr->get(keyObject4));
  ASSERT(valObject == objVal, "valObject and objVal should match.");

  /*9.create new entry witn non serialize object. IllegalArgumentException
   * thrown*/
  // This gives compile time error
  //  class Person
  //  {
  //    int age;
  //    char *name;
  //    public:
  //      Person()
  //      {
  //        age= 1;
  //        name="testuser";
  //      }
  //  };
  //  try{
  //    regPtr->create(new Person(), 1);
  //    FAIL("Expected IllegalArgumentException here");
  //  }catch(IllegalArgumentException ex){
  //    LOG("Expected IllegalArgumentException : %s");
  //  }
  //
  //  try{
  //    regPtr->create(100,new Person());
  //    FAIL("Expected IllegalArgumentException here");
  //  }catch(IllegalArgumentException ex){
  //    LOG("Expected IllegalArgumentException : %s");
  //  }
}

void createEntryTwice(const char* name, const char* key, const char* value) {
  LOG("createEntryTwice() entered.");
  char message[500];
  sprintf(message, "Creating entry -- key: %s  value: %s in region %s\n", key,
          value, name);
  LOG(message);
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableStringPtr valPtr = CacheableString::create(value);
  RegionPtr regPtr = getHelper()->getRegion(name);
  regPtr->create(keyPtr, valPtr);
  try {
    regPtr->create(keyPtr, valPtr);
  } catch (const EntryExistsException& gemfireExcp) {
    LOG(gemfireExcp.getMessage());
    LOG("createEntryTwice() Clean Exit.");
    return;
  }
  ASSERT(false,
         "Creating key twice is not allowed and while doing that exception was "
         "not thrown");
  return;  // This return will never reach
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

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck", "testregion",
                             "CreateVerifyTestRegion"};

const bool USE_ACK = true;
const bool NO_ACK = false;
#include "LocatorHelper.hpp"

DUNIT_TASK_DEFINITION(CLIENT1, CreateNonexistentServerRegion_Pooled_Locator)
  {
    initClient(true);
    createPooledRegion("non-region", USE_ACK, locatorsG, "__TESTPOOL1_");
    try {
      createEntry("non-region", keys[0], vals[0]);
      FAIL(
          "Expected exception when doing operations on a non-existent region.");
    } catch (const CacheServerException& ex) {
      printf(
          "Got expected CacheServerException when performing operation "
          "on a non-existent region: %s\n",
          ex.getMessage());
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,
                      CreateNonexistentServerRegion_Pooled_Locator_Sticky)
  {
    initClient(true);
    createPooledRegionSticky("non-region", USE_ACK, locatorsG, "__TESTPOOL1_");
    try {
      createEntry("non-region", keys[0], vals[0]);
      FAIL(
          "Expected exception when doing operations on a non-existent region.");
    } catch (const CacheServerException& ex) {
      printf(
          "Got expected CacheServerException when performing operation "
          "on a non-existent region: %s\n",
          ex.getMessage());
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreatePoolForUpdateLocatorList)
  {
    /*
    PoolPtr  createPool(const char* poolName, const char* locators, const char*
    serverGroup,
                  const char* servers = NULL, int redundancy = 0, bool
    clientNotification = false, int subscriptionAckInterval = -1,
                  int connections = -1, int loadConditioningInterval = - 1, bool
    isMultiuserMode = false, int updateLocatorListInterval = 5000 )
    */
    initClient(true);
    getHelper()->createPool("__TESTPOOL1_", locatorsG, NULL, 0, false, -1, -1,
                            -1, false);
    LOG("CreatePoolForUpdateLocatorList complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreatePoolForDontUpdateLocatorList)
  {
    /*
    PoolPtr  createPool(const char* poolName, const char* locators, const char*
    serverGroup,
                  const char* servers = NULL, int redundancy = 0, bool
    clientNotification = false, int subscriptionAckInterval = -1,
                  int connections = -1, int loadConditioningInterval = - 1, bool
    isMultiuserMode = false, int updateLocatorListInterval = 5000 )
    */
    initClient(true);
    getHelper()->createPool("__TESTPOOL1_", locatorsG, NULL, 0, false, -1, -1,
                            -1, false);
    LOG("CreatePoolForDontUpdateLocatorList complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyUpdateLocatorListThread)
  {
    int sleepSeconds = 60;
    dunit::sleep(sleepSeconds * 1000);

    PoolPtr pptr = CacheHelper::getPoolPtr("__TESTPOOL1_");
    int updateIntervalSeconds = pptr->getUpdateLocatorListInterval() / 1000;

    int numLocatorListUpdates =
        CacheHelper::getNumLocatorListUpdates("Querying locator list at:");

    int numExpectedLocatorListUpdates = 0;
    if (updateIntervalSeconds > 0)
      numExpectedLocatorListUpdates = sleepSeconds / updateIntervalSeconds;

    if (numExpectedLocatorListUpdates > 0) {
      // Log scraping is fragile! We're conservative since client logs are
      // *appended* from other tasks in this giant testcase...
      //    Assert(numLocatorListUpdates <= numExpectedLocatorListUpdates+1) is
      //    too strict.
      //    Assert(numLocatorListUpdates <= numExpectedLocatorListUpdates+2) is
      //    ideal.
      ASSERT(numLocatorListUpdates >= numExpectedLocatorListUpdates -
                                          1 /*&& numLocatorListUpdates <=
                                               numExpectedLocatorListUpdates+2*/
             ,
             "Got unexpected number of LocatorList updates");
    } else {
      ASSERT(
          numLocatorListUpdates == 0,
          "Got unexpected LocatorList updates with LocatorUpdateInterval set "
          "to 0");
    }

    LOG("VerifyUpdateLocatorListThread complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
  {
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_");
    createPooledRegion(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_");
    createPooledRegion(regionNames[3], NO_ACK, locatorsG, "__TESTPOOL1_");
    LOG("StepOne_Pooled complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator_Sticky)
  {
    createPooledRegionSticky(regionNames[0], USE_ACK, locatorsG,
                             "__TESTPOOL1_");
    createPooledRegionSticky(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_");
    createPooledRegionSticky(regionNames[3], NO_ACK, locatorsG, "__TESTPOOL1_");
    LOG("StepOne_Pooled_Locator_Sticky complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_Locator)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_");
    createPooledRegion(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_");
    createPooledRegion(regionNames[3], NO_ACK, locatorsG, "__TESTPOOL1_");
    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_Locator_Sticky)
  {
    initClient(true);
    createPooledRegionSticky(regionNames[0], USE_ACK, locatorsG,
                             "__TESTPOOL1_");
    createPooledRegionSticky(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_");
    createPooledRegionSticky(regionNames[3], NO_ACK, locatorsG, "__TESTPOOL1_");
    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    createEntry(regionNames[0], keys[0], vals[0]);
    createEntry(regionNames[1], keys[2], vals[2]);
    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree_CreateTests)
  {
    LOG("StepThree_CreateTests started.");
    createAndVerifyEntry(regionNames[3]);
    // createEntry( regionNames[1], keys[2], vals[2] );
    LOG("StepThree_CreateTests complete.");
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

DUNIT_TASK_DEFINITION(CLIENT1, StepFive)
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
    updateEntry(regionNames[0], keys[0], nvals[0]);
    updateEntry(regionNames[1], keys[2], nvals[2]);
    LOG("StepFive complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepSix)
  {
    doNetsearch(regionNames[0], keys[0], vals[0]);
    doNetsearch(regionNames[1], keys[2], vals[2]);
    updateEntry(regionNames[0], keys[1], nvals[1]);
    updateEntry(regionNames[1], keys[3], nvals[3]);
    LOG("StepSix complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSeven)
  { createEntryTwice(regionNames[0], CREATE_TWICE_KEY, CREATE_TWICE_VALUE); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepEight_Pool)
  {
    createPooledRegion(regionNames[2], NO_ACK, locatorsG, "__TESTPOOL1_", false,
                       false);
    RegionPtr reg = getHelper()->getRegion(regionNames[2]);
    LOG("REGION Created with Caching Enabled false");
    CacheableKeyPtr keyPtr = createKey(CREATE_TWICE_KEY);
    CacheableStringPtr valPtr = CacheableString::create(CREATE_TWICE_VALUE);
    try {
      reg->create(keyPtr, valPtr);
      char message[200];
      sprintf(message, "First create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
      reg->create(keyPtr, valPtr);
      sprintf(message, "Second create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
      reg->create(keyPtr, valPtr);
      sprintf(message, "Third create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
    } catch (const EntryExistsException& gemfireExcp) {
      LOG(gemfireExcp.getMessage());
      ASSERT(false,
             "Creating KEY Twice on a caching-enabled false region should be "
             "allowed.");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepEight_Pool_Sticky)
  {
    createPooledRegionSticky(regionNames[2], NO_ACK, locatorsG, "__TESTPOOL1_",
                             false, false);
    RegionPtr reg = getHelper()->getRegion(regionNames[2]);
    LOG("REGION Created with Caching Enabled false");
    CacheableKeyPtr keyPtr = createKey(CREATE_TWICE_KEY);
    CacheableStringPtr valPtr = CacheableString::create(CREATE_TWICE_VALUE);

    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);
    reg0->localInvalidate(createKey(keys[1]));
    reg1->localInvalidate(createKey(keys[3]));
    PoolPtr pool = PoolManager::find("__TESTPOOL1_");
    ASSERT(pool != NULLPTR, "Pool Should have been found");
    doNetsearch(regionNames[0], keys[1], nvals[1]);
    doNetsearch(regionNames[1], keys[3], nvals[3]);
    pool->releaseThreadLocalConnection();
    updateEntry(regionNames[0], keys[0], nvals[0]);
    updateEntry(regionNames[1], keys[2], nvals[2]);
    pool->releaseThreadLocalConnection();
    try {
      reg->create(keyPtr, valPtr);
      char message[200];
      sprintf(message, "First create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
      reg->create(keyPtr, valPtr);
      sprintf(message, "Second create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
      reg->create(keyPtr, valPtr);
      sprintf(message, "Third create on Key %s ", CREATE_TWICE_KEY);
      LOG(message);
    } catch (const EntryExistsException& gemfireExcp) {
      LOG(gemfireExcp.getMessage());
      ASSERT(false,
             "Creating KEY Twice on a caching-enabled false region should be "
             "allowed.");
    }
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

void runDistOpsNotSticky() {
  CALL_TASK(Alter_Client_Grid_Property_1);
  CALL_TASK(Alter_Client_Grid_Property_2);

  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator)

  CALL_TASK(CreateNonexistentServerRegion_Pooled_Locator);
  CALL_TASK(StepOne_Pooled_Locator);
  CALL_TASK(StepTwo_Pooled_Locator);

  CALL_TASK(StepThree);
  CALL_TASK(StepThree_CreateTests);
  CALL_TASK(StepFour);
  CALL_TASK(StepFive);
  CALL_TASK(StepSix);
  CALL_TASK(StepSeven);

  CALL_TASK(StepEight_Pool);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator1);
}

void runDistOpsSticky() {
  CALL_TASK(Alter_Client_Grid_Property_1);
  CALL_TASK(Alter_Client_Grid_Property_2);

  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator)

  CALL_TASK(CreateNonexistentServerRegion_Pooled_Locator_Sticky);
  CALL_TASK(StepOne_Pooled_Locator_Sticky);
  CALL_TASK(StepTwo_Pooled_Locator_Sticky);

  CALL_TASK(StepThree);
  CALL_TASK(StepThree_CreateTests);
  CALL_TASK(StepFour);
  CALL_TASK(StepFive);
  CALL_TASK(StepSix);
  CALL_TASK(StepSeven);

  CALL_TASK(StepEight_Pool_Sticky);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator1);
}

void runDistOpsUpdateLocatorList() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator)

  CALL_TASK(CreatePoolForUpdateLocatorList);

  CALL_TASK(VerifyUpdateLocatorListThread);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseServer1);
  CALL_TASK(CloseLocator1);
}

void runDistOpsDontUpdateLocatorList() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator)

  CALL_TASK(CreatePoolForDontUpdateLocatorList);

  CALL_TASK(VerifyUpdateLocatorListThread);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseServer1);
  CALL_TASK(CloseLocator1);
}

#endif /* THINCLIENTDISTOPS_HPP_ */
