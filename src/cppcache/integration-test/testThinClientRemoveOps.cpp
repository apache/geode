/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef TESTTHINCLIENTREMOVEOPS_HPP_
#define TESTTHINCLIENTREMOVEOPS_HPP_

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include "testobject/PdxType.hpp"
#include "testobject/VariousPdxTypes.hpp"

#include <string>

#define ROOT_NAME "testThinClientRemoveOps"
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

void createRegion(const char* name, bool ackMode,
                  bool clientNotificationEnabled = false,
                  bool cachingEnable = true) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr =
      getHelper()->createRegion(name, ackMode, cachingEnable, NULLPTR,
                                clientNotificationEnabled);
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
  RegionPtr regPtr = getHelper()->createPooledRegion(
      name, ackMode, locators, poolname, cachingEnable,
      clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Pooled Region created.");
}

void putEntry(const char* name, const char* key, const char* value) {
  LOG("putEntry() entered.");
  fprintf(stdout, "Creating entry -- key: %s  value: %s in region %s\n", key,
          value, name);
  fflush(stdout);
  // Put entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableStringPtr valPtr = CacheableString::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT(!regPtr->containsKey(keyPtr),
         "Key should not have been found in region.");
  ASSERT(!regPtr->containsValueForKey(keyPtr),
         "Value should not have been found in region.");

  regPtr->put(keyPtr, valPtr);
  LOG("Created entry.");

  verifyEntry(name, key, value);
  LOG("Entry created.");
}

void localPutEntry(const char* name, const char* key, const char* value) {
  LOG("putEntry() entered.");
  fprintf(stdout, "Creating entry -- key: %s  value: %s in region %s\n", key,
          value, name);
  fflush(stdout);
  // Put entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableStringPtr valPtr = CacheableString::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  regPtr->localPut(keyPtr, valPtr);
  LOG("LocalPut entry done.");

  verifyEntry(name, key, value);
  LOG("Local Entry created.");
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

const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4", "Non-Existent-Key"};
const char* vals[] = {"Value-1", "Value-2", "Value-3", "Value-4",
                      "Non-Existent-Value"};
const char* nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                       "New Value-4"};

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck",
                             "exampleRegion"};

const bool USE_ACK = true;
const bool NO_ACK = false;
//#include "LocatorHelper.hpp"

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) CacheHelper::initServer(1);
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
  {
    if (isLocalServer) CacheHelper::initServer(2, "cacheserver1_expiry.xml");
    LOG("SERVER2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2_locator)
  {
    if (isLocalServer) {
      CacheHelper::initServer(2, "cacheserver1_expiry.xml", locatorsG);
    }
    LOG("SERVER2 with locator started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator)
  {
    if (isLocalServer) CacheHelper::initServer(1, NULL, locatorsG);
    LOG("SERVER1 with locator started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG,
                       "__TESTPOOL1_");
    createPooledRegion(regionNames[1], NO_ACK,  locatorsG, "__TESTPOOL1_");
    LOG("StepOne_Pooled complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSeven_Pooled_Locator)
  {
    initClient(true);
    createPooledRegion(regionNames[2], USE_ACK,  locatorsG,
                       "__TESTPOOL1_");
    LOG("StepSeven_Pooled_Locator complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_Locator)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG,
                       "__TESTPOOL1_");
    createPooledRegion(regionNames[1], NO_ACK,  locatorsG, "__TESTPOOL1_");
    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    putEntry(regionNames[0], keys[0], vals[0]);
    putEntry(regionNames[1], keys[2], vals[2]);
    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFive)
  {
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);

    CacheableKeyPtr keyPtr = CacheableString::create(keys[0]);
    CacheableKeyPtr keyPtr1 = CacheableString::create(keys[2]);

    // Try removing non-existent entry from regions, result should be false.
    ASSERT(reg0->remove(keys[4], vals[4]) == false,
           "Result of remove should be false, as this entry is not present in "
           "first region.");
    ASSERT(reg1->remove(keys[4], vals[4]) == false,
           "Result of remove should be false, as this entry is not present in "
           "second region.");

    // Try removing non-existent key, but existing value from regions, result
    // should be false.
    ASSERT(reg0->remove(keys[4], vals[0]) == false,
           "Result of remove should be false, as this key is not present in "
           "first region.");
    ASSERT(reg1->remove(keys[4], vals[0]) == false,
           "Result of remove should be false, as this key is not present in "
           "second region.");

    // Try removing existent key, but non-existing value from regions, result
    // should be false.
    ASSERT(reg0->remove(keys[0], vals[4]) == false,
           "Result of remove should be false, as this value is not present in "
           "first region.");
    ASSERT(reg1->remove(keys[0], vals[4]) == false,
           "Result of remove should be false, as this value is not present in "
           "second region.");

    // Try removing existent key, and existing value from regions, result should
    // be true.
    ASSERT(reg0->remove(keys[0], vals[0]) == true,
           "Result of remove should be true, as this entry is present in first "
           "region.");
    ASSERT(
        reg1->remove(keys[2], vals[2]) == true,
        "Result of remove should be true, as this entry is present in second "
        "region.");

    ASSERT(reg0->containsKey(keys[0]) == false, "containsKey should be false");
    ASSERT(reg0->containsKeyOnServer(keyPtr) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg1->containsKey(keys[2]) == false, "containsKey should be false");
    ASSERT(reg1->containsKeyOnServer(keyPtr1) == false,
           "containsKeyOnServer should be false");

    // Try removing already deleted entry from regions, result should be false,
    // but no exception.
    ASSERT(reg0->remove(keys[0], vals[0]) == false,
           "Result of remove should be false, as this entry is not present in "
           "first region.");
    ASSERT(reg1->remove(keys[0], vals[0]) == false,
           "Result of remove should be false, as this entry is not present in "
           "second region.");

    // Try locally destroying already deleted entry from regions, It should
    // result
    // into exception.
    try {
      reg0->localDestroy(keys[0]);
      FAIL(
          "local destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "localDestroy operation on already removed entry.");
    }

    try {
      reg1->localDestroy(keys[0]);
      FAIL(
          "local destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "localDestroy operation on already removed entry.");
    }
    LOG("StepFive complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepSix)
  {
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);

    CacheableKeyPtr keyPtr = CacheableString::create(keys[1]);
    CacheableKeyPtr keyPtr1 = CacheableString::create(keys[3]);

    putEntry(regionNames[0], keys[1], nvals[1]);
    putEntry(regionNames[1], keys[3], nvals[3]);

    // Try removing value that is present on client as well as server, result
    // should be true.
    ASSERT(
        reg0->remove(keys[1], nvals[1]) == true,
        "Result of remove should be true, as this value is present locally, & "
        "also present on server.");
    ASSERT(
        reg1->remove(keys[3], nvals[3]) == true,
        "Result of remove should be true, as this value is present locally, & "
        "also present on server.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg0->containsKeyOnServer(keyPtr) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    ASSERT(reg1->containsKeyOnServer(keyPtr1) == false,
           "containsKeyOnServer should be false");
    LOGINFO("Step 6.1 complete.");

    // Try removing value that is present on client but not on server, result
    // should be false.
    putEntry(regionNames[0], keys[1], vals[1]);
    putEntry(regionNames[1], keys[3], vals[3]);
    localPutEntry(regionNames[0], keys[1], nvals[1]);
    localPutEntry(regionNames[1], keys[3], nvals[3]);
    ASSERT(
        reg0->remove(keys[1], nvals[1]) == false,
        "Result of remove should be false, as this value is present locally, "
        "but not present on server.");
    ASSERT(
        reg1->remove(keys[3], nvals[3]) == false,
        "Result of remove should be false, as this value is present locally, "
        "but not present on server.");
    ASSERT(reg0->containsKey(keys[1]) == true, "containsKey should be true");
    ASSERT(reg0->containsKeyOnServer(keyPtr) == true,
           "containsKeyOnServer should be true");
    ASSERT(reg1->containsKey(keys[3]) == true, "containsKey should be true");
    ASSERT(reg1->containsKeyOnServer(keyPtr1) == true,
           "containsKeyOnServer should be true");
    LOGINFO("Step 6.2 complete.");

    // Try removing value that is not present on client but present on server,
    // result should be false.
    reg0->destroy(keys[1]);
    reg1->destroy(keys[3]);
    putEntry(regionNames[0], keys[1], vals[1]);
    putEntry(regionNames[1], keys[3], vals[3]);
    localPutEntry(regionNames[0], keys[1], nvals[1]);
    localPutEntry(regionNames[1], keys[3], nvals[3]);
    ASSERT(reg0->remove(keys[1], vals[1]) == false,
           "Result of remove should be false, as this value is not present "
           "locally, but present only on server.");
    ASSERT(reg1->remove(keys[3], vals[3]) == false,
           "Result of remove should be false, as this value is not present "
           "locally, but present only on server.");
    ASSERT(reg0->containsKey(keys[1]) == true, "containsKey should be true");
    ASSERT(reg0->containsKeyOnServer(keyPtr) == true,
           "containsKeyOnServer should be true");
    ASSERT(reg1->containsKey(keys[3]) == true, "containsKey should be true");
    ASSERT(reg1->containsKeyOnServer(keyPtr1) == true,
           "containsKeyOnServer should be true");
    LOGINFO("Step 6.3 complete.");

    // Try removing value that is invalidated on client but exists on server,
    // result should be false.
    reg0->destroy(keys[1]);
    reg1->destroy(keys[3]);
    putEntry(regionNames[0], keys[1], nvals[1]);
    putEntry(regionNames[1], keys[3], nvals[3]);
    reg0->invalidate(keys[1]);
    reg1->invalidate(keys[3]);
    ASSERT(reg0->remove(keys[1], nvals[1]) == false,
           "Result of remove should be false, as this value is not present "
           "locally, but present only on server.");
    ASSERT(reg1->remove(keys[3], nvals[3]) == false,
           "Result of remove should be false, as this value is not present "
           "locally, but present only on server.");
    ASSERT(reg0->containsKey(keys[1]) == true, "containsKey should be true");
    ASSERT(reg0->containsKeyOnServer(keyPtr) == true,
           "containsKeyOnServer should be true");
    ASSERT(reg1->containsKey(keys[3]) == true, "containsKey should be true");
    ASSERT(reg1->containsKeyOnServer(keyPtr1) == true,
           "containsKeyOnServer should be true");
    LOGINFO("Step 6.4 complete.");

    // Try removing null value, that is invalidated on client but exists on the
    // server, result should be false.
    reg0->destroy(keys[1]);
    reg1->destroy(keys[3]);
    putEntry(regionNames[0], keys[1], vals[1]);
    putEntry(regionNames[1], keys[3], vals[3]);
    reg0->localInvalidate(keys[1]);
    reg1->localInvalidate(keys[3]);
    ASSERT(reg0->remove(keys[1], (CacheablePtr)NULLPTR) == false,
           "Result of remove should be false, as this value is not present "
           "locally, but present only on server.");
    ASSERT(reg1->remove(keys[3], (CacheablePtr)NULLPTR) == false,
           "Result of remove should be false, as this value is not present "
           "locally, but present only on server.");
    ASSERT(reg0->containsKey(keys[1]) == true, "containsKey should be true");
    ASSERT(reg0->containsKeyOnServer(keyPtr) == true,
           "containsKeyOnServer should be true");
    ASSERT(reg1->containsKey(keys[3]) == true, "containsKey should be true");
    ASSERT(reg1->containsKeyOnServer(keyPtr1) == true,
           "containsKeyOnServer should be true");
    LOGINFO("Step 6.5 complete.");

    // Try removing a entry (value) which is not present on client as well as
    // server, result should be false.
    ASSERT(reg0->remove("NewKey1", "NewValue1") == false,
           "Result of remove should be false, as this value is not present "
           "locally, and not present on server.");
    ASSERT(reg1->remove("NewKey3", "NewValue3") == false,
           "Result of remove should be false, as this value is not present "
           "locally, and not present on server.");
    CacheableKeyPtr keyPtr2 = CacheableString::create("NewKey1");
    CacheableKeyPtr keyPtr3 = CacheableString::create("NewKey3");
    ASSERT(reg0->containsKey("NewKey1") == false,
           "containsKey should be false");
    ASSERT(reg0->containsKeyOnServer(keyPtr2) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg1->containsKey("NewKey3") == false,
           "containsKey should be false");
    ASSERT(reg1->containsKeyOnServer(keyPtr3) == false,
           "containsKeyOnServer should be false");
    LOGINFO("Step 6.6 complete.");

    // Try removing a entry with a null value, which is not present on client as
    // well as server, result should be false.
    ASSERT(reg0->remove("NewKey1", (CacheablePtr)NULLPTR) == false,
           "Result of remove should be false, as this value is not present "
           "locally, and not present on server.");
    ASSERT(reg1->remove("NewKey3", (CacheablePtr)NULLPTR) == false,
           "Result of remove should be false, as this value is not present "
           "locally, and not present on server.");
    ASSERT(reg0->containsKey("NewKey1") == false,
           "containsKey should be false");
    ASSERT(reg0->containsKeyOnServer(keyPtr2) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg1->containsKey("NewKey3") == false,
           "containsKey should be false");
    ASSERT(reg1->containsKeyOnServer(keyPtr3) == false,
           "containsKeyOnServer should be false");
    LOGINFO("Step 6.7 complete.");

    // Try removing a entry (value) which is not present on client but exists on
    // the server, result should be true.
    reg0->destroy(keys[1]);
    reg1->destroy(keys[3]);
    putEntry(regionNames[0], keys[1], nvals[1]);
    putEntry(regionNames[1], keys[3], nvals[3]);
    reg0->localDestroy(keys[1]);
    reg1->localDestroy(keys[3]);
    ASSERT(reg0->remove(keys[1], nvals[1]) == true,
           "Result of remove should be true, as this value does not exist "
           "locally, but exists on server.");
    ASSERT(reg1->remove(keys[3], nvals[3]) == true,
           "Result of remove should be true, as this value does not exist "
           "locally, but exists on server.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg0->containsKeyOnServer(keyPtr) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    ASSERT(reg1->containsKeyOnServer(keyPtr1) == false,
           "containsKeyOnServer should be false");
    LOG("Step6.8 complete.");

    putEntry(regionNames[0], keys[1], nvals[1]);
    putEntry(regionNames[1], keys[3], nvals[3]);
    reg0->destroy(keys[1]);
    reg1->destroy(keys[3]);
    ASSERT(reg0->remove(keys[1], (CacheablePtr)NULLPTR) == false,
           "Result of remove should be false, as this value does not exist "
           "locally, but exists on server.");
    ASSERT(reg1->remove(keys[3], (CacheablePtr)NULLPTR) == false,
           "Result of remove should be false, as this value does not exist "
           "locally, but exists on server.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg0->containsKeyOnServer(keyPtr) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    ASSERT(reg1->containsKeyOnServer(keyPtr1) == false,
           "containsKeyOnServer should be false");
    LOG("Step6.8.1 complete.");

    // Try locally removing an entry which is locally destroyed with a NULL.
    reg0->put(keys[1], vals[1]);
    reg1->put(keys[3], vals[3]);
    ASSERT(reg0->remove(keys[1], vals[1]) == true,
           "Result of remove should be true, as this value does not exists "
           "locally.");
    ASSERT(reg0->remove(keys[1], vals[1]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->remove(keys[3], vals[3]) == true,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->remove(keys[3], vals[3]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    LOG("Step6.8.2 complete.");

    //-------------------------------------localRemove
    // Testcases------------------------------------------------
    // Try locally removing an entry (value) which is present on the client.
    // reg0->destroy(keys[1]);
    try {
      reg0->destroy(keys[1]);
      FAIL(
          "destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "destroy operation on already removed entry.");
    }
    try {
      reg1->destroy(keys[3]);
      FAIL(
          "destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "destroy operation on already removed entry.");
    }
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    ASSERT(reg0->localRemove(keys[1], vals[1]) == true,
           "Result of remove should be true, as this value exists locally.");
    ASSERT(reg1->localRemove(keys[3], vals[3]) == true,
           "Result of remove should be true, as this value exists locally.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    LOG("Step6.9 complete.");

    // Try local destroy on entry that is already removed, should get an
    // exception.
    try {
      reg0->localDestroy(keys[1]);
      FAIL(
          "local destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "localDestroy operation on already removed entry.");
    }
    try {
      reg1->localDestroy(keys[3]);
      FAIL(
          "local destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "localDestroy operation on already removed entry.");
    }
    LOG("Step6.10 complete.");

    // Try locally removing an entry (value) which is not present on the client
    // (value mismatch).
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    ASSERT(reg0->localRemove(keys[1], nvals[1]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->localRemove(keys[3], nvals[3]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg0->containsKey(keys[1]) == true, "containsKey should be true");
    ASSERT(reg1->containsKey(keys[3]) == true, "containsKey should be true");
    LOG("Step6.11 complete.");

    // Try locally removing an entry (value) which is invalidated with a value.
    reg0->localDestroy(keys[1]);
    reg1->localDestroy(keys[3]);
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    reg0->invalidate(keys[1]);
    reg1->invalidate(keys[3]);
    ASSERT(reg0->localRemove(keys[1], vals[1]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->localRemove(keys[3], vals[3]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg0->containsKey(keys[1]) == true, "containsKey should be true");
    ASSERT(reg1->containsKey(keys[3]) == true, "containsKey should be true");
    LOG("Step6.12 complete.");

    // Try locally removing an entry (value) which is invalidated with a NULL.
    reg0->localDestroy(keys[1]);
    reg1->localDestroy(keys[3]);
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    reg0->invalidate(keys[1]);
    reg1->invalidate(keys[3]);
    ASSERT(reg0->localRemove(keys[1], (CacheablePtr)NULLPTR) == true,
           "Result of remove should be true, as this value does not exists "
           "locally.");
    ASSERT(reg1->localRemove(keys[3], (CacheablePtr)NULLPTR) == true,
           "Result of remove should be true, as this value does not exists "
           "locally.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    LOG("Step6.13 complete.");

    // Try locally removing an entry (value) with a NULL.
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    ASSERT(reg0->localRemove(keys[1], (CacheablePtr)NULLPTR) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->localRemove(keys[3], (CacheablePtr)NULLPTR) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg0->containsKey(keys[1]) == true, "containsKey should be true");
    ASSERT(reg1->containsKey(keys[3]) == true, "containsKey should be true");
    LOG("Step6.14 complete.");

    // Try locally removing an entry which is locally destroyed with a value.
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    reg0->localDestroy(keys[1]);
    reg1->localDestroy(keys[3]);
    ASSERT(reg0->localRemove(keys[1], vals[1]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->localRemove(keys[3], vals[3]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be true");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be true");
    LOG("Step6.15 complete.");

    // Try locally removing an entry which is locally destroyed with a NULL.
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    reg0->localDestroy(keys[1]);
    reg1->localDestroy(keys[3]);
    ASSERT(reg0->localRemove(keys[1], (CacheablePtr)NULLPTR) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->localRemove(keys[3], (CacheablePtr)NULLPTR) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    LOG("Step6.16 complete.");

    // Try locally removing an entry which is already removed.
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    ASSERT(reg0->localRemove(keys[1], vals[1]) == true,
           "Result of remove should be true, as this value does not exists "
           "locally.");
    ASSERT(reg0->localRemove(keys[1], vals[1]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->localRemove(keys[3], vals[3]) == true,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->localRemove(keys[3], vals[3]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    LOG("Step6.17 complete.");
    // Try locally removing an entry when region scope is not null.

    LOG("StepSix complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepEight)
  {
    RegionPtr reg = getHelper()->getRegion(regionNames[2]);

    CacheableKeyPtr keyPtr = CacheableString::create(keys[0]);
    CacheableKeyPtr keyPtr1 = CacheableString::create(keys[1]);

    // Try removing a entry which is present on client (value) but invalidated
    // on
    // the server, result should be false.
    try {
      reg->destroy(keys[0]);
      FAIL(
          "destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "destroy operation on already removed entry.");
    }
    try {
      reg->destroy(keys[1]);
      FAIL(
          "destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "destroy operation on already removed entry.");
    }
    putEntry(regionNames[2], keys[0], vals[0]);
    putEntry(regionNames[2], keys[1], vals[1]);
    SLEEP(10000);  // This is for expiration on server to execute.
    ASSERT(
        reg->remove(keys[0], vals[0]) == false,
        "Result of remove should be false, as this value is present locally, "
        "but not present on server.");
    ASSERT(
        reg->remove(keys[1], vals[1]) == false,
        "Result of remove should be false, as this value is present locally, "
        "but not present on server.");
    ASSERT(reg->containsKey(keys[0]) == true, "containsKey should be true");
    ASSERT(reg->containsKeyOnServer(keyPtr) == true,
           "containsKeyOnServer should be true");
    ASSERT(reg->containsKey(keys[1]) == true, "containsKey should be true");
    ASSERT(reg->containsKeyOnServer(keyPtr1) == true,
           "containsKeyOnServer should be true");
    LOG("Step 8.1 complete.");

    // Try removing a entry that is not present on client, but invalidated on
    // server with null value, result should be true.
    reg->destroy(keys[0]);
    reg->destroy(keys[1]);
    putEntry(regionNames[2], keys[0], nvals[0]);
    putEntry(regionNames[2], keys[1], nvals[1]);
    reg->localDestroy(keys[0]);
    reg->localDestroy(keys[1]);
    SLEEP(10000);  // This is for expiration on server to execute.
    ASSERT(reg->remove(keys[0], (CacheablePtr)NULLPTR) == true,
           "Result of remove should be true, as this value is not present "
           "locally, & not present on server.");
    ASSERT(reg->remove(keys[1], (CacheablePtr)NULLPTR) == true,
           "Result of remove should be true, as this value is not present "
           "locally, & not present on server.");
    ASSERT(reg->containsKeyOnServer(keyPtr) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg->containsKey(keys[0]) == false, "containsKey should be false");
    ASSERT(reg->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg->containsKeyOnServer(keyPtr1) == false,
           "containsKeyOnServer should be false");
    LOG("Step 8.2 complete.");

    // Try removing a entry with a (value) on client that is invalidated on
    // server
    // with null , result should be false.
    try {
      reg->destroy(keys[0]);
      FAIL(
          "destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "destroy operation on already removed entry.");
    }
    try {
      reg->destroy(keys[1]);
      FAIL(
          "destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "destroy operation on already removed entry.");
    }
    putEntry(regionNames[2], keys[0], nvals[0]);
    putEntry(regionNames[2], keys[1], nvals[1]);
    SLEEP(10000);  // This is for expiration on server to execute.
    ASSERT(
        reg->remove(keys[0], (CacheablePtr)NULLPTR) == false,
        "Result of remove should be false, as this value is present locally, "
        "& not present on server.");
    ASSERT(
        reg->remove(keys[1], (CacheablePtr)NULLPTR) == false,
        "Result of remove should be false, as this value is present locally, "
        "& not present on server.");
    ASSERT(reg->containsKey(keys[0]) == true, "containsKey should be true");
    ASSERT(reg->containsKeyOnServer(keyPtr) == true,
           "containsKeyOnServer should be true");
    ASSERT(reg->containsKey(keys[1]) == true, "containsKey should be true");
    ASSERT(reg->containsKeyOnServer(keyPtr1) == true,
           "containsKeyOnServer should be true");
    LOG("Step 8.3 complete.");

    // Try removing a entry with a entry that is invalidated on the client as
    // well
    // as on server with a null value, result should be true.
    reg->destroy(keys[0]);
    reg->destroy(keys[1]);
    putEntry(regionNames[2], keys[0], nvals[0]);
    putEntry(regionNames[2], keys[1], nvals[1]);
    reg->invalidate(keys[0]);
    reg->invalidate(keys[1]);
    SLEEP(10000);  // This is for expiration on server to execute.
    ASSERT(reg->remove(keys[0], (CacheablePtr)NULLPTR) == true,
           "Result of remove should be true, as this value is not present "
           "locally, & not present on server.");
    ASSERT(reg->remove(keys[1], (CacheablePtr)NULLPTR) == true,
           "Result of remove should be true, as this value is not present "
           "locally, & not present on server.");
    ASSERT(reg->containsKey(keys[0]) == false, "containsKey should be false");
    ASSERT(reg->containsKeyOnServer(keyPtr) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg->containsKeyOnServer(keyPtr1) == false,
           "containsKeyOnServer should be false");

    // Test case for Bug #639, destroy operation on key that is not present in
    // the
    // region reduces the region's size by 1.
    // Steps to reproduce: Put 2 entries in to region. Destroy an entry that is
    // not present in the region. Check for the sizes of keys, values and
    // region.
    // It is observed that regions size is less by 1 from that of keys and
    // values
    // sizes.
    reg->put("Key100", "Value100");
    reg->put("Key200", "Value200");
    LOGINFO("Region 2 puts complete ");
    LOGINFO("Regions size = %d ", reg->size());

    try {
      reg->destroy("key300");
      FAIL(
          "destroy on non=existent key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException& /*ex*/) {
      LOG("Got expected EntryNotFoundException for key300");
    }

    ASSERT(reg->size() == 2, "region size should be equal to 2");

    VectorOfCacheableKey keys;
    reg->keys(keys);
    LOGINFO("Region keys = %d ", keys.size());
    ASSERT(keys.size() == reg->size(),
           "region size should be equal to keys size");

    VectorOfCacheable values;
    reg->values(values);
    LOGINFO("Region values = %d ", values.size());
    ASSERT(values.size() == reg->size(),
           "region size should be equal to values size");

    reg->destroy("Key100");
    reg->keys(keys);
    LOGINFO("Region keys = %d ", keys.size());
    ASSERT(keys.size() == reg->size(),
           "region size should be equal to keys size");

    reg->values(values);
    LOGINFO("Region values = %d ", values.size());
    ASSERT(values.size() == reg->size(),
           "region size should be equal to values size");

    LOG("StepEight complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFiveA)
  {
    putEntry(regionNames[0], keys[0], vals[0]);
    putEntry(regionNames[1], keys[2], vals[2]);
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);

    CacheableKeyPtr keyPtr = CacheableString::create(keys[0]);
    CacheableKeyPtr keyPtr1 = CacheableString::create(keys[2]);

    // Try removing non-existent entry from regions, result should be false.
    ASSERT(reg0->removeEx(keys[4]) == false,
           "Result of remove should be false, as this entry is not present in "
           "first region.");
    ASSERT(reg1->removeEx(keys[4]) == false,
           "Result of remove should be false, as this entry is not present in "
           "second region.");

    // Try removing existent key, and existing value from regions, result should
    // be true.

    ASSERT(reg0->removeEx(keys[0]) == true,
           "Result of remove should be true, as this entry is present in first "
           "region.");
    ASSERT(
        reg1->removeEx(keys[2]) == true,
        "Result of remove should be true, as this entry is present in second "
        "region.");

    ASSERT(reg0->containsKey(keys[0]) == false, "containsKey should be false");
    ASSERT(reg0->containsKeyOnServer(keyPtr) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg1->containsKey(keys[2]) == false, "containsKey should be false");
    ASSERT(reg1->containsKeyOnServer(keyPtr1) == false,
           "containsKeyOnServer should be false");

    // Try removing already deleted entry from regions, result should be false,
    // but no exception.
    ASSERT(reg0->removeEx(keys[0]) == false,
           "Result of remove should be false, as this entry is not present in "
           "first region.");
    ASSERT(reg1->removeEx(keys[0]) == false,
           "Result of remove should be false, as this entry is not present in "
           "second region.");

    // Try locally destroying already deleted entry from regions, It should
    // result
    // into exception.
    try {
      reg0->localDestroy(keys[0]);
      FAIL(
          "local destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "localDestroy operation on already removed entry.");
    }

    try {
      reg1->localDestroy(keys[0]);
      FAIL(
          "local destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "localDestroy operation on already removed entry.");
    }
    LOG("StepFiveA complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, StepTwelve)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);

    int intKey = 1;
    int intVal = 1;
    regPtr0->localCreate(intKey, CacheableInt32::create(intVal));
    CacheableInt32Ptr vInt = dynCast<CacheableInt32Ptr>(regPtr0->get(intKey));
    ASSERT(vInt->value() == 1, "Int Key Int Val Mismatch.");
    regPtr0->localInvalidate(intKey);
    vInt = dynCast<CacheableInt32Ptr>(regPtr0->get(intKey));
    ASSERT(vInt == NULLPTR, "valueshould not be found from sever");

    intKey = 2;
    const char* strVal = "IntKeyStringValue";
    regPtr0->localCreate(intKey, strVal);
    CacheableStringPtr vString =
        dynCast<CacheableStringPtr>(regPtr0->get(intKey));
    ASSERT(strcmp(vString->asChar(), strVal) == 0,
           "Int Key and String Value Mismatch");

    int key1 = 1;
    int val1 = 20;
    try {
      regPtr0->localCreate(CacheableInt32::create(key1),
                           CacheableInt32::create(val1));
      CacheableInt32Ptr vall = dynCast<CacheableInt32Ptr>(regPtr0->get(key1));
      FAIL("Expected EntryExistException here");
    } catch (EntryExistsException e) {
      LOG(" Expected EntryExistsException exception thrown by localCreate");
    }

    int64_t longKey = 1000;
    int64_t longVal = 200;
    regPtr0->localCreate(CacheableInt64::create(longKey),
                         CacheableInt64::create(longVal));
    CacheableInt64Ptr vLong = dynCast<CacheableInt64Ptr>(
        regPtr0->get(CacheableInt64::create(longKey)));
    ASSERT(vLong->value() == 200, "Long Key Long Val Mismatch.");

    const char* strKey = "StrKeyIntValueKey";
    intVal = 1234;
    regPtr0->localCreate(strKey, CacheableInt32::create(intVal));
    CacheableInt32Ptr vIntPtr =
        dynCast<CacheableInt32Ptr>(regPtr0->get(strKey));
    ASSERT(vIntPtr->value() == 1234, "String Key Int Val Mismatch.");

    longKey = 1111;
    strVal = "LongKeyStringValue";
    regPtr0->localCreate(CacheableInt64::create(longKey), strVal);
    CacheableStringPtr vStr = dynCast<CacheableStringPtr>(
        regPtr0->get(CacheableInt64::create(longKey)));
    ASSERT(strcmp(vStr->asChar(), strVal) == 0,
           "Long Key and String Value Mismatch");

    strKey = "StringKey1";
    strVal = "StringKeyStringValue";
    regPtr0->localCreate(strKey, strVal);
    CacheableStringPtr v = dynCast<CacheableStringPtr>(regPtr0->get(strKey));
    ASSERT(strcmp(v->asChar(), strVal) == 0,
           "String Key and String Value Mismatch");

    int i = 1234;
    regPtr0->localCreate(CacheableInt32::create(i), CacheableInt32::create(i));
    CacheableInt32Ptr result =
        dynCast<CacheableInt32Ptr>(regPtr0->get(CacheableInt32::create(i)));
    ASSERT(result->value() == i,
           "localcreate new entry with cacheableInt key and cacheableInt value "
           "Fail");

    regPtr0->localCreate(CacheableInt32::create(12345),
                         CacheableString::create("abced"));
    CacheableStringPtr resultString = dynCast<CacheableStringPtr>(
        regPtr0->get(CacheableInt32::create(12345)));
    ASSERT(strcmp(resultString->asChar(), "abced") == 0,
           "localcreate new with entry cacheableInt key and cacheablestring "
           "value Fail");

    regPtr0->localCreate(CacheableString::create("X"),
                         CacheableString::create("Y"));
    CacheableStringPtr resultStringY =
        dynCast<CacheableStringPtr>(regPtr0->get(CacheableString::create("X")));
    ASSERT(strcmp(resultStringY->asChar(), "Y") == 0,
           "localcreate new with entry cacheablestring key and cacheablestring "
           "value");

    try {
      regPtr0->localCreate(CacheableString::create("X"),
                           CacheableString::create("Y"));
      FAIL(
          "Should have thrown EntryExistsException if entry already exist in "
          "region.");
    } catch (EntryExistsException e) {
      LOG("Expected : check EntryExistsException if entry already exist in "
          "region.");
    }

    regPtr0->localDestroy(CacheableString::create("X"));
    if (dynCast<CacheableStringPtr>(
            regPtr0->get(CacheableString::create("X"))) != NULLPTR) {
      LOG("Expected : localDestroy should have failed.");
    }

    CacheableInt32Ptr x;
    try {
      regPtr0->localCreate(x, 1);
      LOG("Entry with null key and value locally created successfully");
      FAIL("Expected IllegalArgumentException here");
    } catch (IllegalArgumentException ex) {
      LOGINFO("Expected IllegalArgumentException : %s", ex.getMessage());
    }

    try {
      regPtr0->localPut(x, 1);
      LOG("Entry with null key and value locally put successfully");
      FAIL("Expected IllegalArgumentException here");
    } catch (IllegalArgumentException ex) {
      LOGINFO("Expected IllegalArgumentException : %s", ex.getMessage());
    }

    try {
      regPtr0->localDestroy(x);
      LOG("Entry with null key locally deleted successfully");
      FAIL("Expected IllegalArgumentException here");
    } catch (IllegalArgumentException ex) {
      LOGINFO("Expected IllegalArgumentException : %s", ex.getMessage());
    }
    try {
      regPtr0->localInvalidate(x);
      LOG("Entry with null key locally invalidated successfully");
      FAIL("Expected IllegalArgumentException here");
    } catch (IllegalArgumentException ex) {
      LOGINFO("Expected IllegalArgumentException : %s", ex.getMessage());
    }
    try {
      regPtr0->localRemove(x, 1);
      LOG("Entry with null key and value locally removed successfully");
      FAIL("Expected IllegalArgumentException here");
    } catch (IllegalArgumentException ex) {
      LOGINFO("Expected IllegalArgumentException : %s", ex.getMessage());
    }
    try {
      regPtr0->localRemoveEx(x);
      LOG("Entry with null key locally removed if value exist successfully");
      FAIL("Expected IllegalArgumentException here");
    } catch (IllegalArgumentException ex) {
      LOGINFO("Expected IllegalArgumentException : %s", ex.getMessage());
    }

    CacheableKeyPtr keyObject1(new PdxTests::PdxType());
    regPtr0->localCreate(keyObject1, x);
    CacheablePtr retVal = regPtr0->get(keyObject1);
    ASSERT(retVal == x, "retVal and x should match.");
    regPtr0->localInvalidate(keyObject1);
    retVal = regPtr0->get(keyObject1);
    ASSERT(retVal == NULLPTR, "value should not be found");
    ASSERT(regPtr0->localRemove(keyObject1, x) == true,
           "Result of remove should be true, as this value nullptr exists "
           "locally.");
    ASSERT(regPtr0->containsKey(keyObject1) == false,
           "containsKey should be false");
    regPtr0->localCreate(keyObject1, x);
    ASSERT(regPtr0->localRemoveEx(keyObject1) == true,
           "Result of remove should be true, as this value does not exists "
           "locally.");
    ASSERT(regPtr0->containsKey(keyObject1) == false,
           "containsKey should be false");
    regPtr0->localCreate(keyObject1, x);
    regPtr0->localDestroy(keyObject1);
    ASSERT(regPtr0->containsKey(keyObject1) == false,
           "containsKey should be false");
    try {
      // retVal = regPtr0->get(keyObject1);
      retVal = regPtr0->get(x);
      ASSERT(retVal == NULLPTR, "value should not be found");
      FAIL("Expected IllegalArgumentException here for get");
    } catch (Exception) {
      LOG(" Expected exception thrown by get");
    }

    try {
      regPtr0->localPut(keyObject1, x);
    } catch (IllegalArgumentException ex) {
      LOGINFO("Expected IllegalArgumentException : %s", ex.getMessage());
    }
    retVal = regPtr0->get(keyObject1);
    ASSERT(retVal == x, "retVal and x should match.");
    regPtr0->localPut(keyObject1, 1);
    regPtr0->localInvalidate(keyObject1);
    retVal = regPtr0->get(keyObject1);
    ASSERT(retVal == NULLPTR, "value should not be found");
    ASSERT(regPtr0->localRemove(keyObject1, 1) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(regPtr0->containsKey(keyObject1) == true,
           "containsKey should be true");
    regPtr0->localPut(keyObject1, 1);
    ASSERT(regPtr0->localRemoveEx(keyObject1) == true,
           "Result of remove should be true, as this value exists locally.");
    ASSERT(regPtr0->containsKey(keyObject1) == false,
           "containsKey should be false");
    CacheableKeyPtr keyObject2(new PdxTests::PdxType());
    regPtr0->localCreate(keyObject2, 1);
    CacheableInt32Ptr intVal1 = regPtr0->get(keyObject2);
    ASSERT(intVal1->value() == 1, "intVal should be 1.");
    regPtr0->invalidate(keyObject2);
    intVal1 = dynCast<CacheableInt32Ptr>(regPtr0->get(keyObject2));
    ASSERT(intVal1 == NULLPTR, "intVal should be null.");

    CacheableKeyPtr keyObject3(new PdxTests::PdxType());
    regPtr0->localCreate(keyObject3, "testString");
    if (regPtr0->containsKey(keyObject3)) {
      CacheablePtr strVal1 = regPtr0->get(keyObject3);
      ASSERT(strcmp(strVal1->toString()->asChar(), "testString") == 0,
             "strVal should be testString.");
    }
    try {
      if (regPtr0->containsKey(keyObject3)) {
        regPtr0->localCreate(keyObject3, 1);
        FAIL("Expected EntryExistException here");
      }
    } catch (EntryExistsException e) {
      LOG(" Expected EntryExistsException exception thrown by localCreate");
    }

    CacheableKeyPtr keyObject4(new PdxTests::PdxType());
    PdxTests::PdxTypePtr valObject1(new PdxTests::PdxType());
    regPtr0->localCreate(keyObject4, valObject1);
    PdxTests::PdxTypePtr objVal1 =
        dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyObject4));
    ASSERT(valObject1 == objVal1, "valObject and objVal should match.");
    regPtr0->localInvalidate(keyObject4);
    objVal1 = dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyObject4));
    ASSERT(objVal1 == NULLPTR, "valueshould not be found");
    ASSERT(regPtr0->localRemove(keyObject4, valObject1) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(regPtr0->containsKey(keyObject4) == true,
           "containsKey should be true");
    regPtr0->localPut(keyObject4, valObject1);
    ASSERT(regPtr0->localRemove(keyObject4, valObject1) == true,
           "Result of remove should be true, as this value exists locally.");
    ASSERT(regPtr0->containsKey(keyObject4) == false,
           "containsKey should be false");
    regPtr0->localPut(keyObject4, valObject1);
    ASSERT(regPtr0->localRemoveEx(keyObject4) == true,
           "Result of remove should be true, as this value exists locally.");
    ASSERT(regPtr0->containsKey(keyObject4) == false,
           "containsKey should be false");
    regPtr0->localPut(keyObject4, valObject1);
    regPtr0->localDestroy(keyObject4);
    /*try {
          objVal1 = dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyObject4));;//
    need to verify that if entry is deleted then some exception should be thrown
          FAIL("Expected EntryExistException here for get");
    }catch (Exception)
    {
     LOG (" Expected  exception thrown by get");
    }*/

    createRegion("ABC", USE_ACK, true, true);
    RegionPtr regPtr2 = getHelper()->getRegion("ABC");
    regPtr2->localDestroyRegion();
    try {
      regPtr2->localCreate(CacheableString::create("XY"),
                           CacheableString::create("Y"));
      FAIL("localDestroyRegion should have thrown exception");
    } catch (RegionDestroyedException e) {
      LOG(" Expected exception thrown by localCreate, since Region does not "
          "exist");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepSixA)
  {
    RegionPtr reg0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr reg1 = getHelper()->getRegion(regionNames[1]);

    CacheableKeyPtr keyPtr = CacheableString::create(keys[1]);
    CacheableKeyPtr keyPtr1 = CacheableString::create(keys[3]);

    putEntry(regionNames[0], keys[1], nvals[1]);
    putEntry(regionNames[1], keys[3], nvals[3]);

    // Try removing value that is present on client as well as server, result
    // should be true.
    ASSERT(
        reg0->removeEx(keys[1]) == true,
        "Result of remove should be true, as this value is present locally, & "
        "also present on server.");
    ASSERT(
        reg1->removeEx(keys[3]) == true,
        "Result of remove should be true, as this value is present locally, & "
        "also present on server.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg0->containsKeyOnServer(keyPtr) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    ASSERT(reg1->containsKeyOnServer(keyPtr1) == false,
           "containsKeyOnServer should be false");
    LOGINFO("Step 6a.1 complete.");

    // Try removing value that is invalidated on client but exists on server,
    // result should be false.
    try {
      reg0->destroy(keys[0]);
      FAIL(
          "destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "destroy operation on already removed entry.");
    }

    try {
      reg1->destroy(keys[3]);
      FAIL(
          "destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "destroy operation on already removed entry.");
    }

    putEntry(regionNames[0], keys[1], nvals[1]);
    putEntry(regionNames[1], keys[3], nvals[3]);
    reg0->invalidate(keys[1]);
    reg1->invalidate(keys[3]);
    ASSERT(
        reg0->removeEx(keys[1]) == true,
        "Result of remove should be true, as invalidated key can be deleted.");
    ASSERT(
        reg1->removeEx(keys[3]) == true,
        "Result of remove should be true, as invalidated key can be deleted.");
    LOGINFO("Step 6a.2 complete.");

    // Try removing a entry (value) which is not present on client as well as
    // server, result should be false.
    ASSERT(reg0->removeEx("NewKey1") == false,
           "Result of remove should be false, as this value is not present.");
    ASSERT(reg1->removeEx("NewKey3") == false,
           "Result of remove should be false, as this value is not present.");
    CacheableKeyPtr keyPtr2 = CacheableString::create("NewKey1");
    CacheableKeyPtr keyPtr3 = CacheableString::create("NewKey3");
    ASSERT(reg0->containsKey("NewKey1") == false,
           "containsKey should be false");
    ASSERT(reg0->containsKeyOnServer(keyPtr2) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg1->containsKey("NewKey3") == false,
           "containsKey should be false");
    ASSERT(reg1->containsKeyOnServer(keyPtr3) == false,
           "containsKeyOnServer should be false");
    LOGINFO("Step 6a.3 complete.");

    // Try removing a entry (value) which is not present on client but exists on
    // the server, result should be true.
    putEntry(regionNames[0], keys[1], nvals[1]);
    putEntry(regionNames[1], keys[3], nvals[3]);
    reg0->localDestroy(keys[1]);
    reg1->localDestroy(keys[3]);
    ASSERT(reg0->removeEx(keys[1]) == true,
           "Result of remove should be true, as this value does not exist "
           "locally, but exists on server.");
    ASSERT(reg1->removeEx(keys[3]) == true,
           "Result of remove should be true, as this value does not exist "
           "locally, but exists on server.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg0->containsKeyOnServer(keyPtr) == false,
           "containsKeyOnServer should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    ASSERT(reg1->containsKeyOnServer(keyPtr1) == false,
           "containsKeyOnServer should be false");
    LOG("Step 6a.4 complete.");

    //-------------------------------------localRemove
    // Testcases------------------------------------------------
    // Try locally removing an entry (value) which is present on the client.
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    ASSERT(reg0->localRemoveEx(keys[1]) == true,
           "Result of remove should be true, as this value exists locally.");
    ASSERT(reg1->localRemoveEx(keys[3]) == true,
           "Result of remove should be true, as this value exists locally.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    LOG("Step 6a.5 complete.");

    // Try local destroy on entry that is already removed, should get an
    // exception.
    try {
      reg0->localDestroy(keys[1]);
      FAIL(
          "local destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "localDestroy operation on already removed entry.");
    }
    try {
      reg1->localDestroy(keys[3]);
      FAIL(
          "local destroy on already removed key should have thrown "
          "EntryNotFoundException");
    } catch (EntryNotFoundException /*& ex*/) {
      LOG("Got expected EntryNotFoundException for "
          "localDestroy operation on already removed entry.");
    }
    LOG("Step 6a.6 complete.");

    // Try locally removing an entry (value) which is invalidated with a value.
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    reg0->invalidate(keys[1]);
    reg1->invalidate(keys[3]);
    ASSERT(reg0->localRemoveEx(keys[1]) == true,
           "Result of remove should be true.");
    ASSERT(reg1->localRemoveEx(keys[3]) == true,
           "Result of remove should be true.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be true");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be true");
    LOG("Step 6a.7 complete.");

    // Try locally removing an entry which is locally destroyed with a value.
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    reg0->localDestroy(keys[1]);
    reg1->localDestroy(keys[3]);
    ASSERT(reg0->localRemoveEx(keys[1]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->localRemoveEx(keys[3]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be true");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be true");
    LOG("Step 6a.8 complete.");

    // Try locally removing an entry which is already removed.
    reg0->localPut(keys[1], vals[1]);
    reg1->localPut(keys[3], vals[3]);
    ASSERT(reg0->localRemoveEx(keys[1]) == true,
           "Result of remove should be true, as this value does not exists "
           "locally.");
    ASSERT(reg0->localRemoveEx(keys[1]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->localRemoveEx(keys[3]) == true,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg1->localRemoveEx(keys[3]) == false,
           "Result of remove should be false, as this value does not exists "
           "locally.");
    ASSERT(reg0->containsKey(keys[1]) == false, "containsKey should be false");
    ASSERT(reg1->containsKey(keys[3]) == false, "containsKey should be false");
    LOG("Step 6a.9 complete.");
    // Try locally removing an entry when region scope is not null.

    LOG("StepSixA complete.");
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

DUNIT_TASK_DEFINITION(SERVER1, CloseLocator1)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CloseLocator2)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(2);
      LOG("Locator2 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateLocator1)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateLocator2)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(2);
    LOG("Locator2 started");
  }
END_TASK_DEFINITION

void runRemoveOps() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator)

  CALL_TASK(StepOne_Pooled_Locator);
  CALL_TASK(StepTwo_Pooled_Locator);

  CALL_TASK(StepThree);
  CALL_TASK(StepFive);
  CALL_TASK(StepFiveA);
  CALL_TASK(StepSix);
  CALL_TASK(StepSixA);
  CALL_TASK(StepTwelve);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator1);
}

void runRemoveOps1() {
  CALL_TASK(CreateLocator2);
  CALL_TASK(CreateServer2_locator);

  CALL_TASK(StepSeven_Pooled_Locator);

  CALL_TASK(StepEight);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseServer2);
  CALL_TASK(CloseLocator2);
}

DUNIT_MAIN
  {
    runRemoveOps();

    runRemoveOps1();
  }
END_MAIN

#endif /* TESTTHINCLIENTREMOVEOPS_HPP_ */
