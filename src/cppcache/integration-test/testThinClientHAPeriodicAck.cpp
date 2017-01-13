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

#define ROOT_NAME "testThinClientHAPeriodicAck"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

class DupChecker : public CacheListener {
  int m_ops;
  HashMapOfCacheable m_map;

  void check(const EntryEvent& event) {
    m_ops++;

    CacheableKeyPtr key = event.getKey();
    CacheableInt32Ptr value = dynCast<CacheableInt32Ptr>(event.getNewValue());

    HashMapOfCacheable::Iterator item = m_map.find(key);

    if (item != m_map.end()) {
      CacheableInt32Ptr check = dynCast<CacheableInt32Ptr>(item.second());
      ASSERT(check->value() + 1 == value->value(),
             "Duplicate or older value received");
      m_map.update(key, value);
    } else {
      m_map.insert(key, value);
    }
  }

 public:
  DupChecker() : m_ops(0) {}

  ~DupChecker() { m_map.clear(); }

  void validate() {
    ASSERT(m_map.size() == 4, "Expected 4 keys for the region");
    ASSERT(m_ops == 400, "Expected 400 events (100 per key) for the region");

    for (HashMapOfCacheable::Iterator item = m_map.begin(); item != m_map.end();
         item++) {
      CacheableInt32Ptr check = dynCast<CacheableInt32Ptr>(item.second());
      ASSERT(check->value() == 100, "Expected final value to be 100");
    }
  }

  virtual void afterCreate(const EntryEvent& event) { check(event); }

  virtual void afterUpdate(const EntryEvent& event) { check(event); }

  virtual void afterRegionInvalidate(const RegionEvent& event){};
  virtual void afterRegionDestroy(const RegionEvent& event){};
};

typedef SharedPtr<DupChecker> DupCheckerPtr;

///////////////////////////////////////////////////////

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

CacheHelper* cacheHelper = NULL;
static bool isLocator = false;
static bool isLocalServer = false;
static int numberOfLocators = 1;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);
int g_redundancyLevel = 0;
bool g_poolConfig = false;
bool g_poolLocators = false;

void initClient(int redundancyLevel) {
  PropertiesPtr props = Properties::create();
  props->insert("notify-ack-interval", 1);
  props->insert("notify-dupcheck-life", 30);

  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(redundancyLevel, props);
  }
  g_redundancyLevel = redundancyLevel;
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void initClient() {
  PropertiesPtr props = Properties::create();
  props->insert("notify-ack-interval", 1);
  props->insert("notify-dupcheck-life", 30);

  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(true, props);
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

void _verifyIntEntry(const char* name, const char* key, const int val,
                     bool noKey, bool isCreated = false) {
  // Verify key and value exist in this region, in this process.
  int value = val;
  char* buf = reinterpret_cast<char*>(malloc(1024 + strlen(key) + 20));
  ASSERT(buf, "Unable to malloc buffer for logging.");
  if (!isCreated) {
    if (noKey) {
      sprintf(buf, "Verify key %s does not exist in region %s", key, name);
    } else if (val == 0) {
      sprintf(buf, "Verify value for key %s does not exist in region %s", key,
              name);
    } else {
      sprintf(buf, "Verify value for key %s is: %d in region %s", key, value,
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
    if (val != 0) {  // need to have a value!
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
      if (val == 0) {
        if (regPtr->containsValueForKey(keyPtr)) {
          containsValueCnt++;
        } else {
          break;
        }
        ASSERT(containsValueCnt < MAX, "Value found in region.");
      }

      if (val != 0) {
        CacheableInt32Ptr checkPtr =
            dynCast<CacheableInt32Ptr>(regPtr->get(keyPtr));

        ASSERT(checkPtr != NULLPTR, "Value Ptr should not be null.");
        char buf[1024];
        sprintf(buf, "In verify loop, get returned %d for key %s",
                checkPtr->value(), key);
        LOG(buf);
        // if ( strcmp( checkPtr->asChar(), value ) != 0 ){
        if (checkPtr->value() != value) {
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

#define verifyIntEntry(x, y, z) _verifyIntEntry(x, y, z, __LINE__)

void _verifyIntEntry(const char* name, const char* key, const int val,
                     int line) {
  char logmsg[1024];
  sprintf(logmsg, "verifyIntEntry() called from %d.\n", line);
  LOG(logmsg);
  _verifyIntEntry(name, key, val, false);
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

void createIntEntry(const char* name, const char* key, const int value) {
  LOG("createEntry() entered.");
  fprintf(stdout, "Creating entry -- key: %s  value: %d in region %s\n", key,
          value, name);
  fflush(stdout);
  // Create entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableInt32Ptr valPtr = CacheableInt32::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  // ASSERT( !regPtr->containsKey( keyPtr ), "Key should not have been found in
  // region." );
  // ASSERT( !regPtr->containsValueForKey( keyPtr ), "Value should not have been
  // found in region." );

  // regPtr->create( keyPtr, valPtr );
  regPtr->put(keyPtr, valPtr);
  LOG("Created entry.");

  verifyIntEntry(name, key, value);
  LOG("Entry created.");
}

void setCacheListener(const char* regName, DupCheckerPtr checker) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(checker);
}

const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};
const char* vals[] = {"Value-1", "Value-2", "Value-3", "Value-4"};
const char* nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                       "New Value-4"};

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck"};

const bool USE_ACK = true;
const bool NO_ACK = false;

DupCheckerPtr checker1;
DupCheckerPtr checker2;

void initClientAndRegion(int redundancy) {
  PropertiesPtr pp = Properties::create();
  getHelper()->createPoolWithLocators("__TESTPOOL1_", locatorsG, true,
                                      redundancy);
  getHelper()->createRegionAndAttachPool(regionNames[0], USE_ACK,
                                         "__TESTPOOL1_", true);
  getHelper()->createRegionAndAttachPool(regionNames[1], NO_ACK, "__TESTPOOL1_",
                                         true);
}

#include "LocatorHelper.hpp"
#include "ThinClientTasks_C2S2.hpp"

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml");
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
  {
    if (isLocalServer) {
      CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml");
    }
    LOG("SERVER2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, InitClient1_R1)
  {
    initClient();
    initClientAndRegion(1);
    LOG("Initialized client with redundancy level 1.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, InitClient2_R1)
  {
    initClient();
    initClientAndRegion(1);
    LOG("Initialized client with redundancy level 1.");

    checker1 = new DupChecker();
    checker2 = new DupChecker();

    setCacheListener(regionNames[0], checker1);
    setCacheListener(regionNames[1], checker2);

    getHelper()->getRegion(regionNames[0])->registerAllKeys();
    getHelper()->getRegion(regionNames[1])->registerAllKeys();

    LOG("Region creation complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreateEntries)
  {
    for (int value = 1; value <= 100; value++) {
      createIntEntry(regionNames[0], keys[0], value);
      gemfire::millisleep(50);
      createIntEntry(regionNames[0], keys[1], value);
      gemfire::millisleep(50);
      createIntEntry(regionNames[0], keys[2], value);
      gemfire::millisleep(50);
      createIntEntry(regionNames[0], keys[3], value);
      gemfire::millisleep(50);
      createIntEntry(regionNames[1], keys[0], value);
      gemfire::millisleep(50);
      createIntEntry(regionNames[1], keys[1], value);
      gemfire::millisleep(50);
      createIntEntry(regionNames[1], keys[2], value);
      gemfire::millisleep(50);
      createIntEntry(regionNames[1], keys[3], value);
      gemfire::millisleep(50);
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CheckClient2)
  {
    verifyIntEntry(regionNames[0], keys[0], 100);
    verifyIntEntry(regionNames[0], keys[1], 100);
    verifyIntEntry(regionNames[0], keys[2], 100);
    verifyIntEntry(regionNames[0], keys[3], 100);
    verifyIntEntry(regionNames[1], keys[0], 100);
    verifyIntEntry(regionNames[1], keys[1], 100);
    verifyIntEntry(regionNames[1], keys[2], 100);
    verifyIntEntry(regionNames[1], keys[3], 100);

    LOG("Validating checker1 cachelistener");
    checker1->validate();
    LOG("Validating checker2 cachelistener");
    checker2->validate();

    LOG("CheckClient2 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseClient1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseClient2)
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
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML);
    CALL_TASK(CreateServer2_With_Locator_XML);

    CALL_TASK(InitClient1_R1);
    CALL_TASK(InitClient2_R1);

    CALL_TASK(CreateEntries);
    CALL_TASK(CheckClient2);

    CALL_TASK(CloseClient1);
    CALL_TASK(CloseClient2);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseServer2);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
