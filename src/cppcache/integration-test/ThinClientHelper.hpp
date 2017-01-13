/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef TEST_THINCLIENTHELPER_HPP
#define TEST_THINCLIENTHELPER_HPP

#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include "testUtils.hpp"
#include "security/typedefs.hpp"
#include "security/CredentialGenerator.hpp"
#include "security/DummyCredentialGenerator.hpp"
#include "security/CredentialGenerator.cpp"

#include <string>

#ifndef ROOT_NAME
#define ROOT_NAME "ThinClientHelper"
#endif

#ifndef ROOT_SCOPE
#define ROOT_SCOPE DISTRIBUTED_ACK
#endif

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;
using namespace unitTests;

CacheHelper* cacheHelper = NULL;

void initGridClient(const bool isthinClient,
                    const PropertiesPtr& configPtr = NULLPTR) {
  static bool s_isGridClient = true;

  s_isGridClient = !s_isGridClient;
  if (cacheHelper == NULL) {
    PropertiesPtr config = configPtr;
    if (config == NULLPTR) {
      config = Properties::create();
    }
    config->insert("grid-client", s_isGridClient ? "true" : "false");
    cacheHelper = new CacheHelper(isthinClient, config);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void initClient(const bool isthinClient,
                const PropertiesPtr& configPtr = NULLPTR) {
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient, configPtr);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void initClientWithPool(const bool isthinClient, const char* poolName,
                        const char* locators, const char* serverGroup,
                        const PropertiesPtr& configPtr = NULLPTR,
                        int redundancy = 0, bool clientNotification = false,
                        int subscriptionAckInterval = -1, int connections = -1,
                        int loadConditioningInterval = -1,
                        bool prSingleHop = false, bool threadLocal = false) {
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(
        isthinClient, poolName, locators, serverGroup, configPtr, redundancy,
        clientNotification, subscriptionAckInterval, connections,
        loadConditioningInterval, false, prSingleHop, threadLocal);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

/* For HA Clients */
void initClient(int redundancyLevel, const PropertiesPtr& configPtr = NULLPTR) {
  if (cacheHelper == NULL) {
    PropertiesPtr config = configPtr;
    if (config == NULLPTR) {
      config = Properties::create();
    }
    cacheHelper = new CacheHelper(redundancyLevel, config);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void initGridClient(int redundancyLevel,
                    const PropertiesPtr& configPtr = NULLPTR) {
  static bool s_isGridClient = true;

  s_isGridClient = !s_isGridClient;
  if (cacheHelper == NULL) {
    PropertiesPtr config = configPtr;
    if (config == NULLPTR) {
      config = Properties::create();
    }
    config->insert("grid-client", s_isGridClient ? "true" : "false");
    cacheHelper = new CacheHelper(redundancyLevel, config);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void cleanProc() {
  if (cacheHelper != NULL) {
    delete cacheHelper;
    cacheHelper = NULL;
  }
}

void netDown() {
  if (cacheHelper != NULL) {
    TestUtils::getCacheImpl(cacheHelper->cachePtr)->netDown();
  }
}

void revive() {
  if (cacheHelper != NULL) {
    TestUtils::getCacheImpl(cacheHelper->cachePtr)->revive();
  }
}

void crashClient() {
  if (cacheHelper != NULL) {
    TestUtils::getCacheImpl(cacheHelper->cachePtr)->setClientCrashTEST();
  }
}

CacheHelper* getHelper() {
  ASSERT(cacheHelper != NULL, "No cacheHelper initialized.");
  return cacheHelper;
}

const char* testregex[] = {"Key-*1", "Key-*2", "Key-*3",
                           "Key-*4", "Key-*5", "Key-*6"};
const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4", "Key-5", "Key-6"};
const char* vals[] = {"Value-1", "Value-2", "Value-3",
                      "Value-4", "Value-5", "Value-6"};
const char* nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                       "New Value-4", "New Value-5", "New Value-6"};

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck"};

const bool USE_ACK = true;
const bool NO_ACK = false;

void _verifyEntry(const char* name, const char* key, const char* val,
                  bool noKey, bool checkVal = true) {
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
  if (val != NULL && checkVal) {  // need to have a value!
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

void verifyEntry(const char* name, const char* key, const char* val,
                 bool checkVal = true) {
  char logmsg[1024];
  sprintf(logmsg, "verifyEntry() called from %d.\n", __LINE__);
  LOG(logmsg);
  _verifyEntry(name, key, val, false, checkVal);
  LOG("Entry verified.");
}

void _verifyIntEntry(const char* name, const char* key, const int val,
                     bool noKey, bool isCreated = false) {
  // Verify key and value exist in this region, in this process.
  int value = val;
  char* buf = (char*)malloc(1024 + strlen(key) + 20);
  ASSERT(buf, "Unable to malloc buffer for logging.");
  if (!isCreated) {
    if (noKey)
      sprintf(buf, "Verify key %s does not exist in region %s", key, name);
    else if (val == 0)
      sprintf(buf, "Verify value for key %s does not exist in region %s", key,
              name);
    else
      sprintf(buf, "Verify value for key %s is: %d in region %s", key, value,
              name);
    LOG(buf);

    free(buf);
  }

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
      if (val == 0) {
        if (regPtr->containsValueForKey(keyPtr))
          containsValueCnt++;
        else
          break;
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

#define verifyIntEntry(x, y, z) _verifyIntEntry(x, y, z, __LINE__)
void _verifyIntEntry(const char* name, const char* key, const int val,
                     int line) {
  char logmsg[1024];
  sprintf(logmsg, "verifyIntEntry() called from %d.\n", line);
  LOG(logmsg);
  _verifyIntEntry(name, key, val, false);
  LOG("Entry verified.");
}

void createRegion(const char* name, bool ackMode,
                  bool clientNotificationEnabled = false,
                  const CacheListenerPtr& listener = NULLPTR,
                  bool caching = true) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  // ack, caching
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, caching, listener,
                                               clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}
RegionPtr createOverflowRegion(const char* name, bool ackMode, int lel = 0,
                               bool clientNotificationEnabled = true,
                               bool caching = true) {
  std::string bdb_dir = "BDB";
  std::string bdb_dirEnv = "BDBEnv";
  AttributesFactory af;
  af.setCachingEnabled(caching);
  af.setLruEntriesLimit(lel);
  af.setDiskPolicy(DiskPolicyType::OVERFLOWS);

  PropertiesPtr sqLiteProps = Properties::create();
  sqLiteProps->insert("PageSize", "65536");
  sqLiteProps->insert("MaxPageCount", "1073741823");
  std::string sqlite_dir =
      "SqLiteRegionData" +
      std::to_string(static_cast<long long int>(ACE_OS::getpid()));
  sqLiteProps->insert("PersistenceDirectory", sqlite_dir.c_str());
  af.setPersistenceManager("SqLiteImpl", "createSqLiteInstance", sqLiteProps);

  RegionAttributesPtr rattrsPtr = af.createRegionAttributes();
  CachePtr cache = getHelper()->cachePtr;
  CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cache.ptr());
  RegionPtr regionPtr;
  cacheImpl->createRegion(name, rattrsPtr, regionPtr);
  return regionPtr;
}

RegionPtr createPooledRegion(const char* name, bool ackMode,
                             const char* locators,
                             const char* poolname,
                             bool clientNotificationEnabled = false,
                             const CacheListenerPtr& listener = NULLPTR,
                             bool caching = true) {
  LOG("createPooledRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);

  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(true, poolname, locators, NULL);
  }

  // ack, caching
  RegionPtr regPtr = getHelper()->createPooledRegion(
      name, ackMode, locators, poolname, caching, clientNotificationEnabled, 0,
      0, 0, 0, 0, listener);

  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
  return regPtr;
}

PoolPtr findPool(const char* poolName) {
  LOG("findPool() entered.");
  PoolPtr poolPtr = PoolManager::find(poolName);
  ASSERT(poolPtr != NULLPTR, "Failed to find pool.");
  return poolPtr;
}
PoolPtr createPool(const char* poolName, const char* locators,
                   const char* serverGroup, int redundancy = 0,
                   bool clientNotification = false,
                   int subscriptionAckInterval = -1, int connections = -1,
                   int loadConditioningInterval = -1) {
  LOG("createPool() entered.");

  PoolPtr poolPtr = getHelper()->createPool(
      poolName, locators, serverGroup, redundancy, clientNotification,
      subscriptionAckInterval, connections, loadConditioningInterval);
  ASSERT(poolPtr != NULLPTR, "Failed to create pool.");
  LOG("Pool created.");
  return poolPtr;
}

PoolPtr createPoolAndDestroy(const char* poolName, const char* locators,
                             const char* serverGroup, int redundancy = 0,
                             bool clientNotification = false,
                             int subscriptionAckInterval = -1,
                             int connections = -1) {
  LOG("createPoolAndDestroy() entered.");

  PoolPtr poolPtr = getHelper()->createPool(
      poolName, locators, serverGroup, redundancy, clientNotification,
      subscriptionAckInterval, connections);
  ASSERT(poolPtr != NULLPTR, "Failed to create pool.");
  poolPtr->destroy();
  LOG("Pool created and destroyed.");
  return poolPtr;
}
// this will create pool even endpoints and locatorhost has been not defined
PoolPtr createPool2(const char* poolName, const char* locators,
                    const char* serverGroup, const char* servers = NULL,
                    int redundancy = 0, bool clientNotification = false) {
  LOG("createPool2() entered.");

  PoolPtr poolPtr = getHelper()->createPool2(
      poolName, locators, serverGroup, servers, redundancy, clientNotification);
  ASSERT(poolPtr != NULLPTR, "Failed to create pool.");
  LOG("Pool created.");
  return poolPtr;
}

RegionPtr createRegionAndAttachPool(
    const char* name, bool ack, const char* poolName, bool caching = true,
    int ettl = 0, int eit = 0, int rttl = 0, int rit = 0, int lel = 0,
    ExpirationAction::Action action = ExpirationAction::DESTROY) {
  LOG("createRegionAndAttachPool() entered.");
  RegionPtr regPtr = getHelper()->createRegionAndAttachPool(
      name, ack, poolName, caching, ettl, eit, rttl, rit, lel, action);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
  return regPtr;
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

void updateEntry(const char* name, const char* key, const char* value,
                 bool checkVal = true, bool checkKey = true) {
  LOG("updateEntry() entered.");
  fprintf(stdout, "Updating entry -- key: %s  value: %s in region %s\n", key,
          value, name);
  fflush(stdout);
  // Update entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableStringPtr valPtr = CacheableString::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  if (checkKey) {
    ASSERT(regPtr->containsKey(keyPtr),
           "Key should have been found in region.");
  }
  if (checkVal) {
    ASSERT(regPtr->containsValueForKey(keyPtr),
           "Value should have been found in region.");
  }

  regPtr->put(keyPtr, valPtr);
  LOG("Put entry.");

  verifyEntry(name, key, value, checkVal);
  LOG("Entry updated.");
}

void doNetsearch(const char* name, const char* key, const char* value,
                 bool checkVal = true) {
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
  if (checkVal)
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

void createIntEntry(const char* name, const char* key, const int value,
                    bool onlyCreate = false) {
  LOG("createEntry() entered.");
  fprintf(stdout, "Creating entry -- key: %s  value: %d in region %s\n", key,
          value, name);
  fflush(stdout);

  // Create entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableInt32Ptr valPtr = CacheableInt32::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  if (onlyCreate) {
    ASSERT(!regPtr->containsKey(keyPtr),
           "Key should not have been found in region.");
    ASSERT(!regPtr->containsValueForKey(keyPtr),
           "Value should not have been found in region.");
  }

  regPtr->put(keyPtr, valPtr);
  LOG("Created entry.");

  verifyIntEntry(name, key, value);
  LOG("Entry created.");
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
  ASSERT(regPtr->containsValueForKey(keyPtr),
         "Value should have been found in region.");

  regPtr->localInvalidate(keyPtr);
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
  regPtr->localDestroyRegion();
  LOG("Region destroyed.");
}

class RegionOperations {
 public:
  RegionOperations(const char* name)
      : m_regionPtr(getHelper()->getRegion(name)) {}

  void putOp(int keys = 1, const UserDataPtr& aCallbackArgument = NULLPTR) {
    char keybuf[100];
    char valbuf[100];
    for (int i = 1; i <= keys; i++) {
      sprintf(keybuf, "key%d", i);
      sprintf(valbuf, "value%d", i);
      CacheableStringPtr valPtr = CacheableString::create(valbuf);
      m_regionPtr->put(keybuf, valPtr, aCallbackArgument);
    }
  }
  void invalidateOp(int keys = 1,
                    const UserDataPtr& aCallbackArgument = NULLPTR) {
    char keybuf[100];
    char valbuf[100];
    for (int i = 1; i <= keys; i++) {
      sprintf(keybuf, "key%d", i);
      CacheableStringPtr valPtr = CacheableString::create(valbuf);
      m_regionPtr->localInvalidate(keybuf, aCallbackArgument);
    }
  }
  void destroyOp(int keys = 1, const UserDataPtr& aCallbackArgument = NULLPTR) {
    char keybuf[100];
    char valbuf[100];
    for (int i = 1; i <= keys; i++) {
      sprintf(keybuf, "key%d", i);
      CacheableStringPtr valPtr = CacheableString::create(valbuf);
      m_regionPtr->destroy(keybuf, aCallbackArgument);
    }
  }
  void removeOp(int keys = 1, const UserDataPtr& aCallbackArgument = NULLPTR) {
    char keybuf[100];
    char valbuf[100];
    for (int i = 1; i <= keys; i++) {
      sprintf(keybuf, "key%d", i);
      sprintf(valbuf, "value%d", i);
      CacheableStringPtr valPtr = CacheableString::create(valbuf);
      m_regionPtr->remove(keybuf, valPtr, aCallbackArgument);
    }
  }
  RegionPtr m_regionPtr;
};

#endif  // TEST_THINCLIENTHELPER_HPP
