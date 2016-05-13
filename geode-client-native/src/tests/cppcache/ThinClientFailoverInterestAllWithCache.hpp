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

#define ROOT_NAME "DistOps"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

CacheHelper* cacheHelper = NULL;
bool isLocalServer = false;
const char* endPoints = CacheHelper::getTcrEndpoints(isLocalServer, 2);
volatile bool g_poolConfig = false;
volatile bool g_poolLocators = false;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2
static bool isLocator = false;
//static int numberOfLocators = 0;
const char* locatorsG = CacheHelper::getLocatorHostPort( isLocator, 1);
#include "LocatorHelper.hpp"
#include "ThinClientTasks_C2S2.hpp"
void initClient( const bool isthinClient )
{
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void cleanProc()
{
  if (cacheHelper != NULL) {
    delete cacheHelper;
    cacheHelper = NULL;
  }
}

CacheHelper* getHelper()
{
  ASSERT(cacheHelper != NULL, "No cacheHelper initialized.");
  return cacheHelper;
}

void _verifyEntry(const char* name, const char* key, const char* val,
    bool noKey, bool isCreated = false)
{
  // Verify key and value exist in this region, in this process.
  const char* value = (val == 0) ? "" : val;
  char* buf = (char *)malloc(1024 + strlen(key) + strlen(value));
  ASSERT(buf, "Unable to malloc buffer for logging.");
  if (!isCreated) {
    if (noKey) {
      sprintf(buf, "Verify key %s does not exist in region %s", key, name);
    }
    else if (val == 0) {
      sprintf(buf, "Verify value for key %s does not exist in region %s", key, name);
    }
    else {
      sprintf(buf, "Verify value for key %s is: %s in region %s", key, value, name);
    }
    LOG(buf);
  }
  free(buf);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  CacheableKeyPtr keyPtr = createKey(key);

  // if the region is no ack, then we may need to wait...
  if (!isCreated) {
    if (!noKey) { // need to find the key!
      ASSERT(regPtr->containsKey(keyPtr), "Key not found in region.");
    }
    if (val != NULL) { // need to have a value!
      //ASSERT(regPtr->containsValueForKey(keyPtr),
      //    "Value not found in region.");
    }
  }

  // loop up to MAX times, testing condition
  uint32_t MAX = 100;
  // ARB: changed sleep from 10 ms
  uint32_t SLEEP = 10; // milliseconds
  uint32_t containsKeyCnt = 0;
  uint32_t containsValueCnt = 0;
  uint32_t testValueCnt = 0;

  for (int i = MAX; i >= 0; --i)
  {
    if (isCreated) {
      if (!regPtr->containsKey(keyPtr)) {
        containsKeyCnt++;
      }
      else {
        break;
      }
      ASSERT(containsKeyCnt < MAX, "Key has not been created in region.");
    }
    else {
      if (noKey) {
        if (regPtr->containsKey(keyPtr)) {
          ++containsKeyCnt;
        }
        else {
          break;
        }
        ASSERT(containsKeyCnt < MAX, "Key found in region.");
      }
      if (val == NULL) {
        if (regPtr->containsValueForKey(keyPtr)) {
          containsValueCnt++;
        }
        else {
          break;
        }
        ASSERT(containsValueCnt < MAX, "Value found in region.");
      }

      if (val != NULL) {
        CacheableStringPtr checkPtr = dynCast<CacheableStringPtr>(
            regPtr->get(keyPtr));

        ASSERT(checkPtr != NULLPTR, "Value Ptr should not be null.");
        char buf[1024];
        sprintf(buf, "In verify loop, get returned %s for key %s",
            checkPtr->asChar(), key);
        LOG(buf);
        if (strcmp(checkPtr->asChar(), value) != 0) {
          testValueCnt++;
        }
        else {
          break;
        }
        ASSERT(testValueCnt < MAX, "Incorrect value found.");
      }
    }
    dunit::sleep(SLEEP);
  }
}

#define verifyEntry(x, y, z) _verifyEntry(x, y, z, __LINE__)

void _verifyEntry(const char* name, const char* key, const char* val, int line)
{
  char logmsg[1024];
  sprintf(logmsg, "verifyEntry() called from %d.\n", line);
  LOG(logmsg);
  _verifyEntry(name, key, val, false);
  LOG("Entry verified.");
}

#define verifyCreated(x, y) _verifyCreated(x, y, __LINE__)

void _verifyCreated(const char* name, const char* key, int line)
{
  char logmsg[1024];
  sprintf(logmsg, "verifyCreated() called from %d.\n", line);
  LOG(logmsg);
  _verifyEntry(name, key, NULL, false, true);
  LOG("Entry created.");
}

void createRegion(const char* name, bool ackMode, const char* endpoints,
    bool clientNotificationEnabled = false)
{
  LOG("createRegion() entered.");
  LOGINFO("Creating region --  %s  ackMode is %d", name, ackMode);
  // ack, caching
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, true,
      NULLPTR, endpoints, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG( "Region created." );
}
void createPooledRegion( const char * name, bool ackMode, const char * endpoints, const char* locators,const char* poolname, bool clientNotificationEnabled = false, bool cachingEnable = true)
{
  LOG( "createRegion_Pool() entered." );
  fprintf( stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode );
  fflush( stdout );
  RegionPtr regPtr = getHelper()->createPooledRegion(name,ackMode,endpoints, locators, poolname ,cachingEnable, clientNotificationEnabled);
  ASSERT( regPtr != NULLPTR, "Failed to create region." );
  LOG( "Pooled Region created." );
}

void createEntry(const char* name, const char* key, const char* value)
{
  LOG("createEntry() entered.");
  LOGINFO("Creating entry -- key: %s  value: %s in region %s", key,
      value, name);
  // Create entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableStringPtr valPtr = CacheableString::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT(!regPtr->containsKey(keyPtr),
      "Key should not have been found in region.");
  ASSERT(!regPtr->containsValueForKey(keyPtr),
      "Value should not have been found in region.");

  //regPtr->create(keyPtr, valPtr);
  regPtr->put(keyPtr, valPtr);
  LOG("Created entry.");

  verifyEntry(name, key, value);
  LOG("Entry created.");
}

void updateEntry(const char* name, const char* key, const char* value)
{
  LOG("updateEntry() entered.");
  LOGINFO("Updating entry -- key: %s  value: %s in region %s", key,
      value, name);
  // Update entry, verify entry is correct
  CacheableKeyPtr keyPtr = createKey(key);
  CacheableStringPtr valPtr = CacheableString::create(value);

  RegionPtr regPtr = getHelper()->getRegion(name);
  ASSERT(regPtr != NULLPTR, "Region not found.");

  ASSERT(regPtr->containsKey(keyPtr), "Key should have been found in region.");
  //ASSERT(regPtr->containsValueForKey(keyPtr),
  //    "Value should have been found in region.");

  regPtr->put(keyPtr, valPtr);
  LOG("Put entry.");

  verifyEntry(name, key, value);
  LOG("Entry updated.");
}

void doNetsearch(const char* name, const char* key, const char* value)
{
  LOG("doNetsearch() entered.");
  LOGINFO(
      "Netsearching for entry -- key: %s  expecting value: %s in region %s",
      key, value, name);
  // Get entry created in Process A, verify entry is correct
  CacheableKeyPtr keyPtr = CacheableKey::create(key);

  RegionPtr regPtr = getHelper()->getRegion(name);
  LOGINFO("netsearch  region %s", regPtr->getName());
  ASSERT(regPtr != NULLPTR, "Region not found.");

  //ASSERT(!regPtr->containsKey(keyPtr),
  //    "Key should not have been found in region.");
  //ASSERT(!regPtr->containsValueForKey(keyPtr),
  //    "Value should not have been found in region.");

  CacheableStringPtr checkPtr = dynCast<CacheableStringPtr>(
      regPtr->get(keyPtr)); // force a netsearch

  if (checkPtr != NULLPTR) {
    LOG("checkPtr is not null");
    char buf[1024];
    sprintf(buf, "In net search, get returned %s for key %s",
        checkPtr->asChar(), key);
    LOG(buf);
  }
  else {
    LOG("checkPtr is NULL");
  }
  verifyEntry(name, key, value);
  LOG("Netsearch complete.");
}

// End: Utility methods


const char * keys[] = { "Key-1", "Key-2", "Key-3", "Key-4" };
const char * vals[] = { "Value-1", "Value-2", "Value-3", "Value-4" };
const char * nvals[] = { "New Value-1", "New Value-2", "New Value-3",
    "New Value-4" };

const char * regionNames[] = { "DistRegionAck", "DistRegionNoAck" };

const bool USE_ACK = true;
const bool NO_ACK = false;

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
{
  if (isLocalServer)
    CacheHelper::initServer(1, "cacheserver_notify_subscription.xml");
  LOG("SERVER1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
{
  initClient(true);
  createRegion(regionNames[0], USE_ACK, endPoints, true);
  createRegion(regionNames[1], NO_ACK, endPoints, true);
  // create some entries in the cache from client 1
  createEntry(regionNames[0], keys[1], vals[1]);
  createEntry(regionNames[1], keys[3], vals[3]);
  LOG("StepOne complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pool_Locator)
{
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, NULL, locatorsG,"__TEST_POOL1__", true);
  createPooledRegion( regionNames[1], NO_ACK, NULL, locatorsG, "__TEST_POOL1__", true);
  // create some entries in the cache from client 1
  createEntry(regionNames[0], keys[1], vals[1]);
  createEntry(regionNames[1], keys[3], vals[3]);
  LOG("StepOne complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pool_EndPoint)
{
  initClient(true);

  createPooledRegion( regionNames[0], USE_ACK, endPoints, NULL,"__TEST_POOL1__", true);
  createPooledRegion( regionNames[1], NO_ACK, endPoints,NULL,"__TEST_POOL1__", true);;
  // create some entries in the cache from client 1
  createEntry(regionNames[0], keys[1], vals[1]);
  createEntry(regionNames[1], keys[3], vals[3]);
  LOG("StepOne complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo)
{
  // Client two register all keys.
  initClient(true);
  createRegion(regionNames[0], USE_ACK, endPoints, true);
  createRegion(regionNames[1], NO_ACK, endPoints, true);

  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
  VectorOfCacheableKeyPtr resultKeys(new VectorOfCacheableKey());
  // create a local entry to check for no change after register interest
  createEntry(regionNames[0], keys[1], nvals[1]);
  regPtr0->registerAllKeys(false, resultKeys, true);
  regPtr1->registerAllKeys(false, NULLPTR, true);

  // check that initial entries are created properly
  ASSERT(regPtr0->size() == 1, "Expected one entry in region");
  ASSERT(regPtr1->size() == 1, "Expected one entry in region");
  ASSERT(resultKeys->size() == 1, "Expected one key from registerAllKeys");
  ASSERT(strcmp(dynCast<CacheableStringPtr>(
      resultKeys->operator[](0))->asChar(), keys[1]) == 0,
      "Unexpected key from registerAllKeys");

  LOG("StepTwo complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pool_Locator)
{
  // Client two register all keys.
  initClient(true);
 createPooledRegion( regionNames[0], USE_ACK, NULL, locatorsG,"__TEST_POOL1__", true);
  createPooledRegion( regionNames[1], NO_ACK, NULL, locatorsG, "__TEST_POOL1__", true);

  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
  VectorOfCacheableKeyPtr resultKeys(new VectorOfCacheableKey());
  // create a local entry to check for no change after register interest
  createEntry(regionNames[0], keys[1], nvals[1]);
  regPtr0->registerAllKeys(false, resultKeys, true);
  regPtr1->registerAllKeys(false, NULLPTR, true);

  // check that initial entries are created properly
  ASSERT(regPtr0->size() == 1, "Expected one entry in region");
  ASSERT(regPtr1->size() == 1, "Expected one entry in region");
  ASSERT(resultKeys->size() == 1, "Expected one key from registerAllKeys");
  ASSERT(strcmp(dynCast<CacheableStringPtr>(
      resultKeys->operator[](0))->asChar(), keys[1]) == 0,
      "Unexpected key from registerAllKeys");

  LOG("StepTwo complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pool_EndPoint)
{
  // Client two register all keys.
  initClient(true);
  createPooledRegion( regionNames[0], USE_ACK, endPoints, NULL,"__TEST_POOL1__", true);
  createPooledRegion( regionNames[1], NO_ACK, endPoints,NULL,"__TEST_POOL1__", true);;

  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
  VectorOfCacheableKeyPtr resultKeys(new VectorOfCacheableKey());
  // create a local entry to check for no change after register interest
  createEntry(regionNames[0], keys[1], nvals[1]);
  regPtr0->registerAllKeys(false, resultKeys, true);
  regPtr1->registerAllKeys(false, NULLPTR, true);

  // check that initial entries are created properly
  ASSERT(regPtr0->size() == 1, "Expected one entry in region");
  ASSERT(regPtr1->size() == 1, "Expected one entry in region");
  ASSERT(resultKeys->size() == 1, "Expected one key from registerAllKeys");
  ASSERT(strcmp(dynCast<CacheableStringPtr>(
      resultKeys->operator[](0))->asChar(), keys[1]) == 0,
      "Unexpected key from registerAllKeys");

  LOG("StepTwo complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPointFive)
{
  initClient(true);
  // Same tests as in StepTwo with registerRegex(".*")
  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
  regPtr0->unregisterAllKeys();
  regPtr1->unregisterAllKeys();
  regPtr0->localDestroyRegion();
  regPtr1->localDestroyRegion();

  if ( g_poolConfig ) {
    if ( g_poolLocators ) {
      createPooledRegion( regionNames[0], USE_ACK, NULL, locatorsG,"__TEST_POOL1__", true);
      createPooledRegion( regionNames[1], NO_ACK, NULL, locatorsG, "__TEST_POOL1__", true);
    }
    else {
      createPooledRegion( regionNames[0], USE_ACK, endPoints, NULL,"__TEST_POOL1__", true);
      createPooledRegion( regionNames[1], NO_ACK, endPoints,NULL,"__TEST_POOL1__", true);;
    }
  }
  else {
    createRegion(regionNames[0], USE_ACK, endPoints, true);
    createRegion(regionNames[1], NO_ACK, endPoints, true);
  }

  regPtr0 = getHelper()->getRegion(regionNames[0]);
  regPtr1 = getHelper()->getRegion(regionNames[1]);
  VectorOfCacheableKeyPtr resultKeys(new VectorOfCacheableKey());
  // create a local entry to check for no change after register interest
  createEntry(regionNames[0], keys[1], nvals[1]);
  regPtr0->registerRegex(".*", false, resultKeys, true);
  regPtr1->registerRegex(".*", false, NULLPTR, true);

  // check that initial entries are created properly
  ASSERT(regPtr0->size() == 1, "Expected one entry in region");
  ASSERT(regPtr1->size() == 1, "Expected one entry in region");
  ASSERT(resultKeys->size() == 1, "Expected one key from registerAllKeys");
  ASSERT(strcmp(dynCast<CacheableStringPtr>(
      resultKeys->operator[](0))->asChar(), keys[1]) == 0,
      "Unexpected key from registerAllKeys");

  verifyCreated(regionNames[0], keys[1]);
  verifyCreated(regionNames[1], keys[3]);
  verifyEntry(regionNames[0], keys[1], nvals[1]);
  verifyEntry(regionNames[1], keys[3], vals[3]);

  LOG("StepTwoPointFive complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
{
  // check the combination of (resultKeys != NULL) and
  // (getValues == false) in registerAllKeys
  VectorOfCacheableKeyPtr resultKeys(new VectorOfCacheableKey());
  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  regPtr0->registerAllKeys(false, resultKeys, false);

  ASSERT(regPtr0->size() == 1, "Expected one entry in region");
  ASSERT(regPtr0->containsKey(keys[1]), "Expected region to contain the key");
  ASSERT(!regPtr0->containsValueForKey(keys[1]),
      "Expected region to not contain the value");
  ASSERT(resultKeys->size() == 1, "Expected one key from registerAllKeys");
  ASSERT(strcmp(dynCast<CacheableStringPtr>(
      resultKeys->operator[](0))->asChar(), keys[1]) == 0,
      "Unexpected key from registerAllKeys");

  // check the same for registerRegex(".*")
  RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
  resultKeys->clear();
  regPtr1->registerRegex(".*", false, resultKeys, false);

  ASSERT(regPtr1->size() == 1, "Expected one entry in region");
  ASSERT(regPtr1->containsKey(keys[3]), "Expected region to contain the key");
  ASSERT(!regPtr1->containsValueForKey(keys[3]),
      "Expected region to not contain the value");
  ASSERT(resultKeys->size() == 1, "Expected one key from registerRegex");
  ASSERT(strcmp(dynCast<CacheableStringPtr>(
      resultKeys->operator[](0))->asChar(), keys[3]) == 0,
      "Unexpected key from registerRegex");

  createEntry(regionNames[0], keys[0], vals[0]);
  updateEntry(regionNames[0], keys[1], vals[1]);
  createEntry(regionNames[1], keys[2], vals[2]);

  LOG("StepThree complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepFour)
{
  // Client two should recieve all entries
  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

  ASSERT(regPtr0->size() == 2, "Expected two entries in region");
  ASSERT(regPtr1->size() == 2, "Expected two entries in region");

  verifyCreated(regionNames[0], keys[0]);
  verifyCreated(regionNames[1], keys[2]);
  verifyEntry(regionNames[0], keys[0], vals[0]);
  verifyEntry(regionNames[0], keys[1], vals[1]);
  verifyEntry(regionNames[1], keys[2], vals[2]);
  verifyEntry(regionNames[1], keys[3], vals[3]);

  LOG("StepFour complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
{
  if (isLocalServer)
    CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml");
  LOG("SERVER2 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1,CloseServer1)
{
  // failover happens.
  if (isLocalServer) {
    CacheHelper::closeServer(1);
    LOG("SERVER1 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFive)
{
  // Client one update entries
  updateEntry(regionNames[0], keys[0], nvals[0]);
  updateEntry(regionNames[1], keys[2], nvals[2]);
  LOG("StepFive complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepSix)
{
  // Client two should see updates after failover.
  verifyEntry(regionNames[0], keys[0], nvals[0]);
  verifyEntry(regionNames[0], keys[1], vals[1]);
  verifyEntry(regionNames[1], keys[2], nvals[2]);
  verifyEntry(regionNames[1], keys[3], vals[3]);
  LOG("StepSix complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepSeven)
{
  // Client two unregister all keys
  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
  regPtr0->unregisterAllKeys();
  regPtr1->unregisterAllKeys();

  LOG("StepSeven complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepEight)
{
  // Client one update entries
  updateEntry(regionNames[0], keys[0], vals[0]);
  verifyEntry(regionNames[0], keys[1], vals[1]);
  updateEntry(regionNames[1], keys[2], vals[2]);
  verifyEntry(regionNames[1], keys[3], vals[3]);
  LOG("StepEight complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepNine)
{
  // Client two should still have the original values
  verifyEntry(regionNames[0], keys[0], nvals[0]);
  verifyEntry(regionNames[0], keys[1], vals[1]);
  verifyEntry(regionNames[1], keys[2], nvals[2]);
  verifyEntry(regionNames[1], keys[3], vals[3]);
  LOG("StepNine complete.");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,CloseCache1)
{
  cleanProc();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,CloseCache2)
{
  cleanProc();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2,CloseServer2)
{
  if (isLocalServer) {
    CacheHelper::closeServer(2);
    LOG("SERVER2 stopped");
  }
}
END_TASK_DEFINITION

void runThinClientFailoverInterestAllWithCache(bool poolConfig = true, bool isLocator = true)
{
  initLocatorSettings( poolConfig, isLocator );
  if( poolConfig && isLocator )
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML)
  }
  else
  {
    CALL_TASK( CreateServer1 );
  }
  if( !poolConfig )
  {
    CALL_TASK(StepOne);
    CALL_TASK(StepTwo);
  }
  else if( isLocator )
  {
    CALL_TASK(StepOne_Pool_Locator);
    CALL_TASK(StepTwo_Pool_Locator);
  }
  else
  {
    CALL_TASK(StepOne_Pool_EndPoint);
    CALL_TASK(StepTwo_Pool_EndPoint);
  }
  CALL_TASK(StepTwoPointFive);
  CALL_TASK(StepThree);
  CALL_TASK(StepFour);
  if(poolConfig && isLocator)
  {
    CALL_TASK(CreateServer2_With_Locator_XML);
  }
  else
  {
    CALL_TASK( CreateServer2 );
  }
  CALL_TASK(CloseServer1);
  CALL_TASK(StepFive);
  CALL_TASK(StepSix);
  CALL_TASK(StepSeven);
  CALL_TASK(StepEight);
  CALL_TASK(StepNine);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer2);
  if( poolConfig && isLocator ) {
    CALL_TASK( CloseLocator1 );
  }
}
