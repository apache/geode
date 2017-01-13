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

#define ROOT_NAME "ThinClientRIwithlocalRegionDestroy"
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
const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
#include "LocatorHelper.hpp"

class SimpleCacheListener : public CacheListener {
 public:
  int m_totalEvents;

  SimpleCacheListener() : m_totalEvents(0) {}

  ~SimpleCacheListener() {}

 public:
  // The Cache Listener callbacks.
  virtual void afterCreate(const EntryEvent& event) {
    LOGINFO("SimpleCacheListener: Got an afterCreate event.");
    m_totalEvents++;
  }

  virtual void afterUpdate(const EntryEvent& event) {
    LOGINFO("SimpleCacheListener: Got an afterUpdate event.");
    m_totalEvents++;
  }

  virtual void afterInvalidate(const EntryEvent& event) {
    LOGINFO("SimpleCacheListener: Got an afterInvalidate event.");
    m_totalEvents++;
  }

  virtual void afterDestroy(const EntryEvent& event) {
    LOGINFO("SimpleCacheListener: Got an afterDestroy event.");
    m_totalEvents++;
  }

  virtual void afterRegionInvalidate(const RegionEvent& event) {
    LOGINFO("SimpleCacheListener: Got an afterRegionInvalidate event.");
    m_totalEvents++;
  }

  virtual void afterRegionDestroy(const RegionEvent& event) {
    LOGINFO("SimpleCacheListener: Got an afterRegionDestroy event.");
    if (event.remoteOrigin()) {
      m_totalEvents++;
    }
  }

  virtual void close(const RegionPtr& region) {
    LOGINFO("SimpleCacheListener: Got a close event.");
  }
};
typedef SharedPtr<SimpleCacheListener> SimpleCacheListenerPtr;

SimpleCacheListenerPtr eventListener1 = NULLPTR;
SimpleCacheListenerPtr eventListener2 = NULLPTR;

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

const char* testregex[] = {"Key-*1", "Key-*2", "Key-*3", "Key-*4"};
const char* keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};
const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck",
                             "ExampleRegion", "SubRegion1", "SubRegion2"};
const char* vals[] = {"Value-1", "Value-2", "Value-3", "Value-4"};
const char* nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                       "New Value-4"};

const bool USE_ACK = true;
const bool NO_ACK = false;

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pool_Locator)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_",
                       true);
    createPooledRegion(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_", true);
    createPooledRegion(regionNames[2], NO_ACK, locatorsG, "__TESTPOOL1_", true);

    // create subregion
    RegionPtr regptr = getHelper()->getRegion(regionNames[2]);
    RegionAttributesPtr lattribPtr = regptr->getAttributes();
    RegionPtr subregPtr1 = regptr->createSubregion(regionNames[3], lattribPtr);
    RegionPtr subregPtr2 = regptr->createSubregion(regionNames[4], lattribPtr);

    LOGINFO(
        "NIL: CLIENT1 StepOne_Pool_Locator subregions created successfully");

    // Attache Listener
    RegionPtr regionPtr0 = getHelper()->getRegion(regionNames[0]);
    AttributesMutatorPtr attrMutatorPtr = regionPtr0->getAttributesMutator();
    eventListener1 = new SimpleCacheListener();
    attrMutatorPtr->setCacheListener(eventListener1);

    AttributesMutatorPtr subregAttrMutatorPtr =
        subregPtr1->getAttributesMutator();
    eventListener2 = new SimpleCacheListener();
    subregAttrMutatorPtr->setCacheListener(eventListener2);

    LOG("StepOne_Pool complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pool_Locator)
  {
    initClient(true);
    createPooledRegion(regionNames[0], USE_ACK, locatorsG, "__TESTPOOL1_",
                       true);
    createPooledRegion(regionNames[1], NO_ACK, locatorsG, "__TESTPOOL1_", true);
    createPooledRegion(regionNames[2], NO_ACK, locatorsG, "__TESTPOOL1_", true);

    // create subregion
    RegionPtr regptr = getHelper()->getRegion(regionNames[2]);
    RegionAttributesPtr lattribPtr = regptr->getAttributes();
    RegionPtr subregPtr1 = regptr->createSubregion(regionNames[3], lattribPtr);
    RegionPtr subregPtr2 = regptr->createSubregion(regionNames[4], lattribPtr);

    LOGINFO(
        "NIL: CLIENT2 StepTwo_Pool_Locator:: subregions created successfully");
    LOG("StepTwo_Pool complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, registerKeysOnRegion)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    // RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
    /*regPtr0->registerRegex(testregex[0]);
    regPtr1->registerRegex(testregex[1]);*/

    // NIL
    VectorOfCacheableKey keys;
    keys.push_back(CacheableString::create("Key-1"));
    keys.push_back(CacheableString::create("Key-2"));
    regPtr0->registerKeys(keys, false);
    LOGINFO("NIL CLIENT-1 registerAllKeys() done ");

    regPtr0->localDestroyRegion();
    LOGINFO("NIL CLIENT-1 localDestroyRegion() done");
    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, putOps)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);

    for (int index = 0; index < 5; index++) {
      char key[100] = {0};
      char value[100] = {0};
      ACE_OS::sprintf(key, "Key-%d", index);
      ACE_OS::sprintf(value, "Value-%d", index);
      CacheableKeyPtr keyptr = CacheableKey::create(key);
      CacheablePtr valuePtr = CacheableString::create(value);
      regPtr0->put(keyptr, valuePtr);
    }
    LOG("StepFour complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);

    regPtr0->registerAllKeys();
    LOGINFO("NIL CLIENT-1 registerAllKeys() done ");

    regPtr0->localDestroyRegion();
    LOGINFO("NIL CLIENT-1 localDestroyRegion() done");
    LOG("StepThree complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepFour)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);

    for (int index = 0; index < 5; index++) {
      char key[100] = {0};
      char value[100] = {0};
      ACE_OS::sprintf(key, "Key-%d", index);
      ACE_OS::sprintf(value, "Value-%d", index);
      CacheableKeyPtr keyptr = CacheableKey::create(key);
      CacheablePtr valuePtr = CacheableString::create(value);
      regPtr0->put(keyptr, valuePtr);
    }
    LOG("StepFour complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, verifyEventsForDestroyedregion)
  {
    LOGINFO("NIL:LINE_537 eventListener1->m_totalEvents = %d ",
            eventListener1->m_totalEvents);
    ASSERT(eventListener1->m_totalEvents == 0,
           "Region Event count must be zero");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, verifyEventsForDestroyedSubregion)
  {
    LOGINFO("NIL:LINE_543 eventListener2->m_totalEvents = %d ",
            eventListener2->m_totalEvents);
    ASSERT(eventListener2->m_totalEvents == 0,
           "Subregion Event count must be zero");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepFive)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
    regPtr0->registerRegex(testregex[0]);
    regPtr1->registerRegex(testregex[1]);
    LOGINFO("NIL CLIENT-1 registerRegex() done ");

    regPtr0->localDestroyRegion();
    LOGINFO("NIL CLIENT-1 localDestroyRegion() done");
    LOG("NIL: Client-1 StepFive complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepSix)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

    for (int index = 0; index < 5; index++) {
      char key[100] = {0};
      char value[100] = {0};
      ACE_OS::sprintf(key, "Key-%d", index);
      ACE_OS::sprintf(value, "Value-%d", index);
      CacheableKeyPtr keyptr = CacheableKey::create(key);
      CacheablePtr valuePtr = CacheableString::create(value);
      regPtr0->put(keyptr, valuePtr);
      regPtr1->put(keyptr, valuePtr);
    }
    LOG("NIL : Client-2 StepSix complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifyOps)
  {
    // regPtr0 is destroyed
    // RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

    CacheableKeyPtr keyptr = CacheableKey::create("Key-2");

    // regPtr0 is destroyed
    // ASSERT( !regPtr0->containsKey( keyptr ), "Key must not found in region0."
    // );

    ASSERT(regPtr1->containsKey(keyptr), "Key must found in region1.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepSeven)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
    RegionPtr regPtr2 = getHelper()->getRegion(regionNames[2]);

    RegionPtr subregPtr0 = regPtr2->getSubregion(regionNames[3]);
    RegionPtr subregPtr1 = regPtr2->getSubregion(regionNames[4]);

    // 1. registerAllKeys on parent and both subregions
    regPtr2->registerAllKeys();
    subregPtr0->registerAllKeys();
    subregPtr1->registerAllKeys();

    LOGINFO("NIL CLIENT-1 StepSeven ::  registerAllKeys() done ");

    // 2. Now locally destroy SubRegion1
    subregPtr0->localDestroyRegion();
    LOGINFO("NIL CLIENT-1 SubRegion1 locally destroyed successfully");

    LOG("NIL: Client-1 StepSeven complete.");

    /*
    regPtr0->registerRegex(testregex[0]);
    regPtr1->registerRegex(testregex[1]);
    LOGINFO("NIL CLIENT-1 registerRegex() done ");
    */
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepEight)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);
    RegionPtr regPtr2 = getHelper()->getRegion(regionNames[2]);

    RegionPtr subregPtr0 = regPtr2->getSubregion(regionNames[3]);
    RegionPtr subregPtr1 = regPtr2->getSubregion(regionNames[4]);

    for (int index = 0; index < 5; index++) {
      char key[100] = {0};
      char value[100] = {0};
      ACE_OS::sprintf(key, "Key-%d", index);
      ACE_OS::sprintf(value, "Value-%d", index);
      CacheableKeyPtr keyptr = CacheableKey::create(key);
      CacheablePtr valuePtr = CacheableString::create(value);
      regPtr2->put(keyptr, valuePtr);
      subregPtr0->put(keyptr, valuePtr);
      subregPtr1->put(keyptr, valuePtr);
    }

    LOG("NIL : Client-2 StepSix complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, VerifySubRegionOps)
  {
    RegionPtr regPtr2 = getHelper()->getRegion(regionNames[2]);
    // locally destroyed
    // RegionPtr subregPtr0 = regPtr2->getSubregion( regionNames[3] );
    RegionPtr subregPtr1 = regPtr2->getSubregion(regionNames[4]);

    for (int index = 0; index < 5; index++) {
      char key[100] = {0};
      char value[100] = {0};
      ACE_OS::sprintf(key, "Key-%d", index);
      ACE_OS::sprintf(value, "Value-%d", index);
      CacheableKeyPtr keyptr = CacheableKey::create(key);
      CacheablePtr valuePtr = CacheableString::create(value);

      ASSERT(regPtr2->containsKey(keyptr), "Key must found in region1.");
      ASSERT(subregPtr1->containsKey(keyptr), "Key must found in region1.");

      // Need to check in cliet/server logs that client-1 do not receive any
      // notification for subregPtr0
    }
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

// testRegisterKeyForLocalRegionDestroy
void testRegisterKeyForLocalRegionDestroy() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator_XML_Bug849);

  CALL_TASK(StepOne_Pool_Locator);
  CALL_TASK(StepTwo_Pool_Locator);

  CALL_TASK(registerKeysOnRegion);
  CALL_TASK(putOps);
  CALL_TASK(verifyEventsForDestroyedregion);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);
  CALL_TASK(CloseLocator1);
}

void testRegisterAllKeysForLocalRegionDestroy() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator_XML_Bug849);

  CALL_TASK(StepOne_Pool_Locator);
  CALL_TASK(StepTwo_Pool_Locator);

  CALL_TASK(StepThree);
  CALL_TASK(StepFour);
  CALL_TASK(verifyEventsForDestroyedregion);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator1);
}

void testRegisterRegexForLocalRegionDestroy() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator_XML_Bug849);

  CALL_TASK(StepOne_Pool_Locator);
  CALL_TASK(StepTwo_Pool_Locator);

  CALL_TASK(StepFive);
  CALL_TASK(StepSix);
  CALL_TASK(verifyEventsForDestroyedregion);
  CALL_TASK(VerifyOps);

  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator1);
}

void testSubregionForLocalRegionDestroy() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator_XML_Bug849);

  CALL_TASK(StepOne_Pool_Locator);
  CALL_TASK(StepTwo_Pool_Locator);

  CALL_TASK(StepSeven);
  CALL_TASK(StepEight);
  CALL_TASK(verifyEventsForDestroyedSubregion);
  CALL_TASK(VerifySubRegionOps);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator1);
}
