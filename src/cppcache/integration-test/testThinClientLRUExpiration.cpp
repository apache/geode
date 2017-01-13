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

#define ROOT_NAME "testThinClientLRUExpiration"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"
#include "TallyListener.hpp"
#include "TallyWriter.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

CacheHelper* cacheHelper = NULL;
bool isLocalServer = false;

static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);

const char* regionNames[] = {"DistRegionAck1", "DistRegionAck2",
                             "DistRegionAck3", "DistRegionAck4",
                             "DistRegionAck5", "DistRegionAck"};
const bool USE_ACK = true;
const bool NO_ACK ATTR_UNUSED = false;

TallyListenerPtr regListener;
TallyWriterPtr regWriter;
bool registerKey = true;
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
void printAttribute(RegionAttributesPtr& attr) {
  printf("CachingEnable: %s\n",
         attr->getCachingEnabled() ? "enabled" : "disabled");
  printf("InitialCapacity: %d\n", attr->getInitialCapacity());
  printf("LoadFactor: %f\n", attr->getLoadFactor());
  printf("ConcurencyLevel: %d\n", attr->getConcurrencyLevel());
  printf("RegionTimeToLive: %d\n", attr->getRegionTimeToLive());
  printf("RegionIdleTimeout: %d\n", attr->getRegionIdleTimeout());
  printf("EntryTimeToLive: %d\n", attr->getEntryTimeToLive());
  printf("EntryIdleTimeout: %d\n", attr->getEntryIdleTimeout());
  printf("getLruEntriesLimit: %d\n", attr->getLruEntriesLimit());
  printf("RegionTimeToLiveAction: %d\n", attr->getRegionTimeToLiveAction());
  printf("RegionIdleTimeoutAction: %d\n", attr->getRegionIdleTimeoutAction());
  printf("EntryTimeToLiveAction: %d\n", attr->getEntryTimeToLiveAction());
  printf("EntryIdleTimeoutAction: %d\n", attr->getEntryIdleTimeoutAction());
  printf("LruEvictionAction: %d\n", attr->getLruEvictionAction());
  printf("ClientNotification: %s\n",
         attr->getClientNotificationEnabled() ? "true" : "false");
  // printf("getEndPoint: %s\n",attr->getEndpoints());
}

void setCacheListener(const char* regName, TallyListenerPtr regListener) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(regListener);
}

void setCacheWriter(const char* regName, TallyWriterPtr regWriter) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheWriter(regWriter);
}

void getRegionAttr(const char* name) {
  RegionPtr rptr = getHelper()->getRegion(name);
  RegionAttributesPtr m_currRegionAttributesPtr = rptr->getAttributes();
  printAttribute(m_currRegionAttributesPtr);
}

void ValidateDestroyRegion(const char* name) {
  RegionPtr rptr = getHelper()->getRegion(name);
  if (rptr == NULLPTR) {
    return;
  }
  try {
    rptr->put(1, 2);
    FAIL("Put should not be happened");
  } catch (RegionDestroyedException& ex) {
    char buffer[1024];
    sprintf(buffer, "Got expected exception %s: msg = %s", ex.getName(),
            ex.getMessage());
    LOG(buffer);
  } catch (Exception& ex) {
    char buffer[1024];
    sprintf(buffer, "Got unexpected exception %s: msg = %s", ex.getName(),
            ex.getMessage());
    FAIL(buffer);
  }
}

void createRegion(const char* name, bool ackMode, int ettl, int eit, int rttl,
                  int rit, int lel, bool clientNotificationEnabled = false,
                  ExpirationAction::Action action = ExpirationAction::DESTROY) {
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr =  // getHelper()->createRegion( name, ackMode, true,
      // ettl,eit,rttl,rit,lel,action,endpoints,clientNotificationEnabled
      // );
      getHelper()->createRegionAndAttachPool(name, ackMode, "LRUPool", true,
                                             ettl, eit, rttl, rit, lel, action);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  if (registerKey) regPtr->registerAllKeys(false, NULLPTR, false, false);
  LOG("Region created.");
}

void doRgnOperations(const char* name, int n, int rgnOpt = 0) {
  CacheableStringPtr value;
  char buf[16];
  if (rgnOpt == 0) {
    memset(buf, 'A', 15);
    buf[15] = '\0';
    memcpy(buf, "Value - ", 8);
    value = CacheableString::create(buf);
    ASSERT(value != NULLPTR, "Failed to create value.");
  }
  RegionPtr rptr = getHelper()->getRegion(name);
  ASSERT(rptr != NULLPTR, "Region not found.");
  for (int i = 0; i < n; i++) {
    sprintf(buf, "KeyA - %d", i + 1);
    CacheableKeyPtr key = CacheableKey::create(buf);
    switch (rgnOpt) {
      case 0:
        rptr->put(key, value);
        break;
      case 1:
        rptr->invalidate(key);
        break;
      case 2:
        rptr->localInvalidate(key);
        break;
      case 3:
        rptr->destroy(key);
        break;
      case 4:
        rptr->localDestroy(key);
        break;
      case 5:
        rptr->get(key);
        break;
    }
  }
}

void dumpCounters(const char* regName) {
  RegionPtr rptr = getHelper()->getRegion(regName);
  printf("Region size: %d\n", rptr->size());
  if (regListener != NULLPTR) {
    printf("counts:: creates: %d, updates: %d, invalidates: %d, destroys: %d\n",
           regListener->getCreates(), regListener->getUpdates(),
           regListener->getInvalidates(), regListener->getDestroys());
  }
}

int getNumOfEntries(const char* regName, bool isValue = false) {
  static bool useRegionSize = false;

  useRegionSize = !useRegionSize;
  dumpCounters(regName);
  RegionPtr rptr = getHelper()->getRegion(regName);
  if (isValue) {
    VectorOfCacheable v;
    rptr->values(v);
    printf("Region value size: %d\n", v.size());
    return v.size();
  } else if (!useRegionSize) {
    VectorOfCacheableKey v;
    rptr->keys(v);
    printf("Region key size: %d\n", v.size());
    return v.size();
  } else {
    return rptr->size();
  }
}

void localDestroyRegion(const char* name) {
  LOG("localDestroyRegion() entered.");
  RegionPtr regPtr = getHelper()->getRegion(name);
  regPtr->localDestroyRegion();
  ASSERT(regPtr->isDestroyed() == true, "Expected Region to be destroyed");
  LOG("Locally Region destroyed.");
}

void createThinClientRegion(
    const char* regionName, int ettl, int eit, int rttl, int rit, int lel,
    int noOfEntry = 0, int rgnOpt = 0, bool destroyRgn = true,
    bool clientNotificationEnabled = true,
    ExpirationAction::Action action = ExpirationAction::DESTROY) {
  if (destroyRgn) {
    try {
      doRgnOperations(regionName, noOfEntry, rgnOpt);
    } catch (EntryNotFoundException& ex) {
      char buffer[1024];
      sprintf(buffer, "Got expected exception %s: msg = %s", ex.getName(),
              ex.getMessage());
      LOG(buffer);
    }
    localDestroyRegion(regionName);
  }
  createRegion(regionName, USE_ACK, ettl, eit, rttl, rit, lel,
               clientNotificationEnabled, action);
  getRegionAttr(regionName);
}

DUNIT_TASK(SERVER1, CreateServer1)
  {
    if (isLocalServer) CacheHelper::initServer(1, "cacheserver4.xml");
    LOG("SERVER1 started");
  }
END_TASK(CreateServer1)

DUNIT_TASK(CLIENT1, StepOneCase1)
  {
    initClient(true);
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation
    // - [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    getHelper()->createPoolWithLocators("LRUPool", locatorsG, true);
    createThinClientRegion(regionNames[0], 4, 2, 0, 0, 0, 0, 6, false);
  }
END_TASK(StepOneCase1)

DUNIT_TASK(CLIENT2, StepTwoCase1)
  {
    initClient(true);
    // regionName, ettl, eit , rttl,
    // rit,lel,endpoints,noOfEntry,rgnOpetation - [put-0/get-5/destroy-3]
    // ,destroyRgn - [true/false] ,clientNotificationEnabled - [true/false]
    // ,ExpirationAction::Action
    getHelper()->createPoolWithLocators("LRUPool", locatorsG, true);
    createThinClientRegion(regionNames[0], 0, 0, 0, 0, 0, 0, 6, false);
  }
END_TASK(StepTwoCase1)

DUNIT_TASK(CLIENT1, StepThreeCase1)
  {
    doRgnOperations(regionNames[0], 100);
    ACE_OS::sleep(1);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 100, "Expected 100 entries");
    LOG("StepThree complete.");
  }
END_TASK(StepThreeCase1)
DUNIT_TASK(CLIENT2, StepFourCase1)
  {
    doRgnOperations(regionNames[0], 100, 5);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 100, "Expected 100 entries");
    LOG("StepFour complete.");
  }
END_TASK(StepFourCase1)
DUNIT_TASK(CLIENT1, StepFiveCase1)
  {
    // wair 5 sec so all enteries gone
    ACE_OS::sleep(5);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 0, "Expected 0 entries");
    LOG("StepFive complete.");
  }
END_TASK(StepFiveCase1)
DUNIT_TASK(CLIENT2, StepSixCase1)
  {
    ACE_OS::sleep(5);
    // all enteris has been deleted
    // int n = getNumOfEntries(regionNames[0],true);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 0, "Expected 0 entries");
    LOG("StepSix complete.");
  }
END_TASK(StepSixCase1)
DUNIT_TASK(CLIENT1, StepOneCase2)
  {
    // regionName, ettl, eit , rttl,
    // rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false]
    // ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 4, 2, 0, 0, 0, 100, 3, true, true,
                           ExpirationAction::LOCAL_INVALIDATE);
    LOG("StepOneCase2 complete.");
  }
END_TASK(StepOneCase2)

DUNIT_TASK(CLIENT2, StepTwoCase2)
  {
    // regionName, ettl, eit , rttl,
    // rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false]
    // ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 0, 0, 0, 0, 0, 100, 3);
    LOG("StepTwoCase2 complete.");
  }
END_TASK(StepTwoCase2)
DUNIT_TASK(CLIENT1, StepThreeCase2)
  {
    doRgnOperations(regionNames[0], 100);
    // 100 entry with invalidate
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 100, "Expected 100 entries");
    LOG("StepThreeCase2 complete.");
  }
END_TASK(StepThreeCase2)
DUNIT_TASK(CLIENT2, StepFourCase2)
  {
    doRgnOperations(regionNames[0], 100, 5);
    // should have 100
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 100, "Expected 100 entries");
    LOG("StepFourCase2 complete.");
  }
END_TASK(StepFourCase2)
DUNIT_TASK(CLIENT1, StepFiveCase2)
  {
    ACE_OS::sleep(2);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 100, "Expected 100 entries");
    ACE_OS::sleep(5);
    // value should be invalidate as passing true
    n = getNumOfEntries(regionNames[0], true);
    ASSERT(n == 0, "Expected 0 entries");
    LOG("StepFiveCase2 complete.");
  }
END_TASK(StepFiveCase2)
DUNIT_TASK(CLIENT2, StepSixCase2)
  {
    ACE_OS::sleep(2);
    int n = getNumOfEntries(regionNames[0], true);
    ASSERT(n == 100, "Expected 100 entries");
    LOG("StepSixCase2 complete.");
  }
END_TASK(StepSixCase2)

DUNIT_TASK(CLIENT1, StepOneCase3)
  {
    // regionName, ettl, eit , rttl,
    // rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn -
    // [true/false] ,clientNotificationEnabled -
    // [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 0, 0, 0, 0, 5, 10, 3);
  }
END_TASK(StepOneCase3)

DUNIT_TASK(CLIENT2, StepTwoCase3)
  {
    // regionName, ettl, eit , rttl,
    // rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn -
    // [true/false] ,clientNotificationEnabled -
    // [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 0, 0, 0, 0, 0, 10, 3);
  }
END_TASK(StepTwoCase3)
DUNIT_TASK(CLIENT1, StepThreeCase3)
  {
    doRgnOperations(regionNames[0], 10);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 5, "Expected 5 entries");
    LOG("StepThreeCase3 complete.");
  }
END_TASK(StepThreeCase3)
DUNIT_TASK(CLIENT2, StepFourCase3)
  {
    doRgnOperations(regionNames[0], 10, 5);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepFourCase3 complete.");
  }
END_TASK(StepFourCase3)
DUNIT_TASK(CLIENT1, StepFiveCase3)
  {
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 5, "Expected 5 entries");
    n = getNumOfEntries(regionNames[0], true);
    ASSERT(n == 5, "Expected 5 entries");
    LOG("StepFiveCase3 complete.");
  }
END_TASK(StepFiveCase3)
DUNIT_TASK(CLIENT2, StepSixCase3)
  {
    ACE_OS::sleep(2);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 10, "Expected 10 entries");
    n = getNumOfEntries(regionNames[0], true);
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepSixCase3 complete.");
  }
END_TASK(StepSixCase3)

DUNIT_TASK(CLIENT1, StepOneCase4)
  {
    // regionName, ettl, eit , rttl,
    // rit,lel,endpoints,noOfEntry,rgnOpetation
    // - [put-0/get-5/destroy-3]
    // ,destroyRgn - [true/false]
    // ,clientNotificationEnabled -
    // [true/false]
    // ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 5, 0, 0, 0, 5, 10, 3, true, true,
                           ExpirationAction::LOCAL_INVALIDATE);
  }
END_TASK(StepOneCase4)

DUNIT_TASK(CLIENT2, StepTwoCase4)
  {
    // regionName, ettl, eit , rttl,
    // rit,lel,endpoints,noOfEntry,rgnOpetation
    // - [put-0/get-5/destroy-3]
    // ,destroyRgn - [true/false]
    // ,clientNotificationEnabled -
    // [true/false]
    // ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 0, 0, 0, 0, 0, 10, 3);
  }
END_TASK(StepTwoCase4)
DUNIT_TASK(CLIENT1, StepThreeCase4)
  {
    doRgnOperations(regionNames[0], 10);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 5, "Expected 5 entries");
    LOG("StepThreeCase4 complete.");
  }
END_TASK(StepThreeCase4)
DUNIT_TASK(CLIENT2, StepFourCase4)
  {
    doRgnOperations(regionNames[0], 10, 5);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepFourCase4 complete.");
  }
END_TASK(StepFourCase4)
DUNIT_TASK(CLIENT1, StepFiveCase4)
  {
    ACE_OS::sleep(2);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 5, "Expected 5 entries");
    n = getNumOfEntries(regionNames[0], true);
    ASSERT(n == 5, "Expected 5 entries");
    ACE_OS::sleep(4);
    n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 5, "Expected 5 entries");
    LOG("StepFiveCase4 "
        "complete.");
  }
END_TASK(StepFiveCase4)
DUNIT_TASK(CLIENT2, StepSixCase4)
  {
    ACE_OS::sleep(1);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 10, "Expected 10 entries");
    n = getNumOfEntries(regionNames[0], true);
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepSixCase4 "
        "complete.");
  }
END_TASK(StepSixCase4)

DUNIT_TASK(CLIENT1, StepOneCase5)
  {
    // regionName, ettl, eit ,
    // rttl,
    // rit,lel,endpoints,noOfEntry,rgnOpetation
    // -
    // [put-0/get-5/destroy-3]
    // ,destroyRgn -
    // [true/false]
    // ,clientNotificationEnabled
    // - [true/false]
    // ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 5, 0, 0, 0, 5, 10, 3);
  }
END_TASK(StepOneCase5)

DUNIT_TASK(CLIENT2, StepTwoCase5)
  {
    // regionName, ettl, eit
    // , rttl,
    // rit,lel,endpoints,noOfEntry,rgnOpetation
    // -
    // [put-0/get-5/destroy-3]
    // ,destroyRgn -
    // [true/false]
    // ,clientNotificationEnabled
    // - [true/false]
    // ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 0, 0, 0, 0, 0, 10, 3);
  }
END_TASK(StepTwoCase5)
DUNIT_TASK(CLIENT1, StepThreeCase5)
  {
    doRgnOperations(regionNames[0], 10);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 5,
           "Expected 5 "
           "entries");
    LOG("StepTwoCase5 "
        "complete.");
  }
END_TASK(StepThreeCase5)
DUNIT_TASK(CLIENT2, StepFourCase5)
  {
    doRgnOperations(regionNames[0], 10, 5);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 10,
           "Expected "
           "10 "
           "entries");
    LOG("StepFourCase5 "
        "complete.");
  }
END_TASK(StepFourCase5)
DUNIT_TASK(CLIENT1, StepFiveCase5)
  {
    ACE_OS::sleep(2);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 5,
           "Expected "
           "5 "
           "entries");
    n = getNumOfEntries(regionNames[0], true);
    ASSERT(n == 5,
           "Expected "
           "5 "
           "entries");
    ACE_OS::sleep(4);
    n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 0,
           "Expected "
           "0 "
           "entries");
    LOG("StepFiveCase"
        "5 "
        "complete.");
  }
END_TASK(StepFiveCase5)
DUNIT_TASK(CLIENT2, StepSixCase5)
  {
    ACE_OS::sleep(2);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 5,
           "Expecte"
           "d 5 "
           "entrie"
           "s");
    n = getNumOfEntries(regionNames[0], true);
    ASSERT(n == 5,
           "Expecte"
           "d 5 "
           "entrie"
           "s");
    LOG("StepSixCas"
        "e5 "
        "complete"
        ".");
  }
END_TASK(StepSixCase5)
DUNIT_TASK(CLIENT1, StepOneCase6)
  {
    // regionName,
    // ettl, eit ,
    // rttl,
    // rit,lel,endpoints,noOfEntry,rgnOpetation
    // -
    // [put-0/get-5/destroy-3]
    // ,destroyRgn
    // -
    // [true/false]
    // ,clientNotificationEnabled
    // -
    // [true/false]
    // ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 0, 5, 0, 0, 5, 10, 3, true, true,
                           ExpirationAction::LOCAL_INVALIDATE);
  }
END_TASK(StepOneCase6)

DUNIT_TASK(CLIENT2, StepTwoCase6)
  {
    // regionName,
    // ettl, eit
    // , rttl,
    // rit,lel,endpoints,noOfEntry,rgnOpetation
    // -
    // [put-0/get-5/destroy-3]
    // ,destroyRgn
    // -
    // [true/false]
    // ,clientNotificationEnabled
    // -
    // [true/false]
    // ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 0, 0, 0, 0, 0, 10, 3);
  }
END_TASK(StepTwoCase6)
DUNIT_TASK(CLIENT1, StepThreeCase6)
  {
    doRgnOperations(regionNames[0], 10);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 5,
           "Expe"
           "cted"
           " 5 "
           "entr"
           "ie"
           "s");
    LOG("Step"
        "Thre"
        "eCas"
        "e6 "
        "comp"
        "lete"
        ".");
  }
END_TASK(StepThreeCase6)
DUNIT_TASK(CLIENT2, StepFourCase6)
  {
    doRgnOperations(regionNames[0], 10, 5);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 10,
           "Ex"
           "pe"
           "ct"
           "ed"
           " 1"
           "0 "
           "en"
           "tr"
           "ie"
           "s");
    LOG("St"
        "ep"
        "Fo"
        "ur"
        "Ca"
        "se"
        "6 "
        "co"
        "mp"
        "le"
        "te"
        ".");
  }
END_TASK(StepFourCase6)
DUNIT_TASK(CLIENT1, StepFiveCase6)
  {
    ACE_OS::sleep(2);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 5, "Expected 5 entries");
    n = getNumOfEntries(regionNames[0], true);
    ASSERT(n == 5, "Expected 5 entries");
    ACE_OS::sleep(4);
    n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 5, "Expected 5 entries");
    LOG("StepFiveCase6 complete.");
  }
END_TASK(StepFiveCase6)
DUNIT_TASK(CLIENT2, StepSixCase6)
  {
    ACE_OS::sleep(1);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 10, "Expected 10 entries");
    n = getNumOfEntries(regionNames[0], true);
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepSixCase6 complete.");
  }
END_TASK(StepSixCase6)
DUNIT_TASK(CLIENT1, StepOneCase7)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 0, 0, 10, 0, 0, 10, 3);
  }
END_TASK(StepOneCase7)

DUNIT_TASK(CLIENT2, StepTwoCase7)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[0], 0, 0, 0, 0, 0, 10, 3);
  }
END_TASK(StepTwoCase7)
DUNIT_TASK(CLIENT1, StepThreeCase7)
  {
    doRgnOperations(regionNames[0], 10);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepThreeCase7 complete.");
  }
END_TASK(StepThreeCase7)
DUNIT_TASK(CLIENT2, StepFourCase7)
  {
    doRgnOperations(regionNames[0], 10, 5);
    int n = getNumOfEntries(regionNames[0]);
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepFourCase7 complete.");
  }
END_TASK(StepFourCase7)
DUNIT_TASK(CLIENT1, StepFiveCase7)
  {
    ACE_OS::sleep(15);
    ValidateDestroyRegion(regionNames[0]);
    LOG("StepFiveCase7 complete.");
  }
END_TASK(StepFiveCase7)
DUNIT_TASK(CLIENT2, StepSixCase7)
  {
    ACE_OS::sleep(3);
    ValidateDestroyRegion(regionNames[0]);
    LOG("StepSixCase7 complete.");
  }
END_TASK(StepSixCase7)
DUNIT_TASK(CLIENT1, StepOneCase8)
  {
    ACE_OS::sleep(10);
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[1], 0, 0, 8, 0, 0, 0, 6, false);
  }
END_TASK(StepOneCase8)

DUNIT_TASK(CLIENT2, StepTwoCase8)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[1], 0, 0, 0, 0, 0, 0, 6, false);
  }
END_TASK(StepTwoCase8)
DUNIT_TASK(CLIENT1, StepThreeCase8)
  {
    doRgnOperations(regionNames[1], 10);
    int n = getNumOfEntries(regionNames[1]);
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepThreeCase8 complete.");
  }
END_TASK(StepThreeCase8)
DUNIT_TASK(CLIENT2, StepFourCase8)
  {
    doRgnOperations(regionNames[1], 10, 5);
    int n = getNumOfEntries(regionNames[1]);
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepFourCase8 complete.");
  }
END_TASK(StepFourCase8)
DUNIT_TASK(CLIENT1, StepFiveCase8)
  {
    ACE_OS::sleep(5);
    int n = getNumOfEntries(regionNames[1]);
    ASSERT(n == 10, "Expected 0 entries");
    ACE_OS::sleep(10);
    ValidateDestroyRegion(regionNames[1]);
    LOG("StepFiveCase8 complete.");
  }
END_TASK(StepFiveCase8)
DUNIT_TASK(CLIENT2, StepSixCase8)
  {
    ACE_OS::sleep(2);
    ValidateDestroyRegion(regionNames[1]);
    LOG("StepSixCase8 complete.");
  }
END_TASK(StepSixCase8)
DUNIT_TASK(CLIENT1, StepOneCase9)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[2], 4, 0, 8, 0, 5, 0, 6, false);
  }
END_TASK(StepOneCase9)

DUNIT_TASK(CLIENT2, StepTwoCase9)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[2], 0, 0, 0, 0, 0, 0, 6, false);
  }
END_TASK(StepTwoCase9)
DUNIT_TASK(CLIENT1, StepThreeCase9)
  {
    ACE_OS::sleep(2);
    doRgnOperations(regionNames[2], 10);
    int n = getNumOfEntries(regionNames[2]);
    ASSERT(n == 5, "Expected 5 entries");
    LOG("StepThreeCase9 complete.");
  }
END_TASK(StepThreeCase9)
DUNIT_TASK(CLIENT2, StepFourCase9)
  {
    doRgnOperations(regionNames[2], 10, 5);
    int n = getNumOfEntries(regionNames[2]);
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepFourCase9 complete.");
  }
END_TASK(StepFourCase9)
DUNIT_TASK(CLIENT1, StepFiveCase9)
  {
    ACE_OS::sleep(5);
    int n = getNumOfEntries(regionNames[2]);
    ASSERT(n == 0, "Expected 0 entries");
    ACE_OS::sleep(8);
    ValidateDestroyRegion(regionNames[2]);
    LOG("StepFiveCase9 complete.");
  }
END_TASK(StepFiveCase9)
DUNIT_TASK(CLIENT2, StepSixCase9)
  {
    ACE_OS::sleep(3);
    ValidateDestroyRegion(regionNames[2]);
    LOG("StepSixCase9 complete.");
  }
END_TASK(StepSixCase9)
DUNIT_TASK(CLIENT1, StepOneCase10)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[3], 4, 0, 0, 8, 5, 0, 6, false);
  }
END_TASK(StepOneCase10)

DUNIT_TASK(CLIENT2, StepTwoCase10)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[3], 0, 0, 0, 0, 0, 0, 6, false);
  }
END_TASK(StepTwoCase10)
DUNIT_TASK(CLIENT1, StepThreeCase10)
  {
    doRgnOperations(regionNames[3], 10);
    int n = getNumOfEntries(regionNames[3]);
    ASSERT(n == 5, "Expected 5 entries");
    LOG("StepThreeCase10 complete.");
  }
END_TASK(StepThreeCase10)
DUNIT_TASK(CLIENT2, StepFourCase10)
  {
    doRgnOperations(regionNames[3], 10, 5);
    int n = getNumOfEntries(regionNames[3]);
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepFourCase10 complete.");
  }
END_TASK(StepFourCase10)
DUNIT_TASK(CLIENT1, StepFiveCase10)
  {
    ACE_OS::sleep(5);
    int n = getNumOfEntries(regionNames[3]);
    ASSERT(n == 0, "Expected 0 entries");
    ACE_OS::sleep(10);
    ValidateDestroyRegion(regionNames[3]);
    LOG("StepFiveCase10 complete.");
  }
END_TASK(StepFiveCase10)
DUNIT_TASK(CLIENT2, StepSixCase10)
  {
    ACE_OS::sleep(3);
    ValidateDestroyRegion(regionNames[3]);
    LOG("StepSixCase10 complete.");
  }
END_TASK(StepSixCase10)

// tests for local and distributed listener/writer invocation with expiration
DUNIT_TASK(CLIENT1, StepOneCase11)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[4], 4, 0, 0, 0, 5, 0, 6, false);
    regListener = new TallyListener();
    regWriter = new TallyWriter();
    setCacheListener(regionNames[4], regListener);
    setCacheWriter(regionNames[4], regWriter);
  }
END_TASK(StepOneCase11)

DUNIT_TASK(CLIENT2, StepTwoCase11)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[4], 0, 0, 0, 0, 0, 0, 6, false);
    regListener = new TallyListener();
    regWriter = new TallyWriter();
    setCacheListener(regionNames[4], regListener);
    setCacheWriter(regionNames[4], regWriter);
  }
END_TASK(StepTwoCase11)
DUNIT_TASK(CLIENT1, StepThreeCase11)
  {
    doRgnOperations(regionNames[4], 10);
    int n = getNumOfEntries(regionNames[4]);
    regListener->showTallies();
    ASSERT(regWriter->isWriterInvoked() == true, "Writer Should be invoked");
    ASSERT(regListener->isListenerInvoked() == true,
           "Listener Should be invoked");

    ASSERT(regListener->getCreates() == 10, "Should be 10 creates");
    ASSERT(regListener->getUpdates() == 0, "Should be 0 updates");
    ASSERT(regListener->getInvalidates() == 0, "Should be 0 invalidate");
    ASSERT(regListener->getDestroys() == 5, "Should be 5 destroy");
    ASSERT(n == 5, "Expected 5 entries");
    LOG("StepThreeCase11 complete.");
  }
END_TASK(StepThreeCase11)
DUNIT_TASK(CLIENT2, StepFourCase11)
  {
    int n = getNumOfEntries(regionNames[4]);
    regListener->showTallies();
    ASSERT(regWriter->isWriterInvoked() == false,
           "Writer Should not be invoked");
    ASSERT(regListener->isListenerInvoked() == true,
           "Listener Should be invoked");
    ASSERT(regListener->getCreates() == 0, "Should be 0 creates");
    ASSERT(regListener->getUpdates() == 0, "Should be 0 updates");
    // NBS=false will lead to only invalidate messages
    ASSERT(regListener->getInvalidates() == 10, "Should be 10 invalidate");
    // LRU action is LOCAL_DESTROY so no destroys should be received here
    ASSERT(regListener->getDestroys() == 0, "Should be 0 destroy");

    // expect zero entries in NBS=false case
    /*NIL: Changed the asserion to the change in invalidate.
      Now we create new entery for every invalidate event received or
      localInvalidate call
      so expect 10 entries instead of 0 earlier. */
    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepFourCase11 complete.");
  }
END_TASK(StepFourCase11)
DUNIT_TASK(CLIENT1, StepFiveCase11)
  {
    ACE_OS::sleep(5);
    int n = getNumOfEntries(regionNames[4]);

    ASSERT(regWriter->isWriterInvoked() == true, "Writer Should be invoked");
    ASSERT(regListener->isListenerInvoked() == true,
           "Listener Should be invoked");
    regListener->showTallies();
    ASSERT(regListener->getCreates() == 10, "Should be 10 creates");
    ASSERT(regListener->getUpdates() == 0, "Should be 0 updates");
    ASSERT(regListener->getInvalidates() == 0, "Should be 0 invalidate");
    ASSERT(regListener->getDestroys() == 10, "Should be 10 destroy");

    ASSERT(n == 0, "Expected 0 entries");
    LOG("StepFiveCase11 complete.");
  }
END_TASK(StepFiveCase11)
DUNIT_TASK(CLIENT2, StepSixCase11)
  {
    int n = getNumOfEntries(regionNames[4]);
    ASSERT(regWriter->isWriterInvoked() == false,
           "Writer Should not be invoked");
    ASSERT(regListener->isListenerInvoked() == true,
           "Listener Should be invoked");
    regListener->showTallies();
    ASSERT(regListener->getCreates() == 0, "Should be 0 creates");
    ASSERT(regListener->getUpdates() == 0, "Should be 0 updates");
    // NBS=false will lead to only invalidate messages
    ASSERT(regListener->getInvalidates() == 10, "Should be 10 invalidate");
    ASSERT(regListener->getDestroys() == 5, "Should be 5 destroy");

    // expect zero entries in NBS=false case
    /*NIL: Changed the asserion to the change in invalidate.
      Now we create new entery for every invalidate event received or
      localInvalidate call
      so expect 5 entries instead of 0 earlier. */
    ASSERT(n == 5, "Expected 5 entries");

    ACE_OS::sleep(3);
    LOG("StepSixCase11 complete.");
  }
END_TASK(StepSixCase11)

DUNIT_TASK(SERVER1, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK(CloseServer1)
DUNIT_TASK(SERVER1, CreateServer1)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml");
    }
    LOG("SERVER2 started");
  }
END_TASK(CreateServer1)

DUNIT_TASK(CLIENT1, RegisterKeyC1)
  { registerKey = false; }
END_TASK(RegisterKeyC1)

DUNIT_TASK(CLIENT2, RegisterKeyC2)
  { registerKey = false; }
END_TASK(RegisterKeyC2)

DUNIT_TASK(CLIENT1, StepOneCase12)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[5], 4, 0, 0, 0, 5, 0, 6, false);
    regListener = new TallyListener();
    regWriter = new TallyWriter();
    setCacheListener(regionNames[5], regListener);
    setCacheWriter(regionNames[5], regWriter);
  }
END_TASK(StepOneCase12)

DUNIT_TASK(CLIENT2, StepTwoCase12)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    createThinClientRegion(regionNames[5], 0, 0, 0, 0, 0, 0, 6, false);
    regListener = new TallyListener();
    regWriter = new TallyWriter();
    setCacheListener(regionNames[5], regListener);
    setCacheWriter(regionNames[5], regWriter);
  }
END_TASK(StepTwoCase12)
DUNIT_TASK(CLIENT1, StepThreeCase12)
  {
    doRgnOperations(regionNames[5], 10);
    int n = getNumOfEntries(regionNames[5]);
    regListener->showTallies();
    ASSERT(regWriter->isWriterInvoked() == true, "Writer Should be invoked");
    ASSERT(regListener->isListenerInvoked() == true,
           "Listener Should be invoked");

    ASSERT(regListener->getCreates() == 10, "Should be 10 creates");
    ASSERT(regListener->getUpdates() == 0, "Should be 0 updates");
    // invalidate message is not implemented so expecting 0 events..
    ASSERT(regListener->getInvalidates() == 0, "Should be 0 invalidate");
    ASSERT(regListener->getDestroys() == 5, "Should be 5 destroy");
    ASSERT(n == 5, "Expected 5 entries");
    LOG("StepThreeCase12 complete.");
  }
END_TASK(StepThreeCase12)
DUNIT_TASK(CLIENT2, StepFourCase12)
  {
    int n = getNumOfEntries(regionNames[5]);
    ASSERT(regWriter->isWriterInvoked() == false,
           "Writer Should not be invoked");
    ASSERT(regListener->isListenerInvoked() == false,
           "Listener Should not be invoked");

    ASSERT(n == 0, "Expected 0 entries");
    LOG("StepFourCase12 complete.");
  }
END_TASK(StepFourCase12)
DUNIT_TASK(CLIENT1, StepFiveCase12)
  {
    ACE_OS::sleep(5);
    int n = getNumOfEntries(regionNames[5]);

    ASSERT(regWriter->isWriterInvoked() == true, "Writer Should be invoked");
    ASSERT(regListener->isListenerInvoked() == true,
           "Listener Should be invoked");
    regListener->showTallies();
    ASSERT(regListener->getCreates() == 10, "Should be 10 creates");
    ASSERT(regListener->getUpdates() == 0, "Should be 0 updates");
    // invalidate message is not implemented so expecting 0 events..
    ASSERT(regListener->getInvalidates() == 0, "Should be 0 invalidate");
    ASSERT(regListener->getDestroys() == 10, "Should be 10 destroy");

    ASSERT(n == 0, "Expected 0 entries");
    LOG("StepFiveCase12 complete.");
  }
END_TASK(StepFiveCase12)
DUNIT_TASK(CLIENT2, StepSixCase12)
  {
    ACE_OS::sleep(3);
    int n = getNumOfEntries(regionNames[5]);
    ASSERT(regWriter->isWriterInvoked() == false,
           "Writer Should not be invoked");
    ASSERT(regListener->isListenerInvoked() == false,
           "Listener Should not be invoked");
    ASSERT(n == 0, "Expected 0 entries");
    LOG("StepSixCase12 complete.");
  }
END_TASK(StepSixCase12)
DUNIT_TASK(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK(CloseCache1)

DUNIT_TASK(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK(CloseCache2)

DUNIT_TASK(CLIENT1, StepOneCase13)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    initClient(true);
    getHelper()->createPoolWithLocators("LRUPool", locatorsG, true);
    createThinClientRegion(regionNames[5], 4, 0, 0, 0, 5, 0, 6, false);
    regListener = new TallyListener();
    regWriter = new TallyWriter();
    setCacheListener(regionNames[5], regListener);
    setCacheWriter(regionNames[5], regWriter);
  }
END_TASK(StepOneCase13)

DUNIT_TASK(CLIENT2, StepTwoCase13)
  {
    // regionName, ettl, eit , rttl, rit,lel,endpoints,noOfEntry,rgnOpetation -
    // [put-0/get-5/destroy-3] ,destroyRgn - [true/false]
    // ,clientNotificationEnabled - [true/false] ,ExpirationAction::Action
    initClient(true);
    getHelper()->createPoolWithLocators("LRUPool", locatorsG, true);
    createThinClientRegion(regionNames[5], 0, 0, 0, 0, 0, 0, 6, false);
    regListener = new TallyListener();
    regWriter = new TallyWriter();
    setCacheListener(regionNames[5], regListener);
    setCacheWriter(regionNames[5], regWriter);
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[5]);
    regPtr0->registerAllKeys();
  }
END_TASK(StepTwoCase13)

DUNIT_TASK(CLIENT1, StepThreeCase13)
  {
    doRgnOperations(regionNames[5], 10);
    int n = getNumOfEntries(regionNames[5]);
    regListener->showTallies();
    ASSERT(regWriter->isWriterInvoked() == true, "Writer Should be invoked");
    ASSERT(regListener->isListenerInvoked() == true,
           "Listener Should be invoked");

    ASSERT(regListener->getCreates() == 10, "Should be 10 creates");
    ASSERT(regListener->getUpdates() == 0, "Should be 0 updates");
    // invalidate message is not implemented so expecting 0 events..
    ASSERT(regListener->getInvalidates() == 0, "Should be 0 invalidate");
    ASSERT(regListener->getDestroys() == 5, "Should be 5 destroy");
    ASSERT(n == 5, "Expected 5 entries");
    LOG("StepThreeCase13 complete.");
  }
END_TASK(StepThreeCase13)
DUNIT_TASK(CLIENT2, StepFourCase13)
  {
    int n = getNumOfEntries(regionNames[5]);
    ASSERT(regWriter->isWriterInvoked() == false,
           "Writer Should not be invoked");
    ASSERT(regListener->isListenerInvoked() == true,
           "Listener Should be invoked");
    regListener->showTallies();
    // five keys that have not been destroyed in previous test case are there on
    // the server so we receive creates for 5 and updates for the other 5
    ASSERT(regListener->getCreates() == 5, "Should be 5 creates");
    ASSERT(regListener->getUpdates() == 5, "Should be 5 updates");
    // invalidate message is not implemented so expecting 0 events..
    ASSERT(regListener->getInvalidates() == 0, "Should be 0 invalidate");
    ASSERT(regListener->getDestroys() == 0, "Should be 0 destroy");

    ASSERT(n == 10, "Expected 10 entries");
    LOG("StepFourCase13 complete.");
  }
END_TASK(StepFourCase13)
DUNIT_TASK(CLIENT1, StepFiveCase13)
  {
    ACE_OS::sleep(5);
    int n = getNumOfEntries(regionNames[5]);

    ASSERT(regWriter->isWriterInvoked() == true, "Writer Should be invoked");
    ASSERT(regListener->isListenerInvoked() == true,
           "Listener Should be invoked");
    regListener->showTallies();
    ASSERT(regListener->getCreates() == 10, "Should be 10 creates");
    ASSERT(regListener->getUpdates() == 0, "Should be 0 updates");
    // invalidate message is not implemented so expecting 0 events..
    ASSERT(regListener->getInvalidates() == 0, "Should be 0 invalidate");
    ASSERT(regListener->getDestroys() == 10, "Should be 10 destroy");

    ASSERT(n == 0, "Expected 0 entries");
    LOG("StepFiveCase13 complete.");
  }
END_TASK(StepFiveCase13)
DUNIT_TASK(CLIENT2, StepSixCase13)
  {
    ACE_OS::sleep(3);
    int n = getNumOfEntries(regionNames[5]);
    ASSERT(regWriter->isWriterInvoked() == false,
           "Writer Should not be invoked");
    ASSERT(regListener->isListenerInvoked() == true,
           "Listener Should be invoked");
    regListener->showTallies();
    // listener now shows updates for update events received from the server
    ASSERT(regListener->getCreates() == 5, "Should be 5 creates");
    ASSERT(regListener->getUpdates() == 5, "Should be 5 updates");
    // invalidate message is not implemented so expecting 0 events..
    ASSERT(regListener->getInvalidates() == 0, "Should be 0 invalidate");
    ASSERT(regListener->getDestroys() == 5, "Should be 5 destroy");

    ASSERT(n == 5, "Expected 5 entries");

    LOG("StepSixCase13 complete.");
  }
END_TASK(StepSixCase13)
DUNIT_TASK(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK(CloseCache1)

DUNIT_TASK(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK(CloseCache2)

DUNIT_TASK(SERVER1, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK(CloseServer1)
