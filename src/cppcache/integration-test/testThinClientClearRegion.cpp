/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

using namespace gemfire;
using namespace test;
class MyCacheWriter : public CacheWriter {
  uint32_t m_clear;

 public:
  MyCacheWriter() : m_clear(0) {}
  bool beforeRegionClear(const RegionEvent& ev) {
    LOG("beforeRegionClear called");
    m_clear++;
    return true;
  }
  uint32_t getClearCnt() { return m_clear; }
};
class MyCacheListener : public CacheListener {
  uint32_t m_clear;

 public:
  MyCacheListener() : m_clear(0) {}
  void afterRegionClear(const RegionEvent& ev) {
    LOG("afterRegionClear called");
    m_clear++;
  }
  uint32_t getClearCnt() { return m_clear; }
};

static int numberOfLocators = 1;
bool isLocalServer = true;
bool isLocator = true;
const char* locHostPort =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);

DUNIT_TASK(SERVER1, StartServer)
  {
    if (isLocalServer) {
      CacheHelper::initLocator(1);
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locHostPort);
    }
    LOG("SERVER started");
  }
END_TASK(StartServer)

DUNIT_TASK(CLIENT1, SetupClient1)
  {
    initClientWithPool(true, "__TEST_POOL1__", locHostPort, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locHostPort,
                                    "__TEST_POOL1__", true, true);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    AttributesMutatorPtr mtor = regPtr->getAttributesMutator();
    CacheListenerPtr lster(new MyCacheListener());
    CacheWriterPtr wter(new MyCacheWriter());
    mtor->setCacheListener(lster);
    mtor->setCacheWriter(wter);
    regPtr->registerAllKeys();
  }
END_TASK(SetupClient1)

DUNIT_TASK(CLIENT2, SetupClient2)
  {
    initClientWithPool(true, "__TEST_POOL1__", locHostPort, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], false, locHostPort,
                                    "__TEST_POOL1__", true, true);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->registerAllKeys();
    CacheableKeyPtr keyPtr = CacheableKey::create((const char*)"key01");
    CacheableBytesPtr valPtr =
        CacheableBytes::create(reinterpret_cast<const uint8_t*>("value01"), 7);
    regPtr->put(keyPtr, valPtr);
    ASSERT(regPtr->size() == 1, "size incorrect");
  }
END_TASK(SetupClient2)

DUNIT_TASK(CLIENT1, clear)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    ASSERT(regPtr->size() == 1, "size incorrect");
    regPtr->clear();
    ASSERT(regPtr->size() == 0, "size incorrect");
  }
END_TASK(clear)

DUNIT_TASK(CLIENT2, VerifyClear)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    ASSERT(regPtr->size() == 0, "size incorrect");
    CacheableKeyPtr keyPtr = CacheableKey::create((const char*)"key02");
    CacheableBytesPtr valPtr =
        CacheableBytes::create(reinterpret_cast<const uint8_t*>("value02"), 7);
    regPtr->put(keyPtr, valPtr);
    ASSERT(regPtr->size() == 1, "size incorrect");
    regPtr->localClear();
    ASSERT(regPtr->size() == 0, "size incorrect");
    ASSERT(regPtr->containsKeyOnServer(keyPtr), "key should be there");
  }
END_TASK(VerifyClear)

DUNIT_TASK(CLIENT1, VerifyClear1)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    ASSERT(regPtr->size() == 1, "size incorrect");
    regPtr->localClear();
    ASSERT(regPtr->size() == 0, "size incorrect");
    CacheableKeyPtr keyPtr = CacheableKey::create((const char*)"key02");
    ASSERT(regPtr->containsKeyOnServer(keyPtr), "key should be there");
    RegionAttributesPtr attr = regPtr->getAttributes();
    CacheListenerPtr clp = attr->getCacheListener();
    MyCacheListener* mcl = dynamic_cast<MyCacheListener*>(clp.ptr());
    char buf[1024];
    sprintf(buf, "listener clear count=%d", mcl->getClearCnt());
    LOG(buf);
    ASSERT(mcl->getClearCnt() == 2, buf);
    CacheWriterPtr cwp = attr->getCacheWriter();
    MyCacheWriter* mcw = dynamic_cast<MyCacheWriter*>(cwp.ptr());
    sprintf(buf, "writer clear count=%d", mcw->getClearCnt());
    LOG(buf);
    ASSERT(mcw->getClearCnt() == 2, buf);
  }
END_TASK(VerifyClear1)

DUNIT_TASK(SERVER1, StopServer)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      CacheHelper::closeLocator(1);
    }

    LOG("SERVER stopped");
  }
END_TASK(StopServer)
DUNIT_TASK(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK(CloseCache1)
DUNIT_TASK(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK(CloseCache2)
