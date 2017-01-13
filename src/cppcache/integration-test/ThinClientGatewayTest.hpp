#ifndef THINCLIENTGATEWAYTEST_HPP_
#define THINCLIENTGATEWAYTEST_HPP_

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "TallyListener.hpp"
#include "TallyWriter.hpp"
#include "TallyLoader.hpp"

#define SERVER1 s2p1
#define SERVER2 s2p2
#define CLIENT1 s1p1
#define CLIENT2 s1p2

using namespace gemfire;
using namespace test;

#include <gfcpp/GemfireCppCache.hpp>
#include <string>

class MyListener : public CacheListener {
 private:
  int m_events;

 public:
  MyListener() : CacheListener(), m_events(0) {
    LOG("MyListener contructor called");
  }

  virtual ~MyListener() {}

  virtual void afterCreate(const EntryEvent& event) { m_events++; }

  virtual void afterUpdate(const EntryEvent& event) { m_events++; }

  virtual void afterInvalidate(const EntryEvent& event) { m_events++; }

  virtual void afterDestroy(const EntryEvent& event) { m_events++; }

  virtual void afterRegionInvalidate(const RegionEvent& event) {}

  virtual void afterRegionDestroy(const RegionEvent& event) {}
  int getNumEvents() { return m_events; }
};

typedef gemfire::SharedPtr<MyListener> MyListenerPtr;

void setCacheListener(const char* regName, TallyListenerPtr regListener) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(regListener);
}

const char* locHostPort1 = NULL;
const char* locHostPort2 = NULL;
DUNIT_TASK_DEFINITION(SERVER1, StartLocator1)
  {
    CacheHelper::initLocator(1, false, true, 1,
                             CacheHelper::staticLocatorHostPort2);
    locHostPort1 = CacheHelper::getstaticLocatorHostPort1();
    LOGINFO("Locator1 started");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(SERVER2, StartLocator2)
  {
    CacheHelper::initLocator(2, false, true, 2);
    locHostPort2 = CacheHelper::getstaticLocatorHostPort2();
    LOG("Locator2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StartServer1)
  {
    CacheHelper::initServer(1, "gateway1.xml", locHostPort1);
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, StartServer2)
  {
    CacheHelper::initServer(2, "gateway2.xml", locHostPort2);
    LOG("SERVER started");
  }
END_TASK_DEFINITION

MyListenerPtr reg1Listener1 = NULLPTR;

DUNIT_TASK_DEFINITION(SERVER2, SetupClient2)
  {
    // CacheHelper ch = getHelper();
    reg1Listener1 = new MyListener();
    RegionPtr regPtr = createPooledRegion("exampleRegion", false, locHostPort2,
                                          "poolName", true, reg1Listener1);
    regPtr->registerAllKeys();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, SetupClient)
  {
    initClientWithPool(true, "poolName", locHostPort1, NULL);
    RegionPtr regPtr = createRegionAndAttachPool("exampleRegion", true, NULL);
    LOG(" region is created ");
    for (int i = 0; i < 100; i++) {
      LOG(" region is created put");
      regPtr->put(i, i);
    }

    dunit::sleep(10000);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, VerifyClient2Events)
  {
    LOGINFO(" nevents = %d got", reg1Listener1->getNumEvents());
    ASSERT(reg1Listener1->getNumEvents() > 0,
           "Events should be greater than zero");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopServer1)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopServer2)
  {
    CacheHelper::closeServer(2);
    LOG("SERVER2stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopLocator1)
  {
    // stop locator
    CacheHelper::closeLocator(1);
    LOG("Locator1 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopLocator2)
  {
    CacheHelper::closeLocator(1);
    LOG("Locator2 stopped");
  }
END_TASK_DEFINITION

void runListenerInit(bool poolConfig = true, bool isLocator = true) {
  CALL_TASK(StartLocator1);
  CALL_TASK(StartLocator2);
  CALL_TASK(StartServer1);
  CALL_TASK(StartServer2);
  CALL_TASK(SetupClient2);
  CALL_TASK(SetupClient);
  CALL_TASK(VerifyClient2Events);
  CALL_TASK(CloseCache1);
  CALL_TASK(CloseCache2);
  CALL_TASK(StopServer1);
  CALL_TASK(StopServer2);
  CALL_TASK(StopLocator1);
  CALL_TASK(StopLocator2);
}
#endif /*THINCLIENTGATEWAYTEST_HPP_*/
