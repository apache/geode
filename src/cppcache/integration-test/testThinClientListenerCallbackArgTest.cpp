/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testThinClientListenerCallbackArgTest"

#include "fw_dunit.hpp"
#ifndef _SPARC_SOLARIS
// TODO performance - broken on SPARC
#include "ThinClientHelper.hpp"
#include "TallyListener.hpp"
#include "TallyWriter.hpp"
#include "testobject/PdxType.hpp"
#include "testobject/VariousPdxTypes.hpp"
#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

#include "testobject/Portfolio.hpp"
using namespace testobject;

using namespace gemfire;
using namespace test;

bool isLocalServer = true;
static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
#include "LocatorHelper.hpp"

//---------------------------------------------------------------------------------
using namespace gemfire;
class CallbackListener;

typedef gemfire::SharedPtr<CallbackListener> CallbackListenerPtr;

class CallbackListener : public CacheListener {
 private:
  int m_creates;
  int m_updates;
  int m_invalidates;
  int m_destroys;
  int m_regionInvalidate;
  int m_regionDestroy;
  int m_regionClear;
  // CacheableStringPtr m_callbackArg;
  CacheablePtr m_callbackArg;

 public:
  CallbackListener()
      : CacheListener(),
        m_creates(0),
        m_updates(0),
        m_invalidates(0),
        m_destroys(0),
        m_regionInvalidate(0),
        m_regionDestroy(0),
        m_regionClear(0),
        m_callbackArg(NULLPTR) {
    LOG("CallbackListener contructor called");
  }

  virtual ~CallbackListener() {}

  int getCreates() { return m_creates; }

  int getUpdates() { return m_updates; }
  int getInvalidates() { return m_invalidates; }
  int getDestroys() { return m_destroys; }
  int getRegionInvalidates() { return m_regionInvalidate; }
  int getRegionDestroys() { return m_regionDestroy; }
  int getRegionClear() { return m_regionClear; }
  void setCallBackArg(const CacheablePtr& callbackArg) {
    m_callbackArg = callbackArg;
  }

  void check(CacheablePtr eventCallback, int& updateEvent) {
    if (m_callbackArg != NULLPTR) {
      try {
        PortfolioPtr mCallbkArg = dynCast<PortfolioPtr>(m_callbackArg);

        PortfolioPtr callbkArg = dynCast<PortfolioPtr>(eventCallback);

        CacheableStringPtr fromCallback = callbkArg->getPkid();
        CacheableStringPtr mCallback = mCallbkArg->getPkid();

        LOGFINE(" values are %s === %s ", fromCallback->asChar(),
                mCallback->asChar());

        if (*(fromCallback.ptr()) == *(mCallback.ptr())) {
          LOGFINE("values are same");
          updateEvent++;
        } else {
          LOGFINE("values are NOT same");
        }
      } catch (const ClassCastException& ex) {
        LOGFINE(" in class cast exception %s ", ex.getMessage());
        try {
          CacheableStringPtr fromCallback =
              dynCast<CacheableStringPtr>(eventCallback);
          CacheableStringPtr mCallback =
              dynCast<CacheableStringPtr>(m_callbackArg);

          LOGFINE(" values are %s === %s ", fromCallback->asChar(),
                  mCallback->asChar());

          if (*(fromCallback.ptr()) == *(mCallback.ptr())) {
            LOGFINE("values are same");
            updateEvent++;
          } else {
            LOGFINE("values are NOT same");
          }
        } catch (const ClassCastException& ex2) {
          LOGFINE(" in class cast second exception %s ", ex2.getMessage());
        }
      }
    }
  }

  void checkcallbackArg(const EntryEvent& event, int& updateEvent) {
    check(event.getCallbackArgument(), updateEvent);
  }

  void checkcallbackArg(const RegionEvent& event, int& updateEvent) {
    check(event.getCallbackArgument(), updateEvent);
  }

  virtual void afterCreate(const EntryEvent& event) {
    checkcallbackArg(event, m_creates);
  }

  virtual void afterUpdate(const EntryEvent& event) {
    checkcallbackArg(event, m_updates);
  }

  virtual void afterInvalidate(const EntryEvent& event) {
    checkcallbackArg(event, m_invalidates);
  }

  virtual void afterDestroy(const EntryEvent& event) {
    checkcallbackArg(event, m_destroys);
  }

  virtual void afterRegionInvalidate(const RegionEvent& event) {
    checkcallbackArg(event, m_regionInvalidate);
  }

  virtual void afterRegionDestroy(const RegionEvent& event) {
    checkcallbackArg(event, m_regionDestroy);
  }
  virtual void afterRegionClear(const RegionEvent& event) {
    checkcallbackArg(event, m_regionClear);
  }
};
//---------------------------------------------------------------------------------

CallbackListenerPtr reg1Listener1 = NULLPTR;
CacheableStringPtr callBackStrPtr;

CacheablePtr callBackPortFolioPtr;

void setCacheListener(const char* regName, CallbackListenerPtr regListener) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(regListener);
}

void validateEventCount(int line) {
  LOGINFO("ValidateEvents called from line (%d).", line);
  int num = reg1Listener1->getCreates();
  char buf[1024];
  sprintf(buf, "Didn't get expected callback arg in aftercreate event");
  ASSERT(7 == num, buf);
  num = reg1Listener1->getUpdates();
  sprintf(buf, "Didn't get expected callback arg in afterupdate events");
  ASSERT(3 == num, buf);
  num = reg1Listener1->getInvalidates();
  sprintf(buf, "Didn't get expected callback arg in afterInvalidates events");
  ASSERT(2 == num, buf);
  num = reg1Listener1->getDestroys();
  sprintf(buf, "Didn't get expected callback arg in afterdestroy events");
  ASSERT(5 == num, buf);
  num = reg1Listener1->getRegionInvalidates();
  sprintf(buf,
          "Didn't get expected callback arg in afterRegionInvalidates events");
  ASSERT(1 == num, buf);
  num = reg1Listener1->getRegionDestroys();
  sprintf(buf, "Didn't get expected callback arg in afterRegiondestroy events");
  ASSERT(1 == num, buf);
  num = reg1Listener1->getRegionClear();
  sprintf(buf, "Didn't get expected callback arg in afterRegionClear events");
  ASSERT(1 == num, buf);
}

DUNIT_TASK_DEFINITION(SERVER1, StartServer)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription5.xml");
    }
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pool_Locator)
  {
    initClient(true);

    callBackStrPtr = CacheableString::create("Gemstone's Callback");

    createPooledRegion(regionNames[0], false /*ack mode*/, locatorsG,
                       "__TEST_POOL1__", true /*client notification*/);

    Serializable::registerType(Portfolio::createDeserializable);
    Serializable::registerType(Position::createDeserializable);
    reg1Listener1 = new CallbackListener();
    callBackPortFolioPtr = new Portfolio(1, 0, NULLPTR);

    reg1Listener1->setCallBackArg(callBackPortFolioPtr);
    setCacheListener(regionNames[0], reg1Listener1);
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->registerAllKeys();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetupClient2_Pool_Locator)
  {
    initClient(true);

    callBackStrPtr = CacheableString::create("Gemstone's Callback");

    createPooledRegion(regionNames[0], false /*ack mode*/, locatorsG,
                       "__TEST_POOL1__", true /*client notification*/);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, testCreatesAndUpdates)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);

    callBackPortFolioPtr = new Portfolio(1, 0, NULLPTR);
    regPtr->create("aaa", "bbb", callBackPortFolioPtr);
    regPtr->create(keys[1], vals[1], callBackPortFolioPtr);
    regPtr->put(keys[1], nvals[1], callBackPortFolioPtr);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testInvalidates)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);

    callBackPortFolioPtr = new Portfolio(1, 0, NULLPTR);
    regPtr->localCreate(1234, 1234, callBackPortFolioPtr);
    regPtr->localCreate(12345, 12345, callBackPortFolioPtr);
    regPtr->localCreate(12346, 12346, callBackPortFolioPtr);
    regPtr->localPut(1234, vals[1], callBackPortFolioPtr);
    regPtr->localInvalidate(1234, callBackPortFolioPtr);
    ASSERT(regPtr->localRemove(12345, 12345, callBackPortFolioPtr) == true,
           "Result of remove should be true, as this value exists locally.");
    ASSERT(regPtr->containsKey(12345) == false, "containsKey should be false");
    ASSERT(regPtr->localRemoveEx(12346, callBackPortFolioPtr) == true,
           "Result of remove should be true, as this value exists locally.");
    ASSERT(regPtr->containsKey(12346) == false, "containsKey should be false");
    regPtr->invalidate(keys[1], callBackPortFolioPtr);
    regPtr->invalidateRegion(callBackPortFolioPtr);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, testDestroy)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);

    callBackPortFolioPtr = new Portfolio(1, 0, NULLPTR);
    regPtr->destroy(keys[1], callBackPortFolioPtr);
    ASSERT(regPtr->removeEx("aaa", callBackPortFolioPtr) == true,
           "Result of remove should be true, as this value exists.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, testRemove)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->remove(keys[1], nvals[1], callBackPortFolioPtr);
    regPtr->destroyRegion(callBackPortFolioPtr);
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, testlocalClear)
  {
    RegionPtr regPtr = getHelper()->getRegion(regionNames[0]);
    regPtr->localClear(callBackPortFolioPtr);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testValidate)
  {
    dunit::sleep(10000);
    validateEventCount(__LINE__);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StopClient1)
  {
    cleanProc();
    LOG("CLIENT1 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StopClient2)
  {
    cleanProc();
    LOG("CLIENT2 stopped");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StopServer)
  {
    if (isLocalServer) CacheHelper::closeServer(1);
    LOG("SERVER stopped");
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator_XML5)

    CALL_TASK(SetupClient1_Pool_Locator);
    CALL_TASK(SetupClient2_Pool_Locator);
    CALL_TASK(testCreatesAndUpdates);
    CALL_TASK(testInvalidates);
    CALL_TASK(testDestroy);
    CALL_TASK(testCreatesAndUpdates);
    CALL_TASK(testlocalClear);
    CALL_TASK(testRemove);
    CALL_TASK(testValidate);
    CALL_TASK(StopClient1);
    CALL_TASK(StopClient2);
    CALL_TASK(StopServer);
    CALL_TASK(CloseLocator1);
  }
END_MAIN

#endif
