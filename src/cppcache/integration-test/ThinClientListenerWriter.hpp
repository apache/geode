/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef THINCLIENTLISTENERWRITER_HPP_
#define THINCLIENTLISTENERWRITER_HPP_

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "TallyListener.hpp"
#include "TallyWriter.hpp"

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define CLIENT3 s2p2

class SimpleCacheListener;
typedef gemfire::SharedPtr<SimpleCacheListener> SimpleCacheListenerPtr;

// The SimpleCacheListener class.
class SimpleCacheListener : public CacheListener {
 private:
  int m_creates;
  int m_clears;

 public:
  // The Cache Listener callbacks.
  virtual void afterCreate(const EntryEvent& event);
  virtual void afterUpdate(const EntryEvent& event);
  virtual void afterInvalidate(const EntryEvent& event);
  virtual void afterDestroy(const EntryEvent& event);
  virtual void afterRegionInvalidate(const RegionEvent& event);
  virtual void afterRegionDestroy(const RegionEvent& event);
  virtual void close(const RegionPtr& region);
  virtual void afterRegionClear(const RegionEvent& event);

  SimpleCacheListener() : CacheListener(), m_creates(0), m_clears(0) {
    LOGINFO("SimpleCacheListener contructor called");
  }

  virtual ~SimpleCacheListener() {}
  int getCreates() { return m_creates; }

  int getClears() { return m_clears; }
};

void SimpleCacheListener::afterCreate(const EntryEvent& event) {
  LOGINFO("SimpleCacheListener: Got an afterCreate event for %s region .",
          event.getRegion()->getName());
  m_creates++;
}

void SimpleCacheListener::afterUpdate(const EntryEvent& event) {
  LOGINFO("SimpleCacheListener: Got an afterUpdate event for %s region .",
          event.getRegion()->getName());
}

void SimpleCacheListener::afterInvalidate(const EntryEvent& event) {
  LOGINFO("SimpleCacheListener: Got an afterInvalidate event for %s region .",
          event.getRegion()->getName());
}

void SimpleCacheListener::afterDestroy(const EntryEvent& event) {
  LOGINFO("SimpleCacheListener: Got an afterDestroy event for %s region .",
          event.getRegion()->getName());
}

void SimpleCacheListener::afterRegionInvalidate(const RegionEvent& event) {
  LOGINFO(
      "SimpleCacheListener: Got an afterRegionInvalidate event for %s region .",
      event.getRegion()->getName());
}

void SimpleCacheListener::afterRegionDestroy(const RegionEvent& event) {
  LOGINFO(
      "SimpleCacheListener: Got an afterRegionDestroy event for %s region .",
      event.getRegion()->getName());
}

void SimpleCacheListener::close(const RegionPtr& region) {
  LOGINFO("SimpleCacheListener: Got an close event for %s region .",
          region.ptr()->getName());
}

void SimpleCacheListener::afterRegionClear(const RegionEvent& event) {
  LOGINFO("SimpleCacheListener: Got an afterRegionClear event for %s region .",
          event.getRegion()->getName());
  m_clears++;
}

/*
 * start server with NBS true/false
 * start one cache less client which will entry operations (
 * put/invalidate/destroy ).
 * start 2nd cacheless client with cache listener,  writer and client
 * notification true.
 * verify that listener is invoked and writer is not being invoked in 2nd client
 */

static bool isLocalServer = false;
static bool isLocator = false;
static int numberOfLocators = 0;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);
const char* poolName = "__TESTPOOL1_";
TallyListenerPtr regListener;
SimpleCacheListenerPtr parentRegCacheListener;
SimpleCacheListenerPtr subRegCacheListener;

SimpleCacheListenerPtr distRegCacheListener;
TallyWriterPtr regWriter;

#include "LocatorHelper.hpp"
const char* myRegNames[] = {"DistRegionAck", "DistRegionNoAck", "ExampleRegion",
                            "SubRegion1", "SubRegion2"};
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

void validateEvents() {
  SLEEP(5000);
  regListener->showTallies();
  ASSERT(regListener->getCreates() == 0, "Should be 0 creates");
  ASSERT(regListener->getUpdates() == 0, "Should be 0 updates");
  // invalidate message is not implemented so expecting 0 events..
  ASSERT(regListener->getInvalidates() == 10, "Should be 10 Invalidate");
  ASSERT(regListener->getDestroys() == 5, "Should be 5 destroy");
  ASSERT(regWriter->isWriterInvoked() == false, "Writer Should not be invoked");
}
DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithNBSTrue)
  {
    // starting server with notify_subscription true
    if (isLocalServer)
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml");
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer)
      CacheHelper::initServer(1, "cacheserver_notify_subscriptionBug849.xml");
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithNBSFalse)
  {
    // starting server with notify_subscription false
    if (isLocalServer) CacheHelper::initServer(1, "cacheserver.xml");
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator_NBSFalse)
  {
    // starting server with notify_subscription false
    if (isLocalServer) CacheHelper::initServer(1, "cacheserver.xml", locatorsG);
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1_Pooled_Locator)
  {
    initClient(true);
    LOG("Creating region in CLIENT1, no-ack, no-cache, no-listener");
    createPooledRegion(regionNames[0], false, locatorsG, poolName, true,
                       NULLPTR, false);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetupClient1withCachingEnabled_Pooled_Locator)
  {
    initClient(true);
    LOG("Creating region in CLIENT1, no-ack, no-cache, no-listener");
    createPooledRegion(myRegNames[0], false, locatorsG, poolName, true,
                       NULLPTR, true);
    createPooledRegion(myRegNames[1], false, locatorsG, poolName, true,
                       NULLPTR, true);
    createPooledRegion(myRegNames[2], false, locatorsG, poolName, true,
                       NULLPTR, true);

    // create subregion
    RegionPtr exmpRegptr = getHelper()->getRegion(myRegNames[2]);
    RegionAttributesPtr lattribPtr = exmpRegptr->getAttributes();
    RegionPtr subregPtr1 =
        exmpRegptr->createSubregion(myRegNames[3], lattribPtr);
    RegionPtr subregPtr2 =
        exmpRegptr->createSubregion(myRegNames[4], lattribPtr);

    LOGINFO(
        " CLIENT1 SetupClient1withCachingEnabled_Pooled_Locator subRegions "
        "created successfully");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Register2WithTrue)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    regPtr0->registerAllKeys();
  }
END_TASK_DEFINITION

// RegisterKeys
DUNIT_TASK_DEFINITION(CLIENT2, RegisterKeys)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(myRegNames[0]);

    RegionPtr exmpRegPtr = getHelper()->getRegion(myRegNames[2]);
    RegionPtr subregPtr0 = exmpRegPtr->getSubregion(myRegNames[3]);
    RegionPtr subregPtr1 = exmpRegPtr->getSubregion(myRegNames[4]);

    // 1. registerAllKeys on parent and both subregions
    regPtr0->registerAllKeys();
    exmpRegPtr->registerAllKeys();
    subregPtr0->registerAllKeys();
    subregPtr1->registerAllKeys();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Register2WithFalse)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    regPtr0->registerAllKeys(false, NULLPTR, false, false);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, Register3WithFalse)
  {
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    regPtr0->registerAllKeys(false, NULLPTR, false, false);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetupClient2_Pooled_Locator)
  {
    initClient(true);
    LOG("Creating region in CLIENT2 , no-ack, no-cache, with-listener and "
        "writer");
    regListener = new TallyListener();
    createPooledRegion(regionNames[0], false, locatorsG, poolName, true,
                       regListener, false);
    regWriter = new TallyWriter();
    setCacheWriter(regionNames[0], regWriter);
    RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
    // regPtr0->registerAllKeys();
  }
END_TASK_DEFINITION

//
DUNIT_TASK_DEFINITION(CLIENT2, SetupClient2withCachingEnabled_Pooled_Locator)
  {
    initClient(true);
    LOG("Creating region in CLIENT2 , no-ack, no-cache, with-listener and "
        "writer");
    parentRegCacheListener = new SimpleCacheListener();
    distRegCacheListener = new SimpleCacheListener();

    createPooledRegion(myRegNames[0], false, locatorsG, poolName, true,
                       distRegCacheListener, true);
    createPooledRegion(myRegNames[1], false, locatorsG, poolName, true,
                       NULL, true);
    createPooledRegion(myRegNames[2], false, locatorsG, poolName, true,
                       parentRegCacheListener, true);

    regWriter = new TallyWriter();
    setCacheWriter(myRegNames[2], regWriter);

    // create subregion
    RegionPtr exmpRegptr = getHelper()->getRegion(myRegNames[2]);
    RegionAttributesPtr lattribPtr = exmpRegptr->getAttributes();
    RegionPtr subregPtr1 =
        exmpRegptr->createSubregion(myRegNames[3], lattribPtr);
    RegionPtr subregPtr2 =
        exmpRegptr->createSubregion(myRegNames[4], lattribPtr);

    LOGINFO(
        "CLIENT2 SetupClient2withCachingEnabled_Pooled_Locator:: subRegions "
        "created successfully");

    // Attach Listener to subRegion
    // Attache Listener

    AttributesMutatorPtr subregAttrMutatorPtr =
        subregPtr1->getAttributesMutator();
    subRegCacheListener = new SimpleCacheListener();
    subregAttrMutatorPtr->setCacheListener(subRegCacheListener);

    LOG("StepTwo_Pool complete.");
  }
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(CLIENT3, SetupClient3_Pooled_Locator)
  {
    // client with no registerAllKeys.....
    initClient(true);
    LOG("Creating region in CLIENT2 , no-ack, no-cache, with-listener and "
        "writer");
    regListener = new TallyListener();
    createPooledRegion(regionNames[0], false, locatorsG, poolName, true,
                       regListener, false);
    regWriter = new TallyWriter();
    setCacheWriter(regionNames[0], regWriter);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, doOperations)
  {
    LOG("do entry operation from client 1");
    RegionOperations region(regionNames[0]);
    region.putOp(5);
    SLEEP(1000);  // let the events reach at other end.
    region.putOp(5);
    SLEEP(1000);
    region.invalidateOp(5);
    SLEEP(1000);
    region.destroyOp(5);
    SLEEP(1000);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, validateListenerWriterWithNBSTrue)
  {
    SLEEP(5000);
    LOG("Verifying TallyListener has received verious events.");
    regListener->showTallies();
    ASSERT(regListener->getCreates() == 5, "Should be 5 creates");
    ASSERT(regListener->getUpdates() == 5, "Should be 5 updates");
    // invalidate message is not implemented so expecting 0 events..
    ASSERT(regListener->getInvalidates() == 0, "Should be 0 Invalidate");
    ASSERT(regListener->getDestroys() == 5, "Should be 5 destroy");
    ASSERT(regWriter->isWriterInvoked() == false,
           "Writer Should not be invoked");

    LOGINFO("Total cleared Entries = %d ", regListener->getClears());
  }
END_TASK_DEFINITION

//
DUNIT_TASK_DEFINITION(CLIENT1, doEventOperations)
  {
    LOG("do entry operation from client 1");

    RegionPtr regPtr0 = getHelper()->getRegion(myRegNames[0]);
    RegionPtr exmpRegPtr = getHelper()->getRegion(myRegNames[2]);

    RegionPtr subregPtr1 = exmpRegPtr->getSubregion(myRegNames[3]);
    RegionPtr subregPtr2 = exmpRegPtr->getSubregion(myRegNames[4]);

    for (int index = 0; index < 5; index++) {
      char key[100] = {0};
      char value[100] = {0};
      ACE_OS::sprintf(key, "Key-%d", index);
      ACE_OS::sprintf(value, "Value-%d", index);
      CacheableKeyPtr keyptr = CacheableKey::create(key);
      CacheablePtr valuePtr = CacheableString::create(value);
      regPtr0->put(keyptr, valuePtr);
      exmpRegPtr->put(keyptr, valuePtr);
      subregPtr1->put(keyptr, valuePtr);
      subregPtr2->put(keyptr, valuePtr);
    }

    LOGINFO(
        "CLIENT-1 localCaching Enabled After Put ....ExampleRegion.size() = %d",
        exmpRegPtr->size());
    ASSERT(exmpRegPtr->size() == 5,
           "Total number of entries in the region should be 5");
    // SLEEP( 1000 ); // let the events reach at other end.

    LOGINFO(
        "CLIENT-1 localCaching Enabled After Put ....DistRegionAck.size() = %d",
        regPtr0->size());

    // TEST COVERAGE FOR cacheListener.afterRegionClear() API
    exmpRegPtr->clear();
    LOGINFO("CLIENT-1 AFTER Clear() call ....reg.size() = %d",
            exmpRegPtr->size());
    ASSERT(exmpRegPtr->size() == 0,
           "Total number of entries in the region should be 0");

    LOGINFO("CLIENT-1 AFTER Clear() call ....SubRegion-1.size() = %d",
            subregPtr1->size());
    ASSERT(subregPtr1->size() == 5,
           "Total number of entries in the region should be 0");

    LOGINFO("CLIENT-1 AFTER Clear() call ....SubRegion-2.size() = %d",
            subregPtr2->size());
    ASSERT(subregPtr2->size() == 5,
           "Total number of entries in the region should be 0");

    SLEEP(1000);
  }
END_TASK_DEFINITION

//
DUNIT_TASK_DEFINITION(CLIENT2, validateListenerWriterEventsWithNBSTrue)
  {
    SLEEP(5000);
    LOG("Verifying SimpleListerner has received verious events.");
    // regListener->showTallies();
    LOGINFO(" distRegCacheListener->getCreates() = %d",
            distRegCacheListener->getCreates());

    // LOGINFO(" parentRegCacheListener->getCreates() = %d",
    // parentRegCacheListener->getCreates());
    ASSERT(parentRegCacheListener->getCreates() == 10, "Should be 10 creates");
    ASSERT(regWriter->isWriterInvoked() == false,
           "Writer Should not be invoked");

    // Verify that the region.clear event is received and it has cleared all
    // entries in region
    // LOGINFO("parentRegCacheListener::m_clears = %d ",
    // parentRegCacheListener->getClears());
    ASSERT(parentRegCacheListener->getClears() == 1,
           "region.clear() should be called once");

    RegionPtr exmpRegPtr = getHelper()->getRegion(myRegNames[2]);
    // LOGINFO(" Total Entries in ExampleRegion = %d ", exmpRegPtr->size());
    ASSERT(exmpRegPtr->size() == 0,
           "Client-2 ExampleRegion.clear() should have called and so "
           "Exampleregion size is expected to 0 ");

    // Verify entries in Sub-Region.
    RegionPtr subregPtr1 = exmpRegPtr->getSubregion(myRegNames[3]);
    RegionPtr subregPtr2 = exmpRegPtr->getSubregion(myRegNames[4]);

    // LOGINFO(" Total Entries in SubRegion-1 = %d ", subregPtr1->size());
    // LOGINFO(" Total Entries in SubRegion-2 = %d ", subregPtr2->size());
    ASSERT(subRegCacheListener->getCreates() == 5,
           "should be 5 creates for SubRegion-1 ");
    ASSERT(subRegCacheListener->getClears() == 0,
           "should be 0 clears for SubRegion-1 ");
    ASSERT(subregPtr1->size() == 5,
           "Client-2 SubRegion-1 should contains 5 entries ");
    ASSERT(subregPtr2->size() == 5,
           "Client-2 SubRegion-2 should contains 5 entries ");

    // LOGINFO(" SubRegion-1 CREATES:: subRegCacheListener::m_creates = %d ",
    // subRegCacheListener->getCreates());
    // LOGINFO(" SubRegion-1 CLEARS:: subRegCacheListener::m_clears = %d ",
    // subRegCacheListener->getClears());

    LOGINFO(
        "validateListenerWriterEventsWithNBSTrue :: Event Validation "
        "Passed....!!");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, validateListenerWriterWithNBSFalse)
  { validateEvents(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, validateListenerWriterWithNBSFalseForClient3)
  { validateEvents(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, CloseCache3)
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

#endif /*THINCLIENTLISTENERWRITER_HPP_*/
