/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientDurableInterest.hpp
 *
 *  Created on: Nov 3, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTDURABLEINTEREST_HPP_
#define THINCLIENTDURABLEINTEREST_HPP_

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

/* This is to test
1- If client doesn't do explicit registration on reconnect, durable events shud
be recieved.
2- If client do explicit Unregistration on reconnect, no events should be
recieved.
*/

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define FEEDER s2p2

class OperMonitor : public CacheListener {
  int m_ops;
  HashMapOfCacheable m_map;
  int m_id;

  void check(const EntryEvent& event) {
    m_ops++;

    CacheableKeyPtr key = event.getKey();
    CacheableInt32Ptr value = dynCast<CacheableInt32Ptr>(event.getNewValue());

    char buf[256];
    sprintf(buf,
            "Received event for Cachelistener id =%d with key %s and value %d.",
            m_id, key->toString()->asChar(), value->value());
    LOG(buf);

    HashMapOfCacheable::Iterator item = m_map.find(key);

    if (item != m_map.end()) {
      m_map.update(key, value);
    } else {
      m_map.insert(key, value);
    }
  }

 public:
  OperMonitor() : m_ops(0), m_id(-1) {}

  OperMonitor(int id) : m_ops(0), m_id(id) {
    LOGINFO("Inside OperMonitor %d ", m_id);
  }

  ~OperMonitor() { m_map.clear(); }

  void validate(int keyCount, int eventcount, int durableValue,
                int nonDurableValue) {
    LOG("validate called");
    char buf[256] = {'\0'};

    sprintf(buf, "Expected %d keys for the region, Actual = %d", keyCount,
            m_map.size());
    ASSERT(m_map.size() == keyCount, buf);

    sprintf(buf, "Expected %d events for the region, Actual = %d", eventcount,
            m_ops);
    ASSERT(m_ops == eventcount, buf);

    for (HashMapOfCacheable::Iterator item = m_map.begin(); item != m_map.end();
         item++) {
      CacheableStringPtr keyPtr = dynCast<CacheableStringPtr>(item.first());
      CacheableInt32Ptr valuePtr = dynCast<CacheableInt32Ptr>(item.second());

      if (strchr(keyPtr->toString(), 'D') == NULL) { /*Non Durable Key */
        sprintf(buf,
                "Expected final value for nonDurable Keys = %d, Actual = %d",
                nonDurableValue, valuePtr->value());
        ASSERT(valuePtr->value() == nonDurableValue, buf);
      } else { /*Durable Key */
        sprintf(buf, "Expected final value for Durable Keys = %d, Actual = %d",
                durableValue, valuePtr->value());
        ASSERT(valuePtr->value() == durableValue, buf);
      }
    }
  }

  virtual void afterCreate(const EntryEvent& event) { check(event); }

  virtual void afterUpdate(const EntryEvent& event) { check(event); }
  virtual void afterRegionInvalidate(const RegionEvent& event){};
  virtual void afterRegionDestroy(const RegionEvent& event){};
  virtual void afterRegionLive(const RegionEvent& event) {
    LOG("afterRegionLive called.");
  }
};
typedef SharedPtr<OperMonitor> OperMonitorPtr;

const char* mixKeys[] = {"Key-1", "D-Key-1", "Key-2", "D-Key-2"};
const char* testRegex[] = {"D-Key-.*", "Key-.*"};
OperMonitorPtr mon1, mon2;

#include "ThinClientDurableInit.hpp"
#include "ThinClientTasks_C2S2.hpp"

void setCacheListener(const char* regName, OperMonitorPtr monitor) {
  LOGINFO("setCacheListener to %s ", regName);
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(monitor);
}

void initClientWithIntrest(int ClientIdx, OperMonitorPtr& mon) {
  if (mon == NULLPTR) {
    mon = new OperMonitor;
  }

  initClientAndRegion(0, ClientIdx);

  setCacheListener(regionNames[0], mon);

  try {
    getHelper()->cachePtr->readyForEvents();
  } catch (...) {
    LOG("Exception occured while sending readyForEvents");
  }

  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  regPtr0->registerRegex(testRegex[0], true);
  regPtr0->registerRegex(testRegex[1], false);
}

void initClientWithIntrest2(int ClientIdx, OperMonitorPtr& monitor1,
                            OperMonitorPtr& monitor2) {
  initClientAndTwoRegionsAndTwoPools(0, ClientIdx, 60);
  if (monitor1 == NULLPTR) {
    monitor1 = new OperMonitor(1);
  }
  if (monitor2 == NULLPTR) {
    monitor2 = new OperMonitor(2);
  }
  setCacheListener(regionNames[0], monitor1);
  setCacheListener(regionNames[1], monitor2);
}

void initClientNoIntrest(int ClientIdx, OperMonitorPtr mon) {
  initClientAndRegion(0, ClientIdx);

  setCacheListener(regionNames[0], mon);

  getHelper()->cachePtr->readyForEvents();
}

void initClientRemoveIntrest(int ClientIdx, OperMonitorPtr mon) {
  initClientAndRegion(0, ClientIdx);
  setCacheListener(regionNames[0], mon);
  getHelper()->cachePtr->readyForEvents();

  // Only unregister durable Intrest.
  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  regPtr0->registerRegex(testRegex[0], true);
  regPtr0->unregisterRegex(testRegex[0]);
}

void feederUpdate(int value) {
  createIntEntry(regionNames[0], mixKeys[0], value);
  gemfire::millisleep(10);
  createIntEntry(regionNames[0], mixKeys[1], value);
  gemfire::millisleep(10);
}

void feederUpdate1(int value) {
  createIntEntry(regionNames[0], mixKeys[0], value);
  gemfire::millisleep(10);
  createIntEntry(regionNames[0], mixKeys[1], value);
  gemfire::millisleep(10);

  createIntEntry(regionNames[1], mixKeys[2], value);
  gemfire::millisleep(10);
  createIntEntry(regionNames[1], mixKeys[3], value);
  gemfire::millisleep(10);
}

DUNIT_TASK_DEFINITION(FEEDER, FeederInit)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], USE_ACK, locatorsG,
                                    "__TEST_POOL1__", true, true);
    getHelper()->createPooledRegion(regionNames[1], NO_ACK, locatorsG,
                                    "__TEST_POOL1__", true, true);
    LOG("FeederInit complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Clnt1Init)
  {
    initClientWithIntrest(0, mon1);
    LOG("Clnt1Init complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Clnt12Init)
  {
    initClientWithIntrest2(0, mon1, mon2);
    LOG("Clnt12Init complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Clnt2Init)
  {
    initClientWithIntrest(1, mon2);
    LOG("Clnt2Init complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederUpdate1)
  {
    feederUpdate(1);
    // Time to confirm that events has been dispatched.
    SLEEP(100);
    LOG("FeederUpdate1 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederUpdate12)
  {
    feederUpdate1(1);
    // Time to confirm that events has been dispatched.
    SLEEP(100);
    LOG("FeederUpdate1 complete.");
  }
END_TASK_DEFINITION

/* Close Client 1 with option keep alive = true*/
DUNIT_TASK_DEFINITION(CLIENT1, Clnt1Down)
  {
    // sleep 10 sec to allow periodic ack (1 sec) to go out
    SLEEP(10000);
    getHelper()->disconnect(true);
    cleanProc();
    LOG("Clnt1Down complete: Keepalive = True");
  }
END_TASK_DEFINITION

/* Close Client 2 with option keep alive = true*/
DUNIT_TASK_DEFINITION(CLIENT2, Clnt2Down)
  {
    getHelper()->disconnect(true);
    cleanProc();
    LOG("Clnt2Down complete: Keepalive = True");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Clnt1Up)
  {
    // No RegisterIntrest again
    initClientNoIntrest(0, mon1);
    LOG("Clnt1Up complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Clnt2Up)
  {
    initClientRemoveIntrest(1, mon2);
    LOG("Clnt2Up complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederUpdate2)
  {
    feederUpdate(2);
    // Time to confirm that events has been dispatched.
    SLEEP(100);
    LOG("FeederUpdate complete.");
  }
END_TASK_DEFINITION

/* For No register again */
DUNIT_TASK_DEFINITION(CLIENT1, ValidateClient1ListenerEventPayloads)
  {
    // Only durable should be get
    mon1->validate(2, 3, 2, 1);
    LOG("Client 1 Verify.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, ValidateClient2ListenerEventPayloads)
  {
    // no 2nd feed
    mon2->validate(2, 2, 1, 1);
    LOG("Client 2 Verify.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, CloseFeeder)
  {
    cleanProc();
    LOG("FEEDER closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseClient1)
  {
    mon1 = NULLPTR;
    cleanProc();
    LOG("CLIENT1 closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseClient12)
  {
    mon1 = NULLPTR;
    mon2 = NULLPTR;
    cleanProc();
    LOG("CLIENT12 closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseClient2)
  {
    mon2 = NULLPTR;
    cleanProc();
    LOG("CLIENT2 closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer)
  {
    CacheHelper::closeServer(1);
    CacheHelper::closeServer(2);
    LOG("SERVER closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, closeServer)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER closed");
  }
END_TASK_DEFINITION

#endif /* THINCLIENTDURABLEINTEREST_HPP_ */
