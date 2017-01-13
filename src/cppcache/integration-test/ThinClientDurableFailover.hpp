/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientDurableFailover.hpp
 *
 *  Created on: Nov 4, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTDURABLEFAILOVER_HPP_
#define THINCLIENTDURABLEFAILOVER_HPP_

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

/* Testing Parameters              Param's Value
Termination :                   Keepalive = true/ false, Client crash
Restart Time:                   Before Timeout / After Timeout
Register Interest               Durable/ Non Durable

Descripton:  There is One server , one feeder and two clients. Both clients
comes up ->
feeder feed -> both clients go down in same way ( keepalive = true/ false ,
crash )->
feeder feed -> Client1 comes up -> Client2 comes up after timeout -> verify ->
Shutdown

*/

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define FEEDER s2p2

class OperMonitor : public CacheListener {
  int m_ops;
  HashMapOfCacheable m_map;

  void check(const EntryEvent& event) {
    m_ops++;

    CacheableKeyPtr key = event.getKey();
    CacheableInt32Ptr value = NULLPTR;
    try {
      value = dynCast<CacheableInt32Ptr>(event.getNewValue());
    } catch (Exception) {
      //  do nothing.
    }

    CacheableStringPtr keyPtr = dynCast<CacheableStringPtr>(key);
    if (keyPtr != NULLPTR && value != NULLPTR) {
      char buf[256] = {'\0'};
      sprintf(buf, " Got Key: %s, Value: %d", keyPtr->toString(),
              value->value());
      LOG(buf);
    }

    if (value != NULLPTR) {
      HashMapOfCacheable::Iterator item = m_map.find(key);

      if (item != m_map.end()) {
        m_map.update(key, value);
      } else {
        m_map.insert(key, value);
      }
    }
  }

 public:
  OperMonitor() : m_ops(0) {}

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

  virtual void afterCreate(const EntryEvent& event) {
    LOG("afterCreate called");
    check(event);
  }

  virtual void afterUpdate(const EntryEvent& event) {
    LOG("afterUpdate called");
    check(event);
  }

  virtual void afterDestroy(const EntryEvent& event) {
    LOG("afterDestroy called");
    check(event);
  }

  virtual void afterRegionInvalidate(const RegionEvent& event){};
  virtual void afterRegionDestroy(const RegionEvent& event){};
};
typedef SharedPtr<OperMonitor> OperMonitorPtr;

void setCacheListener(const char* regName, OperMonitorPtr monitor) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(monitor);
}

OperMonitorPtr mon1 = NULLPTR;
OperMonitorPtr mon2 = NULLPTR;

#include "ThinClientDurableInit.hpp"
#include "ThinClientTasks_C2S2.hpp"

/* Total 10 Keys , alternate durable and non-durable */
const char* mixKeys[] = {"Key-1", "D-Key-1", "L-Key", "LD-Key"};
const char* testRegex[] = {"D-Key-.*", "Key-.*"};

void initClientCache(int redundancy, int durableTimeout, OperMonitorPtr& mon,
                     int sleepDuration = 0, int durableIdx = 0) {
  if (sleepDuration) SLEEP(sleepDuration);

  if (mon == NULLPTR) {
    mon = new OperMonitor();
  }

  //  35 sec ack interval to ensure primary clears its Q only
  // after the secondary comes up and is able to receive the QRM
  // otherwise it will get the unacked events from GII causing the
  // client to get 2 extra / replayed events.
  initClientAndRegion(redundancy, durableIdx, 1, 1, 300);

  setCacheListener(regionNames[0], mon);

  getHelper()->cachePtr->readyForEvents();

  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);

  // for R =1 it will get a redundancy error
  try {
    regPtr0->registerRegex(testRegex[0], true);
  } catch (Exception) {
    //  do nothing.
  }
  try {
    regPtr0->registerRegex(testRegex[1], false);
  } catch (Exception) {
    //  do nothing.
  }
}

void feederUpdate(int value) {
  createIntEntry(regionNames[0], mixKeys[0], value);
  gemfire::millisleep(10);
  createIntEntry(regionNames[0], mixKeys[1], value);
  gemfire::millisleep(10);
}

/* Close Client 1 with option keep alive = true*/
DUNIT_TASK_DEFINITION(CLIENT1, CloseClient1WithKeepAlive)
  {
    // sleep 30 sec to allow clients' periodic acks (1 sec) to go out
    // this is along with the 5 sec sleep after feeder update and
    // tied to the notify-ack-interval setting of 35 sec.
    SLEEP(30000);
    getHelper()->disconnect(true);
    cleanProc();
    LOG("CloseClient1WithKeepAlive complete: Keepalive = True");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StartServer1)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                              locatorsG);
    }

    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, StartServer2)
  {
    if (isLocalServer) {
      CacheHelper::initServer(2, "cacheserver_notify_subscription2.xml",
                              locatorsG);
    }

    //  sleep for 3 seconds to allow redundancy monitor to detect new server.
    gemfire::millisleep(3000);
    LOG("SERVER started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederInit)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], USE_ACK, locatorsG,
                                    "__TEST_POOL1__", true, true);
    LOG("FeederInit complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, InitClient1NoRedundancy)
  { initClientCache(0, 300, mon1); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, InitClient1WithRedundancy)
  { initClientCache(1, 300, mon1); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederUpdate1)
  {
    feederUpdate(1);

    //  Wait 5 seconds for events to be removed from ha queues.
    gemfire::millisleep(5000);

    LOG("FeederUpdate1 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederUpdate2)
  {
    feederUpdate(2);

    //  Wait 5 seconds for events to be removed from ha queues.
    gemfire::millisleep(5000);

    LOG("FeederUpdate2 complete.");
  }
END_TASK_DEFINITION

// R =0 and clientDown, Intermediate events lost.
DUNIT_TASK_DEFINITION(CLIENT1, VerifyClientDownWithEventsLost)
  {
    LOG("Client Verify 1.");
    mon1->validate(2, 2, 1, 1);
  }
END_TASK_DEFINITION

// R =1 and clientDown, Durable events recieved
DUNIT_TASK_DEFINITION(CLIENT1, VerifyClientDownDurableEventsRecieved)
  {
    LOG("Client Verify 2.");
    mon1->validate(2, 3, 2, 1);
  }
END_TASK_DEFINITION

// No clientDown, All events recieved
DUNIT_TASK_DEFINITION(CLIENT1, VeryifyNoClientDownAllEventsReceived)
  {
    LOG("Client Verify 3.");
    mon1->validate(2, 4, 2, 2);
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

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    CacheHelper::closeServer(1);
    //  Wait 2 seconds to allow client failover.
    gemfire::millisleep(2000);
    LOG("SERVER closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer2)
  {
    CacheHelper::closeServer(2);
    LOG("SERVER closed");
  }
END_TASK_DEFINITION

void doThinClientDurableFailoverClientClosedNoRedundancy() {
  CALL_TASK(StartLocator);

  CALL_TASK(StartServer1);

  CALL_TASK(FeederInit);

  CALL_TASK(InitClient1NoRedundancy);

  CALL_TASK(StartServer2);

  CALL_TASK(FeederUpdate1);

  CALL_TASK(CloseClient1WithKeepAlive);

  CALL_TASK(CloseServer1);

  CALL_TASK(FeederUpdate2);

  CALL_TASK(InitClient1NoRedundancy);

  CALL_TASK(VerifyClientDownWithEventsLost);

  CALL_TASK(CloseClient1);
  CALL_TASK(CloseFeeder);
  CALL_TASK(CloseServer2);

  CALL_TASK(CloseLocator);
}

void doThinClientDurableFailoverClientNotClosedRedundancy() {
  CALL_TASK(StartLocator);

  CALL_TASK(StartServer1);

  CALL_TASK(FeederInit);

  CALL_TASK(InitClient1WithRedundancy);

  CALL_TASK(StartServer2);

  CALL_TASK(FeederUpdate1);

  CALL_TASK(CloseServer1);

  CALL_TASK(FeederUpdate2);

  CALL_TASK(VeryifyNoClientDownAllEventsReceived);

  CALL_TASK(CloseClient1);
  CALL_TASK(CloseFeeder);
  CALL_TASK(CloseServer2);

  CALL_TASK(CloseLocator);
}

void doThinClientDurableFailoverClientClosedRedundancy() {
  CALL_TASK(StartLocator);

  CALL_TASK(StartServer1);

  CALL_TASK(FeederInit);

  CALL_TASK(InitClient1WithRedundancy);

  CALL_TASK(StartServer2);

  CALL_TASK(FeederUpdate1);

  CALL_TASK(CloseClient1WithKeepAlive);

  CALL_TASK(CloseServer1);

  CALL_TASK(FeederUpdate2);

  CALL_TASK(InitClient1WithRedundancy);

  CALL_TASK(VerifyClientDownDurableEventsRecieved);

  CALL_TASK(CloseClient1);
  CALL_TASK(CloseFeeder);
  CALL_TASK(CloseServer2);

  CALL_TASK(CloseLocator);
}

#endif /* THINCLIENTDURABLEFAILOVER_HPP_ */
