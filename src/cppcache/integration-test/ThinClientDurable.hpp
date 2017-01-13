/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientDurable.hpp
 *
 *  Created on: Oct 31, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTDURABLE_HPP_
#define THINCLIENTDURABLE_HPP_

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

/* Testing Parameters              Param's Value
Termination :                   Keepalive = true/ false, Client crash / Netdown
Restart Time:                   Before Timeout / After Timeout
Register Interest               Durable/ Non Durable ,  Regex / List
Intermediate Feeding       true (Region 1 ) / false ( Region 2)

Descripton:  There is One server , one feeder and two clients. Both clients
comes up -> feeder feed in both regions
both clients go down in same way ( keepalive = true/ false etc.  ) -> feeder
feed  ( only in region 1 )
-> Both comes up ->  verify -> Shutdown

Client 1 is with R =0 and Client 2 with R = 1
*/

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define FEEDER s2p2

class OperMonitor : public CacheListener {
  int m_ops;
  HashMapOfCacheable m_map;
  std::string m_clientName, m_regionName;

  void check(const EntryEvent& event) {
    m_ops++;

    CacheableKeyPtr key = event.getKey();
    CacheableInt32Ptr value = NULLPTR;
    try {
      value = dynCast<CacheableInt32Ptr>(event.getNewValue());
    } catch (Exception) {
      //  do nothing.
    }

    char buff[128] = {'\0'};
    CacheableStringPtr keyPtr = dynCast<CacheableStringPtr>(key);
    if (value != NULLPTR) {
      sprintf(buff, "Event [%s, %d] called for %s:%s", keyPtr->toString(),
              value->value(), m_clientName.c_str(), m_regionName.c_str());

      HashMapOfCacheable::Iterator item = m_map.find(key);
      if (item != m_map.end()) {
        m_map.update(key, value);
      } else {
        m_map.insert(key, value);
      }
    } else {
      sprintf(buff, "Event Key=%s called for %s:%s", keyPtr->toString(),
              m_clientName.c_str(), m_regionName.c_str());
    }
    LOG(buff);
  }

 public:
  OperMonitor(const char* clientName, const char* regionName)
      : m_ops(0), m_clientName(clientName), m_regionName(regionName) {}

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

OperMonitorPtr mon1C1 = NULLPTR;
OperMonitorPtr mon2C1 = NULLPTR;

OperMonitorPtr mon1C2 = NULLPTR;
OperMonitorPtr mon2C2 = NULLPTR;

/* Total 10 Keys , alternate durable and non-durable */
const char* mixKeys[] = {"Key-1", "D-Key-1", "L-Key", "LD-Key"};
const char* testRegex[] = {"D-Key-.*", "Key-.*"};

#include "ThinClientDurableInit.hpp"
#include "ThinClientTasks_C2S2.hpp"

void initClientCache(int durableIdx, int redundancy, int durableTimeout,
                     OperMonitorPtr& mon1, OperMonitorPtr& mon2,
                     int sleepDuration = 0) {
  // Sleep before starting , Used for Timeout testing.
  if (sleepDuration) SLEEP(sleepDuration);

  initClientAndTwoRegions(durableIdx, redundancy, durableTimeout);

  setCacheListener(regionNames[0], mon1);
  setCacheListener(regionNames[1], mon2);

  getHelper()->cachePtr->readyForEvents();

  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  RegionPtr regPtr1 = getHelper()->getRegion(regionNames[1]);

  // Register Regex in both region.
  regPtr0->registerRegex(testRegex[0], true);
  regPtr0->registerRegex(testRegex[1], false);
  regPtr1->registerRegex(testRegex[0], true);
  regPtr1->registerRegex(testRegex[1], false);

  // Register List in both regions
  VectorOfCacheableKey v;
  CacheableKeyPtr ldkey = CacheableKey::create(mixKeys[3]);
  v.push_back(ldkey);
  regPtr0->registerKeys(v, true);
  regPtr1->registerKeys(v, true);
  v.clear();
  CacheableKeyPtr lkey = CacheableKey::create(mixKeys[2]);
  v.push_back(lkey);
  regPtr0->registerKeys(v);
  regPtr1->registerKeys(v);

  LOG("Clnt1Init complete.");
}

void feederUpdate(int value, int ignoreR2 = false) {
  for (int regIdx = 0; regIdx < 2; regIdx++) {
    if (ignoreR2 && regIdx == 1) {
      continue;
    }
    createIntEntry(regionNames[regIdx], mixKeys[0], value);
    gemfire::millisleep(10);
    createIntEntry(regionNames[regIdx], mixKeys[1], value);
    gemfire::millisleep(10);
    createIntEntry(regionNames[regIdx], mixKeys[2], value);
    gemfire::millisleep(10);
    createIntEntry(regionNames[regIdx], mixKeys[3], value);
    gemfire::millisleep(10);

    destroyEntry(regionNames[regIdx], mixKeys[0]);
    gemfire::millisleep(10);
    destroyEntry(regionNames[regIdx], mixKeys[1]);
    gemfire::millisleep(10);
    destroyEntry(regionNames[regIdx], mixKeys[2]);
    gemfire::millisleep(10);
    destroyEntry(regionNames[regIdx], mixKeys[3]);
    gemfire::millisleep(10);
  }
}

DUNIT_TASK_DEFINITION(FEEDER, FeederInit)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regionNames[0], USE_ACK, locatorsG,
                                    "__TEST_POOL1__", true, true);
    getHelper()->createPooledRegion(regionNames[1], USE_ACK, locatorsG,
                                    "__TEST_POOL1__", true, true);
    LOG("FeederInit complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, InitClient1Timeout300)
  {
    if (mon1C1 == NULLPTR) {
      mon1C1 = new OperMonitor(durableIds[0], regionNames[0]);
    }
    if (mon2C1 == NULLPTR) {
      mon2C1 = new OperMonitor(durableIds[0], regionNames[1]);
    }
    initClientCache(0, 0 /* Redundancy */, 300 /* D Timeout */, mon1C1, mon2C1);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, InitClient1Timeout30)
  {
    if (mon1C1 == NULLPTR) {
      mon1C1 = new OperMonitor(durableIds[0], regionNames[0]);
    }
    if (mon2C1 == NULLPTR) {
      mon2C1 = new OperMonitor(durableIds[0], regionNames[1]);
    }
    initClientCache(0, 0 /* Redundancy */, 30 /* D Timeout */, mon1C1, mon2C1);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, InitClient1DelayedStart)
  {
    if (mon1C1 == NULLPTR) {
      mon1C1 = new OperMonitor(durableIds[0], regionNames[0]);
    }
    if (mon2C1 == NULLPTR) {
      mon2C1 = new OperMonitor(durableIds[0], regionNames[1]);
    }
    initClientCache(0, 0 /* Redundancy */, 30 /* D Timeout */, mon1C1, mon2C1,
                    35000 /* Sleep before starting */);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, InitClient2Timeout300)
  {
    if (mon1C2 == NULLPTR) {
      mon1C2 = new OperMonitor(durableIds[1], regionNames[0]);
    }
    if (mon2C2 == NULLPTR) {
      mon2C2 = new OperMonitor(durableIds[1], regionNames[1]);
    }
    initClientCache(1, 1 /* Redundancy */, 300 /* D Timeout */, mon1C2, mon2C2);
  }
END_TASK_DEFINITION

// Client 2 don't need to sleep for timeout as C1 does before it
DUNIT_TASK_DEFINITION(CLIENT2, InitClient2Timeout30)
  {
    if (mon1C2 == NULLPTR) {
      mon1C2 = new OperMonitor(durableIds[1], regionNames[0]);
    }
    if (mon2C2 == NULLPTR) {
      mon2C2 = new OperMonitor(durableIds[1], regionNames[1]);
    }
    initClientCache(1, 1 /* Redundancy */, 30 /* D Timeout */, mon1C2, mon2C2);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ReviveClient1)
  { revive(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ReviveClient1Delayed)
  {
    SLEEP(35000);
    revive();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, ReviveClient2AndWait)
  {
    revive();
    // Give Time to revive connections.
    SLEEP(15000);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederUpdate1)
  {
    feederUpdate(1);

    //  Wait 5 seconds for events to be removed from ha queues.
    gemfire::millisleep(5000);

    LOG("FeederUpdate1 complete.");
  }
END_TASK_DEFINITION

/* Close Client 1 with option keep alive = true*/
DUNIT_TASK_DEFINITION(CLIENT1, CloseClient1KeepAliveTrue)
  {
    getHelper()->disconnect(true);
    cleanProc();
    LOG("Clnt1Down complete: Keepalive = True");
  }
END_TASK_DEFINITION

/* Close Client 1 with option keep alive = false*/
DUNIT_TASK_DEFINITION(CLIENT1, CloseClient1KeepAliveFalse)
  {
    getHelper()->disconnect();
    cleanProc();
    LOG("Clnt1Down complete: Keepalive = false");
  }
END_TASK_DEFINITION

/* Close Client 1 Abruptly*/
DUNIT_TASK_DEFINITION(CLIENT1, CrashClient1)
  {
    // TODO: fix for pool case
    crashClient();
    getHelper()->disconnect();
    cleanProc();
    LOG("Clnt1Down complete: Crashed");
  }
END_TASK_DEFINITION

/* Disconnect Client 1 (netdown) */
DUNIT_TASK_DEFINITION(CLIENT1, DisconnectClient1)
  {
    // TODO: fix for pool case
    netDown();
    LOG("Clnt1Down complete: Network disconnection has been simulated");
  }
END_TASK_DEFINITION

/* Close Client 2 with option keep alive = true*/
DUNIT_TASK_DEFINITION(CLIENT2, CloseClient2KeepAliveTrue)
  {
    getHelper()->disconnect(true);
    cleanProc();
    LOG("Clnt2Down complete: Keepalive = True");
  }
END_TASK_DEFINITION

/* Close Client 2 with option keep alive = false*/
DUNIT_TASK_DEFINITION(CLIENT2, CloseClient2KeepAliveFalse)
  {
    getHelper()->disconnect();
    cleanProc();
    LOG("Clnt2Down complete: Keepalive = false");
  }
END_TASK_DEFINITION

/* Close Client 2 Abruptly*/
DUNIT_TASK_DEFINITION(CLIENT2, CrashClient2)
  {
    crashClient();
    getHelper()->disconnect();
    cleanProc();
    LOG("Clnt2Down complete: Crashed");
  }
END_TASK_DEFINITION

/* Disconnect Client 2 (netdown) */
DUNIT_TASK_DEFINITION(CLIENT2, DisconnectClient2)
  {
    netDown();
    LOG("Clnt2Down complete: Network disconnection has been simulated");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederUpdate2)
  {
    feederUpdate(2, true);
    LOG("FeederUpdate2 complete.");
  }
END_TASK_DEFINITION

/* Verify that clients receive feeder update 1  */
DUNIT_TASK_DEFINITION(CLIENT1, VerifyFeederUpdate_1_C1)
  {
    LOG("Client 1 Verify first feeder update.");
    mon1C1->validate(4, 8, 1, 1);
    mon2C1->validate(4, 8, 1, 1);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyFeederUpdate_1_C2)
  {
    LOG("Client 2 Verify first feeder udpate.");
    mon1C2->validate(4, 8, 1, 1);
    mon2C2->validate(4, 8, 1, 1);
  }
END_TASK_DEFINITION

/* For Keep Alive = True or crash, netdown  */
DUNIT_TASK_DEFINITION(CLIENT1, VerifyClient1)
  {
    LOG("Client 1 Verify.");
    mon1C1->validate(4, 12, 2, 1);
    mon2C1->validate(4, 8, 1, 1);
  }
END_TASK_DEFINITION

/* For Keep Alive = false */
DUNIT_TASK_DEFINITION(CLIENT1, VerifyClient1KeepAliveFalse)
  {
    LOG("Client 1 Verify.");
    mon1C1->validate(4, 8, 1, 1);
    mon2C1->validate(4, 8, 1, 1);
  }
END_TASK_DEFINITION

/* For Keep Alive = True or crash, netdown  */
DUNIT_TASK_DEFINITION(CLIENT2, VerifyClient2)
  {
    LOG("Client 2 Verify.");
    mon1C2->validate(4, 12, 2, 1);
    mon2C2->validate(4, 8, 1, 1);
  }
END_TASK_DEFINITION

/* For Keep Alive = false */
DUNIT_TASK_DEFINITION(CLIENT2, VerifyClient2KeepAliveFalse)
  {
    LOG("Client 2 Verify.");
    mon1C2->validate(4, 8, 1, 1);
    mon2C2->validate(4, 8, 1, 1);
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
    mon1C1 = NULLPTR;
    mon2C1 = NULLPTR;
    cleanProc();
    LOG("CLIENT1 closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseClient2)
  {
    mon1C2 = NULLPTR;
    mon2C2 = NULLPTR;
    cleanProc();
    LOG("CLIENT2 closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServers)
  {
    CacheHelper::closeServer(1);
    CacheHelper::closeServer(2);
    LOG("SERVERs closed");
  }
END_TASK_DEFINITION

#endif /* THINCLIENTDURABLE_HPP_ */
