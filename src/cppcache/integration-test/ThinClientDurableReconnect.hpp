/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThinClientDurableReconnect.hpp
 *
 *  Created on: Nov 3, 2008
 *      Author: abhaware
 */

#ifndef THINCLIENTDURABLERECONNECT_HPP_
#define THINCLIENTDURABLERECONNECT_HPP_

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

/* This is to test
1- Durable Client always reconnect to secondary to avoid data loss.
*/

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define FEEDER s2p2

class OperMonitor : public CacheListener {
  bool m_first, m_second, m_close;

 public:
  OperMonitor() : m_first(false), m_second(false), m_close(false) {}

  void validate() {
    LOG("validate called");
    ASSERT(m_first, "m_first event not recieved");
    ASSERT(m_close && m_second, "m_second event not recieved");
  }

  virtual void afterCreate(const EntryEvent& event) {
    if (!m_close) {
      m_first = true;
      LOG("First Event Recieved");
    } else {
      m_second = true;
      LOG("Duplicate Recieved");
    }
  }
  virtual void close(const RegionPtr& region) {
    m_close = true;
    LOG("Listener Close Called");
  }
};
typedef SharedPtr<OperMonitor> OperMonitorPtr;

void setCacheListener(const char* regName, OperMonitorPtr monitor) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(monitor);
}

OperMonitorPtr mon1 = NULLPTR;

const char* mixKeys[] = {"D-Key-1"};

#include "ThinClientDurableInit.hpp"
#include "ThinClientTasks_C2S2.hpp"

void initClientCache(int redundancy, OperMonitorPtr& mon) {
  initClientAndRegion(redundancy, 0, 60000, 1, 300);

  if (mon == NULLPTR) {
    mon = new OperMonitor();
  }

  setCacheListener(regionNames[0], mon);

  getHelper()->cachePtr->readyForEvents();
  RegionPtr regPtr0 = getHelper()->getRegion(regionNames[0]);
  regPtr0->registerAllKeys(true);
}

DUNIT_TASK_DEFINITION(CLIENT1, ClientInit)
  { initClientCache(1, mon1); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, FeederInit)
  {
    initClient(true);

    createRegion(regionNames[0], USE_ACK, true);
    LOG("FeederInit complete.");

    createIntEntry(regionNames[0], mixKeys[0], 1);

    LOG("FeederUpdate complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ClientDown)
  {
    getHelper()->disconnect(true);
    cleanProc();
    LOG("Clnt1Down complete: Keepalive = True");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ClientReInit)
  { initClientCache(1, mon1); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Verify)
  {
    LOG("Client Verify");
    mon1->validate();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(FEEDER, CloseFeeder)
  {
    cleanProc();
    LOG("FEEDER closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseClient)
  {
    mon1 = NULLPTR;
    cleanProc();
    LOG("CLIENT1 closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServers)
  {
    CacheHelper::closeServer(1);
    CacheHelper::closeServer(2);
    LOG("SERVERs closed");
  }
END_TASK_DEFINITION

void doThinClientDurableReconnect() {
  CALL_TASK(StartLocator);

  startServers();

  CALL_TASK(ClientInit);
  CALL_TASK(FeederInit);
  CALL_TASK(ClientDown);
  CALL_TASK(ClientReInit);
  CALL_TASK(Verify);
  CALL_TASK(CloseFeeder);
  CALL_TASK(CloseClient);
  CALL_TASK(CloseServers);

  closeLocator();
}

#endif /* THINCLIENTDURABLERECONNECT_HPP_ */
