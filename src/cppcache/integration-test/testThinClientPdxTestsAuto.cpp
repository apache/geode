/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * testThinClientPdxSerializer.cpp
 *
 *  Created on: Apr 19, 2012
 *      Author: vrao
 */

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include <string>

#include "ThinClientHelper.hpp"
#include <Utils.hpp>

#include "testobject/PdxAutoMegaType.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define LOCATOR s2p2
#define SERVER1 s2p1

bool isLocator = false;
bool isLocalServer = false;

const char* poolNames[] = {"Pool1", "Pool2", "Pool3"};
const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
bool isPoolConfig = false;  // To track if pool case is running
// const char * qRegionNames[] = { "Portfolios", "Positions", "Portfolios2",
// "Portfolios3" };
static bool m_useWeakHashMap ATTR_UNUSED = false;

void initClient(const bool isthinClient, bool isPdxIgnoreUnreadFields) {
  LOGINFO("initClient: isPdxIgnoreUnreadFields = %d ", isPdxIgnoreUnreadFields);
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient, isPdxIgnoreUnreadFields, false,
                                  NULLPTR, false);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void stepOne(bool isPdxIgnoreUnreadFields = false) {
  // Create just one pool and attach all regions to that.
  initClient(true, isPdxIgnoreUnreadFields);
  isPoolConfig = true;
  createPool(poolNames[0], locHostPort, NULL, 0, true);
  createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                            false /*Caching disabled*/);
  LOG("StepOne complete.");
}

DUNIT_TASK_DEFINITION(LOCATOR, StartLocator)
  {
    // starting locator 1 2
    if (isLocator) {
      CacheHelper::initLocator(1);
    }
    LOG("Locator started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc_PDX)
  {
    LOG("Starting Step One with Pool + Locator lists");
    stepOne(true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc)
  {
    LOG("Starting Step One with Pool + Locator lists");
    stepOne();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserverPdx.xml", locHostPort);
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator1)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver.xml", locHostPort);
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator2)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserverForPdx.xml", locHostPort);
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator3)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserverPdxSerializer.xml", locHostPort);
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLoc)
  {
    LOG("Starting Step Two with Pool + Locator");
    stepOne();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLoc_PDX)
  {
    LOG("Starting Step Two with Pool + Locator");
    stepOne(true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
    LOG("cleanProc 1...");
    isPoolConfig = false;
    cleanProc();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  {
    LOG("cleanProc 2...");
    isPoolConfig = false;
    cleanProc();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer)
  {
    LOG("closing Server1...");
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATOR, CloseLocator)
  {
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Client1PutMega)
  {
    try {
      Serializable::registerPdxType(
          PdxAutoTests::PdxAutoMegaType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxAutoTests::PdxAutoMegaType* pdxObj = new PdxAutoTests::PdxAutoMegaType();
    pdxObj->initPdxAutoMegaType();

    PdxAutoTests::PdxAutoMegaTypePtr obj1(pdxObj);

    CacheablePtr pdxobj(obj1);
    regPtr0->put(keyport, pdxobj);

    PdxAutoTests::PdxAutoMegaTypePtr obj2 =
        dynCast<PdxAutoTests::PdxAutoMegaTypePtr>(regPtr0->get(keyport));
    LOGDEBUG("Task:Client1PutMega: Checking get is success or not ");
    ASSERT(obj2 != NULLPTR,
           "Task JavaPutGet:Client1PutMega Should not be null");

    ASSERT(obj2->equals(obj1) == true,
           "Auto Generated pdx objects should be equal");
  }
END_TASK_DEFINITION

void runPdxAutoMegaTests() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator2)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  CALL_TASK(Client1PutMega)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

DUNIT_MAIN
  { runPdxAutoMegaTests(); }
END_MAIN
