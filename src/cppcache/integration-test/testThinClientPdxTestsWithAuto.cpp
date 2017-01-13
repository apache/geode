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

#define ROOT_NAME "testThinClientPdxTestsAuto"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "ThinClientHelper.hpp"
#include "testobject/PdxClassV1WithAuto.hpp"
#include "testobject/PdxClassV2WithAuto.hpp"
#include "testobject/VariousPdxTypesWithAuto.hpp"
#include "testobject/PdxTypeWithAuto.hpp"
#include <Utils.hpp>
#include "CachePerfStats.hpp"
#include <LocalRegion.hpp>

using namespace gemfire;
using namespace PdxTestsAuto;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define CLIENT3 s2p2
#define LOCATOR s2p2
#define SERVER1 s2p1

bool isLocator = false;
bool isLocalServer = false;

const char* poolNames[] = {"Pool1", "Pool2", "Pool3"};
const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
bool isPoolConfig = false;  // To track if pool case is running
// const char * qRegionNames[] = { "Portfolios", "Positions", "Portfolios2",
// "Portfolios3" };
static bool m_useWeakHashMap = false;

void initClient(const bool isthinClient, bool isPdxIgnoreUnreadFields) {
  LOGINFO("isPdxIgnoreUnreadFields = %d ", isPdxIgnoreUnreadFields);
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient, isPdxIgnoreUnreadFields, false,
                                  NULLPTR, false);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void initClient2WithClientName(const bool isthinClient,
                               const PropertiesPtr& configPtr = NULLPTR) {
  if (cacheHelper == NULL) {
    PropertiesPtr config = configPtr;
    if (config == NULLPTR) {
      config = Properties::create();
    }
    config->insert("name", "Client-2");
    cacheHelper = new CacheHelper(isthinClient, config);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void stepOneForClient2(bool pool = false, bool locator = false,
                       bool isPdxIgnoreUnreadFields = false) {
  // Create just one pool and attach all regions to that.
  initClient2WithClientName(true);

  if (locator) {
    isPoolConfig = true;
    createPool(poolNames[0], locHostPort, NULL, 0, true);
    createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                              false /*Caching disabled*/);
  } else {
    isPoolConfig = true;
    createPool(poolNames[0], NULL, NULL, 0, true);
    createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0], false);
  }
  LOG("StepOne complete.");
}
void stepOne(bool pool = false, bool locator = false,
             bool isPdxIgnoreUnreadFields = false) {
  try {
    // Serializable::registerType(Position::createDeserializable);
    // Serializable::registerType(Portfolio::createDeserializable);
  } catch (const IllegalStateException&) {
    // ignore exception
  }
  // Create just one pool and attach all regions to that.
  initClient(true, isPdxIgnoreUnreadFields);
  if (!pool) {
    createRegion("DistRegionAck", USE_ACK, false /*Caching disabled*/);
  } else if (locator) {
    isPoolConfig = true;
    createPool(poolNames[0], locHostPort, NULL, 0, true);
    createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                              false /*Caching disabled*/);
  } else {
    isPoolConfig = true;
    createPool(poolNames[0], NULL, NULL, 0, true);
    createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0], false);
  }
  LOG("StepOne complete.");
}

void initClient1(bool pool = false, bool locator = false,
                 bool isPdxIgnoreUnreadFields = false) {
  // Create just one pool and attach all regions to that.
  initClient(true, isPdxIgnoreUnreadFields);
  if (locator) {
    isPoolConfig = true;
    createPool(poolNames[0], locHostPort, NULL, 0, false);
    createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                              true /*Caching enabled*/);
  } else {
    isPoolConfig = true;
    createPool(poolNames[0], NULL, NULL, 0, false);
    createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0], true);
  }
  LOG("StepOne complete.");
}

void initClient2(bool pool = false, bool locator = false,
                 bool isPdxIgnoreUnreadFields = false) {
  // Create just one pool and attach all regions to that.
  initClient(true, isPdxIgnoreUnreadFields);
  if (locator) {
    isPoolConfig = true;
    createPool(poolNames[0], locHostPort, NULL, 0,
               true /*ClientNotification enabled*/);
    createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                              true /*Caching enabled*/);
  } else {
    isPoolConfig = true;
    createPool(poolNames[0], NULL, NULL, 0, true);
    createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0], true);
  }
  LOG("StepOne complete.");
}

void initClient3(bool pool = false, bool locator = false,
                 bool isPdxIgnoreUnreadFields = false) {
  // Create just one pool and attach all regions to that.
  initClient(true, isPdxIgnoreUnreadFields);
  if (locator) {
    isPoolConfig = true;
    createPool(poolNames[0], locHostPort, NULL, 0,
               true /*ClientNotification enabled*/);
    createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                              true /*Caching enabled*/);
  } else {
    isPoolConfig = true;
    createPool(poolNames[0], NULL, NULL, 0, true);
    createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0], true);
  }
  LOG("StepOne complete.");
}

DUNIT_TASK_DEFINITION(SERVER1, StartLocator)
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
    stepOne(true, true, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc)
  {
    LOG("Starting Step One with Pool + Locator lists");
    stepOne(true, true);
  }
END_TASK_DEFINITION

// StepOnePoolLoc_PdxMetadataTest
DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc_PdxMetadataTest)
  {
    LOG("Starting Step One with Pool + Locator lists");
    initClient1(true, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) CacheHelper::initServer(1, "cacheserverPdx.xml");
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION
//
DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserverPdx.xml", locHostPort);
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer_PdxMetadataTest)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) CacheHelper::initServer(1, "cacheserverPdx2.xml");
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator_PdxMetadataTest)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserverPdx2.xml", locHostPort);
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) CacheHelper::initServer(1, "cacheserver.xml");
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

DUNIT_TASK_DEFINITION(SERVER1, CreateServer2)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserverForPdxWithAuto.xml");
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServerWithLocator2)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserverForPdxWithAuto.xml", locHostPort);
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
  {
    LOG("Starting Step One");
    stepOne();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolEP)
  {
    LOG("Starting Step One with Pool + Explicit server list");
    stepOne(true);
  }
END_TASK_DEFINITION

////StepOnePoolEP
DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolEP_PdxMetadataTest)
  {
    LOG("Starting Step One with Pool + Explicit server list");
    initClient1(true, false);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolEP_PDX)
  {
    LOG("Starting Step One with Pool + Explicit server list");
    stepOne(true, false, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolEP)
  {
    LOG("Starting Step One with Pool + Explicit server list");
    stepOne(true);
  }
END_TASK_DEFINITION

// DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolEPBug866)
//{
//  LOG("Starting Step One with Pool + Explicit server list");
//  stepOneForClient2(true);
//}
// END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLocBug866)
  {
    LOG("Starting Step Two with Pool + Locator");
    stepOneForClient2(true, true);
  }
END_TASK_DEFINITION

// StepTwoPoolEP_PdxMetadataTest
DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolEP_PdxMetadataTest)
  {
    LOG("Starting Step One with Pool + Explicit server list");
    initClient2(true, false);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, StepThreePoolEP_PdxMetadataTest)
  {
    LOG("Starting Step One with Pool + Explicit server list");
    initClient3(true, false);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolEP_PDX)
  {
    LOG("Starting Step One with Pool + Explicit server list");
    stepOne(true, false, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLoc)
  {
    LOG("Starting Step Two with Pool + Locator");
    stepOne(true, true);
  }
END_TASK_DEFINITION

// StepTwoPoolLoc_PdxMetadataTest
DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLoc_PdxMetadataTest)
  {
    LOG("Starting Step Two with Pool + Locator");
    initClient2(true, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, StepThreePoolLoc_PdxMetadataTest)
  {
    LOG("Starting Step Two with Pool + Locator");
    initClient3(true, true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLoc_PDX)
  {
    LOG("Starting Step Two with Pool + Locator");
    stepOne(true, false, true);
  }
END_TASK_DEFINITION

void checkPdxInstanceToStringAtServer(RegionPtr regionPtr) {
  CacheableKeyPtr keyport = CacheableKey::create("success");
  CacheableBooleanPtr boolPtr =
      dynCast<CacheableBooleanPtr>(regionPtr->get(keyport));
  bool val = boolPtr->value();
  // TODO::Enable asser and disable LOGINFO
  ASSERT(val == true, "checkPdxInstanceToStringAtServer: Val should be true");
  LOGINFO("NIL::checkPdxInstanceToStringAtServer:139: val = %d", val);
}

DUNIT_TASK_DEFINITION(CLIENT1, testPutWithMultilevelInheritance)
  {
    try {
      Serializable::registerPdxType(PdxTestsAuto::Child::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    int expectedExceptionCount ATTR_UNUSED = 0;

    // Put operation
    CacheableKeyPtr keyport = CacheableKey::create(1);
    CacheablePtr pdxobj(new PdxTestsAuto::Child());
    regPtr0->put(keyport, pdxobj);
    LOGINFO("TASK::testPutWithMultilevelInheritance:: Put successful");

    // Get Operation and check fromDataExceptionCount, Expected is 41.
    PdxTestsAuto::ChildPtr obj2 =
        dynCast<PdxTestsAuto::ChildPtr>(regPtr0->get(keyport));
    // LOGINFO("Task: testPutWithMultilevelInheritance: got members :: %d %d %d
    // %d
    // %d %d ", obj2->getMember_a(), obj2->getMember_b(), obj2->getMember_c(),
    // obj2->getMember_d(), obj2->getMember_e(), obj2->getMember_f());
    bool isEqual = (dynCast<PdxTestsAuto::ChildPtr>(pdxobj))->equals(obj2);
    LOGINFO("testPutWithMultilevelInheritance:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true, "Objects of type class Child should be equal");

    LOGINFO("TASK::testPutWithMultilevelInheritance:: Get successful");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, testGetWithMultilevelInheritance)
  {
    try {
      Serializable::registerPdxType(PdxTestsAuto::Child::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport1 = CacheableKey::create(1);
    PdxTestsAuto::ChildPtr obj1 =
        dynCast<PdxTestsAuto::ChildPtr>(regPtr0->get(keyport1));

    CacheablePtr pdxobj(new PdxTestsAuto::Child());
    bool isEqual = (dynCast<PdxTestsAuto::ChildPtr>(pdxobj))->equals(obj1);
    LOGINFO("testPutWithMultilevelInheritance:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true, "Objects of type class Child should be equal");
    // LOGINFO("Task: testGetWithMultilevelInheritance: got members :: %d %d %d
    // %d
    // %d %d ", obj1->getMember_a(), obj1->getMember_b(), obj1->getMember_c(),
    // obj1->getMember_d(), obj1->getMember_e(), obj1->getMember_f());
    LOGINFO(
        "TASK::testGetWithMultilevelInheritance GET completed Successfully");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, JavaPutGet)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    CacheablePtr pdxobj(new PdxTestsAuto::PdxType());
    regPtr0->put(keyport, pdxobj);
    LOGINFO("JavaPutGet:.. Put Done");

    PdxTestsAuto::PdxTypePtr obj2 =
        dynCast<PdxTestsAuto::PdxTypePtr>(regPtr0->get(keyport));

    CacheableBooleanPtr boolPtr =
        dynCast<CacheableBooleanPtr>(regPtr0->get("success"));
    bool isEqual = boolPtr.ptr()->value();
    LOGDEBUG("Task:JavaPutGet: isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Task JavaPutGet:Objects of type PdxType should be equal");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, JavaGet)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    LOGDEBUG("JavaGet-1 Line_309");
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport1 = CacheableKey::create(1);
    CacheablePtr pdxobj(new PdxTestsAuto::PdxType());
    LOGDEBUG("JavaGet-2 Line_314");
    PdxTestsAuto::PdxTypePtr obj1 =
        dynCast<PdxTestsAuto::PdxTypePtr>(regPtr0->get(keyport1));
    LOGDEBUG("JavaGet-3 Line_316");
    CacheableKeyPtr keyport2 = CacheableKey::create("putFromjava");
    LOGDEBUG("JavaGet-4 Line_316");
    PdxTestsAuto::PdxTypePtr obj2 =
        dynCast<PdxTestsAuto::PdxTypePtr>(regPtr0->get(keyport2));
    LOGDEBUG("JavaGet-5 Line_320");
  }
END_TASK_DEFINITION

///***************************************************************/
DUNIT_TASK_DEFINITION(CLIENT2, putAtVersionTwoR21)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypesR2V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxTypesR2V2Ptr np(new PdxTestsAuto::PdxTypesR2V2());

    regPtr0->put(keyport, np);

    PdxTestsAuto::PdxTypesR2V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesR2V2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("putAtVersionTwoR21:.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypesR2V2 should be equal at putAtVersionTwoR21");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOneR22)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypesV1R2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxTypesV1R2Ptr np(new PdxTestsAuto::PdxTypesV1R2());

    PdxTestsAuto::PdxTypesV1R2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesV1R2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("getPutAtVersionOneR22:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxTypesV1R2 should be equal at "
           "getPutAtVersionOneR22");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwoR23)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxTypesR2V2Ptr np(new PdxTestsAuto::PdxTypesR2V2());

    PdxTestsAuto::PdxTypesR2V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesR2V2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("getPutAtVersionTwoR23:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxTypesR2V2 should be equal at "
           "getPutAtVersionTwoR23");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOneR24)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxTypesV1R2Ptr np(new PdxTestsAuto::PdxTypesV1R2());

    PdxTestsAuto::PdxTypesV1R2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesV1R2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("getPutAtVersionOneR24:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxTypesV1R2 should be equal at "
           "getPutAtVersionOneR24");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putAtVersionOne31)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxType3V1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxType3V1Ptr np(new PdxTestsAuto::PdxType3V1());

    regPtr0->put(keyport, np);

    PdxTestsAuto::PdxType3V1Ptr pRet =
        dynCast<PdxTestsAuto::PdxType3V1Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("Task:putAtVersionOne31: isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxType3V1 should be equal at putAtVersionOne31");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo32)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes3V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxTypes3V2Ptr np(new PdxTestsAuto::PdxTypes3V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxTypes3V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypes3V2Ptr>(regPtr0->get(keyport));
    bool isEqual = np->equals(pRet);
    LOGDEBUG("Task:getPutAtVersionTwo32.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypes3V2 should be equal at getPutAtVersionTwo32");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne33)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxType3V1Ptr np(new PdxTestsAuto::PdxType3V1());

    PdxTestsAuto::PdxType3V1Ptr pRet =
        dynCast<PdxTestsAuto::PdxType3V1Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("getPutAtVersionOne33:.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxType3V1 should be equal at getPutAtVersionOne33");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo34)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    PdxTestsAuto::PdxTypes3V2Ptr np(new PdxTestsAuto::PdxTypes3V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxTypes3V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypes3V2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("Task:getPutAtVersionTwo34: isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxType3V1 should be equal at getPutAtVersionTwo34");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putAtVersionOne21)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxType2V1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxType2V1Ptr np(new PdxTestsAuto::PdxType2V1());

    regPtr0->put(keyport, np);

    PdxTestsAuto::PdxType2V1Ptr pRet =
        dynCast<PdxTestsAuto::PdxType2V1Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("Task:putAtVersionOne21:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxType2V1 should be equal at putAtVersionOne21");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo22)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes2V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxTypes2V2Ptr np(new PdxTestsAuto::PdxTypes2V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxTypes2V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypes2V2Ptr>(regPtr0->get(keyport));
    bool isEqual = np->equals(pRet);
    LOGDEBUG("Task:getPutAtVersionTwo22.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypes2V2 should be equal at getPutAtVersionTwo22");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne23)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    PdxTestsAuto::PdxType2V1Ptr np(new PdxTestsAuto::PdxType2V1());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxType2V1Ptr pRet =
        dynCast<PdxTestsAuto::PdxType2V1Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("Task:getPutAtVersionOne23: isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxType2V1 should be equal at getPutAtVersionOne23");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo24)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxTypes2V2Ptr np(new PdxTestsAuto::PdxTypes2V2());
    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxTypes2V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypes2V2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("Task:getPutAtVersionTwo24.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypes2V2 should be equal at getPutAtVersionTwo24");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putAtVersionOne11)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxType1V1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxType1V1Ptr np(new PdxTestsAuto::PdxType1V1());

    regPtr0->put(keyport, np);

    PdxTestsAuto::PdxType1V1Ptr pRet =
        dynCast<PdxTestsAuto::PdxType1V1Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("NIL:putAtVersionOne11:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxType1V1 should be equal at putAtVersionOne11 "
           "Line_170");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, putAtVersionTwo1)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypesR1V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    PdxTestsAuto::PdxTypesR1V2::reset(false);

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxTypesR1V2Ptr np(new PdxTestsAuto::PdxTypesR1V2());

    regPtr0->put(keyport, np);

    PdxTestsAuto::PdxTypesR1V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesR1V2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("NIL:putAtVersionTwo1:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxTypesR1V2 should be equal at putAtVersionTwo1");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne2)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypesV1R1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    PdxTestsAuto::PdxTypesV1R1Ptr np(new PdxTestsAuto::PdxTypesV1R1());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxTypesV1R1Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesV1R1Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("NIL:getPutAtVersionOne2:.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypesV1R1 should be equal at getPutAtVersionOne2");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo3)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxTypesR1V2Ptr np(new PdxTestsAuto::PdxTypesR1V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxTypesR1V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesR1V2Ptr>(regPtr0->get(keyport));
    bool isEqual = np->equals(pRet);
    LOGDEBUG("NIL:getPutAtVersionTwo3.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypesR1V2 should be equal at getPutAtVersionTwo3");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne4)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxTypesV1R1Ptr np(new PdxTestsAuto::PdxTypesV1R1());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxTypesV1R1Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesV1R1Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("getPutAtVersionOne4: isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypesV1R1 should be equal at getPutAtVersionOne4");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo5)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxTypesR1V2Ptr np(new PdxTestsAuto::PdxTypesR1V2());

    // GET
    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxTypesR1V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesR1V2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("Task:getPutAtVersionTwo5.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypesR1V2 should be equal at getPutAtVersionTwo5");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne6)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxTypesV1R1Ptr np(new PdxTestsAuto::PdxTypesV1R1());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxTypesV1R1Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesV1R1Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("Task getPutAtVersionOne6:.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypesV1R1 should be equal at getPutAtVersionOne6");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, putV2PdxUI)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    // PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV2::reset(false);

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV2Ptr np(
        new PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV2());
    CacheableKeyPtr keyport = CacheableKey::create(1);
    regPtr0->put(keyport, np);

    PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV2Ptr>(
            regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("NIL:putV2PdxUI:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxTypesIgnoreUnreadFieldsV2 should be equal at "
           "putV2PdxUI ");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION
//
DUNIT_TASK_DEFINITION(CLIENT1, putV1PdxUI)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    // PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV1::reset(false);
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV1Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV1Ptr>(
            regPtr0->get(keyport));
    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getV2PdxUI)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTypesIgnoreUnreadFieldsV2Ptr np(new PdxTypesIgnoreUnreadFieldsV2());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV2Ptr>(
            regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("Task:getV2PdxUI:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxTypesIgnoreUnreadFieldsV2 should be equal at "
           "getV2PdxUI ");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo12)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes1V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    PdxTestsAuto::PdxTypes1V2Ptr np(new PdxTestsAuto::PdxTypes1V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxTypes1V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypes1V2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("NIL:getPutAtVersionTwo12:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxType1V2 should be equal at getPutAtVersionTwo12 "
           "Line_197");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne13)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxType1V1Ptr np(new PdxTestsAuto::PdxType1V1());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxType1V1Ptr pRet =
        dynCast<PdxType1V1Ptr>(regPtr0->get(keyport));
    bool isEqual = np->equals(pRet);

    LOGDEBUG("NIL:getPutAtVersionOne13:221.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxType1V2 should be equal at getPutAtVersionOne13 "
           "Line_215");

    LOGDEBUG("NIL:getPutAtVersionOne13: PUT remote object -1");
    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo14)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxTypes1V2Ptr np(new PdxTestsAuto::PdxTypes1V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxTypes1V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypes1V2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("NIL:getPutAtVersionTwo14:241.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypes1V2 should be equal at getPutAtVersionTwo14 "
        "Line_242");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, getPutAtVersionOne15)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxType1V1Ptr np(new PdxTestsAuto::PdxType1V1());

    // GET
    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::PdxType1V1Ptr pRet =
        dynCast<PdxTestsAuto::PdxType1V1Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("NIL:getPutAtVersionOne15:784.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxType1V2 should be equal at getPutAtVersionOne15 "
           "Line_272");

    regPtr0->put(keyport, pRet);
    LOGDEBUG(
        "NIL:getPutAtVersionOne15 m_useWeakHashMap = %d and "
        "TestUtils::testNumberOfPreservedData() = %d",
        m_useWeakHashMap, TestUtils::testNumberOfPreservedData());
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo16)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxTypes1V2Ptr np(new PdxTestsAuto::PdxTypes1V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxTypes1V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypes1V2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("NIL:getPutAtVersionTwo14:.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypes1V2 should be equal at getPutAtVersionTwo14");

    regPtr0->put(keyport, pRet);
    LOGDEBUG(
        "getPutAtVersionTwo16 m_useWeakHashMap = %d and "
        "TestUtils::testNumberOfPreservedData() = %d",
        m_useWeakHashMap, TestUtils::testNumberOfPreservedData());
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAndVerifyPdxInGet)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);

    CacheablePtr pdxobj(new PdxTestsAuto::PdxType());

    regPtr0->put(keyport, pdxobj);

    PdxTestsAuto::PdxTypePtr obj2 =
        dynCast<PdxTestsAuto::PdxTypePtr>(regPtr0->get(keyport));

    checkPdxInstanceToStringAtServer(regPtr0);

    ASSERT(cacheHelper->getCache()->getPdxReadSerialized() == false,
           "Pdx read serialized property should be false.");

    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
    LOGINFO("PdxSerializations = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
    LOGINFO("PdxDeSerializations = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
    LOGINFO("PdxSerializationBytes = %ld ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
    LOGINFO(
        "PdxDeSerializationBytes = %ld ",
        lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
    ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
               lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
           "Total pdxDeserializations should be equal to Total "
           "pdxSerializations.");
    ASSERT(
        lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
            lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes(),
        "Total pdxDeserializationBytes should be equal to Total "
        "pdxSerializationsBytes.");

    LOG("StepThree complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAndVerifyNestedPdxInGet)
  {
    LOG("PutAndVerifyNestedPdxInGet started.");

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::NestedPdx::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    PdxTestsAuto::NestedPdxPtr p1(new PdxTestsAuto::NestedPdx());
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);

    regPtr0->put(keyport, p1);

    PdxTestsAuto::NestedPdxPtr obj2 =
        dynCast<PdxTestsAuto::NestedPdxPtr>(regPtr0->get(keyport));

    ASSERT(obj2->equals(p1) == true, "Nested pdx objects should be equal");

    LOG("PutAndVerifyNestedPdxInGet complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAndVerifyPdxInGFSInGet)
  {
    try {
      Serializable::registerType(
          PdxTestsAuto::PdxInsideIGFSerializable::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::NestedPdx::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes3::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes4::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes5::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes6::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes7::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes8::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxInsideIGFSerializablePtr np(
        new PdxTestsAuto::PdxInsideIGFSerializable());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    regPtr0->put(keyport, np);

    // GET
    PdxTestsAuto::PdxInsideIGFSerializablePtr pRet =
        dynCast<PdxTestsAuto::PdxInsideIGFSerializablePtr>(
            regPtr0->get(keyport));
    ASSERT(pRet->equals(np) == true,
           "TASK PutAndVerifyPdxInIGFSInGet: PdxInsideIGFSerializable objects "
           "should be equal");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyPdxInGFSGetOnly)
  {
    try {
      Serializable::registerType(
          PdxTestsAuto::PdxInsideIGFSerializable::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::NestedPdx::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes3::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes4::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes5::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes6::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes7::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes8::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTestsAuto::PdxInsideIGFSerializablePtr orig(
        new PdxTestsAuto::PdxInsideIGFSerializable());

    // GET
    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxInsideIGFSerializablePtr pRet =
        dynCast<PdxTestsAuto::PdxInsideIGFSerializablePtr>(
            regPtr0->get(keyport));
    ASSERT(
        pRet->equals(orig) == true,
        "TASK:VerifyPdxInIGFSGetOnly, PdxInsideIGFSerializable objects should "
        "be equal");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyNestedGetOnly)
  {
    LOG("VerifyNestedGetOnly started.");

    try {
      Serializable::registerPdxType(NestedPdx::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    PdxTestsAuto::NestedPdxPtr p1(new PdxTestsAuto::NestedPdx());
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTestsAuto::NestedPdxPtr obj2 =
        dynCast<PdxTestsAuto::NestedPdxPtr>(regPtr0->get(keyport));

    ASSERT(obj2->equals(p1) == true, "Nested pdx objects should be equal");

    LOG("VerifyNestedGetOnly complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyGetOnly)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxTypePtr obj2 =
        dynCast<PdxTestsAuto::PdxTypePtr>(regPtr0->get(keyport));

    checkPdxInstanceToStringAtServer(regPtr0);

    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
    LOGINFO("PdxSerializations = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
    LOGINFO("PdxDeSerializations = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
    LOGINFO("PdxSerializationBytes = %ld ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
    LOGINFO(
        "PdxDeSerializationBytes = %ld ",
        lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
    ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() <
               lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
           "Total pdxDeserializations should be less than Total "
           "pdxSerializations.");
    ASSERT(
        lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() <
            lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes(),
        "Total pdxDeserializationBytes should be less than Total "
        "pdxSerializationsBytes.");
    LOG("StepFour complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAndVerifyVariousPdxTypes)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes3::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes4::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes5::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes6::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes7::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes8::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes9::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes10::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    bool flag = false;
    {
      PdxTestsAuto::PdxTypes1Ptr p1(new PdxTestsAuto::PdxTypes1());
      CacheableKeyPtr keyport = CacheableKey::create(11);
      regPtr0->put(keyport, p1);
      PdxTestsAuto::PdxTypes1Ptr pRet =
          dynCast<PdxTestsAuto::PdxTypes1Ptr>(regPtr0->get(keyport));

      flag = p1->equals(pRet);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true,
             "Objects of type PdxTestsAuto::PdxTypes1 should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be equal to Total "
          "pdxSerializations.");
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
              lregPtr->getCacheImpl()
                  ->m_cacheStats->getPdxDeSerializationBytes(),
          "Total pdxDeserializationBytes should be equal to Total "
          "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes2Ptr p2(new PdxTestsAuto::PdxTypes2());
      CacheableKeyPtr keyport2 = CacheableKey::create(12);
      regPtr0->put(keyport2, p2);
      PdxTestsAuto::PdxTypes2Ptr pRet2 =
          dynCast<PdxTestsAuto::PdxTypes2Ptr>(regPtr0->get(keyport2));

      flag = p2->equals(pRet2);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true,
             "Objects of type PdxTestsAuto::PdxTypes2 should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be equal to Total "
          "pdxSerializations.");
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
              lregPtr->getCacheImpl()
                  ->m_cacheStats->getPdxDeSerializationBytes(),
          "Total pdxDeserializationBytes should be equal to Total "
          "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes3Ptr p3(new PdxTestsAuto::PdxTypes3());
      CacheableKeyPtr keyport3 = CacheableKey::create(13);
      regPtr0->put(keyport3, p3);
      PdxTestsAuto::PdxTypes3Ptr pRet3 =
          dynCast<PdxTestsAuto::PdxTypes3Ptr>(regPtr0->get(keyport3));

      flag = p3->equals(pRet3);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true,
             "Objects of type PdxTestsAuto::PdxTypes3 should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be equal to Total "
          "pdxSerializations.");
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
              lregPtr->getCacheImpl()
                  ->m_cacheStats->getPdxDeSerializationBytes(),
          "Total pdxDeserializationBytes should be equal to Total "
          "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes4Ptr p4(new PdxTestsAuto::PdxTypes4());
      CacheableKeyPtr keyport4 = CacheableKey::create(14);
      regPtr0->put(keyport4, p4);
      PdxTestsAuto::PdxTypes4Ptr pRet4 =
          dynCast<PdxTestsAuto::PdxTypes4Ptr>(regPtr0->get(keyport4));

      flag = p4->equals(pRet4);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true,
             "Objects of type PdxTestsAuto::PdxTypes4 should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be equal to Total "
          "pdxSerializations.");
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
              lregPtr->getCacheImpl()
                  ->m_cacheStats->getPdxDeSerializationBytes(),
          "Total pdxDeserializationBytes should be equal to Total "
          "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes5Ptr p5(new PdxTestsAuto::PdxTypes5());
      CacheableKeyPtr keyport5 = CacheableKey::create(15);
      regPtr0->put(keyport5, p5);
      PdxTestsAuto::PdxTypes5Ptr pRet5 =
          dynCast<PdxTestsAuto::PdxTypes5Ptr>(regPtr0->get(keyport5));

      flag = p5->equals(pRet5);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true,
             "Objects of type PdxTestsAuto::PdxTypes5 should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be equal to Total "
          "pdxSerializations.");
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
              lregPtr->getCacheImpl()
                  ->m_cacheStats->getPdxDeSerializationBytes(),
          "Total pdxDeserializationBytes should be equal to Total "
          "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes6Ptr p6(new PdxTestsAuto::PdxTypes6());
      CacheableKeyPtr keyport6 = CacheableKey::create(16);
      regPtr0->put(keyport6, p6);
      PdxTestsAuto::PdxTypes6Ptr pRet6 =
          dynCast<PdxTestsAuto::PdxTypes6Ptr>(regPtr0->get(keyport6));

      flag = p6->equals(pRet6);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true,
             "Objects of type PdxTestsAuto::PdxTypes6 should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be equal to Total "
          "pdxSerializations.");
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
              lregPtr->getCacheImpl()
                  ->m_cacheStats->getPdxDeSerializationBytes(),
          "Total pdxDeserializationBytes should be equal to Total "
          "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes7Ptr p7(new PdxTestsAuto::PdxTypes7());
      CacheableKeyPtr keyport7 = CacheableKey::create(17);
      regPtr0->put(keyport7, p7);
      PdxTestsAuto::PdxTypes7Ptr pRet7 =
          dynCast<PdxTestsAuto::PdxTypes7Ptr>(regPtr0->get(keyport7));

      flag = p7->equals(pRet7);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true,
             "Objects of type PdxTestsAuto::PdxTypes7 should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be equal to Total "
          "pdxSerializations.");
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
              lregPtr->getCacheImpl()
                  ->m_cacheStats->getPdxDeSerializationBytes(),
          "Total pdxDeserializationBytes should be equal to Total "
          "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes8Ptr p8(new PdxTestsAuto::PdxTypes8());
      CacheableKeyPtr keyport8 = CacheableKey::create(18);
      regPtr0->put(keyport8, p8);
      PdxTestsAuto::PdxTypes8Ptr pRet8 =
          dynCast<PdxTestsAuto::PdxTypes8Ptr>(regPtr0->get(keyport8));

      flag = p8->equals(pRet8);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true,
             "Objects of type PdxTestsAuto::PdxTypes8 should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be equal to Total "
          "pdxSerializations.");
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
              lregPtr->getCacheImpl()
                  ->m_cacheStats->getPdxDeSerializationBytes(),
          "Total pdxDeserializationBytes should be equal to Total "
          "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes9Ptr p9(new PdxTestsAuto::PdxTypes9());
      CacheableKeyPtr keyport9 = CacheableKey::create(19);
      regPtr0->put(keyport9, p9);
      PdxTestsAuto::PdxTypes9Ptr pRet9 =
          dynCast<PdxTestsAuto::PdxTypes9Ptr>(regPtr0->get(keyport9));

      flag = p9->equals(pRet9);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true,
             "Objects of type PdxTestsAuto::PdxTypes9 should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be equal to Total "
          "pdxSerializations.");
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
              lregPtr->getCacheImpl()
                  ->m_cacheStats->getPdxDeSerializationBytes(),
          "Total pdxDeserializationBytes should be equal to Total "
          "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes10Ptr p10(new PdxTestsAuto::PdxTypes10());
      CacheableKeyPtr keyport10 = CacheableKey::create(20);
      regPtr0->put(keyport10, p10);
      PdxTestsAuto::PdxTypes10Ptr pRet10 =
          dynCast<PdxTestsAuto::PdxTypes10Ptr>(regPtr0->get(keyport10));

      flag = p10->equals(pRet10);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true,
             "Objects of type PdxTestsAuto::PdxTypes10 should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be equal to Total "
          "pdxSerializations.");
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
              lregPtr->getCacheImpl()
                  ->m_cacheStats->getPdxDeSerializationBytes(),
          "Total pdxDeserializationBytes should be equal to Total "
          "pdxSerializationsBytes.");
    }

    LOG("NIL:329:StepFive complete.\n");
  }
END_TASK_DEFINITION

////TestCase-2
////c1.client1PutsV1Object
DUNIT_TASK_DEFINITION(CLIENT1, client1PutsV1Object)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxType3V1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    PdxTestsAuto::PdxType3V1::reset(false);
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxType3V1Ptr np(new PdxTestsAuto::PdxType3V1());

    regPtr0->put(keyport, np);
  }
END_TASK_DEFINITION
////c2.client2GetsV1ObjectAndPutsV2Object
DUNIT_TASK_DEFINITION(CLIENT2, client2GetsV1ObjectAndPutsV2Object)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes3V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    PdxTestsAuto::PdxTypes3V2::reset(false);
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    // get v1 object
    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTestsAuto::PdxTypes3V2Ptr pRet =
        dynCast<PdxTestsAuto::PdxTypes3V2Ptr>(regPtr0->get(keyport));

    // now put v2 object
    PdxTestsAuto::PdxTypes3V2Ptr np(new PdxTestsAuto::PdxTypes3V2());
    regPtr0->put(keyport, np);

    LOGDEBUG("Task:client2GetsV1ObjectAndPutsV2Object Done successfully ");
  }
END_TASK_DEFINITION
// c3.client3GetsV2Object
DUNIT_TASK_DEFINITION(CLIENT3, client3GetsV2Object)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    CacheablePtr args = CacheableKey::create("compareDotNETPdxTypes");
    CacheableKeyPtr key = CacheableKey::create(1);
    CacheableVectorPtr routingObj = CacheableVector::create();
    routingObj->push_back(key);

    ExecutionPtr funcExec = FunctionService::onRegion(regPtr0);

    ResultCollectorPtr collector = funcExec->execute("IterateRegion", true);
    ASSERT(collector != NULLPTR, "onRegion collector NULL");

    CacheableVectorPtr result = collector->getResult();
    LOGINFO("NIL:: testTCPDXTests:verifyDotNetPdxTypes result->size = %d ",
            result->size());
    if (result == NULLPTR) {
      ASSERT(false, "echo String : result is NULL");
    } else {
      bool gotResult = false;
      for (int i = 0; i < result->size(); i++) {
        try {
          CacheableBooleanPtr boolValue =
              dynCast<CacheableBooleanPtr>(result->operator[](i));
          LOGINFO("NIL::verifyDotNetPdxTypes boolValue is %d ",
                  boolValue->value());
          bool resultVal = boolValue->value();
          ASSERT(resultVal == true,
                 "Function should return true NIL LINE_1508");
          gotResult = true;
        } catch (ClassCastException& ex) {
          LOG("exFuncNameSendException casting to int for arrayList arguement "
              "exception.");
          std::string logmsg = "";
          logmsg += ex.getName();
          logmsg += ": ";
          logmsg += ex.getMessage();
          LOG(logmsg.c_str());
          ex.printStackTrace();
          LOG("exFuncNameSendException now casting to "
              "UserFunctionExecutionExceptionPtr for arrayList arguement "
              "exception.");
          UserFunctionExecutionExceptionPtr uFEPtr =
              dynCast<UserFunctionExecutionExceptionPtr>(result->operator[](i));
          ASSERT(uFEPtr != NULLPTR, "uFEPtr exception is NULL");
          LOGINFO("Done casting to uFEPtr");
          LOGINFO("Read expected uFEPtr exception %s ",
                  uFEPtr->getMessage()->asChar());
        } catch (...) {
          FAIL(
              "exFuncNameSendException casting to string for bool arguement "
              "Unknown exception.");
        }
      }
      ASSERT(gotResult == true, "Function should (gotResult) return true ");
    }
  }
END_TASK_DEFINITION
// END TestCase-2

DUNIT_TASK_DEFINITION(CLIENT2, VerifyVariousPdxGets)
  {
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes3::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes4::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes5::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes6::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes7::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes8::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes9::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxTypes10::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    // TODO::Uncomment it once PortfolioPdx/PositionPdx Classes are ready
    // Serializable::registerPdxType(PdxTestsAuto.PortfolioPdx.CreateDeserializable);
    // Serializable::registerPdxType(PdxTestsAuto.PositionPdx.CreateDeserializable);

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    bool flag = false;
    {
      PdxTestsAuto::PdxTypes1Ptr p1(new PdxTestsAuto::PdxTypes1());
      CacheableKeyPtr keyport = CacheableKey::create(11);
      PdxTestsAuto::PdxTypes1Ptr pRet =
          dynCast<PdxTestsAuto::PdxTypes1Ptr>(regPtr0->get(keyport));

      flag = p1->equals(pRet);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTestsAuto::PdxTypes1 "
             "should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() <
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be less than Total "
          "pdxSerializations.");
      ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() <
                 lregPtr->getCacheImpl()
                     ->m_cacheStats->getPdxDeSerializationBytes(),
             "Total pdxDeserializationBytes should be less than Total "
             "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes2Ptr p2(new PdxTestsAuto::PdxTypes2());
      CacheableKeyPtr keyport2 = CacheableKey::create(12);
      PdxTestsAuto::PdxTypes2Ptr pRet2 =
          dynCast<PdxTestsAuto::PdxTypes2Ptr>(regPtr0->get(keyport2));

      flag = p2->equals(pRet2);
      LOGDEBUG("VerifyVariousPdxGets:. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTestsAuto::PdxTypes2 "
             "should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() <
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be less than Total "
          "pdxSerializations.");
      ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() <
                 lregPtr->getCacheImpl()
                     ->m_cacheStats->getPdxDeSerializationBytes(),
             "Total pdxDeserializationBytes should be less than Total "
             "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes3Ptr p3(new PdxTestsAuto::PdxTypes3());
      CacheableKeyPtr keyport3 = CacheableKey::create(13);
      PdxTestsAuto::PdxTypes3Ptr pRet3 =
          dynCast<PdxTestsAuto::PdxTypes3Ptr>(regPtr0->get(keyport3));

      flag = p3->equals(pRet3);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTestsAuto::PdxTypes3 "
             "should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() <
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be less than Total "
          "pdxSerializations.");
      ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() <
                 lregPtr->getCacheImpl()
                     ->m_cacheStats->getPdxDeSerializationBytes(),
             "Total pdxDeserializationBytes should be less than Total "
             "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes4Ptr p4(new PdxTestsAuto::PdxTypes4());
      CacheableKeyPtr keyport4 = CacheableKey::create(14);
      PdxTestsAuto::PdxTypes4Ptr pRet4 =
          dynCast<PdxTestsAuto::PdxTypes4Ptr>(regPtr0->get(keyport4));

      flag = p4->equals(pRet4);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTestsAuto::PdxTypes4 "
             "should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() <
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be less than Total "
          "pdxSerializations.");
      ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() <
                 lregPtr->getCacheImpl()
                     ->m_cacheStats->getPdxDeSerializationBytes(),
             "Total pdxDeserializationBytes should be less than Total "
             "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes5Ptr p5(new PdxTestsAuto::PdxTypes5());
      CacheableKeyPtr keyport5 = CacheableKey::create(15);
      PdxTestsAuto::PdxTypes5Ptr pRet5 =
          dynCast<PdxTestsAuto::PdxTypes5Ptr>(regPtr0->get(keyport5));

      flag = p5->equals(pRet5);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTestsAuto::PdxTypes5 "
             "should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() <
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be less than Total "
          "pdxSerializations.");
      ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() <
                 lregPtr->getCacheImpl()
                     ->m_cacheStats->getPdxDeSerializationBytes(),
             "Total pdxDeserializationBytes should be less than Total "
             "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes6Ptr p6(new PdxTestsAuto::PdxTypes6());
      CacheableKeyPtr keyport6 = CacheableKey::create(16);
      PdxTestsAuto::PdxTypes6Ptr pRet6 =
          dynCast<PdxTestsAuto::PdxTypes6Ptr>(regPtr0->get(keyport6));

      flag = p6->equals(pRet6);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTestsAuto::PdxTypes6 "
             "should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() <
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be less than Total "
          "pdxSerializations.");
      ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() <
                 lregPtr->getCacheImpl()
                     ->m_cacheStats->getPdxDeSerializationBytes(),
             "Total pdxDeserializationBytes should be less than Total "
             "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes7Ptr p7(new PdxTestsAuto::PdxTypes7());
      CacheableKeyPtr keyport7 = CacheableKey::create(17);
      PdxTestsAuto::PdxTypes7Ptr pRet7 =
          dynCast<PdxTestsAuto::PdxTypes7Ptr>(regPtr0->get(keyport7));

      flag = p7->equals(pRet7);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTestsAuto::PdxTypes7 "
             "should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() <
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be less than Total "
          "pdxSerializations.");
      ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() <
                 lregPtr->getCacheImpl()
                     ->m_cacheStats->getPdxDeSerializationBytes(),
             "Total pdxDeserializationBytes should be less than Total "
             "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes8Ptr p8(new PdxTestsAuto::PdxTypes8());
      CacheableKeyPtr keyport8 = CacheableKey::create(18);
      PdxTestsAuto::PdxTypes8Ptr pRet8 =
          dynCast<PdxTestsAuto::PdxTypes8Ptr>(regPtr0->get(keyport8));

      flag = p8->equals(pRet8);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTestsAuto::PdxTypes8 "
             "should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() <
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be less than Total "
          "pdxSerializations.");
      ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() <
                 lregPtr->getCacheImpl()
                     ->m_cacheStats->getPdxDeSerializationBytes(),
             "Total pdxDeserializationBytes should be less than Total "
             "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes9Ptr p9(new PdxTestsAuto::PdxTypes9());
      CacheableKeyPtr keyport9 = CacheableKey::create(19);
      PdxTestsAuto::PdxTypes9Ptr pRet9 =
          dynCast<PdxTestsAuto::PdxTypes9Ptr>(regPtr0->get(keyport9));

      flag = p9->equals(pRet9);
      LOGDEBUG("VerifyVariousPdxGets:. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTestsAuto::PdxTypes9 "
             "should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() <
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be less than Total "
          "pdxSerializations.");
      ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() <
                 lregPtr->getCacheImpl()
                     ->m_cacheStats->getPdxDeSerializationBytes(),
             "Total pdxDeserializationBytes should be less than Total "
             "pdxSerializationsBytes.");
    }

    {
      PdxTestsAuto::PdxTypes10Ptr p10(new PdxTestsAuto::PdxTypes10());
      CacheableKeyPtr keyport10 = CacheableKey::create(20);
      PdxTestsAuto::PdxTypes10Ptr pRet10 =
          dynCast<PdxTestsAuto::PdxTypes10Ptr>(regPtr0->get(keyport10));

      flag = p10->equals(pRet10);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTestsAuto::PdxTypes10 "
             "should be equal");
      checkPdxInstanceToStringAtServer(regPtr0);

      LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
      LOGINFO("PdxSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
      LOGINFO("PdxDeSerializations = %d ",
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
      LOGINFO(
          "PdxSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
      LOGINFO(
          "PdxDeSerializationBytes = %ld ",
          lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
      ASSERT(
          lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() <
              lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
          "Total pdxDeserializations should be less than Total "
          "pdxSerializations.");
      ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() <
                 lregPtr->getCacheImpl()
                     ->m_cacheStats->getPdxDeSerializationBytes(),
             "Total pdxDeserializationBytes should be less than Total "
             "pdxSerializationsBytes.");
    }
    LOG("NIL:436:StepSix complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putCharTypes)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::CharTypes::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    LOG("PdxTestsAuto::CharTypes Registered Successfully....");

    LOG("Trying to populate PDX objects.....\n");
    CacheablePtr pdxobj(new PdxTestsAuto::CharTypes());
    CacheableKeyPtr keyport = CacheableKey::create(1);

    // PUT Operation
    regPtr0->put(keyport, pdxobj);
    LOG("PdxTestsAuto::CharTypes: PUT Done successfully....");

    // locally destroy PdxTestsAuto::PdxType
    regPtr0->localDestroy(keyport);
    LOG("localDestroy() operation....Done");

    LOG("Done populating PDX objects.....Success\n");
    LOG("STEP putCharTypes complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getCharTypes)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    LOG("Trying to GET PDX objects.....\n");
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::CharTypes::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    PdxTestsAuto::CharTypesPtr localPdxptr(new PdxTestsAuto::CharTypes());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    LOG("Client-2 PdxTestsAuto::CharTypes GET OP Start....");
    PdxTestsAuto::CharTypesPtr remotePdxptr =
        dynCast<PdxTestsAuto::CharTypesPtr>(regPtr0->get(keyport));
    LOG("Client-2 PdxTestsAuto::CharTypes GET OP Done....");

    PdxTestsAuto::CharTypes* localPdx = localPdxptr.ptr();
    PdxTestsAuto::CharTypes* remotePdx =
        dynamic_cast<PdxTestsAuto::CharTypes*>(remotePdxptr.ptr());

    LOGINFO("testThinClientPdxTests:StepFour before equal() check");
    ASSERT(remotePdx->equals(*localPdx) == true,
           "PdxTestsAuto::PdxTypes should be equal.");

    LOGINFO("testThinClientPdxTests:StepFour equal check done successfully");

    LOG("STEP: getCharTypes complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    // QueryHelper * qh = &QueryHelper::getHelper();
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    LOG("PdxTestsAuto::PdxClassV1 Registered Successfully....");

    LOG("Trying to populate PDX objects.....\n");
    CacheablePtr pdxobj(new PdxTestsAuto::PdxType());
    CacheableKeyPtr keyport = CacheableKey::create(1);

    // PUT Operation
    regPtr0->put(keyport, pdxobj);
    LOG("PdxTestsAuto::PdxType: PUT Done successfully....");

    // PUT CacheableObjectArray as a Value
    CacheableKeyPtr keyport2 = CacheableKey::create(2);
    CacheableObjectArrayPtr m_objectArray;

    m_objectArray = CacheableObjectArray::create();
    m_objectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(1, "street0", "city0")));
    m_objectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(2, "street1", "city1")));
    m_objectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(3, "street2", "city2")));
    m_objectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(4, "street3", "city3")));
    m_objectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(5, "street4", "city4")));
    m_objectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(6, "street5", "city5")));
    m_objectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(7, "street6", "city6")));
    m_objectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(8, "street7", "city7")));
    m_objectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(9, "street8", "city8")));
    m_objectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(10, "street9", "city9")));

    // PUT Operation
    regPtr0->put(keyport2, m_objectArray);

    // locally destroy PdxTestsAuto::PdxType
    regPtr0->localDestroy(keyport);
    regPtr0->localDestroy(keyport2);

    LOG("localDestroy() operation....Done");

    // This is merely for asserting statistics
    regPtr0->get(keyport);
    regPtr0->get(keyport2);

    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
    LOGINFO("PdxTestsAutoPdxSerializations = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
    LOGINFO("PdxTestsAuto PdxDeSerializations = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
    LOGINFO("PdxTestsAutoPdxSerializationBytes = %ld ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
    LOGINFO(
        "PdxTestsAutoPdxDeSerializationBytes = %ld ",
        lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());
    ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations() ==
               lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
           "Total pdxDeserializations should be equal to Total "
           "pdxSerializations.");
    ASSERT(
        lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes() ==
            lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes(),
        "Total pdxDeserializationBytes should be equal to Total "
        "pdxSerializationsBytes.");

    // Now update new keys with updated stats values, so that other client can
    // verify these values with its stats.
    CacheableKeyPtr keyport3 = CacheableKey::create(3);
    CacheableKeyPtr keyport4 = CacheableKey::create(4);
    regPtr0->put(
        keyport3,
        CacheableInt32::create(
            lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations()));
    regPtr0->put(keyport4,
                 CacheableInt64::create(
                     lregPtr->getCacheImpl()
                         ->m_cacheStats->getPdxDeSerializationBytes()));

    LOG("Done populating PDX objects.....Success\n");
    LOG("StepThree complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepFour)
  {
    // initClient(true);
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    // QueryHelper * qh = &QueryHelper::getHelper();

    LOG("Trying to GET PDX objects.....\n");
    try {
      Serializable::registerPdxType(
          PdxTestsAuto::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          PdxTestsAuto::Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    // Create local CacheableObjectArray
    CacheableObjectArrayPtr m_localObjectArray;
    m_localObjectArray = CacheableObjectArray::create();
    m_localObjectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(1, "street0", "city0")));
    m_localObjectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(2, "street1", "city1")));
    m_localObjectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(3, "street2", "city2")));
    m_localObjectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(4, "street3", "city3")));
    m_localObjectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(5, "street4", "city4")));
    m_localObjectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(6, "street5", "city5")));
    m_localObjectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(7, "street6", "city6")));
    m_localObjectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(8, "street7", "city7")));
    m_localObjectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(9, "street8", "city8")));
    m_localObjectArray->push_back(PdxTestsAuto::AddressPtr(
        new PdxTestsAuto::Address(10, "street9", "city9")));

    // Get remote CacheableObjectArray on key 2
    CacheableKeyPtr keyport2 = CacheableKey::create(2);
    LOGINFO("Client-2 PdxTestsAuto::PdxType GET OP Start....");
    CacheableObjectArrayPtr remoteCObjArray =
        dynCast<CacheableObjectArrayPtr>(regPtr0->get(keyport2));

    LOGINFO(
        "Client-2 PdxTestsAuto::PdxType GET OP Done.. Received CObjeArray Size "
        "= "
        "%d",
        remoteCObjArray->size());
    ASSERT(remoteCObjArray->size() == 10,
           "PdxTestsAuto StepFour: CacheableObjectArray Size should be equal "
           "to 10");

    // Compare local vs remote CacheableObjectArray elements.
    bool isEqual = true;
    for (int i = 0; i < remoteCObjArray->size(); i++) {
      PdxTestsAuto::Address* rAddr1 =
          dynamic_cast<PdxTestsAuto::Address*>(remoteCObjArray->at(i).ptr());
      PdxTestsAuto::Address* lAddr1 =
          dynamic_cast<PdxTestsAuto::Address*>(m_localObjectArray->at(i).ptr());
      LOGINFO(
          "Remote PdxTestsAuto::Address:: %d th element  AptNum=%d  street=%s  "
          "city=%s ",
          i, rAddr1->getAptNum(), rAddr1->getStreet(), rAddr1->getCity());
      if (!rAddr1->equals(*lAddr1)) {
        isEqual = false;
        break;
      }
    }
    ASSERT(
        isEqual == true,
        "PdxTestsAuto StepFour: CacheableObjectArray elements are not matched");

    PdxTestsAuto::PdxTypePtr localPdxptr(new PdxTestsAuto::PdxType());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    LOG("Client-2 PdxTestsAuto::PdxType GET OP Start....");
    PdxTestsAuto::PdxTypePtr remotePdxptr =
        dynCast<PdxTestsAuto::PdxTypePtr>(regPtr0->get(keyport));
    LOG("Client-2 PdxTestsAuto::PdxType GET OP Done....");

    //
    PdxTestsAuto::PdxType* localPdx = localPdxptr.ptr();
    PdxTestsAuto::PdxType* remotePdx =
        dynamic_cast<PdxTestsAuto::PdxType*>(remotePdxptr.ptr());

    // ToDo open this equals check
    LOGINFO("testThinClientPdxTests:StepFour before equal() check");
    ASSERT(remotePdx->equals(*localPdx, false) == true,
           "PdxTestsAuto::PdxTypes should be equal.");
    LOGINFO("testThinClientPdxTests:StepFour equal check done successfully");
    LOGINFO("GET OP Result: Char Val=%c", remotePdx->getChar());
    LOGINFO("NIL GET OP Result: Char[0] val=%c", remotePdx->getCharArray()[0]);
    LOGINFO("NIL GET OP Result: Char[1] val=%c", remotePdx->getCharArray()[1]);
    LOGINFO("GET OP Result: Array of byte arrays [0]=%x",
            remotePdx->getArrayOfByteArrays()[0][0]);
    LOGINFO("GET OP Result: Array of byte arrays [1]=%x",
            remotePdx->getArrayOfByteArrays()[1][0]);
    LOGINFO("GET OP Result: Array of byte arrays [2]=%x",
            remotePdx->getArrayOfByteArrays()[1][1]);

    CacheableInt32* element =
        dynamic_cast<CacheableInt32*>(remotePdx->getArrayList()->at(0).ptr());
    LOGINFO("GET OP Result_1233: Array List element Value =%d",
            element->value());

    CacheableInt32* remoteKey = NULL;
    CacheableString* remoteVal = NULL;

    for (CacheableHashTable::Iterator iter = remotePdx->getHashTable()->begin();
         iter != remotePdx->getHashTable()->end(); ++iter) {
      remoteKey = dynamic_cast<CacheableInt32*>(iter.first().ptr());
      remoteVal = dynamic_cast<CacheableString*>(iter.second().ptr());
      LOGINFO("HashTable Key Val = %d", remoteKey->value());
      LOGINFO("HashTable Val = %s", remoteVal->asChar());
      //(*iter1).first.value();
      // output.writeObject( *iter );
    }

    // Now get values for key3 and 4 to asset against stats of this client
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(regPtr0.ptr()));
    LOGINFO("PdxSerializations = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializations());
    LOGINFO("PdxDeSerializations = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations());
    LOGINFO("PdxSerializationBytes = %ld ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes());
    LOGINFO(
        "PdxDeSerializationBytes = %ld ",
        lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes());

    CacheableKeyPtr keyport3 = CacheableKey::create(3);
    CacheableKeyPtr keyport4 = CacheableKey::create(4);
    CacheableInt32Ptr int32Ptr =
        dynCast<CacheableInt32Ptr>(regPtr0->get(keyport3));
    CacheableInt64Ptr int64Ptr =
        dynCast<CacheableInt64Ptr>(regPtr0->get(keyport4));
    ASSERT(int32Ptr->value() ==
               lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializations(),
           "Total pdxDeserializations should be equal to Total "
           "pdxSerializations.");
    ASSERT(
        int64Ptr->value() ==
            lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes(),
        "Total pdxDeserializationBytes should be equal to Total "
        "pdxSerializationsBytes.");

    LOG("Done Getting PDX objects.....Success\n");

    LOG("StepFour complete.\n");
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

DUNIT_TASK_DEFINITION(CLIENT3, CloseCache3)
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

DUNIT_TASK_DEFINITION(CLIENT1, SetWeakHashMapToTrueC1)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxTypesIgnoreUnreadFieldsV2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetWeakHashMapToTrueC1BM)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxType1V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2BM)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxTypes1V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1BM)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxType1V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2BM)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxTypes1V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetWeakHashMapToTrueC1BM2)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxType2V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2BM2)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxTypes2V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1BM2)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxType2V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2BM2)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxTypes2V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1, SetWeakHashMapToTrueC1BM3)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxType3V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2BM3)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxTypes3V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1BM3)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxType3V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2BM3)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxTypes3V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetWeakHashMapToTrueC1BMR1)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxTypesV1R1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2BMR1)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxTypesR1V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1BMR1)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxTypesV1R1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2BMR1)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxTypesR1V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1, SetWeakHashMapToTrueC1BMR2)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxTypesV1R2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2BMR2)
  {
    m_useWeakHashMap = true;
    PdxTestsAuto::PdxTypesR2V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1BMR2)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxTypesV1R2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2BMR2)
  {
    m_useWeakHashMap = false;
    PdxTestsAuto::PdxTypesR2V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION
///

void runPdxDistOps(bool poolConfig = false, bool withLocators = false) {
  if (!poolConfig) {
    CALL_TASK(CreateServer)
    CALL_TASK(StepOne)
  } else if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }
  // StepThree: Put some portfolio/Position objects
  CALL_TASK(PutAndVerifyPdxInGet)
  CALL_TASK(VerifyGetOnly)
  CALL_TASK(PutAndVerifyVariousPdxTypes)
  CALL_TASK(VerifyVariousPdxGets)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runPdxTestForCharTypes(bool poolConfig = false,
                            bool withLocators = false) {
  if (!poolConfig) {
    CALL_TASK(CreateServer)
    CALL_TASK(StepOne)
  } else if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }
  // StepThree: Put some portfolio/Position objects
  CALL_TASK(putCharTypes)
  CALL_TASK(getCharTypes)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runPdxPutGetTest(bool poolConfig = false, bool withLocators = false) {
  if (!poolConfig) {
    CALL_TASK(CreateServer)
    CALL_TASK(StepOne)
  } else if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }
  // StepThree: Put some portfolio/Position objects
  CALL_TASK(StepThree)
  CALL_TASK(StepFour)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runBasicMergeOpsR2(bool poolConfig = false, bool withLocators = false) {
  if (!poolConfig) {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOne)
  } else if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(putAtVersionTwoR21)

  CALL_TASK(getPutAtVersionOneR22)

  for (int i = 0; i < 10; i++) {
    CALL_TASK(getPutAtVersionTwoR23);
    CALL_TASK(getPutAtVersionOneR24);
  }

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runBasicMergeOpsR1(bool poolConfig = false, bool withLocators = false) {
  if (!poolConfig) {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOne)
  } else if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(putAtVersionTwo1)

  CALL_TASK(getPutAtVersionOne2)

  CALL_TASK(getPutAtVersionTwo3)

  CALL_TASK(getPutAtVersionOne4)

  for (int i = 0; i < 10; i++) {
    CALL_TASK(getPutAtVersionTwo5);
    CALL_TASK(getPutAtVersionOne6);
  }

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runBasicMergeOps(bool poolConfig = false, bool withLocators = false) {
  if (!poolConfig) {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOne)
  } else if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(putAtVersionOne11)

  CALL_TASK(getPutAtVersionTwo12)

  CALL_TASK(getPutAtVersionOne13)

  CALL_TASK(getPutAtVersionTwo14)

  for (int i = 0; i < 10; i++) {
    CALL_TASK(getPutAtVersionOne15);
    CALL_TASK(getPutAtVersionTwo16);
  }
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runBasicMergeOps2(bool poolConfig = false, bool withLocators = false) {
  if (!poolConfig) {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOne)
  } else if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(putAtVersionOne21)

  CALL_TASK(getPutAtVersionTwo22)

  for (int i = 0; i < 10; i++) {
    CALL_TASK(getPutAtVersionOne23);
    CALL_TASK(getPutAtVersionTwo24);
  }
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runBasicMergeOps3(bool poolConfig = false, bool withLocators = false) {
  if (!poolConfig) {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOne)
  } else if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(putAtVersionOne31)

  CALL_TASK(getPutAtVersionTwo32)

  for (int i = 0; i < 10; i++) {
    CALL_TASK(getPutAtVersionOne33);
    CALL_TASK(getPutAtVersionTwo34);
  }
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runJavaInteroperableOps(bool poolConfig = false,
                             bool withLocators = false) {
  if (!poolConfig) {
    CALL_TASK(CreateServer2)
    CALL_TASK(StepOne)
  } else if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator2)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer2)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(JavaPutGet)  // c1
  CALL_TASK(JavaGet)     // c2

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void testPolymorphicUseCase(bool poolConfig = false,
                            bool withLocators = false) {
  if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator2)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer2)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(testPutWithMultilevelInheritance)
  CALL_TASK(testGetWithMultilevelInheritance)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runNestedPdxOps(bool poolConfig = false, bool withLocators = false) {
  if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(PutAndVerifyNestedPdxInGet)

  CALL_TASK(VerifyNestedGetOnly)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runPdxInGFSOps(bool poolConfig = false, bool withLocators = false) {
  if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc)
    CALL_TASK(StepTwoPoolLoc)
  } else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP)
    CALL_TASK(StepTwoPoolEP)
  }

  CALL_TASK(PutAndVerifyPdxInGFSInGet)

  CALL_TASK(VerifyPdxInGFSGetOnly)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runPdxIgnoreUnreadFieldTest(bool poolConfig = false,
                                 bool withLocators = false) {
  if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator1)
    CALL_TASK(StepOnePoolLoc_PDX)
    CALL_TASK(StepTwoPoolLoc_PDX)
  } else {
    CALL_TASK(CreateServer1)
    CALL_TASK(StepOnePoolEP_PDX)
    CALL_TASK(StepTwoPoolEP_PDX)
  }

  CALL_TASK(putV2PdxUI)

  CALL_TASK(putV1PdxUI)

  CALL_TASK(getV2PdxUI)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void runPdxBankTest(bool poolConfig = false, bool withLocators = false) {
  if (withLocators) {
    CALL_TASK(StartLocator)
    CALL_TASK(CreateServerWithLocator_PdxMetadataTest)
    CALL_TASK(StepOnePoolLoc_PdxMetadataTest)
    CALL_TASK(StepTwoPoolLoc_PdxMetadataTest)
    CALL_TASK(StepThreePoolLoc_PdxMetadataTest)
  } else {
    CALL_TASK(CreateServer_PdxMetadataTest)
    CALL_TASK(StepOnePoolEP_PdxMetadataTest)
    CALL_TASK(StepTwoPoolEP_PdxMetadataTest)
    CALL_TASK(StepThreePoolEP_PdxMetadataTest)
  }

  CALL_TASK(client1PutsV1Object)  // c1

  CALL_TASK(client2GetsV1ObjectAndPutsV2Object)  // c2

  CALL_TASK(client3GetsV2Object)  // c3

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseCache3)  //

  CALL_TASK(CloseServer)

  if (poolConfig && withLocators) {
    CALL_TASK(CloseLocator)
  }
}

void enableWeakHashMapC1() { CALL_TASK(SetWeakHashMapToTrueC1) }
void enableWeakHashMapC2() { CALL_TASK(SetWeakHashMapToTrueC2) }

void disableWeakHashMapC1() { CALL_TASK(setWeakHashMapToFlaseC1) }
void disableWeakHashMapC2() { CALL_TASK(SetWeakHashMapToFalseC2) }
///
void enableWeakHashMapC1BM() { CALL_TASK(SetWeakHashMapToTrueC1BM) }
void enableWeakHashMapC2BM() { CALL_TASK(SetWeakHashMapToTrueC2BM) }

void disableWeakHashMapC1BM() { CALL_TASK(setWeakHashMapToFlaseC1BM) }
void disableWeakHashMapC2BM() { CALL_TASK(SetWeakHashMapToFalseC2BM) }
//
void enableWeakHashMapC1BM2() { CALL_TASK(SetWeakHashMapToTrueC1BM2) }
void enableWeakHashMapC2BM2() { CALL_TASK(SetWeakHashMapToTrueC2BM2) }

void disableWeakHashMapC1BM2() { CALL_TASK(setWeakHashMapToFlaseC1BM2) }
void disableWeakHashMapC2BM2() { CALL_TASK(SetWeakHashMapToFalseC2BM2) }
//
void enableWeakHashMapC1BM3() { CALL_TASK(SetWeakHashMapToTrueC1BM3) }
void enableWeakHashMapC2BM3() { CALL_TASK(SetWeakHashMapToTrueC2BM3) }

void disableWeakHashMapC1BM3() { CALL_TASK(setWeakHashMapToFlaseC1BM3) }
void disableWeakHashMapC2BM3() { CALL_TASK(SetWeakHashMapToFalseC2BM3) }
/////
void enableWeakHashMapC1BMR1() { CALL_TASK(SetWeakHashMapToTrueC1BMR1) }
void enableWeakHashMapC2BMR1() { CALL_TASK(SetWeakHashMapToTrueC2BMR1) }

void disableWeakHashMapC1BMR1() { CALL_TASK(setWeakHashMapToFlaseC1BMR1) }
void disableWeakHashMapC2BMR1() { CALL_TASK(SetWeakHashMapToFalseC2BMR1) }
///////
void enableWeakHashMapC1BMR2() { CALL_TASK(SetWeakHashMapToTrueC1BMR2) }
void enableWeakHashMapC2BMR2() { CALL_TASK(SetWeakHashMapToTrueC2BMR2) }

void disableWeakHashMapC1BMR2() { CALL_TASK(setWeakHashMapToFlaseC1BMR2) }
void disableWeakHashMapC2BMR2() { CALL_TASK(SetWeakHashMapToFalseC2BMR2) }

DUNIT_MAIN
  {
    // PDXTEST::Test with Pool with EP
    runPdxTestForCharTypes(true);
    runPdxTestForCharTypes(true, true);

    // PUT-GET Test with values of type CacheableObjectArray and PdxType object
    runPdxPutGetTest(true);

    // PdxDistOps-PdxTestsAuto::PdxType PUT/GET Test across clients
    {
      runPdxDistOps(true);
      runPdxDistOps(true, true);
    }

    // BasicMergeOps
    {
      enableWeakHashMapC1BM();
      enableWeakHashMapC2BM();
      runBasicMergeOps(true);  // Only Pool with endpoints, No locator..

      disableWeakHashMapC1BM();
      disableWeakHashMapC2BM();
      runBasicMergeOps(true, true);  // pool with locators
    }

    // BasicMergeOps2
    {
      enableWeakHashMapC1BM2();
      enableWeakHashMapC2BM2();
      runBasicMergeOps2(true);  // pool with server endpoints.

      disableWeakHashMapC1BM2();
      disableWeakHashMapC2BM2();
      runBasicMergeOps2(true, true);  // pool with locators
    }

    // BasicMergeOps3
    {
      enableWeakHashMapC1BM3();
      enableWeakHashMapC2BM3();
      runBasicMergeOps3(true);  // pool with server endpoints

      disableWeakHashMapC1BM3();
      disableWeakHashMapC2BM3();
      runBasicMergeOps3(true, true);  // pool with locators
    }

    // BasicMergeOpsR1
    {
      enableWeakHashMapC1BMR1();
      enableWeakHashMapC2BMR1();
      runBasicMergeOpsR1(true);  // pool with server endpoints

      disableWeakHashMapC1BMR1();
      disableWeakHashMapC2BMR1();
      runBasicMergeOpsR1(true, true);  // pool with locators
    }

    // BasicMergeOpsR2
    {
      enableWeakHashMapC1BMR2();
      enableWeakHashMapC2BMR2();
      runBasicMergeOpsR2(true);  // pool with server endpoints

      disableWeakHashMapC1BMR2();
      disableWeakHashMapC2BMR2();
      runBasicMergeOpsR2(true, true);  // pool with locators
    }

    // JavaInteroperableOps
    {
      runJavaInteroperableOps(true);        // pool with server endpoints
      runJavaInteroperableOps(true, true);  // pool with locators
    }

    ////PDXReaderWriterInvalidUsage
    //{
    //  testReaderWriterInvalidUsage(true);  //pool with server endpoints
    //  testReaderWriterInvalidUsage(true, true);  // pool with locators
    //}

    // NestedPdxOps
    {
      runNestedPdxOps(true);        // pool with server endpoints
      runNestedPdxOps(true, true);  // pool with locators
    }

    // Pdxobject In Gemfire Serializable Ops
    {
      runPdxInGFSOps(true);        // pool with server endpoints
      runPdxInGFSOps(true, true);  // pool with locators
    }
  }
END_MAIN
