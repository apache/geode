/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * testThinClientPdxTests.cpp
 *
 *  Created on: Sep 30, 2011
 *      Author: npatel
 */

//#include "ThinClientPdxTests.hpp"
/*DUNIT_MAIN
{
        //LOG("NIL:DUNIT_MAIN:PDXTests function called");
        runPdxTests(true, false);
}
END_MAIN*/

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include <string>

#define ROOT_NAME "testThinClientPdxTests"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "ThinClientHelper.hpp"
#include "testobject/PdxClassV1.hpp"
#include "testobject/PdxClassV2.hpp"
#include "testobject/VariousPdxTypes.hpp"
#include "testobject/InvalidPdxUsage.hpp"
#include "QueryStrings.hpp"
#include "QueryHelper.hpp"
#include <Utils.hpp>
#include <gfcpp/Query.hpp>
#include <gfcpp/QueryService.hpp>
#include "CachePerfStats.hpp"
#include <LocalRegion.hpp>

using namespace gemfire;
using namespace test;
using namespace testData;
using namespace PdxTests;

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

template <typename T1, typename T2>
bool genericValCompare(T1 value1, T2 value2) /*const*/
{
  if (value1 != value2) return false;
  return true;
}

void initClient(const bool isthinClient, bool isPdxIgnoreUnreadFields,
                const PropertiesPtr& configPtr = NULLPTR) {
  LOGINFO("isPdxIgnoreUnreadFields = %d ", isPdxIgnoreUnreadFields);
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient, isPdxIgnoreUnreadFields, false,
                                  configPtr, false);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

//////////

void initClientN(const bool isthinClient, bool isPdxIgnoreUnreadFields,
                 bool isPdxReadSerialized = false,
                 const PropertiesPtr& configPtr = NULLPTR) {
  LOGINFO("isPdxIgnoreUnreadFields = %d ", isPdxIgnoreUnreadFields);
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient, isPdxIgnoreUnreadFields,
                                  isPdxReadSerialized, configPtr, false);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}

void stepOneN(bool isPdxIgnoreUnreadFields = false,
              bool isPdxReadSerialized = false,
              PropertiesPtr config = NULLPTR) {
  try {
    // Serializable::registerType(Position::createDeserializable);
    // Serializable::registerType(Portfolio::createDeserializable);
  } catch (const IllegalStateException&) {
    // ignore exception
  }
  // Create just one pool and attach all regions to that.
  initClientN(true, isPdxIgnoreUnreadFields, isPdxReadSerialized, config);

  isPoolConfig = true;
  createPool(poolNames[0], locHostPort, NULL, 0, true);
  createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                            false /*Caching disabled*/);

  LOG("StepOne complete.");
}

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc1)
  {
    LOG("Starting Step One with Pool + Locator lists");
    stepOneN(false, true, NULLPTR);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLoc1)
  {
    LOG("Starting Step Two with Pool + Locator");
    stepOneN(false, true, NULLPTR);
  }
END_TASK_DEFINITION
///////////////

void initClient1WithClientName(const bool isthinClient,
                               const PropertiesPtr& configPtr = NULLPTR) {
  if (cacheHelper == NULL) {
    PropertiesPtr config = configPtr;
    if (config == NULLPTR) {
      config = Properties::create();
    }
    config->insert("name", "Client-1");
    cacheHelper = new CacheHelper(isthinClient, config);
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

void stepOneForClient1(bool isPdxIgnoreUnreadFields = false) {
  // Create just one pool and attach all regions to that.
  initClient1WithClientName(true);

  isPoolConfig = true;
  createPool(poolNames[0], locHostPort, NULL, 0, true);
  createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                            false /*Caching disabled*/);
  LOG("StepOne complete.");
}

void stepOneForClient2(bool isPdxIgnoreUnreadFields = false) {
  // Create just one pool and attach all regions to that.
  initClient2WithClientName(true);

  isPoolConfig = true;
  createPool(poolNames[0], locHostPort, NULL, 0, true);
  createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                            false /*Caching disabled*/);
  LOG("StepOne complete.");
}
void stepOne(bool isPdxIgnoreUnreadFields = false,
             PropertiesPtr config = NULLPTR) {
  try {
    // Serializable::registerType(Position::createDeserializable);
    // Serializable::registerType(Portfolio::createDeserializable);
  } catch (const IllegalStateException&) {
    // ignore exception
  }
  // Create just one pool and attach all regions to that.
  initClient(true, isPdxIgnoreUnreadFields, config);
  isPoolConfig = true;
  createPool(poolNames[0], locHostPort, NULL, 0, true);
  createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                            false /*Caching disabled*/);
  LOG("StepOne complete.");
}

void initClient1(bool isPdxIgnoreUnreadFields = false) {
  // Create just one pool and attach all regions to that.
  initClient(true, isPdxIgnoreUnreadFields);
  isPoolConfig = true;
  createPool(poolNames[0], locHostPort, NULL, 0, false);
  createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                            true /*Caching enabled*/);
  LOG("StepOne complete.");
}

void initClient2(bool isPdxIgnoreUnreadFields = false) {
  // Create just one pool and attach all regions to that.
  initClient(true, isPdxIgnoreUnreadFields);
  isPoolConfig = true;
  createPool(poolNames[0], locHostPort, NULL, 0,
             true /*ClientNotification enabled*/);
  createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                            true /*Caching enabled*/);
  LOG("StepOne complete.");
}

void initClient3(bool isPdxIgnoreUnreadFields = false) {
  // Create just one pool and attach all regions to that.
  initClient(true, isPdxIgnoreUnreadFields);
  isPoolConfig = true;
  createPool(poolNames[0], locHostPort, NULL, 0,
             true /*ClientNotification enabled*/);
  createRegionAndAttachPool("DistRegionAck", USE_ACK, poolNames[0],
                            true /*Caching enabled*/);
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
    stepOne(true);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc)
  {
    LOG("Starting Step One with Pool + Locator lists");
    stepOne();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLocSysConfig)
  {
    LOG("Starting Step One with Pool + Locator lists");
    PropertiesPtr config = Properties::create();
    config->insert("on-client-disconnect-clear-pdxType-Ids", "true");
    stepOne(false, config);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLocSysConfig)
  {
    LOG("Starting Step One with Pool + Locator lists");
    PropertiesPtr config = Properties::create();
    config->insert("on-client-disconnect-clear-pdxType-Ids", "true");
    stepOne(false, config);
  }
END_TASK_DEFINITION

// StepOnePoolLoc_PdxMetadataTest
DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLoc_PdxMetadataTest)
  {
    LOG("Starting Step One with Pool + Locator lists");
    initClient1();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer)
  {
    LOG("Starting SERVER1...");
    if (isLocalServer) CacheHelper::initServer(1, "cacheserverPdx.xml");
    LOG("SERVER1 started");
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

DUNIT_TASK_DEFINITION(CLIENT1, StepOnePoolLocBug866)
  {
    LOG("Starting Step One with Pool + Locator lists");
    stepOneForClient1();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLocBug866)
  {
    LOG("Starting Step Two with Pool + Locator");
    stepOneForClient2();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLoc)
  {
    LOG("Starting Step Two with Pool + Locator");
    stepOne();
  }
END_TASK_DEFINITION

// StepTwoPoolLoc_PdxMetadataTest
DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLoc_PdxMetadataTest)
  {
    LOG("Starting Step Two with Pool + Locator");
    initClient2();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, StepThreePoolLoc_PdxMetadataTest)
  {
    LOG("Starting Step Two with Pool + Locator");
    initClient3();
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwoPoolLoc_PDX)
  {
    LOG("Starting Step Two with Pool + Locator");
    stepOne(true);
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

// testPdxWriterAPIsWithInvalidArgs
DUNIT_TASK_DEFINITION(CLIENT1, testPdxWriterAPIsWithInvalidArgs)
  {
    try {
      Serializable::registerPdxType(InvalidPdxUsage::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(
          AddressWithInvalidAPIUsage::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    int expectedExceptionCount = 0;

    // Put operation
    CacheableKeyPtr keyport = CacheableKey::create(1);
    CacheablePtr pdxobj(new PdxTests::InvalidPdxUsage());
    regPtr0->put(keyport, pdxobj);

    // Check the exception count:: expected is 41.
    expectedExceptionCount = (dynCast<PdxTests::InvalidPdxUsagePtr>(pdxobj))
                                 ->gettoDataExceptionCount();
    // LOGINFO("TASK::testPdxWriterAPIsWithInvalidArgs:: toData ExceptionCount
    // ::
    // %d ", expectedExceptionCount);
    ASSERT(expectedExceptionCount == 41,
           "Task testPdxWriterAPIsWithInvalidArgs:Did not get expected "
           "toDataExceptionCount");

    // Get Operation and check fromDataExceptionCount, Expected is 41.
    PdxTests::InvalidPdxUsagePtr obj2 =
        dynCast<PdxTests::InvalidPdxUsagePtr>(regPtr0->get(keyport));
    // LOGINFO("TASK::testPdxWriterAPIsWithInvalidArgs:: fromData ExceptionCOunt
    // :: %d ", obj2->getfromDataExceptionCount());
    expectedExceptionCount = obj2->getfromDataExceptionCount();
    ASSERT(expectedExceptionCount == 41,
           "Task testPdxWriterAPIsWithInvalidArgs:Did not get expected "
           "fromDataExceptionCount");

    LOGINFO("TASK::testPdxWriterAPIsWithInvalidArgs completed Successfully");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, testPdxReaderAPIsWithInvalidArgs)
  {
    try {
      Serializable::registerPdxType(InvalidPdxUsage::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(
          AddressWithInvalidAPIUsage::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    int expectedExceptionCount = 0;
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    // Get Operation. Check fromDataExceptionCount. Expected is 41.
    CacheableKeyPtr keyport1 = CacheableKey::create(1);
    PdxTests::InvalidPdxUsagePtr obj1 =
        dynCast<PdxTests::InvalidPdxUsagePtr>(regPtr0->get(keyport1));

    // Check the exception count:: expected is 41.
    // LOGINFO("TASK::testPdxReaderAPIsWithInvalidArgs:: fromDataExceptionCount
    // ::
    // %d ", obj1->getfromDataExceptionCount());
    expectedExceptionCount = obj1->getfromDataExceptionCount();
    ASSERT(expectedExceptionCount == 41,
           "Task testPdxReaderAPIsWithInvalidArgs:Did not get expected "
           "fromDataExceptionCount");

    LOGINFO("TASK::testPdxReaderAPIsWithInvalidArgs completed Successfully");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testPutWithMultilevelInheritance)
  {
    try {
      Serializable::registerPdxType(PdxTests::Child::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    int expectedExceptionCount ATTR_UNUSED = 0;

    // Put operation
    CacheableKeyPtr keyport = CacheableKey::create(1);
    CacheablePtr pdxobj(new PdxTests::Child());
    regPtr0->put(keyport, pdxobj);
    LOGINFO("TASK::testPutWithMultilevelInheritance:: Put successful");

    // Get Operation and check fromDataExceptionCount, Expected is 41.
    PdxTests::ChildPtr obj2 =
        dynCast<PdxTests::ChildPtr>(regPtr0->get(keyport));
    // LOGINFO("Task: testPutWithMultilevelInheritance: got members :: %d %d %d
    // %d
    // %d %d ", obj2->getMember_a(), obj2->getMember_b(), obj2->getMember_c(),
    // obj2->getMember_d(), obj2->getMember_e(), obj2->getMember_f());
    bool isEqual = (dynCast<PdxTests::ChildPtr>(pdxobj))->equals(obj2);
    LOGINFO("testPutWithMultilevelInheritance:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true, "Objects of type class Child should be equal");

    LOGINFO("TASK::testPutWithMultilevelInheritance:: Get successful");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, testGetWithMultilevelInheritance)
  {
    try {
      Serializable::registerPdxType(PdxTests::Child::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport1 = CacheableKey::create(1);
    PdxTests::ChildPtr obj1 =
        dynCast<PdxTests::ChildPtr>(regPtr0->get(keyport1));

    CacheablePtr pdxobj(new PdxTests::Child());
    bool isEqual = (dynCast<PdxTests::ChildPtr>(pdxobj))->equals(obj1);
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

// Added for the LinkedList testcase

DUNIT_TASK_DEFINITION(CLIENT1, JavaPutGet1)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    CacheableInt32Ptr valPtr = CacheableInt32::create(123);
    regPtr0->put(keyport, valPtr);

    CacheableInt32Ptr getVal =
        dynCast<CacheableInt32Ptr>(regPtr0->get(keyport));

    CacheableBooleanPtr boolPtr =
        dynCast<CacheableBooleanPtr>(regPtr0->get("success"));

    bool isEqual = boolPtr.ptr()->value();
    ASSERT(isEqual == true,
           "Task JavaPutGet:Objects of type PdxType should be equal");

    LOGINFO("Task:JavaPutGet PDX-ON read-serialized = %d",
            cacheHelper->getCache()->getPdxReadSerialized());
    PdxInstancePtr jsonDoc = dynCast<PdxInstancePtr>(regPtr0->get("jsondoc1"));
    CacheableStringPtr toString = jsonDoc->toString();
    LOGINFO("Task:JavaPutGet: Result = %s ", toString->asChar());
    /*
    int16_t age = 0;
    jsonDoc->getField("age", age);

    char* stringVal = NULL;
    jsonDoc->getField("firstName", &stringVal);

    char* stringVal1 = NULL;
    jsonDoc->getField("lastName", &stringVal1);
    */

    CacheablePtr object2 = NULLPTR;
    jsonDoc->getField("kids", object2);
    CacheableLinkedListPtr listPtr = dynCast<CacheableLinkedListPtr>(object2);
    LOGINFO("Task:JavaPutGet: list size = %d", listPtr->size());

    CacheableLinkedListPtr m_linkedlist = CacheableLinkedList::create();
    m_linkedlist->push_back(CacheableString::create("Manan"));
    m_linkedlist->push_back(CacheableString::create("Nishka"));

    ASSERT(genericValCompare(m_linkedlist->size(), listPtr->size()) == true,
           "LinkedList size should be equal");
    for (int j = 0; j < m_linkedlist->size(); j++) {
      genericValCompare(m_linkedlist->at(j), listPtr->at(j));
    }

    LOGINFO("Task:JavaPutGet Tese-cases completed successfully!");
  }
END_TASK_DEFINITION

// END

DUNIT_TASK_DEFINITION(CLIENT1, JavaPutGet)
  {
    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    CacheablePtr pdxobj(new PdxTests::PdxType());
    regPtr0->put(keyport, pdxobj);

    PdxTests::PdxTypePtr obj2 =
        dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyport));

    CacheableBooleanPtr boolPtr =
        dynCast<CacheableBooleanPtr>(regPtr0->get("success"));
    bool isEqual = boolPtr.ptr()->value();
    LOGDEBUG("Task:JavaPutGet: isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Task JavaPutGet:Objects of type PdxType should be equal");
    /*
    LOGINFO("NILKATH JavaPutGet new test.");
    dynamic_cast<CacheImpl*>(cacheHelper->getCache().ptr())->setPdxReadSerialized(true);
    LOGINFO("NILKATH JavaPutGet PDX-ON read-serialized = %d",
    cacheHelper->getCache()->getPdxReadSerialized());
    PdxInstancePtr jsonDoc = dynCast<PdxInstancePtr>(regPtr0->get("jsondoc1"));
    int age = 0;
    jsonDoc->getField("age", age);
    LOGINFO("NILKATH:: Task:JavaPutGet: age = %d", age);

    dynamic_cast<CacheImpl*>(cacheHelper->getCache().ptr())->setPdxReadSerialized(false);
    LOGINFO("NILKATH JavaPutGet PDX-OFF read-serialized = %d",
    cacheHelper->getCache()->getPdxReadSerialized());
    */
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, JavaGet)
  {
    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    LOGDEBUG("JavaGet-1 Line_309");
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport1 = CacheableKey::create(1);
    CacheablePtr pdxobj(new PdxTests::PdxType());
    LOGDEBUG("JavaGet-2 Line_314");
    PdxTests::PdxTypePtr obj1 =
        dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyport1));
    LOGDEBUG("JavaGet-3 Line_316");
    CacheableKeyPtr keyport2 = CacheableKey::create("putFromjava");
    LOGDEBUG("JavaGet-4 Line_316");
    PdxTests::PdxTypePtr obj2 =
        dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyport2));
    LOGDEBUG("JavaGet-5 Line_320");
  }
END_TASK_DEFINITION
/***************************************************************/
DUNIT_TASK_DEFINITION(CLIENT2, putAtVersionTwoR21)
  {
    try {
      Serializable::registerPdxType(
          PdxTests::PdxTypesR2V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTypesR2V2Ptr np(new PdxTypesR2V2());

    regPtr0->put(keyport, np);

    PdxTypesR2V2Ptr pRet = dynCast<PdxTypesR2V2Ptr>(regPtr0->get(keyport));

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
          PdxTests::PdxTypesV1R2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTypesV1R2Ptr np(new PdxTypesV1R2());

    PdxTypesV1R2Ptr pRet = dynCast<PdxTypesV1R2Ptr>(regPtr0->get(keyport));

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

    PdxTypesR2V2Ptr np(new PdxTypesR2V2());

    PdxTypesR2V2Ptr pRet = dynCast<PdxTypesR2V2Ptr>(regPtr0->get(keyport));

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
    PdxTypesV1R2Ptr np(new PdxTypesV1R2());

    PdxTypesV1R2Ptr pRet = dynCast<PdxTypesV1R2Ptr>(regPtr0->get(keyport));

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
      Serializable::registerPdxType(PdxTests::PdxType3V1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxType3V1Ptr np(new PdxType3V1());

    regPtr0->put(keyport, np);

    PdxType3V1Ptr pRet = dynCast<PdxType3V1Ptr>(regPtr0->get(keyport));

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
          PdxTests::PdxTypes3V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTypes3V2Ptr np(new PdxTypes3V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTypes3V2Ptr pRet = dynCast<PdxTypes3V2Ptr>(regPtr0->get(keyport));
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
    PdxType3V1Ptr np(new PdxType3V1());

    PdxType3V1Ptr pRet = dynCast<PdxType3V1Ptr>(regPtr0->get(keyport));

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

    PdxTypes3V2Ptr np(new PdxTypes3V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTypes3V2Ptr pRet = dynCast<PdxTypes3V2Ptr>(regPtr0->get(keyport));

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
      Serializable::registerPdxType(PdxTests::PdxType2V1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxType2V1Ptr np(new PdxType2V1());

    regPtr0->put(keyport, np);

    PdxType2V1Ptr pRet = dynCast<PdxType2V1Ptr>(regPtr0->get(keyport));

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
          PdxTests::PdxTypes2V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTypes2V2Ptr np(new PdxTypes2V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTypes2V2Ptr pRet = dynCast<PdxTypes2V2Ptr>(regPtr0->get(keyport));
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

    PdxType2V1Ptr np(new PdxType2V1());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxType2V1Ptr pRet = dynCast<PdxType2V1Ptr>(regPtr0->get(keyport));

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
    PdxTypes2V2Ptr np(new PdxTypes2V2());
    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTypes2V2Ptr pRet = dynCast<PdxTypes2V2Ptr>(regPtr0->get(keyport));

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
      Serializable::registerPdxType(PdxTests::PdxType1V1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTests::PdxType1V1Ptr np(new PdxTests::PdxType1V1());

    regPtr0->put(keyport, np);

    PdxTests::PdxType1V1Ptr pRet =
        dynCast<PdxTests::PdxType1V1Ptr>(regPtr0->get(keyport));

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
          PdxTests::PdxTypesR1V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    PdxTests::PdxTypesR1V2::reset(false);

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTypesR1V2Ptr np(new PdxTypesR1V2());

    regPtr0->put(keyport, np);

    PdxTypesR1V2Ptr pRet = dynCast<PdxTypesR1V2Ptr>(regPtr0->get(keyport));

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
          PdxTests::PdxTypesV1R1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    PdxTypesV1R1Ptr np(new PdxTypesV1R1());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTypesV1R1Ptr pRet = dynCast<PdxTypesV1R1Ptr>(regPtr0->get(keyport));

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
    PdxTypesR1V2Ptr np(new PdxTypesR1V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTypesR1V2Ptr pRet = dynCast<PdxTypesR1V2Ptr>(regPtr0->get(keyport));
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
    PdxTypesV1R1Ptr np(new PdxTypesV1R1());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTypesV1R1Ptr pRet = dynCast<PdxTypesV1R1Ptr>(regPtr0->get(keyport));

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
    PdxTypesR1V2Ptr np(new PdxTypesR1V2());

    // GET
    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTypesR1V2Ptr pRet = dynCast<PdxTypesR1V2Ptr>(regPtr0->get(keyport));

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
    PdxTypesV1R1Ptr np(new PdxTypesV1R1());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTypesV1R1Ptr pRet = dynCast<PdxTypesV1R1Ptr>(regPtr0->get(keyport));

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
          PdxTests::PdxTypesIgnoreUnreadFieldsV2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    // PdxTests::PdxTypesIgnoreUnreadFieldsV2::reset(false);

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTypesIgnoreUnreadFieldsV2Ptr np(new PdxTypesIgnoreUnreadFieldsV2());
    CacheableKeyPtr keyport = CacheableKey::create(1);
    regPtr0->put(keyport, np);

    PdxTypesIgnoreUnreadFieldsV2Ptr pRet =
        dynCast<PdxTypesIgnoreUnreadFieldsV2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("NIL:putV2PdxUI:.. isEqual = %d", isEqual);
    ASSERT(isEqual == true,
           "Objects of type PdxTypesIgnoreUnreadFieldsV2 should be equal at "
           "putV2PdxUI ");

    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putV1PdxUI)
  {
    try {
      Serializable::registerPdxType(
          PdxTests::PdxTypesIgnoreUnreadFieldsV1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    // PdxTests::PdxTypesIgnoreUnreadFieldsV1::reset(false);
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTypesIgnoreUnreadFieldsV1Ptr pRet =
        dynCast<PdxTypesIgnoreUnreadFieldsV1Ptr>(regPtr0->get(keyport));
    regPtr0->put(keyport, pRet);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getV2PdxUI)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTypesIgnoreUnreadFieldsV2Ptr np(new PdxTypesIgnoreUnreadFieldsV2());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTypesIgnoreUnreadFieldsV2Ptr pRet =
        dynCast<PdxTypesIgnoreUnreadFieldsV2Ptr>(regPtr0->get(keyport));

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
          PdxTests::PdxTypes1V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    PdxTypes1V2Ptr np(new PdxTypes1V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxTypes1V2Ptr pRet = dynCast<PdxTypes1V2Ptr>(regPtr0->get(keyport));

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
    PdxType1V1Ptr np(new PdxType1V1());

    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxType1V1Ptr pRet = dynCast<PdxType1V1Ptr>(regPtr0->get(keyport));
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
    PdxTypes1V2Ptr np(new PdxTypes1V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTypes1V2Ptr pRet = dynCast<PdxTypes1V2Ptr>(regPtr0->get(keyport));

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
    PdxType1V1Ptr np(new PdxType1V1());

    // GET
    CacheableKeyPtr keyport = CacheableKey::create(1);

    PdxType1V1Ptr pRet = dynCast<PdxType1V1Ptr>(regPtr0->get(keyport));

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
    if (m_useWeakHashMap == false) {
      ASSERT(TestUtils::testNumberOfPreservedData() == 0,
             "testNumberOfPreservedData should be zero at Line_288");
    } else {
      ASSERT(
          TestUtils::testNumberOfPreservedData() > 0,
          "testNumberOfPreservedData should be Greater than zero at Line_292");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getPutAtVersionTwo16)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxTypes1V2Ptr np(new PdxTypes1V2());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTypes1V2Ptr pRet = dynCast<PdxTypes1V2Ptr>(regPtr0->get(keyport));

    bool isEqual = np->equals(pRet);
    LOGDEBUG("NIL:getPutAtVersionTwo14:.. isEqual = %d", isEqual);
    ASSERT(
        isEqual == true,
        "Objects of type PdxTypes1V2 should be equal at getPutAtVersionTwo14");

    regPtr0->put(keyport, pRet);

    if (m_useWeakHashMap == false) {
      ASSERT(TestUtils::testNumberOfPreservedData() == 0,
             "getPutAtVersionTwo16:testNumberOfPreservedData should be zero");
    } else {
      // it has extra fields, so no need to preserve data
      ASSERT(TestUtils::testNumberOfPreservedData() == 0,
             "getPutAtVersionTwo16:testNumberOfPreservedData should be zero");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Puts2)
  {
    try {
      Serializable::registerPdxType(PdxTests::PdxTypes1::createDeserializable);
      Serializable::registerPdxType(PdxTests::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);

    CacheablePtr pdxobj(new PdxTests::PdxTypes1());

    regPtr0->put(keyport, pdxobj);

    CacheableKeyPtr keyport2 = CacheableKey::create(2);

    CacheablePtr pdxobj2(new PdxTests::PdxTypes2());

    regPtr0->put(keyport2, pdxobj2);

    // ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes()
    // ==
    // lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes(),
    //"Total pdxDeserializationBytes should be equal to Total
    // pdxSerializationsBytes.");

    LOG("Stepone two puts complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, forCleanup)
  {
    LOGINFO("Do put to clean the pdxtype registry");
    try {
      RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

      CacheableKeyPtr keyport = CacheableKey::create(1);

      CacheablePtr pdxobj(new PdxTests::PdxTypes1());

      regPtr0->put(keyport, pdxobj);
    } catch (...) {
      // ignore
    }
    LOGINFO("Wake up");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, Puts22)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);

    CacheablePtr pdxobj(new PdxTests::PdxTypes1());

    regPtr0->put(keyport, pdxobj);

    CacheableKeyPtr keyport2 = CacheableKey::create(2);

    CacheablePtr pdxobj2(new PdxTests::PdxTypes2());

    regPtr0->put(keyport2, pdxobj2);

    // ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxSerializationBytes()
    // ==
    // lregPtr->getCacheImpl()->m_cacheStats->getPdxDeSerializationBytes(),
    //"Total pdxDeserializationBytes should be equal to Total
    // pdxSerializationsBytes.");

    LOG("Puts22 complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, Get2)
  {
    try {
      Serializable::registerPdxType(PdxTests::PdxTypes1::createDeserializable);
      Serializable::registerPdxType(PdxTests::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(2);
    PdxTests::PdxTypes2Ptr obj2 =
        dynCast<PdxTests::PdxTypes2Ptr>(regPtr0->get(keyport));

    LOG("Get2 complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAndVerifyPdxInGet)
  {
    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);

    CacheablePtr pdxobj(new PdxTests::PdxType());

    regPtr0->put(keyport, pdxobj);

    PdxTests::PdxTypePtr obj2 =
        dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyport));

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
      Serializable::registerPdxType(NestedPdx::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTests::PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTests::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    NestedPdxPtr p1(new NestedPdx());
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);

    regPtr0->put(keyport, p1);

    NestedPdxPtr obj2 = dynCast<NestedPdxPtr>(regPtr0->get(keyport));

    ASSERT(obj2->equals(p1) == true, "Nested pdx objects should be equal");

    LOG("PutAndVerifyNestedPdxInGet complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutMixedVersionNestedPdx)
  {
    LOG("PutMixedVersionNestedPdx started.");

    try {
      Serializable::registerPdxType(
          MixedVersionNestedPdx::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTests::PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTests::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    LOG("MixedVersionNestedPdxPtr p1(new MixedVersionNestedPdx());	");
    MixedVersionNestedPdxPtr p1(new MixedVersionNestedPdx());
    MixedVersionNestedPdxPtr p2(new MixedVersionNestedPdx());
    MixedVersionNestedPdxPtr p3(new MixedVersionNestedPdx());
    LOG("RegionPtr regPtr0 = getHelper()->getRegion(\"DistRegionAck\");");
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    LOG("CacheableKeyPtr keyport1 = CacheableKey::create(1);");
    CacheableKeyPtr keyport1 = CacheableKey::create(1);
    CacheableKeyPtr keyport2 = CacheableKey::create(2);
    CacheableKeyPtr keyport3 = CacheableKey::create(3);

    LOG("regPtr0->put(keyport1, p1 );");
    regPtr0->put(keyport1, p1);
    LOG("regPtr0->put(keyport2, p2 );");
    regPtr0->put(keyport2, p2);
    LOG("regPtr0->put(keyport3, p3 );");
    regPtr0->put(keyport3, p3);
    LOG("PutMixedVersionNestedPdx complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAndVerifyPdxInGFSInGet)
  {
    try {
      Serializable::registerType(
          PdxInsideIGFSerializable::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(NestedPdx::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes3::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes4::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes5::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes6::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes7::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes8::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxInsideIGFSerializablePtr np(new PdxInsideIGFSerializable());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    regPtr0->put(keyport, np);

    // GET
    PdxInsideIGFSerializablePtr pRet =
        dynCast<PdxInsideIGFSerializablePtr>(regPtr0->get(keyport));
    ASSERT(pRet->equals(np) == true,
           "TASK PutAndVerifyPdxInIGFSInGet: PdxInsideIGFSerializable objects "
           "should be equal");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyPdxInGFSGetOnly)
  {
    try {
      Serializable::registerType(
          PdxInsideIGFSerializable::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(NestedPdx::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes3::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes4::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes5::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes6::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes7::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes8::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    PdxInsideIGFSerializablePtr orig(new PdxInsideIGFSerializable());

    // GET
    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxInsideIGFSerializablePtr pRet =
        dynCast<PdxInsideIGFSerializablePtr>(regPtr0->get(keyport));
    ASSERT(
        pRet->equals(orig) == true,
        "TASK:VerifyPdxInIGFSGetOnly, PdxInsideIGFSerializable objects should "
        "be equal");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyMixedVersionNestedGetOnly)
  {
    LOG("VerifyMixedVersionNestedGetOnly started.");

    try {
      Serializable::registerPdxType(
          MixedVersionNestedPdx::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTests::PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTests::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    MixedVersionNestedPdxPtr p1(new MixedVersionNestedPdx());
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport1 = CacheableKey::create(1);
    CacheableKeyPtr keyport2 = CacheableKey::create(2);
    CacheableKeyPtr keyport3 = CacheableKey::create(3);

    MixedVersionNestedPdxPtr obj1 =
        dynCast<MixedVersionNestedPdxPtr>(regPtr0->get(keyport1));
    MixedVersionNestedPdxPtr obj2 =
        dynCast<MixedVersionNestedPdxPtr>(regPtr0->get(keyport2));
    MixedVersionNestedPdxPtr obj3 =
        dynCast<MixedVersionNestedPdxPtr>(regPtr0->get(keyport3));

    ASSERT(obj1->equals(p1) == true, "Nested pdx objects should be equal");

    LOG("VerifyMixedVersionNestedGetOnly complete.");
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
      Serializable::registerPdxType(PdxTests::PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTests::PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    NestedPdxPtr p1(new NestedPdx());
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);

    NestedPdxPtr obj2 = dynCast<NestedPdxPtr>(regPtr0->get(keyport));

    ASSERT(obj2->equals(p1) == true, "Nested pdx objects should be equal");

    LOG("VerifyNestedGetOnly complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, VerifyGetOnly)
  {
    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTests::PdxTypePtr obj2 =
        dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyport));

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
      Serializable::registerPdxType(PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes3::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes4::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes5::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes6::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes7::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(PdxTypes8::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes9::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes10::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    // TODO
    // Serializable::registerPdxType(PdxTests.PortfolioPdx.CreateDeserializable);
    // Serializable::registerPdxType(PdxTests.PositionPdx.CreateDeserializable);

    // Region region0 = CacheHelper.GetVerifyRegion<object,
    // object>(m_regionNames[0]);
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    bool flag = false;
    {
      PdxTypes1Ptr p1(new PdxTypes1());
      CacheableKeyPtr keyport = CacheableKey::create(11);
      regPtr0->put(keyport, p1);
      PdxTypes1Ptr pRet = dynCast<PdxTypes1Ptr>(regPtr0->get(keyport));

      flag = p1->equals(pRet);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true, "Objects of type PdxTypes1 should be equal");
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
      PdxTypes2Ptr p2(new PdxTypes2());
      CacheableKeyPtr keyport2 = CacheableKey::create(12);
      regPtr0->put(keyport2, p2);
      PdxTypes2Ptr pRet2 = dynCast<PdxTypes2Ptr>(regPtr0->get(keyport2));

      flag = p2->equals(pRet2);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true, "Objects of type PdxTypes2 should be equal");
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
      PdxTypes3Ptr p3(new PdxTypes3());
      CacheableKeyPtr keyport3 = CacheableKey::create(13);
      regPtr0->put(keyport3, p3);
      PdxTypes3Ptr pRet3 = dynCast<PdxTypes3Ptr>(regPtr0->get(keyport3));

      flag = p3->equals(pRet3);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true, "Objects of type PdxTypes3 should be equal");
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
      PdxTypes4Ptr p4(new PdxTypes4());
      CacheableKeyPtr keyport4 = CacheableKey::create(14);
      regPtr0->put(keyport4, p4);
      PdxTypes4Ptr pRet4 = dynCast<PdxTypes4Ptr>(regPtr0->get(keyport4));

      flag = p4->equals(pRet4);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true, "Objects of type PdxTypes4 should be equal");
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
      PdxTypes5Ptr p5(new PdxTypes5());
      CacheableKeyPtr keyport5 = CacheableKey::create(15);
      regPtr0->put(keyport5, p5);
      PdxTypes5Ptr pRet5 = dynCast<PdxTypes5Ptr>(regPtr0->get(keyport5));

      flag = p5->equals(pRet5);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true, "Objects of type PdxTypes5 should be equal");
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
      PdxTypes6Ptr p6(new PdxTypes6());
      CacheableKeyPtr keyport6 = CacheableKey::create(16);
      regPtr0->put(keyport6, p6);
      PdxTypes6Ptr pRet6 = dynCast<PdxTypes6Ptr>(regPtr0->get(keyport6));

      flag = p6->equals(pRet6);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true, "Objects of type PdxTypes6 should be equal");
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
      PdxTypes7Ptr p7(new PdxTypes7());
      CacheableKeyPtr keyport7 = CacheableKey::create(17);
      regPtr0->put(keyport7, p7);
      PdxTypes7Ptr pRet7 = dynCast<PdxTypes7Ptr>(regPtr0->get(keyport7));

      flag = p7->equals(pRet7);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true, "Objects of type PdxTypes7 should be equal");
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
      PdxTypes8Ptr p8(new PdxTypes8());
      CacheableKeyPtr keyport8 = CacheableKey::create(18);
      regPtr0->put(keyport8, p8);
      PdxTypes8Ptr pRet8 = dynCast<PdxTypes8Ptr>(regPtr0->get(keyport8));

      flag = p8->equals(pRet8);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true, "Objects of type PdxTypes8 should be equal");
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
      PdxTypes9Ptr p9(new PdxTypes9());
      CacheableKeyPtr keyport9 = CacheableKey::create(19);
      regPtr0->put(keyport9, p9);
      PdxTypes9Ptr pRet9 = dynCast<PdxTypes9Ptr>(regPtr0->get(keyport9));

      flag = p9->equals(pRet9);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true, "Objects of type PdxTypes9 should be equal");
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
      PdxTypes10Ptr p10(new PdxTypes10());
      CacheableKeyPtr keyport10 = CacheableKey::create(20);
      regPtr0->put(keyport10, p10);
      PdxTypes10Ptr pRet10 = dynCast<PdxTypes10Ptr>(regPtr0->get(keyport10));

      flag = p10->equals(pRet10);
      LOGDEBUG("PutAndVerifyVariousPdxTypes:.. flag = %d", flag);
      ASSERT(flag == true, "Objects of type PdxTypes10 should be equal");
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

// TestCase-1
// C1.generateJavaPdxType
DUNIT_TASK_DEFINITION(CLIENT1, generateJavaPdxType)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    CacheablePtr args = CacheableKey::create("saveAllJavaPdxTypes");
    CacheableKeyPtr key = CacheableKey::create(1);
    CacheableVectorPtr routingObj = CacheableVector::create();
    routingObj->push_back(key);

    ExecutionPtr funcExec = FunctionService::onRegion(regPtr0);

    ResultCollectorPtr collector = funcExec->withArgs(args)
                                       ->withFilter(routingObj)
                                       ->execute("ComparePdxTypes", true);
    ASSERT(collector != NULLPTR, "onRegion collector NULL");

    CacheableVectorPtr result = collector->getResult();
    LOGINFO("NIL:: testTCPDXTests: result->size = %d ", result->size());
    if (result == NULLPTR) {
      ASSERT(false, "echo String : result is NULL");
    } else {
      //
      bool gotResult = false;
      for (int i = 0; i < result->size(); i++) {
        try {
          CacheableBooleanPtr boolValue =
              dynCast<CacheableBooleanPtr>(result->operator[](i));
          LOGINFO("NIL:: boolValue is %d ", boolValue->value());
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
      //
    }
  }
END_TASK_DEFINITION
// C1.putAllPdxTypes
DUNIT_TASK_DEFINITION(CLIENT1, putAllPdxTypes)
  {
    try {
      Serializable::registerPdxType(PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes3::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes4::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes5::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes6::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes7::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes8::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes9::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes10::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    // TODO::Uncomment it once PortfolioPdx/PositionPdx Classes are ready
    // Serializable::registerPdxType(PdxTests.PortfolioPdx.CreateDeserializable);
    // Serializable::registerPdxType(PdxTests.PositionPdx.CreateDeserializable);

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    PdxTypes1Ptr p1(new PdxTypes1());
    CacheableKeyPtr keyport1 = CacheableKey::create(p1->getClassName());
    regPtr0->put(keyport1, p1);

    PdxTypes2Ptr p2(new PdxTypes2());
    CacheableKeyPtr keyport2 = CacheableKey::create(p2->getClassName());
    regPtr0->put(keyport2, p2);

    PdxTypes3Ptr p3(new PdxTypes3());
    CacheableKeyPtr keyport3 = CacheableKey::create(p3->getClassName());
    regPtr0->put(keyport3, p3);

    PdxTypes4Ptr p4(new PdxTypes4());
    CacheableKeyPtr keyport4 = CacheableKey::create(p4->getClassName());
    regPtr0->put(keyport4, p4);

    PdxTypes5Ptr p5(new PdxTypes5());
    CacheableKeyPtr keyport5 = CacheableKey::create(p5->getClassName());
    regPtr0->put(keyport5, p5);

    PdxTypes6Ptr p6(new PdxTypes6());
    CacheableKeyPtr keyport6 = CacheableKey::create(p6->getClassName());
    regPtr0->put(keyport6, p6);

    PdxTypes7Ptr p7(new PdxTypes7());
    CacheableKeyPtr keyport7 = CacheableKey::create(p7->getClassName());
    regPtr0->put(keyport7, p7);

    PdxTypes8Ptr p8(new PdxTypes8());
    CacheableKeyPtr keyport8 = CacheableKey::create(p8->getClassName());
    regPtr0->put(keyport8, p8);

    PdxTypes9Ptr p9(new PdxTypes9());
    CacheableKeyPtr keyport9 = CacheableKey::create(p9->getClassName());
    regPtr0->put(keyport9, p9);

    PdxTypes10Ptr p10(new PdxTypes10());
    CacheableKeyPtr keyport10 = CacheableKey::create(p10->getClassName());
    regPtr0->put(keyport10, p10);

    //
  }
END_TASK_DEFINITION

// C1.verifyDotNetPdxTypes
DUNIT_TASK_DEFINITION(CLIENT1, verifyDotNetPdxTypes)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    CacheablePtr args = CacheableKey::create("compareDotNETPdxTypes");
    CacheableKeyPtr key = CacheableKey::create(1);
    CacheableVectorPtr routingObj = CacheableVector::create();
    routingObj->push_back(key);

    ExecutionPtr funcExec = FunctionService::onRegion(regPtr0);

    ResultCollectorPtr collector = funcExec->withArgs(args)
                                       ->withFilter(routingObj)
                                       ->execute("ComparePdxTypes", true);
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
// END TestCase-1

// TestCase-2
// c1.client1PutsV1Object
DUNIT_TASK_DEFINITION(CLIENT1, client1PutsV1Object)
  {
    try {
      Serializable::registerPdxType(PdxTests::PdxType3V1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    PdxTests::PdxType3V1::reset(false);
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxType3V1Ptr np(new PdxType3V1());

    regPtr0->put(keyport, np);
  }
END_TASK_DEFINITION
// c2.client2GetsV1ObjectAndPutsV2Object
DUNIT_TASK_DEFINITION(CLIENT2, client2GetsV1ObjectAndPutsV2Object)
  {
    try {
      Serializable::registerPdxType(
          PdxTests::PdxTypes3V2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    PdxTests::PdxTypes3V2::reset(false);
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    // get v1 object
    CacheableKeyPtr keyport = CacheableKey::create(1);
    PdxTypes3V2Ptr pRet = dynCast<PdxTypes3V2Ptr>(regPtr0->get(keyport));

    // now put v2 object
    PdxTypes3V2Ptr np(new PdxTypes3V2());
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
      Serializable::registerPdxType(PdxTypes1::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes2::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes3::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes4::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes5::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes6::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes7::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes8::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes9::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    try {
      Serializable::registerPdxType(PdxTypes10::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }
    // TODO::Uncomment it once PortfolioPdx/PositionPdx Classes are ready
    // Serializable::registerPdxType(PdxTests.PortfolioPdx.CreateDeserializable);
    // Serializable::registerPdxType(PdxTests.PositionPdx.CreateDeserializable);

    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");
    bool flag = false;
    {
      PdxTypes1Ptr p1(new PdxTypes1());
      CacheableKeyPtr keyport = CacheableKey::create(11);
      PdxTypes1Ptr pRet = dynCast<PdxTypes1Ptr>(regPtr0->get(keyport));

      flag = p1->equals(pRet);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTypes1 should be equal");
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
      PdxTypes2Ptr p2(new PdxTypes2());
      CacheableKeyPtr keyport2 = CacheableKey::create(12);
      PdxTypes2Ptr pRet2 = dynCast<PdxTypes2Ptr>(regPtr0->get(keyport2));

      flag = p2->equals(pRet2);
      LOGDEBUG("VerifyVariousPdxGets:. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTypes2 should be equal");
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
      PdxTypes3Ptr p3(new PdxTypes3());
      CacheableKeyPtr keyport3 = CacheableKey::create(13);
      PdxTypes3Ptr pRet3 = dynCast<PdxTypes3Ptr>(regPtr0->get(keyport3));

      flag = p3->equals(pRet3);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTypes3 should be equal");
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
      PdxTypes4Ptr p4(new PdxTypes4());
      CacheableKeyPtr keyport4 = CacheableKey::create(14);
      PdxTypes4Ptr pRet4 = dynCast<PdxTypes4Ptr>(regPtr0->get(keyport4));

      flag = p4->equals(pRet4);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTypes4 should be equal");
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
      PdxTypes5Ptr p5(new PdxTypes5());
      CacheableKeyPtr keyport5 = CacheableKey::create(15);
      PdxTypes5Ptr pRet5 = dynCast<PdxTypes5Ptr>(regPtr0->get(keyport5));

      flag = p5->equals(pRet5);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTypes5 should be equal");
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
      PdxTypes6Ptr p6(new PdxTypes6());
      CacheableKeyPtr keyport6 = CacheableKey::create(16);
      PdxTypes6Ptr pRet6 = dynCast<PdxTypes6Ptr>(regPtr0->get(keyport6));

      flag = p6->equals(pRet6);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTypes6 should be equal");
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
      PdxTypes7Ptr p7(new PdxTypes7());
      CacheableKeyPtr keyport7 = CacheableKey::create(17);
      PdxTypes7Ptr pRet7 = dynCast<PdxTypes7Ptr>(regPtr0->get(keyport7));

      flag = p7->equals(pRet7);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTypes7 should be equal");
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
      PdxTypes8Ptr p8(new PdxTypes8());
      CacheableKeyPtr keyport8 = CacheableKey::create(18);
      PdxTypes8Ptr pRet8 = dynCast<PdxTypes8Ptr>(regPtr0->get(keyport8));

      flag = p8->equals(pRet8);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTypes8 should be equal");
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
      PdxTypes9Ptr p9(new PdxTypes9());
      CacheableKeyPtr keyport9 = CacheableKey::create(19);
      PdxTypes9Ptr pRet9 = dynCast<PdxTypes9Ptr>(regPtr0->get(keyport9));

      flag = p9->equals(pRet9);
      LOGDEBUG("VerifyVariousPdxGets:. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTypes9 should be equal");
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
      PdxTypes10Ptr p10(new PdxTypes10());
      CacheableKeyPtr keyport10 = CacheableKey::create(20);
      PdxTypes10Ptr pRet10 = dynCast<PdxTypes10Ptr>(regPtr0->get(keyport10));

      flag = p10->equals(pRet10);
      LOGDEBUG("VerifyVariousPdxGets:.. flag = %d", flag);
      ASSERT(flag == true,
             "VerifyVariousPdxGets:Objects of type PdxTypes10 should be equal");
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

DUNIT_TASK_DEFINITION(CLIENT1, putOperation)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    regPtr0->put(1, 1);

    // Verify the CLientName.::putOperation
    // RegionPtr testReg = getHelper()->getRegion("testregion");
    CacheablePtr valuePtr1 = regPtr0->get("clientName1");
    const char* clientName1 =
        (dynCast<CacheableStringPtr>(valuePtr1))->asChar();
    LOGINFO(" C1.putOperation Got ClientName1 = %s ", clientName1);
    ASSERT(strcmp(clientName1, "Client-1") == 0,
           "ClientName for Client-1 is not set");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getOperation)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    CacheableKeyPtr keyport = CacheableKey::create(1);
    CacheablePtr value = regPtr0->get(keyport);

    // Verify Client Name for C2
    CacheablePtr valuePtr2 = regPtr0->get("clientName2");
    const char* clientName2 =
        (dynCast<CacheableStringPtr>(valuePtr2))->asChar();
    LOGINFO(" C2.getOperation Got ClientName2 = %s ", clientName2);
    ASSERT(strcmp(clientName2, "Client-2") == 0,
           "ClientName for Client-2 is not set");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putCharTypes)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    try {
      Serializable::registerPdxType(PdxTests::CharTypes::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    LOG("PdxTests::CharTypes Registered Successfully....");

    LOG("Trying to populate PDX objects.....\n");
    CacheablePtr pdxobj(new PdxTests::CharTypes());
    CacheableKeyPtr keyport = CacheableKey::create(1);

    // PUT Operation
    regPtr0->put(keyport, pdxobj);
    LOG("PdxTests::CharTypes: PUT Done successfully....");

    // locally destroy PdxTests::PdxType
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
      Serializable::registerPdxType(PdxTests::CharTypes::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    PdxTests::CharTypesPtr localPdxptr(new PdxTests::CharTypes());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    LOG("Client-2 PdxTests::CharTypes GET OP Start....");
    PdxTests::CharTypesPtr remotePdxptr =
        dynCast<PdxTests::CharTypesPtr>(regPtr0->get(keyport));
    LOG("Client-2 PdxTests::CharTypes GET OP Done....");

    PdxTests::CharTypes* localPdx = localPdxptr.ptr();
    PdxTests::CharTypes* remotePdx =
        dynamic_cast<PdxTests::CharTypes*>(remotePdxptr.ptr());

    LOGINFO("testThinClientPdxTests:StepFour before equal() check");
    ASSERT(remotePdx->equals(*localPdx) == true,
           "PdxTests::PdxTypes should be equal.");

    LOGINFO("testThinClientPdxTests:StepFour equal check done successfully");

    // LOGINFO("GET OP Result: Char Val=%c", remotePdx->getChar());
    // LOGINFO("NIL GET OP Result: Char[0] val=%c",
    // remotePdx->getCharArray()[0]);
    // LOGINFO("NIL GET OP Result: Char[1] val=%c",
    // remotePdx->getCharArray()[1]);

    LOG("STEP: getCharTypes complete.\n");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepThree)
  {
    RegionPtr regPtr0 = getHelper()->getRegion("DistRegionAck");

    // QueryHelper * qh = &QueryHelper::getHelper();
    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    LOG("PdxClassV1 Registered Successfully....");

    LOG("Trying to populate PDX objects.....\n");
    CacheablePtr pdxobj(new PdxTests::PdxType());
    CacheableKeyPtr keyport = CacheableKey::create(1);

    // PUT Operation
    regPtr0->put(keyport, pdxobj);
    LOG("PdxTests::PdxType: PUT Done successfully....");

    // PUT CacheableObjectArray as a Value
    CacheableKeyPtr keyport2 = CacheableKey::create(2);
    CacheableObjectArrayPtr m_objectArray;

    m_objectArray = CacheableObjectArray::create();
    m_objectArray->push_back(AddressPtr(new Address(1, "street0", "city0")));
    m_objectArray->push_back(AddressPtr(new Address(2, "street1", "city1")));
    m_objectArray->push_back(AddressPtr(new Address(3, "street2", "city2")));
    m_objectArray->push_back(AddressPtr(new Address(4, "street3", "city3")));
    m_objectArray->push_back(AddressPtr(new Address(5, "street4", "city4")));
    m_objectArray->push_back(AddressPtr(new Address(6, "street5", "city5")));
    m_objectArray->push_back(AddressPtr(new Address(7, "street6", "city6")));
    m_objectArray->push_back(AddressPtr(new Address(8, "street7", "city7")));
    m_objectArray->push_back(AddressPtr(new Address(9, "street8", "city8")));
    m_objectArray->push_back(AddressPtr(new Address(10, "street9", "city9")));

    // PUT Operation
    regPtr0->put(keyport2, m_objectArray);

    // locally destroy PdxTests::PdxType
    regPtr0->localDestroy(keyport);
    regPtr0->localDestroy(keyport2);

    LOG("localDestroy() operation....Done");

    // This is merely for asserting statistics
    regPtr0->get(keyport);
    regPtr0->get(keyport2);

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
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    try {
      Serializable::registerPdxType(Address::createDeserializable);
    } catch (const IllegalStateException&) {
      // ignore exception
    }

    // Create local CacheableObjectArray
    CacheableObjectArrayPtr m_localObjectArray;
    m_localObjectArray = CacheableObjectArray::create();
    m_localObjectArray->push_back(
        AddressPtr(new Address(1, "street0", "city0")));
    m_localObjectArray->push_back(
        AddressPtr(new Address(2, "street1", "city1")));
    m_localObjectArray->push_back(
        AddressPtr(new Address(3, "street2", "city2")));
    m_localObjectArray->push_back(
        AddressPtr(new Address(4, "street3", "city3")));
    m_localObjectArray->push_back(
        AddressPtr(new Address(5, "street4", "city4")));
    m_localObjectArray->push_back(
        AddressPtr(new Address(6, "street5", "city5")));
    m_localObjectArray->push_back(
        AddressPtr(new Address(7, "street6", "city6")));
    m_localObjectArray->push_back(
        AddressPtr(new Address(8, "street7", "city7")));
    m_localObjectArray->push_back(
        AddressPtr(new Address(9, "street8", "city8")));
    m_localObjectArray->push_back(
        AddressPtr(new Address(10, "street9", "city9")));

    // Get remote CacheableObjectArray on key 2
    CacheableKeyPtr keyport2 = CacheableKey::create(2);
    LOGINFO("Client-2 PdxTests::PdxType GET OP Start....");
    CacheableObjectArrayPtr remoteCObjArray =
        dynCast<CacheableObjectArrayPtr>(regPtr0->get(keyport2));

    LOGINFO(
        "Client-2 PdxTests::PdxType GET OP Done.. Received CObjeArray Size = "
        "%d",
        remoteCObjArray->size());
    ASSERT(
        remoteCObjArray->size() == 10,
        "PdxTests StepFour: CacheableObjectArray Size should be equal to 10");

    // Compare local vs remote CacheableObjectArray elements.
    bool isEqual = true;
    for (int i = 0; i < remoteCObjArray->size(); i++) {
      Address* rAddr1 = dynamic_cast<Address*>(remoteCObjArray->at(i).ptr());
      Address* lAddr1 = dynamic_cast<Address*>(m_localObjectArray->at(i).ptr());
      LOGINFO("Remote Address:: %d th element  AptNum=%d  street=%s  city=%s ",
              i, rAddr1->getAptNum(), rAddr1->getStreet(), rAddr1->getCity());
      if (!rAddr1->equals(*lAddr1)) {
        isEqual = false;
        break;
      }
    }
    ASSERT(isEqual == true,
           "PdxTests StepFour: CacheableObjectArray elements are not matched");

    PdxTests::PdxTypePtr localPdxptr(new PdxTests::PdxType());

    CacheableKeyPtr keyport = CacheableKey::create(1);
    LOG("Client-2 PdxTests::PdxType GET OP Start....");
    PdxTests::PdxTypePtr remotePdxptr =
        dynCast<PdxTests::PdxTypePtr>(regPtr0->get(keyport));
    LOG("Client-2 PdxTests::PdxType GET OP Done....");

    //
    PdxTests::PdxType* localPdx = localPdxptr.ptr();
    PdxTests::PdxType* remotePdx =
        dynamic_cast<PdxTests::PdxType*>(remotePdxptr.ptr());

    // ToDo open this equals check
    LOGINFO("testThinClientPdxTests:StepFour before equal() check");
    ASSERT(remotePdx->equals(*localPdx, false) == true,
           "PdxTests::PdxTypes should be equal.");
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

    // LOGINFO("GET OP Result: IntVal1=%d", obj2->getInt1());
    // LOGINFO("GET OP Result: IntVal2=%d", obj2->getInt2());
    // LOGINFO("GET OP Result: IntVal3=%d", obj2->getInt3());
    // LOGINFO("GET OP Result: IntVal4=%d", obj2->getInt4());
    // LOGINFO("GET OP Result: IntVal5=%d", obj2->getInt5());
    // LOGINFO("GET OP Result: IntVal6=%d", obj2->getInt6());

    // LOGINFO("GET OP Result: BoolVal=%d", obj2->getBool());
    // LOGINFO("GET OP Result: ByteVal=%d", obj2->getByte());
    // LOGINFO("GET OP Result: ShortVal=%d", obj2->getShort());

    // LOGINFO("GET OP Result: IntVal=%d", obj2->getInt());

    // LOGINFO("GET OP Result: LongVal=%ld", obj2->getLong());
    // LOGINFO("GET OP Result: FloatVal=%f", obj2->getFloat());
    // LOGINFO("GET OP Result: DoubleVal=%lf", obj2->getDouble());
    // LOGINFO("GET OP Result: StringVal=%s", obj2->getString());
    // LOGINFO("GET OP Result: BoolArray[0]=%d", obj2->getBoolArray()[0]);
    // LOGINFO("GET OP Result: BoolArray[1]=%d", obj2->getBoolArray()[1]);
    // LOGINFO("GET OP Result: BoolArray[2]=%d", obj2->getBoolArray()[2]);

    // LOGINFO("GET OP Result: ByteArray[0]=%d", obj2->getByteArray()[0]);
    // LOGINFO("GET OP Result: ByteArray[1]=%d", obj2->getByteArray()[1]);

    // LOGINFO("GET OP Result: ShortArray[0]=%d", obj2->getShortArray()[0]);
    // LOGINFO("GET OP Result: IntArray[0]=%d", obj2->getIntArray()[0]);
    // LOGINFO("GET OP Result: LongArray[1]=%lld", obj2->getLongArray()[1]);
    // LOGINFO("GET OP Result: FloatArray[0]=%f", obj2->getFloatArray()[0]);
    // LOGINFO("GET OP Result: DoubleArray[1]=%lf", obj2->getDoubleArray()[1]);

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
    PdxTests::PdxTypesIgnoreUnreadFieldsV1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2)
  {
    m_useWeakHashMap = true;
    PdxTests::PdxTypesIgnoreUnreadFieldsV2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxTypesIgnoreUnreadFieldsV1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxTypesIgnoreUnreadFieldsV2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1, SetWeakHashMapToTrueC1BM)
  {
    m_useWeakHashMap = true;
    PdxTests::PdxType1V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2BM)
  {
    m_useWeakHashMap = true;
    PdxTests::PdxTypes1V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1BM)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxType1V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2BM)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxTypes1V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1, SetWeakHashMapToTrueC1BM2)
  {
    m_useWeakHashMap = true;
    PdxTests::PdxType2V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2BM2)
  {
    m_useWeakHashMap = true;
    PdxTests::PdxTypes2V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1BM2)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxType2V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2BM2)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxTypes2V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1, SetWeakHashMapToTrueC1BM3)
  {
    m_useWeakHashMap = true;
    PdxTests::PdxType3V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2BM3)
  {
    m_useWeakHashMap = true;
    PdxTests::PdxTypes3V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1BM3)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxType3V1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2BM3)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxTypes3V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1, SetWeakHashMapToTrueC1BMR1)
  {
    m_useWeakHashMap = true;
    PdxTests::PdxTypesV1R1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2BMR1)
  {
    m_useWeakHashMap = true;
    PdxTests::PdxTypesR1V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1BMR1)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxTypesV1R1::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2BMR1)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxTypesR1V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION
///
DUNIT_TASK_DEFINITION(CLIENT1, SetWeakHashMapToTrueC1BMR2)
  {
    m_useWeakHashMap = true;
    PdxTests::PdxTypesV1R2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToTrueC2BMR2)
  {
    m_useWeakHashMap = true;
    PdxTests::PdxTypesR2V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, setWeakHashMapToFlaseC1BMR2)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxTypesV1R2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, SetWeakHashMapToFalseC2BMR2)
  {
    m_useWeakHashMap = false;
    PdxTests::PdxTypesR2V2::reset(m_useWeakHashMap);
  }
END_TASK_DEFINITION
///

void runPdxLongRunningClientTest(bool poolConfig = false,
                                 bool withLocators = false) {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator)
  CALL_TASK(StepOnePoolLocSysConfig)
  CALL_TASK(StepTwoPoolLocSysConfig)

  // StepThree: Put some portfolio/Position objects
  CALL_TASK(Puts2)

  // now close server
  CALL_TASK(CloseServer)

  CALL_TASK(forCleanup)
  // now start server
  CALL_TASK(CreateServerWithLocator)

  // do put again
  CALL_TASK(Puts22)

  CALL_TASK(Get2)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void runPdxDistOps() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  // StepThree: Put some portfolio/Position objects
  CALL_TASK(PutAndVerifyPdxInGet)
  CALL_TASK(VerifyGetOnly)
  CALL_TASK(PutAndVerifyVariousPdxTypes)
  CALL_TASK(VerifyVariousPdxGets)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void runPdxTestForCharTypes(bool poolConfig = false,
                            bool withLocators = false) {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  // StepThree: Put some portfolio/Position objects
  CALL_TASK(putCharTypes)
  CALL_TASK(getCharTypes)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void testBug866() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator)
  CALL_TASK(StepOnePoolLocBug866)
  CALL_TASK(StepTwoPoolLocBug866)

  // StepThree: Put some portfolio/Position objects
  CALL_TASK(putOperation)
  CALL_TASK(getOperation)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void runPdxPutGetTest() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  // StepThree: Put some portfolio/Position objects
  CALL_TASK(StepThree)
  CALL_TASK(StepFour)
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void runBasicMergeOpsR2() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator1)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  CALL_TASK(putAtVersionTwoR21)

  CALL_TASK(getPutAtVersionOneR22)

  for (int i = 0; i < 10; i++) {
    CALL_TASK(getPutAtVersionTwoR23);
    CALL_TASK(getPutAtVersionOneR24);
  }

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void runBasicMergeOpsR1() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator1)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

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

  CALL_TASK(CloseLocator)
}

void runBasicMergeOps() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator1)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

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

  CALL_TASK(CloseLocator)
}

void runBasicMergeOps2() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator1)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  CALL_TASK(putAtVersionOne21)

  CALL_TASK(getPutAtVersionTwo22)

  for (int i = 0; i < 10; i++) {
    CALL_TASK(getPutAtVersionOne23);
    CALL_TASK(getPutAtVersionTwo24);
  }
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void runBasicMergeOps3() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator1)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  CALL_TASK(putAtVersionOne31)

  CALL_TASK(getPutAtVersionTwo32)

  for (int i = 0; i < 10; i++) {
    CALL_TASK(getPutAtVersionOne33);
    CALL_TASK(getPutAtVersionTwo34);
  }
  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void runJavaInteroperableOps() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator2)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  CALL_TASK(JavaPutGet)  // c1
  CALL_TASK(JavaGet)     // c2

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

// runJavaInterOpsUsingLinkedList
void runJavaInterOpsUsingLinkedList() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator2)
  CALL_TASK(StepOnePoolLoc1)
  CALL_TASK(StepTwoPoolLoc1)

  CALL_TASK(JavaPutGet1)  // c1

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

// test case that checks for Invalid Usage and corr. IllegalStatException for
// PDXReader And PDXWriter APIs.
void _disable_see_bug_999_testReaderWriterInvalidUsage() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator2)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  CALL_TASK(testPdxWriterAPIsWithInvalidArgs)
  CALL_TASK(testPdxReaderAPIsWithInvalidArgs)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

//
void testPolymorphicUseCase() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator2)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  CALL_TASK(testPutWithMultilevelInheritance)
  CALL_TASK(testGetWithMultilevelInheritance)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void runNestedPdxOps() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator1)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  CALL_TASK(PutAndVerifyNestedPdxInGet)

  CALL_TASK(VerifyNestedGetOnly)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void runNestedPdxOpsWithVersioning() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator1)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  CALL_TASK(PutMixedVersionNestedPdx)

  CALL_TASK(VerifyMixedVersionNestedGetOnly)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void runPdxInGFSOps() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator1)
  CALL_TASK(StepOnePoolLoc)
  CALL_TASK(StepTwoPoolLoc)

  CALL_TASK(PutAndVerifyPdxInGFSInGet)

  CALL_TASK(VerifyPdxInGFSGetOnly)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void runPdxIgnoreUnreadFieldTest() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator1)
  CALL_TASK(StepOnePoolLoc_PDX)
  CALL_TASK(StepTwoPoolLoc_PDX)

  CALL_TASK(putV2PdxUI)

  CALL_TASK(putV1PdxUI)

  CALL_TASK(getV2PdxUI)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

// runPdxMetadataCheckTest
void runPdxMetadataCheckTest() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator_PdxMetadataTest)
  CALL_TASK(StepOnePoolLoc_PdxMetadataTest)
  CALL_TASK(StepTwoPoolLoc_PdxMetadataTest)

  CALL_TASK(generateJavaPdxType)

  CALL_TASK(putAllPdxTypes)

  CALL_TASK(verifyDotNetPdxTypes)

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}
// END runPdxMetadataCheckTest

// runPdxBankTest
void runPdxBankTest() {
  CALL_TASK(StartLocator)
  CALL_TASK(CreateServerWithLocator_PdxMetadataTest)
  CALL_TASK(StepOnePoolLoc_PdxMetadataTest)
  CALL_TASK(StepTwoPoolLoc_PdxMetadataTest)
  CALL_TASK(StepThreePoolLoc_PdxMetadataTest)

  CALL_TASK(client1PutsV1Object)  // c1

  CALL_TASK(client2GetsV1ObjectAndPutsV2Object)  // c2

  CALL_TASK(client3GetsV2Object)  // c3

  CALL_TASK(CloseCache1)
  CALL_TASK(CloseCache2)
  CALL_TASK(CloseCache3)  //

  CALL_TASK(CloseServer)

  CALL_TASK(CloseLocator)
}

void enableWeakHashMapC1() { CALL_TASK(SetWeakHashMapToTrueC1) }
void enableWeakHashMapC2() { CALL_TASK(SetWeakHashMapToTrueC2) }

void disableWeakHashMapC1() { CALL_TASK(setWeakHashMapToFlaseC1) }
void disableWeakHashMapC2() { CALL_TASK(SetWeakHashMapToFalseC2) }
/////
void enableWeakHashMapC1BM() { CALL_TASK(SetWeakHashMapToTrueC1BM) }
void enableWeakHashMapC2BM() { CALL_TASK(SetWeakHashMapToTrueC2BM) }

void disableWeakHashMapC1BM() { CALL_TASK(setWeakHashMapToFlaseC1BM) }
void disableWeakHashMapC2BM() { CALL_TASK(SetWeakHashMapToFalseC2BM) }
////
void enableWeakHashMapC1BM2() { CALL_TASK(SetWeakHashMapToTrueC1BM2) }
void enableWeakHashMapC2BM2() { CALL_TASK(SetWeakHashMapToTrueC2BM2) }

void disableWeakHashMapC1BM2() { CALL_TASK(setWeakHashMapToFlaseC1BM2) }
void disableWeakHashMapC2BM2() { CALL_TASK(SetWeakHashMapToFalseC2BM2) }
////
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
    {
      runPdxLongRunningClientTest();
    }
    // NON PDX UnitTest for Ticket#866 on NC OR SR#13306117704. Set client name
    // via native client API
    testBug866();

    runPdxTestForCharTypes();

    // PUT-GET Test with values of type CacheableObjectArray and PdxType object
    runPdxPutGetTest();

    // PdxDistOps-PdxTests::PdxType PUT/GET Test across clients
    {
      runPdxDistOps();
    }

    // BasicMergeOps
    {
      enableWeakHashMapC1BM();
      enableWeakHashMapC2BM();
      runBasicMergeOps();

    }

    // BasicMergeOps2
    {
      enableWeakHashMapC1BM2();
      enableWeakHashMapC2BM2();
      runBasicMergeOps2();
    }

    // BasicMergeOps3
    {
      enableWeakHashMapC1BM3();
      enableWeakHashMapC2BM3();
      runBasicMergeOps3();
    }

    // BasicMergeOpsR1
    {
      enableWeakHashMapC1BMR1();
      enableWeakHashMapC2BMR1();
      runBasicMergeOpsR1();
    }

    // BasicMergeOpsR2
    {
      enableWeakHashMapC1BMR2();
      enableWeakHashMapC2BMR2();
      runBasicMergeOpsR2();
    }

    // JavaInteroperableOps
    {
      runJavaInteroperableOps();
    }

    // PDXReaderWriterInvalidUsage
    {
        // disable see bug 999 for more details.
        // testReaderWriterInvalidUsage();
    }

    // Test LinkedList
    {
      runJavaInterOpsUsingLinkedList();
    }

    // NestedPdxOps
    {
      runNestedPdxOps();
    }

    // MixedVersionNestedPdxOps
    { runNestedPdxOpsWithVersioning(); }

    // Pdxobject In Gemfire Serializable Ops
    //{
    //  runPdxInGFSOps();
    //}

    {
      enableWeakHashMapC1();
      enableWeakHashMapC2();
      runPdxIgnoreUnreadFieldTest();
    }

    // PdxMetadataCheckTest
    {
      runPdxMetadataCheckTest();
    }

    // PdxBankTest
    {
      runPdxBankTest();
    }

    // Polymorphic-multilevel inheritance
    {
      testPolymorphicUseCase();
    }
  }
END_MAIN
