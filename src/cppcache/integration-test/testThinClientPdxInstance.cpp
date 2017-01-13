/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
* testThinClientPdxInstance.cpp
*
*/

#ifndef TESTTHINCLIENTPDXINSTANCE_HPP_
#define TESTTHINCLIENTPDXINSTANCE_HPP_

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/High_Res_Timer.h>
#include "testobject/PdxType.hpp"
#include "testobject/NestedPdxObject.hpp"
#include <ace/OS.h>
#include <string>
#include "CachePerfStats.hpp"
#include <LocalRegion.hpp>
#define ROOT_NAME "testThinClientPdxInstance"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

#include <ace/Date_Time.h>

using namespace gemfire;
using namespace test;
using namespace testobject;
using namespace PdxTests;

bool isLocalServer = false;

CacheHelper* cacheHelper = NULL;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2
static bool isLocator = false;
const bool USE_ACK = true;
const bool NO_ACK ATTR_UNUSED = false;

const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck"};

template <typename T1, typename T2>
bool genericValCompare(T1 value1, T2 value2) /*const*/
{
  if (value1 != value2) return false;
  return true;
}

template <typename T1, typename T2>
bool genericCompare(T1* value1, T2* value2, int length) /*const*/
{
  int i = 0;
  while (i < length) {
    if (value1[i] != value2[i]) {
      return false;
    } else {
      i++;
    }
  }
  return true;
}

template <typename T1, typename T2>
bool generic2DCompare(T1** value1, T2** value2, int length,
                      int* arrLengths) /*const*/
{
  for (int j = 0; j < length; j++) {
    for (int k = 0; k < arrLengths[j]; k++) {
      LOGINFO("generic2DCompare value1 = %d \t value2 = %d ", value1[j][k],
              value2[j][k]);
      if (value1[j][k] != value2[j][k]) return false;
    }
  }
  LOGINFO("PdxObject::genericCompare Line_34");
  return true;
}

void initClient(const bool isthinClient, bool isPdxReadSerailized) {
  LOGINFO("isPdxReadSerailized = %d ", isPdxReadSerailized);
  if (cacheHelper == NULL) {
    PropertiesPtr config = Properties::create();
    config->insert("enable-time-statistics", "true");
    LOGINFO("enabled-time-statistics ");
    cacheHelper = new CacheHelper(isthinClient, false, isPdxReadSerailized,
                                  config, false);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}
static int clientWithXml = 0;
void initClient(const char* clientXmlFile) {
  if (cacheHelper == NULL) {
    PropertiesPtr config = Properties::create();
    if (clientWithXml > 2) config->insert("grid-client", "true");
    clientWithXml += 1;
    config->insert("enable-time-statistics", "true");
    cacheHelper = new CacheHelper(NULL, clientXmlFile, config);
  }
  ASSERT(cacheHelper, "Failed to create a CacheHelper client instance.");
}
void cleanProc() {
  if (cacheHelper != NULL) {
    delete cacheHelper;
    cacheHelper = NULL;
  }
}
CacheHelper* getHelper() {
  ASSERT(cacheHelper != NULL, "No cacheHelper initialized.");
  return cacheHelper;
}
void createPooledRegion(const char* name, bool ackMode, 
                        const char* locators, const char* poolname,
                        bool clientNotificationEnabled = false,
                        bool cachingEnable = true) {
  LOG("createRegion_Pool() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr =
      getHelper()->createPooledRegion(name, ackMode, locators, poolname,
                                      cachingEnable, clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Pooled Region created.");
}
DUNIT_TASK_DEFINITION(SERVER1, CreateLocator1)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_pdxinstance_hashcode.xml");
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_pdxinstance_hashcode.xml",
                              locatorsG);
    }
    LOG("SERVER1 with locator started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator_PdxReadSerialized)
  {
    initClient(true, true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG,
                       "__TEST_POOL1__", false, false);
    LOG("StepOne_Pooled_Locator complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_Locator_PdxReadSerialized)
  {
    initClient(true, true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG,
                       "__TEST_POOL1__", false, false);
    LOG("StepOne_Pooled_Locator complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,
                      StepTwo_Pooled_Locator_CachingEnabled_PdxReadSerialized)
  {
    initClient(true, true);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG,
                       "__TEST_POOL1__", false, true);
    LOG("StepTwo_Pooled_Locator complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
  {
    initClient(true, false);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG,
                       "__TEST_POOL1__", false, false);
    LOG("StepOne_Pooled_Locator complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_Locator)
  {
    initClient(true, false);
    createPooledRegion(regionNames[0], USE_ACK,  locatorsG,
                       "__TEST_POOL1__", false, false);
    LOG("StepTwo_Pooled_Locator complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, testReadSerializedXMLProperty_Client2)
  {
    ASSERT(cacheHelper->getCache()->getPdxReadSerialized() == true,
           "Pdx read serialized property should be true.");
    LOG("testReadSerializedXMLProperty complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, testReadSerializedXMLProperty_Client1)
  {
    ASSERT(cacheHelper->getCache()->getPdxReadSerialized() == true,
           "Pdx read serialized property should be true.");
    LOG("testReadSerializedXMLProperty complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putPdxWithIdentityField)
  {
    LOG("putPdxWithIdentityField started ");

    try {
      Serializable::registerPdxType(SerializePdx::createDeserializable);
      LOG("SerializePdx Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("SerializePdx IllegalStateException");
    }

    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);
    // Creating object of type PdxObject
    /*SerializePdxPtr*/ CacheablePtr sp(new SerializePdx(true));

    // PUT Operation
    rptr->put(CacheableInt32::create(1), sp);
    LOG("pdxPut 1 completed ");

    LOG("putPdxWithIdentityField complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putCacheableObjectArrayWithPdxFields)
  {
    LOG("putCacheableObjectArrayWithPdxFields started ");

    try {
      Serializable::registerPdxType(Address::createDeserializable);
      LOG("Address Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("Address IllegalStateException");
    }

    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);

    // Creating CacheableobjectArray of type PdxObject
    CacheableObjectArrayPtr objectArray = CacheableObjectArray::create();
    objectArray->push_back(AddressPtr(new Address(1, "street0", "city0")));
    objectArray->push_back(AddressPtr(new Address(2, "street1", "city1")));
    objectArray->push_back(AddressPtr(new Address(3, "street2", "city2")));
    objectArray->push_back(AddressPtr(new Address(4, "street3", "city3")));
    objectArray->push_back(AddressPtr(new Address(5, "street4", "city4")));
    objectArray->push_back(AddressPtr(new Address(6, "street5", "city5")));
    objectArray->push_back(AddressPtr(new Address(7, "street6", "city6")));
    objectArray->push_back(AddressPtr(new Address(8, "street7", "city7")));
    objectArray->push_back(AddressPtr(new Address(9, "street8", "city8")));
    objectArray->push_back(AddressPtr(new Address(10, "street9", "city9")));

    // PUT Operation
    rptr->put(CacheableInt32::create(100), objectArray);
    LOG("putCacheableObjectArrayWithPdxFields on key 100 completed ");

    LOG("putCacheableObjectArrayWithPdxFields complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, verifyPdxIdentityField)
  {
    LOG("verifyPdxIdentityField started ");

    try {
      Serializable::registerPdxType(SerializePdx::createDeserializable);
      LOG("SerializePdx Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("SerializePdx IllegalStateException");
    }

    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);

    PdxInstancePtr pi =
        dynCast<PdxInstancePtr>(rptr->get(CacheableInt32::create(1)));
    LOG("PdxInstancePtr get complete");

    LOG("Statistics for for (PdxTests.PdxType) PdxInstance");
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(rptr.ptr()));

    LOGINFO(
        "pdxInstanceDeserializations for (PdxTests.PdxType) PdxInstance  = %d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializations());
    LOGINFO("pdxInstanceCreations for (PdxTests.PdxType) PdxInstance  = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations());
    LOGINFO(
        "pdxInstanceDeserializationTime for(PdxTests.PdxType) PdxInstance  = "
        "%d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializationTime());

    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializations() == 0,
           "pdxInstanceDeserialization should be equal to 0.");
    ASSERT(
        lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations() == 1,
        "pdxInstanceCreations should be equal to 1.");
    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializationTime() > 0,
           "pdxInstanceDeserializationTime should be greater than equal to 0.");

    ASSERT(pi->getFieldNames()->length() == 4,
           "number of fields should be four in SerializePdx");

    ASSERT(pi->isIdentityField("i1") == true,
           "SerializePdx1.i1 should be identity field");

    ASSERT(pi->isIdentityField("i2") == false,
           "SerializePdx1.i2 should NOT be identity field");

    ASSERT(pi->hasField("i1") == true,
           "SerializePdx1.i1 should be in PdxInstance stream");

    ASSERT(pi->hasField("i3") == false,
           "There is no field i3 in SerializePdx1's PdxInstance stream");

    CacheableKeyPtr javaPdxHCKey = CacheableKey::create("javaPdxHC");
    CacheablePtr pIPtr2 = dynCast<CacheablePtr>(rptr->get(javaPdxHCKey));
    LOG("javaPdxHCKey get done");
    CacheableInt32* val = dynamic_cast<CacheableInt32*>(pIPtr2.ptr());
    LOG("javaPdxHCKey cast done");
    int javaPdxHC = val->value();
    LOGINFO("javaPdxHC hash code = %d ", javaPdxHC);

    int pdxInstHashcode = pi->hashcode();

    ASSERT(
        javaPdxHC == pdxInstHashcode,
        "Pdxhashcode for identity field object SerializePdx1 not matched with "
        "java pdx hash code.");

    PdxInstancePtr pi2 =
        dynCast<PdxInstancePtr>(rptr->get(CacheableInt32::create(1)));

    LOGINFO(
        "pdxInstanceDeserializations for (PdxTests.PdxType) PdxInstance  = %d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializations());
    LOGINFO("pdxInstanceCreations for (PdxTests.PdxType) PdxInstance  = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations());
    LOGINFO(
        "pdxInstanceDeserializationTime for(PdxTests.PdxType) PdxInstance  = "
        "%d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializationTime());

    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializations() == 0,
           "pdxInstanceDeserialization should be equal to 0.");
    ASSERT(
        lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations() == 2,
        "pdxInstanceCreations should be equal to 2.");
    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializationTime() > 0,
           "pdxInstanceDeserializationTime should be greater than 0.");

    bool ret = false;
    ret = (*pi.ptr() == *pi2.ptr());
    LOGINFO("PdxObject ret = %d ", ret);
    ASSERT(ret == true, "Both PdxInstances should be equal.");

    LOG("verifyPdxIdentityField complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, verifyCacheableObjectArrayWithPdxField)
  {
    LOG("verifyCacheableObjectArrayWithPdxField started ");

    try {
      Serializable::registerPdxType(Address::createDeserializable);
      LOG("Address Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("Address IllegalStateException");
    }

    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);

    CacheableObjectArrayPtr objectArrayPtr =
        rptr->get(CacheableInt32::create(100));
    LOG("PdxInstancePtr get on key 100 complete");

    for (int i = 0; i < objectArrayPtr->size(); i++) {
      PdxInstancePtr pi = objectArrayPtr->at(i);
      LOG("PdxInstancePtr obtained from CacheableObjectArray");

      PdxInstanceFactoryPtr pifPtr =
          cacheHelper->getCache()->createPdxInstanceFactory("PdxTests.Address");
      LOG("PdxInstanceFactoryPtr created for PdxTests.Address....");

      pifPtr->writeInt("_aptNumber", i + 1);
      char streetStr[256] = {0};
      sprintf(streetStr, "street%d", i);
      pifPtr->writeString("_street", streetStr);
      char cityStr[256] = {0};
      sprintf(cityStr, "city%d", i);
      pifPtr->writeString("_city", cityStr);

      AddressPtr addrPtr = dynCast<AddressPtr>(pi->getObject());
      LOG("AddressPtr created using PdxInstance getObject()....");
      AddressPtr newAddrPtr(new Address(i + 1, streetStr, cityStr));
      LOG("AddressPtr created using new....");
      ASSERT(addrPtr.ptr()->equals(*(newAddrPtr.ptr())) == true,
             "Both PdxInstances should be equal.");

      PdxInstancePtr retPtr = pifPtr->create();
      LOG("PdxInstancePtr created....");

      bool ret = false;
      ret = (*pi.ptr() == *retPtr.ptr());
      LOGINFO("PdxObject ret = %d ", ret);
      ASSERT(ret == true, "Both PdxInstances should be equal.");
    }

    LOG("verifyCacheableObjectArrayWithPdxField complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putPdxWithNullIdentityFields)
  {
    LOG("putPdxWithNullIdentityFields started ");

    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);
    // Creating object of type PdxObject
    SerializePdxPtr sp(new SerializePdx(false));

    // PUT Operation
    rptr->put(CacheableInt32::create(2), sp);
    LOG("pdxPut 1 completed ");

    LOG("putPdxWithNullIdentityFields complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, verifyPdxNullIdentityFieldHC)
  {
    LOG("verifyPdxNullIdentityFieldHC started ");

    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);

    PdxInstancePtr pi =
        dynCast<PdxInstancePtr>(rptr->get(CacheableInt32::create(2)));
    LOG("PdxInstancePtr get complete");

    CacheableKeyPtr javaPdxHCKey = CacheableKey::create("javaPdxHC");
    CacheablePtr pIPtr2 = dynCast<CacheablePtr>(rptr->get(javaPdxHCKey));
    LOG("javaPdxHCKey get done");
    CacheableInt32* val = dynamic_cast<CacheableInt32*>(pIPtr2.ptr());
    LOG("javaPdxHCKey cast done");
    int javaPdxHC = val->value();
    LOGINFO("javaPdxHC hash code = %d ", javaPdxHC);

    int pdxInstHashcode = pi->hashcode();

    ASSERT(
        javaPdxHC == pdxInstHashcode,
        "Pdxhashcode for identity field object SerializePdx1 not matched with "
        "java pdx hash code.");

    PdxInstancePtr pi2 =
        dynCast<PdxInstancePtr>(rptr->get(CacheableInt32::create(2)));

    bool ret = false;
    ret = (*pi.ptr() == *pi2.ptr());
    LOGINFO("PdxObject ret = %d ", ret);
    ASSERT(ret == true, "Both PdxInstances should be equal.");

    VectorOfCacheableKey keys1;
    keys1.push_back(CacheableInt32::create(1));
    keys1.push_back(CacheableInt32::create(2));
    HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
    valuesMap->clear();
    rptr->getAll(keys1, valuesMap, NULLPTR, true);
    LOG("getAll on Pdx objects completed.");

    ASSERT(valuesMap->size() == keys1.size(), "getAll size did not match");

    LOG("verifyPdxNullIdentityFieldHC complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, pdxPut)
  {
    LOG("pdxPut started ");

    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
      LOG("PdxObject Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("PdxObject IllegalStateException");
    }

    // Creating object of type PdxObject
    CacheablePtr pdxobj(new PdxTests::PdxType());

    CacheableKeyPtr keyport = CacheableKey::create("pdxput");
    CacheableKeyPtr keyport1 = CacheableKey::create("pdxput2");
    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);

    // PUT Operation
    rptr->put(keyport, pdxobj);
    LOG("pdxPut 1 completed ");

    PdxInstancePtr pIPtr1 = dynCast<PdxInstancePtr>(rptr->get(keyport));

    LOG("Statistics for for (PdxTests.PdxType) PdxInstance");
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(rptr.ptr()));

    LOGINFO(
        "pdxInstanceDeserializations for (PdxTests.PdxType) PdxInstance = %d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializations());
    LOGINFO("pdxInstanceCreations for (PdxTests.PdxType) PdxInstance = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations());
    LOGINFO(
        "pdxInstanceDeserializationTime for(PdxTests.PdxType) PdxInstance = "
        "%d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializationTime());

    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializations() == 0,
           "pdxInstanceDeserialization should be equal to 0.");
    ASSERT(
        lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations() == 1,
        "pdxInstanceCreations should be equal to 1.");
    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializationTime() > 0,
           "pdxInstanceDeserializationTime should be greater than 0.");

    CacheableStringPtr toString = pIPtr1->toString();
    LOGINFO("pdxinstance toString = %s ", toString->asChar());
    int pdxInstHashcode = pIPtr1->hashcode();
    LOGINFO("pdxinstance hash code = %d ", pdxInstHashcode);

    CacheableKeyPtr javaPdxHCKey = CacheableKey::create("javaPdxHC");
    CacheablePtr pIPtr2 = dynCast<CacheablePtr>(rptr->get(javaPdxHCKey));
    LOG("In verifyPdxInstanceHashcode get done");
    CacheableInt32* val = dynamic_cast<CacheableInt32*>(pIPtr2.ptr());
    LOG("In verifyPdxInstanceHashcode cast done");
    int javaPdxHC = val->value();
    LOGINFO("javaPdxHC hash code = %d ", javaPdxHC);

    ASSERT(javaPdxHC == pdxInstHashcode,
           "Pdxhashcode hashcode not matched with java pdx hash code.");

    // Creating object of type ParentPdx
    CacheablePtr parentPdxObj(new ParentPdx(1));

    LOG("pdxPut parentPdxObj created ");
    rptr->put(keyport1, parentPdxObj);

    LOG("pdxPut 2 completed ");

    LOG("pdxPut complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, getObject)
  {
    LOG("getObject started ");

    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
      LOG("PdxObject Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("PdxObject IllegalStateException");
    }

    try {
      Serializable::registerPdxType(ChildPdx::createDeserializable);
      LOG("ChildPdx Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("ChildPdx IllegalStateException");
    }

    try {
      Serializable::registerPdxType(ParentPdx::createDeserializable);
      LOG("ParentPdx Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("ParentPdx IllegalStateException");
    }

    CacheableKeyPtr keyport = CacheableKey::create("pdxput");
    CacheableKeyPtr keyport1 = CacheableKey::create("pdxput2");
    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);

    PdxInstancePtr pIPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    LOG("PdxObject get Successful....");

    LOGINFO("pdxinstance classname = %s ", pIPtr->getClassName());
    ASSERT(strcmp(pIPtr->getClassName(), "PdxTests.PdxType") == 0,
           "pdxInstance.getClassName should return PdxTests.PdxType.");

    PdxSerializablePtr pt = pIPtr->getObject();

    LOG("PdxObject getObject Successful....");

    LOG("Statistics for for (PdxTests.PdxType) PdxInstance ");
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(rptr.ptr()));

    LOGINFO(
        "pdxInstanceDeserializations for (PdxTests.PdxType) PdxInstance  = %d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializations());
    LOGINFO("pdxInstanceCreations for (PdxTests.PdxType) PdxInstance  = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations());
    LOGINFO(
        "pdxInstanceDeserializationTime for(PdxTests.PdxType) PdxInstance  = "
        "%d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializationTime());

    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializations() == 1,
           "pdxInstanceDeserialization should be equal to 1.");
    ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations() > 1,
           "pdxInstanceCreations should be greater than 1.");
    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializationTime() > 0,
           "pdxInstanceDeserializationTime should be greater than 0.");

    PdxTests::PdxTypePtr ptorig(new PdxTests::PdxType());
    LOG("PdxObject ptorig Successful....");
    PdxTests::PdxType* obj1 = ptorig.ptr();
    LOG("obj1 Successful....");
    PdxTests::PdxType* obj2 = dynamic_cast<PdxTests::PdxType*>(pt.ptr());
    LOG("obj2 Successful....");

    ASSERT(obj1->equals(*obj2, true) == true,
           "PdxInstance.getObject not equals original object.");

    PdxInstancePtr pIPtr1 = dynCast<PdxInstancePtr>(rptr->get(keyport1));
    LOG("ParentPdxObject get Successful....");
    CacheableStringPtr toString = pIPtr1->toString();
    LOGINFO("ParentPdxObject toString = %s ", toString->asChar());
    ParentPdxPtr pt1 = dynCast<ParentPdxPtr>(pIPtr1->getObject());
    LOG("ParentPdxObject getObject Successful....");

    LOGINFO(
        "pdxInstanceDeserializations for (PdxTests.PdxType) PdxInstance  = %d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializations());
    LOGINFO("pdxInstanceCreations for (PdxTests.PdxType) PdxInstance  = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations());
    LOGINFO(
        "pdxInstanceDeserializationTime for(PdxTests.PdxType) PdxInstance  = "
        "%d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializationTime());

    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializations() == 2,
           "pdxInstanceDeserialization should be equal to 2.");
    ASSERT(lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations() > 1,
           "pdxInstanceCreations should be greater than 1.");
    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializationTime() > 0,
           "pdxInstanceDeserializationTime should be greater than 0.");

    ParentPdxPtr parentPdxObj(new ParentPdx(1));
    LOG("ParentPdxObject parentPdxObj Successful....");
    ParentPdx* parentObj1 = parentPdxObj.ptr();
    LOG("parentObj1 Successful....");
    ParentPdx* parentObj2 = dynamic_cast<ParentPdx*>(pt1.ptr());
    LOG("parentObj2 Successful....");

    ASSERT(parentObj1->equals(*parentObj2, true) == true,
           "PdxInstance.getObject not equals original object.");

    LOG("getObject complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, verifyPdxInstanceEquals)
  {
    LOG("Task verifyPdxInstanceEquals started.");

    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
      LOG("PdxObject Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("PdxObject IllegalStateException");
    }

    CacheableKeyPtr keyport = CacheableKey::create("pdxput");
    CacheableKeyPtr keyport1 = CacheableKey::create("pdxput2");
    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);

    PdxInstancePtr pIPtr1 = dynCast<PdxInstancePtr>(rptr->get(keyport));
    LOG("get1 Successfully....");
    PdxInstancePtr pIPtr2 = dynCast<PdxInstancePtr>(rptr->get(keyport));
    LOG("get2 Successfully....");

    bool ret = false;
    ret = (*pIPtr1.ptr() == *pIPtr2.ptr());
    LOGINFO("PdxObject ret = %d ", ret);
    ASSERT(ret == true, "Both PdxInstances should be equal.");

    pIPtr1 = dynCast<PdxInstancePtr>(rptr->get(keyport1));
    LOG("parentPdx get1 Successfully....");
    pIPtr2 = dynCast<PdxInstancePtr>(rptr->get(keyport1));
    LOG("parentPdx get2 Successfully....");

    ret = false;
    ret = (*pIPtr1.ptr() == *pIPtr2.ptr());
    LOGINFO("parentPdx ret = %d ", ret);
    ASSERT(ret == true, "Both PdxInstances should be equal.");

    LOG("Task verifyPdxInstanceEquals complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, verifyPdxInstanceHashcode)
  {
    LOG("verifyPdxInstanceHashcode started ");

    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyport1 = CacheableKey::create("javaPdxHC");
    CacheableKeyPtr keyport2 = CacheableKey::create("pdxput2");
    PdxInstancePtr pIPtr1 = dynCast<PdxInstancePtr>(rptr->get(keyport2));
    int pdxInstHashcode = pIPtr1->hashcode();
    LOGINFO("pdxinstance hash code = %d ", pdxInstHashcode);

    CacheablePtr pIPtr2 = dynCast<CacheablePtr>(rptr->get(keyport1));
    LOG("In verifyPdxInstanceHashcode get done");
    CacheableInt32* val1 = dynamic_cast<CacheableInt32*>(pIPtr2.ptr());
    LOG("In verifyPdxInstanceHashcode cast done");
    int javaPdxHC = val1->value();
    LOGINFO("javaPdxHC hash code again = %d ", javaPdxHC);

    ASSERT(javaPdxHC == pdxInstHashcode,
           "Pdxhashcode hashcode not matched with java padx hash code.");

    LOG("verifyPdxInstanceHashcode complete");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, accessPdxInstance)
  {
    LOG("accessPdxInstance started ");

    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
      LOG("PdxObject Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("PdxObject IllegalStateException");
    }

    try {
      Serializable::registerPdxType(Address::createDeserializable);
      LOG("Address Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("Address IllegalStateException");
    }

    CacheableKeyPtr keyport = CacheableKey::create("pdxput");
    CacheableKeyPtr keyport1 = CacheableKey::create("pdxput2");
    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);

    PdxInstancePtr pIPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    LOGINFO("PdxInstancePtr created ");

    PdxTests::PdxTypePtr pdxobjPtr(new PdxTests::PdxType());

    bool bval = 0;
    pIPtr->getField("m_bool", bval);
    ASSERT(pdxobjPtr->getBool() == bval, "bool values should be equal");
    ASSERT(pIPtr->getFieldType("m_bool") == PdxFieldTypes::BOOLEAN,
           "Type Value BOOLEAN Mismatch");

    wchar_t charVal = ' ';
    pIPtr->getField("m_char", charVal);
    ASSERT(pdxobjPtr->getChar() == charVal, "char values should be equal");
    ASSERT(pIPtr->getFieldType("m_char") == PdxFieldTypes::CHAR,
           "Type Value CHAR Mismatch");

    signed char byteVal = 0;
    pIPtr->getField("m_byte", byteVal);
    ASSERT(pdxobjPtr->getByte() == byteVal, "byte values should be equal");
    ASSERT(pIPtr->getFieldType("m_byte") == PdxFieldTypes::BYTE,
           "Type Value BYTE Mismatch");

    pIPtr->getField("m_sbyte", byteVal);
    ASSERT(pdxobjPtr->getSByte() == byteVal, "Sbyte values should be equal");
    ASSERT(pIPtr->getFieldType("m_sbyte") == PdxFieldTypes::BYTE,
           "Type Value BYTE Mismatch");

    int16_t shortVal = 0;
    pIPtr->getField("m_int16", shortVal);
    ASSERT(pdxobjPtr->getShort() == shortVal, "shortVal should be equal");
    ASSERT(pIPtr->getFieldType("m_int16") == PdxFieldTypes::SHORT,
           "Type Value SHORT Mismatch");

    pIPtr->getField("m_uint16", shortVal);
    ASSERT(pdxobjPtr->getUint16() == shortVal, "m_uint16 should be equal");
    ASSERT(pIPtr->getFieldType("m_uint16") == PdxFieldTypes::SHORT,
           "Type Value SHORT Mismatch");

    int val = 0;
    pIPtr->getField("m_int32", val);
    ASSERT(pdxobjPtr->getInt() == val, "int32 values should be equal");
    ASSERT(pIPtr->getFieldType("m_int32") == PdxFieldTypes::INT,
           "Type Value INT Mismatch");

    pIPtr->getField("m_uint32", val);
    ASSERT(pdxobjPtr->getUInt() == val, "m_uint32 values should be equal");
    ASSERT(pIPtr->getFieldType("m_uint32") == PdxFieldTypes::INT,
           "Type Value INT Mismatch");

    int64_t longVal = 0;
    pIPtr->getField("m_long", longVal);
    ASSERT(pdxobjPtr->getLong() == longVal, "int64 values should be equal");
    ASSERT(pIPtr->getFieldType("m_long") == PdxFieldTypes::LONG,
           "Type Value LONG Mismatch");

    pIPtr->getField("m_ulong", longVal);
    ASSERT(pdxobjPtr->getULong() == longVal, "m_ulong values should be equal");
    ASSERT(pIPtr->getFieldType("m_ulong") == PdxFieldTypes::LONG,
           "Type Value LONG Mismatch");

    float floatVal = 0.0f;
    pIPtr->getField("m_float", floatVal);
    ASSERT(pdxobjPtr->getFloat() == floatVal, "floatVal should be equal");
    ASSERT(pIPtr->getFieldType("m_float") == PdxFieldTypes::FLOAT,
           "Type Value FLOAT Mismatch");

    double doubleVal = 0.0;
    pIPtr->getField("m_double", doubleVal);
    ASSERT(pdxobjPtr->getDouble() == doubleVal, "doubleVal should be equal");
    ASSERT(pIPtr->getFieldType("m_double") == PdxFieldTypes::DOUBLE,
           "Type Value DOUBLE Mismatch");

    char* stringVal = NULL;
    pIPtr->getField("m_string", &stringVal);
    ASSERT(strcmp(stringVal, pdxobjPtr->getString()) == 0,
           "stringVal should be equal");
    ASSERT(pIPtr->getFieldType("m_string") == PdxFieldTypes::STRING,
           "Type Value STRING Mismatch");

    char** stringArrayVal = NULL;
    int32_t stringArrayLen = 0;
    pIPtr->getField("m_stringArray", &stringArrayVal, stringArrayLen);
    ASSERT(genericValCompare(pdxobjPtr->getStringArrayLength(),
                             stringArrayLen) == true,
           "stringArrayLen should be equal");
    ASSERT(pIPtr->getFieldType("m_stringArray") == PdxFieldTypes::STRING_ARRAY,
           "Type Value STRING_ARRAY Mismatch");
    char** strArray = pdxobjPtr->getStringArray();
    for (int i = 0; i < stringArrayLen; i++) {
      ASSERT(strcmp(strArray[i], stringArrayVal[i]) == 0,
             "All stringVals should be equal");
    }

    signed char* byteArray = NULL;
    int32_t byteArrayLength = 0;
    pIPtr->getField("m_byteArray", &byteArray, byteArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getByteArrayLength(),
                             byteArrayLength) == true,
           "byteArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getByteArray(), byteArray,
                          byteArrayLength) == true,
           "byteArray should be equal");
    ASSERT(pIPtr->getFieldType("m_byteArray") == PdxFieldTypes::BYTE_ARRAY,
           "Type Value BYTE_ARRAY Mismatch");

    wchar_t* charArray = NULL;
    int32_t charArrayLength = 0;
    pIPtr->getField("m_charArray", &charArray, charArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getCharArrayLength(),
                             charArrayLength) == true,
           "charArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getCharArray(), charArray,
                          charArrayLength) == true,
           "charArray should be equal");
    ASSERT(pIPtr->getFieldType("m_charArray") == PdxFieldTypes::CHAR_ARRAY,
           "Type Value CHAR_ARRAY Mismatch");

    pIPtr->getField("m_sbyteArray", &byteArray, byteArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getByteArrayLength(),
                             byteArrayLength) == true,
           "sbyteArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getSByteArray(), byteArray,
                          byteArrayLength) == true,
           "m_sbyteArray should be equal");
    ASSERT(pIPtr->getFieldType("m_sbyteArray") == PdxFieldTypes::BYTE_ARRAY,
           "Type Value BYTE_ARRAY Mismatch");

    bool* boolArray = NULL;
    int32_t boolArrayLength = 0;
    pIPtr->getField("m_boolArray", &boolArray, boolArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getBoolArrayLength(),
                             boolArrayLength) == true,
           "boolArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getBoolArray(), boolArray,
                          boolArrayLength) == true,
           "boolArray should be equal");
    ASSERT(pIPtr->getFieldType("m_boolArray") == PdxFieldTypes::BOOLEAN_ARRAY,
           "Type Value BOOLEAN_ARRAY Mismatch");

    int16_t* shortArray = NULL;
    int32_t shortArrayLength = 0;
    pIPtr->getField("m_int16Array", &shortArray, shortArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getShortArrayLength(),
                             shortArrayLength) == true,
           "shortArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getShortArray(), shortArray,
                          shortArrayLength) == true,
           "shortArray should be equal");
    ASSERT(pIPtr->getFieldType("m_int16Array") == PdxFieldTypes::SHORT_ARRAY,
           "Type Value SHORT_ARRAY Mismatch");

    pIPtr->getField("m_uint16Array", &shortArray, shortArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getShortArrayLength(),
                             shortArrayLength) == true,
           "shortArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getUInt16Array(), shortArray,
                          shortArrayLength) == true,
           "m_uint16Array should be equal");
    ASSERT(pIPtr->getFieldType("m_uint16Array") == PdxFieldTypes::SHORT_ARRAY,
           "Type Value SHORT_ARRAY Mismatch");

    int32_t* intArray = NULL;
    int32_t intArrayLength = 0;
    pIPtr->getField("m_int32Array", &intArray, intArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getIntArrayLength(), intArrayLength) ==
               true,
           "intArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getIntArray(), intArray, intArrayLength) ==
               true,
           "intArray should be equal");
    ASSERT(pIPtr->getFieldType("m_int32Array") == PdxFieldTypes::INT_ARRAY,
           "Type Value INT_ARRAY Mismatch");

    pIPtr->getField("m_uint32Array", &intArray, intArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getIntArrayLength(), intArrayLength) ==
               true,
           "intArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getUIntArray(), intArray,
                          intArrayLength) == true,
           "m_uint32Array should be equal");
    ASSERT(pIPtr->getFieldType("m_uint32Array") == PdxFieldTypes::INT_ARRAY,
           "Type Value INT_ARRAY Mismatch");

    int64_t* longArray = NULL;
    int32_t longArrayLength = 0;
    pIPtr->getField("m_longArray", &longArray, longArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getLongArrayLength(),
                             longArrayLength) == true,
           "longArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getLongArray(), longArray,
                          longArrayLength) == true,
           "longArray should be equal");
    ASSERT(pIPtr->getFieldType("m_longArray") == PdxFieldTypes::LONG_ARRAY,
           "Type Value LONG_ARRAY Mismatch");

    pIPtr->getField("m_ulongArray", &longArray, longArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getLongArrayLength(),
                             longArrayLength) == true,
           "longArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getULongArray(), longArray,
                          longArrayLength) == true,
           "m_ulongArray should be equal");
    ASSERT(pIPtr->getFieldType("m_ulongArray") == PdxFieldTypes::LONG_ARRAY,
           "Type Value LONG_ARRAY Mismatch");

    double* doubleArray = NULL;
    int32_t doubleArrayLength = 0;
    pIPtr->getField("m_doubleArray", &doubleArray, doubleArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getDoubleArrayLength(),
                             doubleArrayLength) == true,
           "doubleArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getDoubleArray(), doubleArray,
                          doubleArrayLength) == true,
           "doubleArray should be equal");
    ASSERT(pIPtr->getFieldType("m_doubleArray") == PdxFieldTypes::DOUBLE_ARRAY,
           "Type Value DOUBLE_ARRAY Mismatch");

    float* floatArray = NULL;
    int32_t floatArrayLength = 0;
    pIPtr->getField("m_floatArray", &floatArray, floatArrayLength);
    ASSERT(genericValCompare(pdxobjPtr->getFloatArrayLength(),
                             floatArrayLength) == true,
           "floatArrayLength should be equal");
    ASSERT(genericCompare(pdxobjPtr->getFloatArray(), floatArray,
                          floatArrayLength) == true,
           "floatArray should be equal");
    ASSERT(pIPtr->getFieldType("m_floatArray") == PdxFieldTypes::FLOAT_ARRAY,
           "Type Value FLOAT_ARRAY Mismatch");

    CacheablePtr object = NULLPTR;
    pIPtr->getField("m_pdxEnum", object);
    ASSERT(object != NULLPTR, "enumObject should not be NULL");
    CacheableEnumPtr enumObject = dynCast<CacheableEnumPtr>(object);
    ASSERT(
        enumObject->getEnumOrdinal() == pdxobjPtr->getEnum()->getEnumOrdinal(),
        "enumObject ordinal should be equal");
    ASSERT(strcmp(enumObject->getEnumClassName(),
                  pdxobjPtr->getEnum()->getEnumClassName()) == 0,
           "enumObject classname should be equal");
    ASSERT(strcmp(enumObject->getEnumName(),
                  pdxobjPtr->getEnum()->getEnumName()) == 0,
           "enumObject enumname should be equal");
    ASSERT(pIPtr->getFieldType("m_pdxEnum") == PdxFieldTypes::OBJECT,
           "Type Value OBJECT Mismatch");

    CacheableDatePtr dateObject = NULLPTR;
    pIPtr->getField("m_dateTime", dateObject);
    ASSERT(dateObject != NULLPTR, "date should not be NULL");
    ASSERT((*(dateObject.ptr()) == *(pdxobjPtr->getDate().ptr())) == true,
           "dateObject should be equal");
    ASSERT(pIPtr->getFieldType("m_dateTime") == PdxFieldTypes::DATE,
           "Type Value DATE Mismatch");

    CacheablePtr object2 = NULLPTR;
    pIPtr->getField("m_map", object2);
    ASSERT(object2 != NULLPTR, "object2 should not be NULL");
    CacheableHashMapPtr mapObject = dynCast<CacheableHashMapPtr>(object2);
    ASSERT(genericValCompare(pdxobjPtr->getHashMap()->size(),
                             mapObject->size()) == true,
           "mapobject size should be equal");
    ASSERT(pIPtr->getFieldType("m_map") == PdxFieldTypes::OBJECT,
           "Type Value OBJECT Mismatch");

    pIPtr->getField("m_vector", object2);
    ASSERT(object2 != NULLPTR, "object2 should not be NULL");
    CacheableVectorPtr vec = dynCast<CacheableVectorPtr>(object2);
    ASSERT(
        genericValCompare(pdxobjPtr->getVector()->size(), vec->size()) == true,
        "vec size should be equal");
    ASSERT(pIPtr->getFieldType("m_vector") == PdxFieldTypes::OBJECT,
           "Type Value OBJECT Mismatch");

    pIPtr->getField("m_arraylist", object2);
    ASSERT(object2 != NULLPTR, "object2 should not be NULL");
    CacheableArrayListPtr arrList = dynCast<CacheableArrayListPtr>(object2);
    ASSERT(genericValCompare(pdxobjPtr->getArrayList()->size(),
                             arrList->size()) == true,
           "arrList size should be equal");
    ASSERT(pIPtr->getFieldType("m_arraylist") == PdxFieldTypes::OBJECT,
           "Type Value OBJECT Mismatch");

    pIPtr->getField("m_chs", object2);
    ASSERT(object2 != NULLPTR, "object2 should not be NULL");
    CacheableHashSetPtr hashSet = dynCast<CacheableHashSetPtr>(object2);
    ASSERT(genericValCompare(pdxobjPtr->getHashSet()->size(),
                             hashSet->size()) == true,
           "hashSet size should be equal");
    ASSERT(pIPtr->getFieldType("m_chs") == PdxFieldTypes::OBJECT,
           "Type Value OBJECT Mismatch");

    pIPtr->getField("m_clhs", object2);
    ASSERT(object2 != NULLPTR, "object2 should not be NULL");
    CacheableLinkedHashSetPtr linkedHashSet =
        dynCast<CacheableLinkedHashSetPtr>(object2);
    ASSERT(genericValCompare(pdxobjPtr->getLinkedHashSet()->size(),
                             linkedHashSet->size()) == true,
           "linkedHashSet size should be equal");
    ASSERT(pIPtr->getFieldType("m_clhs") == PdxFieldTypes::OBJECT,
           "Type Value OBJECT Mismatch");

    int8_t** byteByteArrayVal = NULL;
    int32_t byteArrayLen = 0;
    int32_t* elementLength = NULL;
    pIPtr->getField("m_byteByteArray", &byteByteArrayVal, byteArrayLen,
                    elementLength);
    ASSERT(genericValCompare(pdxobjPtr->getbyteByteArrayLength(),
                             byteArrayLen) == true,
           "byteByteArrayLength should be equal");
    int8_t** bytArray = pdxobjPtr->getArrayOfByteArrays();
    ASSERT(generic2DCompare(byteByteArrayVal, bytArray, byteArrayLen,
                            elementLength) == true,
           "byteByteArray should be equal");
    ASSERT(pIPtr->getFieldType("m_byteByteArray") ==
               PdxFieldTypes::ARRAY_OF_BYTE_ARRAYS,
           "Type Value ARRAY_OF_BYTE_ARRAYS Mismatch");

    CacheableObjectArrayPtr objectArray = NULLPTR;
    pIPtr->getField("m_objectArray", objectArray);
    ASSERT(objectArray != NULLPTR, "objectArray should not be NULL");
    ASSERT(genericValCompare(pdxobjPtr->getCacheableObjectArray()->size(),
                             objectArray->size()) == true,
           "objectArray size should be equal");
    ASSERT(pIPtr->getFieldType("m_objectArray") == PdxFieldTypes::OBJECT_ARRAY,
           "Type Value OBJECT_ARRAY Mismatch");

    CacheableObjectArrayPtr objectArrayEmptyFieldName = NULLPTR;
    pIPtr->getField("", objectArrayEmptyFieldName);
    ASSERT(objectArrayEmptyFieldName != NULLPTR,
           "objectArrayEmptyFieldName should not be NULL");
    ASSERT(genericValCompare(
               pdxobjPtr->getCacheableObjectArrayEmptyPdxFieldName()->size(),
               objectArrayEmptyFieldName->size()) == true,
           "objectArrayEmptyFieldName size should be equal");

    for (int i = 0; i < objectArray->size(); i++) {
      PdxInstancePtr pi = objectArray->at(i);
      LOG("PdxInstancePtr obtained from CacheableObjectArray");

      PdxInstanceFactoryPtr pifPtr =
          cacheHelper->getCache()->createPdxInstanceFactory("PdxTests.Address");
      LOG("PdxInstanceFactoryPtr created for PdxTests.Address....");

      pifPtr->writeInt("_aptNumber", i + 1);
      char streetStr[256] = {0};
      sprintf(streetStr, "street%d", i);
      pifPtr->writeString("_street", streetStr);
      char cityStr[256] = {0};
      sprintf(cityStr, "city%d", i);
      pifPtr->writeString("_city", cityStr);

      AddressPtr addrPtr = dynCast<AddressPtr>(pi->getObject());
      LOG("AddressPtr created using PdxInstance getObject()....");
      AddressPtr newAddrPtr(new Address(i + 1, streetStr, cityStr));
      LOG("AddressPtr created using new....");
      ASSERT(addrPtr.ptr()->equals(*(newAddrPtr.ptr())) == true,
             "Both PdxInstances should be equal.");

      PdxInstancePtr retPtr = pifPtr->create();
      LOG("PdxInstancePtr created....");

      bool ret = false;
      ret = (*pi.ptr() == *retPtr.ptr());
      LOGINFO("PdxObject ret = %d ", ret);
      ASSERT(ret == true, "Both PdxInstances should be equal.");
    }
    LOGINFO("PdxInstancePtr for ParentPdx accessPdxInstance ");
    pIPtr = dynCast<PdxInstancePtr>(rptr->get(keyport1));
    LOGINFO("PdxInstancePtr for ParentPdx object got ");

    CacheablePtr childObjPtr;
    pIPtr->getField("m_childPdx", childObjPtr);
    ASSERT(childObjPtr != NULLPTR, "childObjPtr should not be NULL");
    LOGINFO("got childPdx field ");
    PdxInstancePtr cpi = dynCast<PdxInstancePtr>(childObjPtr);
    LOGINFO("cast to pdxinstance done ");
    ChildPdxPtr cpo = dynCast<ChildPdxPtr>(cpi->getObject());
    LOGINFO("got childPdx getObject ");
    ChildPdxPtr childpdxobjPtr(new ChildPdx(1));
    LOGINFO("created new childPdx");
    ASSERT((cpo.ptr()->equals(*childpdxobjPtr.ptr())) == true,
           "child pdx should be equal");
    LOG("accessPdxInstance complete");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, modifyPdxInstance)
  {
    LOG("modifyPdxInstance started ");

    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyport = CacheableKey::create("pdxput");
    CacheableKeyPtr keyport1 = CacheableKey::create("pdxput2");

    PdxInstancePtr pIPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    LOG("modifyPdxInstance get complete.");

    WritablePdxInstancePtr wpiPtr(pIPtr->createWriter());

    ASSERT(pIPtr != NULLPTR, "pIPtr != NULLPTR expected");
    int val = 0;
    int newVal = 0;
    ASSERT(pIPtr->hasField("m_int32") == true, "m_id1 = true expected");
    pIPtr->getField("m_int32", val);
    wpiPtr->setField("m_int32", val + 1);
    rptr->put(keyport, wpiPtr);
    PdxInstancePtr newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_int32") == true, "m_int32 = true expected");
    newPiPtr->getField("m_int32", newVal);
    ASSERT(val + 1 == newVal, "val + 1 == newVal expected");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_int32", false);
      FAIL(
          "setField on m_int32 with bool value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_int32 with bool value caught expected "
          "IllegalStateException");
    }

    bool boolVal = true;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_bool", false);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_bool") == true, "m_bool = true expected");
    newPiPtr->getField("m_bool", boolVal);
    ASSERT(boolVal == false, "bool is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_bool", 500);
      FAIL(
          "setField on m_bool with int value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_bool with int value caught expected "
          "IllegalStateException");
    }

    wchar_t charVal = ' ';
    wchar_t setVal = 'D';
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_char", setVal);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_char") == true, "m_char = true expected");
    newPiPtr->getField("m_char", charVal);
    ASSERT(charVal == setVal, "char is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_char", 500.0);
      FAIL(
          "setField on m_char with float value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_char with float value caught expected "
          "IllegalStateException");
    }

    signed char byteVal = 0;
    signed char setByteVal = 0x75;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_byte", setByteVal);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_byte") == true, "m_byte = true expected");
    newPiPtr->getField("m_byte", byteVal);
    ASSERT(byteVal == setByteVal, "byte is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    signed char setSByteVal = 0x57;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_sbyte", setSByteVal);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_sbyte") == true, "m_sbyte = true expected");
    newPiPtr->getField("m_sbyte", byteVal);
    ASSERT(byteVal == setSByteVal, "m_sbyte is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_byte", static_cast<int64_t>(0x56787878));
      FAIL(
          "setField on m_byte with int64_t value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_byte with int64_t value caught expected "
          "IllegalStateException");
    }

    int16_t shortVal = 0;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_int16", static_cast<int16_t>(0x5678));
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_int16") == true, "m_int16 = true expected");
    newPiPtr->getField("m_int16", shortVal);
    ASSERT(shortVal == 0x5678, "short is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_int16", static_cast<int64_t>(0x56787878));
      FAIL(
          "setField on m_int16 with int64_t value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_int16 with int64_t value caught expected "
          "IllegalStateException");
    }

    int64_t longVal = 0;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_long", static_cast<int64_t>(0x56787878));
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_long") == true, "m_long = true expected");
    newPiPtr->getField("m_long", longVal);
    ASSERT(longVal == 0x56787878, "long is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_long", 18389.34f);
      FAIL(
          "setField on m_long with float value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_long with float value caught expected "
          "IllegalStateException");
    }

    float fVal = 0.0f;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_float", 18389.34f);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_float") == true, "m_float = true expected");
    newPiPtr->getField("m_float", fVal);
    ASSERT(fVal == 18389.34f, "fval is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_float", 'D');
      FAIL(
          "setField on m_float with char value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_float with char value caught expected "
          "IllegalStateException");
    }

    double dVal = 0.0;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_double", 18389.34);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_double") == true, "m_double = true expected");
    newPiPtr->getField("m_double", dVal);
    ASSERT(dVal == 18389.34, "dval is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_double", 'D');
      FAIL(
          "setField on m_double with char value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_double with char value caught expected "
          "IllegalStateException");
    }

    int arrayLen = 0;

    bool setBoolArray[] = {true, false, true, false, true, true, false, true};
    bool* getBoolArray = NULL;
    wpiPtr->setField("m_boolArray", setBoolArray, 8);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_boolArray") == true,
           "m_boolArray = true expected");
    newPiPtr->getField("m_boolArray", &getBoolArray, arrayLen);
    ASSERT(arrayLen == 8, "Arraylength == 8 expected");
    ASSERT(genericCompare(setBoolArray, getBoolArray, arrayLen) == true,
           "boolArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_boolArray", 'D');
      FAIL(
          "setField on m_boolArray with char value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_boolArray with char value caught expected "
          "IllegalStateException");
    }

    signed char setByteArray[] = {0x34, 0x64, 0x34, 0x64};
    signed char* getByteArray = NULL;
    wpiPtr->setField("m_byteArray", setByteArray, 4);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_byteArray") == true,
           "m_byteArray = true expected");
    newPiPtr->getField("m_byteArray", &getByteArray, arrayLen);
    ASSERT(arrayLen == 4, "Arraylength == 4 expected");
    ASSERT(genericCompare(setByteArray, getByteArray, arrayLen) == true,
           "byteArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_byteArray", "string");
      FAIL(
          "setField on m_byteArray with string value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_byteArray with string value caught expected "
          "IllegalStateException");
    }

    wchar_t setCharArray[] = {'c', 'v', 'c', 'v'};
    wchar_t* getCharArray = NULL;
    wpiPtr->setField("m_charArray", setCharArray, 4);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_charArray") == true,
           "m_charArray = true expected");
    newPiPtr->getField("m_charArray", &getCharArray, arrayLen);
    ASSERT(arrayLen == 4, "Arraylength == 4 expected");
    ASSERT(genericCompare(setCharArray, getCharArray, arrayLen) == true,
           "charArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_charArray", "string");
      FAIL(
          "setField on m_charArray with string value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_charArray with string value caught expected "
          "IllegalStateException");
    }

    int16_t setShortArray[] = {0x2332, 0x4545, 0x88, 0x898};
    int16_t* getShortArray = NULL;
    wpiPtr->setField("m_int16Array", setShortArray, 4);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_int16Array") == true,
           "m_int16Array = true expected");
    newPiPtr->getField("m_int16Array", &getShortArray, arrayLen);
    ASSERT(arrayLen == 4, "Arraylength == 4 expected");
    ASSERT(genericCompare(setShortArray, getShortArray, arrayLen) == true,
           "shortArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_int16Array", setCharArray);
      FAIL(
          "setField on m_int16Array with setCharArray value should throw "
          "expected IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_int16Array with setCharArray value caught expected "
          "IllegalStateException");
    }

    int32_t setIntArray[3] = {23, 676868, 34343};
    int32_t* newValArray = NULL;
    wpiPtr->setField("m_int32Array", setIntArray, 3);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_int32Array") == true,
           "m_int32Array = true expected");
    newPiPtr->getField("m_int32Array", &newValArray, arrayLen);
    ASSERT(arrayLen == 3, "Arraylength == 3 expected");
    ASSERT(genericCompare(setIntArray, newValArray, arrayLen) == true,
           "intArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_int32Array", setShortArray, 3);
      FAIL(
          "setField on m_int32Array with setShortArray value should throw "
          "expected IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_int32Array with setShortArray value caught expected "
          "IllegalStateException");
    }

    int64_t setLongArray[] = {3245435, 3425435};
    int64_t* getLongArray = NULL;
    wpiPtr->setField("m_longArray", setLongArray, 2);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_longArray") == true,
           "m_longArray = true expected");
    newPiPtr->getField("m_longArray", &getLongArray, arrayLen);
    ASSERT(arrayLen == 2, "Arraylength == 2 expected");
    ASSERT(genericCompare(setLongArray, getLongArray, arrayLen) == true,
           "longArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_longArray", setIntArray, 3);
      FAIL(
          "setField on m_longArray with setIntArray value should throw "
          "expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_longArray with setIntArray value caught expected "
          "IllegalStateException");
    }

    float setFloatArray[] = {232.565f, 234323354.67f};
    float* getFloatArray = NULL;
    wpiPtr->setField("m_floatArray", setFloatArray, 2);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_floatArray") == true,
           "m_floatArray = true expected");
    newPiPtr->getField("m_floatArray", &getFloatArray, arrayLen);
    ASSERT(arrayLen == 2, "Arraylength == 2 expected");
    ASSERT(genericCompare(setFloatArray, getFloatArray, arrayLen) == true,
           "floatArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_floatArray", setLongArray, 2);
      FAIL(
          "setField on m_floatArray with setLongArray value should throw "
          "expected IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_floatArray with setLongArray value caught expected "
          "IllegalStateException");
    }

    double setDoubleArray[] = {23423432.00, 43242354315.00};
    double* getDoubleArray = NULL;
    wpiPtr->setField("m_doubleArray", setDoubleArray, 2);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_doubleArray") == true,
           "m_doubleArray = true expected");
    newPiPtr->getField("m_doubleArray", &getDoubleArray, arrayLen);
    ASSERT(arrayLen == 2, "Arraylength == 2 expected");
    ASSERT(genericCompare(setDoubleArray, getDoubleArray, arrayLen) == true,
           "doubleArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_doubleArray", setFloatArray, 2);
      FAIL(
          "setField on m_doubleArray with setFloatArray value should throw "
          "expected IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_doubleArray with setFloatArray value caught expected "
          "IllegalStateException");
    }

    const wchar_t* setWideString = L"change the string";
    wpiPtr->setField("m_string", setWideString);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_string") == true, "m_string = true expected");
    wchar_t* wideStringVal = NULL;
    ASSERT(newPiPtr->hasField("m_string") == true, "m_string = true expected");
    newPiPtr->getField("m_string", &wideStringVal);
    ASSERT(wcscmp(wideStringVal, setWideString) == 0,
           "wide stringVal should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_string", setFloatArray, 2);
      FAIL(
          "setField on m_string with setFloatArray value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_string with setFloatArray value caught expected "
          "IllegalStateException");
    }

    const char* setString = "change the string";
    wpiPtr->setField("m_string", setString);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_string") == true, "m_string = true expected");
    char* stringVal = NULL;
    ASSERT(newPiPtr->hasField("m_string") == true, "m_string = true expected");
    newPiPtr->getField("m_string", &stringVal);
    ASSERT(strcmp(stringVal, setString) == 0, "stringVal should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    CacheableDatePtr dateVal;
    wpiPtr = pIPtr->createWriter();
    time_t timeofday = 0;
    const ACE_Time_Value currentTime = ACE_OS::gettimeofday();
    timeofday = currentTime.sec();
    CacheableDatePtr datePtr = CacheableDate::create(timeofday);
    wpiPtr->setField("m_dateTime", datePtr);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_dateTime") == true,
           "m_dateTime = true expected");
    ASSERT(pIPtr->hasField("m_dateTime") == true, "m_date = true expected");
    newPiPtr->getField("m_dateTime", dateVal);
    ASSERT((*(dateVal.ptr()) == *(datePtr.ptr())) == true,
           "dateObject should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_dateTime", setString);
      FAIL(
          "setField on m_dateTime with setString value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_dateTime with setString value caught expected "
          "IllegalStateException");
    }

    CacheablePtr object;
    wpiPtr = pIPtr->createWriter();
    CacheableVectorPtr setVec = CacheableVector::create();
    setVec->push_back(CacheableInt32::create(3));
    setVec->push_back(CacheableInt32::create(4));
    wpiPtr->setField("m_vector", (CacheablePtr)setVec);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_vector") == true, "m_vector = true expected");
    ASSERT(pIPtr->hasField("m_vector") == true, "m_vector = true expected");
    newPiPtr->getField("m_vector", object);
    CacheableVectorPtr vecVal = dynCast<CacheableVectorPtr>(object);
    ASSERT(genericValCompare(setVec->size(), vecVal->size()) == true,
           "vec size should be equal");
    for (int j = 0; j < vecVal->size(); j++) {
      genericValCompare(setVec->at(j), vecVal->at(j));
    }

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_vector", setString);
      FAIL(
          "setField on m_vector with setString value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_vector with setString value caught expected "
          "IllegalStateException");
    }

    wpiPtr = pIPtr->createWriter();
    CacheableArrayListPtr setarr = CacheableArrayList::create();
    setarr->push_back(CacheableInt32::create(3));
    setarr->push_back(CacheableInt32::create(4));
    setarr->push_back(CacheableInt32::create(5));
    wpiPtr->setField("m_arraylist", (CacheablePtr)setarr);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_arraylist") == true,
           "m_arraylist = true expected");
    ASSERT(pIPtr->hasField("m_arraylist") == true,
           "m_arraylist = true expected");
    newPiPtr->getField("m_arraylist", object);
    CacheableArrayListPtr arrVal = dynCast<CacheableArrayListPtr>(object);
    ASSERT(genericValCompare(setarr->size(), arrVal->size()) == true,
           "arrList size should be equal");
    for (int j = 0; j < arrVal->size(); j++) {
      genericValCompare(setarr->at(j), arrVal->at(j));
    }

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_arraylist", setFloatArray, 2);
      FAIL(
          "setField on m_arraylist with setFloatArray value should throw "
          "expected IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_arraylist with setFloatArray value caught expected "
          "IllegalStateException");
    }

    wpiPtr = pIPtr->createWriter();
    CacheableHashSetPtr hashset = CacheableHashSet::create();
    hashset->insert(CacheableInt32::create(3));
    hashset->insert(CacheableInt32::create(4));
    hashset->insert(CacheableInt32::create(5));
    wpiPtr->setField("m_chs", (CacheablePtr)hashset);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_chs") == true, "m_chs = true expected");
    ASSERT(pIPtr->hasField("m_chs") == true, "m_chs = true expected");
    newPiPtr->getField("m_chs", object);
    CacheableHashSetPtr hashsetVal = dynCast<CacheableHashSetPtr>(object);
    ASSERT(genericValCompare(hashset->size(), hashsetVal->size()) == true,
           "m_chs size should be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_chs", 100);
      FAIL(
          "setField on m_chs with int value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_chs with int value caught expected "
          "IllegalStateException");
    }

    wpiPtr = pIPtr->createWriter();
    CacheableHashMapPtr hashmap = CacheableHashMap::create();
    hashmap->insert(CacheableInt32::create(3), CacheableInt32::create(3));
    hashmap->insert(CacheableInt32::create(4), CacheableInt32::create(4));
    hashmap->insert(CacheableInt32::create(5), CacheableInt32::create(5));
    wpiPtr->setField("m_map", (CacheablePtr)hashmap);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_map") == true, "m_map = true expected");
    ASSERT(pIPtr->hasField("m_map") == true, "m_map = true expected");
    newPiPtr->getField("m_map", object);
    CacheableHashMapPtr hashmapVal = dynCast<CacheableHashMapPtr>(object);
    ASSERT(genericValCompare(hashmap->size(), hashmapVal->size()) == true,
           "m_map size should be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_map", 0.0f);
      FAIL(
          "setField on m_map with float value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_map with float value caught expected "
          "IllegalStateException");
    }

    wpiPtr = pIPtr->createWriter();
    CacheableLinkedHashSetPtr linkedhashset = CacheableLinkedHashSet::create();
    linkedhashset->insert(CacheableInt32::create(3));
    linkedhashset->insert(CacheableInt32::create(4));
    linkedhashset->insert(CacheableInt32::create(5));
    wpiPtr->setField("m_clhs", (CacheablePtr)linkedhashset);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(newPiPtr->hasField("m_clhs") == true, "m_clhs = true expected");
    ASSERT(pIPtr->hasField("m_clhs") == true, "m_clhs = true expected");
    newPiPtr->getField("m_clhs", object);
    CacheableLinkedHashSetPtr linkedhashsetVal =
        dynCast<CacheableLinkedHashSetPtr>(object);
    ASSERT(genericValCompare(linkedhashsetVal->size(), linkedhashset->size()) ==
               true,
           "m_clhs size should be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_clhs", 'c');
      FAIL(
          "setField on m_clhs with char value should throw expected "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_clhs with char value caught expected "
          "IllegalStateException");
    }

    LOGINFO("Testing byteByteArray");
    int8_t** getbyteByteArray = NULL;
    int byteByteArrayLength = 0;
    int32_t* elementLength = NULL;
    int8_t** setbyteByteArray = new int8_t*[4];
    setbyteByteArray[0] = new int8_t[1];
    setbyteByteArray[1] = new int8_t[2];
    setbyteByteArray[2] = new int8_t[1];
    setbyteByteArray[3] = new int8_t[2];
    setbyteByteArray[0][0] = 0x23;
    setbyteByteArray[1][0] = 0x34;
    setbyteByteArray[1][1] = 0x55;
    setbyteByteArray[2][0] = 0x23;
    setbyteByteArray[3][0] = 0x34;
    setbyteByteArray[3][1] = 0x55;
    wpiPtr = pIPtr->createWriter();
    int* lengthArr = new int[4];
    lengthArr[0] = 1;
    lengthArr[1] = 2;
    lengthArr[2] = 1;
    lengthArr[3] = 2;
    wpiPtr->setField("m_byteByteArray", setbyteByteArray, 4, lengthArr);
    LOGINFO("Testing byteByteArray setField");
    rptr->put(keyport, wpiPtr);
    LOGINFO("Testing byteByteArray put");
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    LOGINFO("Testing byteByteArray get");
    ASSERT(newPiPtr->hasField("m_byteByteArray") == true,
           "m_byteByteArray = true expected");
    LOGINFO("Testing byteByteArray hasField");
    ASSERT(pIPtr->hasField("m_byteByteArray") == true,
           "m_byteByteArray = true expected");
    newPiPtr->getField("m_byteByteArray", &getbyteByteArray,
                       byteByteArrayLength, elementLength);
    LOGINFO("Testing byteByteArray getField");
    ASSERT(genericValCompare(4, byteByteArrayLength) == true,
           "byteByteArrayLength should be equal");
    ASSERT(generic2DCompare(getbyteByteArray, setbyteByteArray,
                            byteByteArrayLength, elementLength) == true,
           "byteByteArray should be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_byteByteArray", (CacheablePtr)linkedhashset);
      FAIL(
          "setField on m_byteByteArray with linkedhashset value should throw "
          "expected IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_byteByteArray with linkedhashset value caught "
          "expected "
          "IllegalStateException");
    }

    wchar_t** setWideStringArray = new wchar_t*[3];
    const wchar_t* wstr1 = L"test1";
    const wchar_t* wstr2 = L"test2";
    const wchar_t* wstr3 = L"test3";
    size_t size = wcslen(wstr1);
    for (int i = 0; i < 3; i++) {
      setWideStringArray[i] = new wchar_t[size];
    }
    setWideStringArray[0] = const_cast<wchar_t*>(wstr1);
    setWideStringArray[1] = const_cast<wchar_t*>(wstr2);
    setWideStringArray[2] = const_cast<wchar_t*>(wstr3);
    wchar_t** getWideStringArray = NULL;
    wpiPtr->setField("m_stringArray", setWideStringArray, 3);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_stringArray") == true,
           "m_stringArray = true expected");
    newPiPtr->getField("m_stringArray", &getWideStringArray, arrayLen);
    ASSERT(arrayLen == 3, "Arraylength == 3 expected");
    for (int i = 0; i < arrayLen; i++) {
      LOGINFO("set string is %ls ", setWideStringArray[i]);
      LOGINFO("get string is %ls ", getWideStringArray[i]);
      ASSERT(wcscmp(setWideStringArray[i], getWideStringArray[i]) == 0,
             "All stringVals should be equal");
    }
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    try {
      wpiPtr->setField("m_stringArray", setbyteByteArray);
      FAIL(
          "setField on m_stringArray with setbyteByteArray value should throw "
          "expected IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("setField on m_stringArray with setbyteByteArray value caught "
          "expected "
          "IllegalStateException");
    }

    char** setStringArray = new char*[3];
    const char* str1 = "test1";
    const char* str2 = "test2";
    const char* str3 = "test3";
    size = strlen(const_cast<char*>(str1));
    for (int i = 0; i < 3; i++) {
      setStringArray[i] = new char[size];
    }
    setStringArray[0] = const_cast<char*>(str1);
    setStringArray[1] = const_cast<char*>(str2);
    setStringArray[2] = const_cast<char*>(str3);
    char** getStringArray = NULL;
    wpiPtr->setField("m_stringArray", setStringArray, 3);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    newPiPtr->getField("m_stringArray", &getStringArray, arrayLen);
    ASSERT(arrayLen == 3, "Arraylength == 3 expected");
    for (int i = 0; i < arrayLen; i++) {
      LOGINFO("set string is %s ", setStringArray[i]);
      LOGINFO("get string is %s ", getStringArray[i]);
      ASSERT(strcmp(setStringArray[i], getStringArray[i]) == 0,
             "All stringVals should be equal");
    }
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    LOGINFO("PdxInstancePtr for ParentPdx modifyPdxInstance ");
    pIPtr = dynCast<PdxInstancePtr>(rptr->get(keyport1));
    LOGINFO("PdxInstancePtr for ParentPdx object got ");

    CacheablePtr childObjPtr;
    pIPtr->getField("m_childPdx", childObjPtr);
    ASSERT(childObjPtr != NULLPTR, "childObjPtr should not be NULL");
    LOGINFO("got childPdx field ");
    PdxInstancePtr cpi = dynCast<PdxInstancePtr>(childObjPtr);
    LOGINFO("cast to pdxinstance done ");
    wpiPtr = pIPtr->createWriter();
    ChildPdxPtr childpdxobjPtr(new ChildPdx(2));
    LOGINFO("created new childPdx");
    wpiPtr->setField("m_childPdx", (CacheablePtr)childpdxobjPtr);
    LOGINFO("childPdx seField done");
    rptr->put(keyport1, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport1));
    LOGINFO("ChildPdx object put get done");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");
    newPiPtr->getField("m_childPdx", childObjPtr);
    ASSERT(childObjPtr != NULLPTR, "childObjPtr should not be NULL");
    LOGINFO("got childPdx field ");
    PdxInstancePtr cpi1 = dynCast<PdxInstancePtr>(childObjPtr);
    ASSERT((*cpi.ptr() == *cpi1.ptr()) == false,
           "PdxInstance should not be equal");

    ChildPdxPtr cpo = dynCast<ChildPdxPtr>(cpi1->getObject());
    LOGINFO("got childPdx getObject ");
    ASSERT((cpo.ptr()->equals(*childpdxobjPtr.ptr())) == true,
           "child pdx should be equal");

    char parentCharVal = ' ';
    char parentCharSetVal = 'Z';
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_char", parentCharSetVal);
    rptr->put(keyport1, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport1));
    ASSERT(newPiPtr->hasField("m_char") == true, "m_char = true expected");
    newPiPtr->getField("m_char", parentCharVal);
    ASSERT(parentCharVal == parentCharSetVal, "char is not equal");

    wchar_t parentWideCharVal = ' ';
    wchar_t parentWideCharSetVal = L'Z';
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_wideChar", parentWideCharSetVal);
    rptr->put(keyport1, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport1));
    ASSERT(newPiPtr->hasField("m_wideChar") == true,
           "m_wideChar = true expected");
    newPiPtr->getField("m_wideChar", parentWideCharVal);
    ASSERT(parentWideCharVal == parentWideCharSetVal,
           "m_wideChar is not equal");

    wchar_t setParentWideCharArray[] = {L'c', L'v', L'c', L'v'};
    wchar_t* getParentWideCharArray = NULL;
    wpiPtr->setField("m_wideCharArray", setParentWideCharArray, 4);
    rptr->put(keyport1, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport1));
    ASSERT(newPiPtr->hasField("m_wideCharArray") == true,
           "m_wideCharArray = true expected");
    newPiPtr->getField("m_wideCharArray", &getParentWideCharArray, arrayLen);
    ASSERT(arrayLen == 4, "Arraylength == 4 expected");
    ASSERT(genericCompare(setParentWideCharArray, getParentWideCharArray,
                          arrayLen) == true,
           "m_wideCharArray should be equal");

    wchar_t setParentCharArray[] = {'c', 'v', 'c', 'v'};
    wchar_t* getParentCharArray = NULL;
    wpiPtr->setField("m_charArray", setParentCharArray, 4);
    rptr->put(keyport1, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport1));
    ASSERT(newPiPtr->hasField("m_charArray") == true,
           "m_charArray = true expected");
    newPiPtr->getField("m_charArray", &getParentCharArray, arrayLen);
    ASSERT(arrayLen == 4, "Arraylength == 4 expected");
    ASSERT(genericCompare(setParentCharArray, getParentCharArray, arrayLen) ==
               true,
           "charArray should be equal");
    LOG("modifyPdxInstance complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, modifyPdxInstanceAndCheckLocally)
  {
    LOG("modifyPdxInstanceAndCheckLocally started ");

    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);
    CacheableKeyPtr keyport = CacheableKey::create("pdxput");

    PdxInstancePtr pIPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    LOG("modifyPdxInstanceAndCheckLocally get complete.");

    LOG("Statistics for for (PdxTests.PdxType) PdxInstance ");
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(rptr.ptr()));

    LOGINFO(
        "pdxInstanceDeserializations for (PdxTests.PdxType) PdxInstance  = %d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializations());
    LOGINFO("pdxInstanceCreations for (PdxTests.PdxType) PdxInstance  = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations());
    LOGINFO(
        "pdxInstanceDeserializationTime for(PdxTests.PdxType) PdxInstance  = "
        "%d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializationTime());

    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializations() == 0,
           "pdxInstanceDeserialization should be equal to 0.");
    ASSERT(
        lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations() == 1,
        "pdxInstanceCreations should be equal to 1.");
    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializationTime() > 0,
           "pdxInstanceDeserializationTime should be greater than 0.");

    WritablePdxInstancePtr wpiPtr(pIPtr->createWriter());

    ASSERT(pIPtr != NULLPTR, "pIPtr != NULLPTR expected");
    int val = 0;
    int newVal = 0;
    ASSERT(pIPtr->hasField("m_int32") == true, "m_int32 = true expected");
    pIPtr->getField("m_int32", val);
    LOGINFO("PdxInstance val is %d ", val);
    ASSERT(val == 591768540, "val = 591768540 expected");

    wpiPtr->setField("m_int32", val + 1);
    rptr->put(keyport, wpiPtr);
    LOG("modifyPdxInstanceAndCheckLocally put complete.");
    PdxInstancePtr newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    LOG("modifyPdxInstanceAndCheckLocally get complete.");
    ASSERT(newPiPtr->hasField("m_int32") == true, "m_id1 = true expected");
    newPiPtr->getField("m_int32", newVal);
    LOGINFO("PdxInstance newVal is %d ", newVal);
    ASSERT(val + 1 == newVal, "val + 1 == newVal expected");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");
    int arrayLen = 0;

    bool setBoolArray[] = {true, false, true, false, true, true, false, true};
    bool* getBoolArray = NULL;
    wpiPtr->setField("m_boolArray", setBoolArray, 8);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_boolArray") == true,
           "m_boolArray = true expected");
    newPiPtr->getField("m_boolArray", &getBoolArray, arrayLen);
    ASSERT(arrayLen == 8, "Arraylength == 8 expected");
    ASSERT(genericCompare(setBoolArray, getBoolArray, arrayLen) == true,
           "boolArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    signed char setByteArray[] = {0x34, 0x64, 0x34, 0x64};
    signed char* getByteArray = NULL;
    wpiPtr->setField("m_byteArray", setByteArray, 4);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_byteArray") == true,
           "m_byteArray = true expected");
    newPiPtr->getField("m_byteArray", &getByteArray, arrayLen);
    ASSERT(arrayLen == 4, "Arraylength == 4 expected");
    ASSERT(genericCompare(setByteArray, getByteArray, arrayLen) == true,
           "byteArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    int16_t setShortArray[] = {0x2332, 0x4545, 0x88, 0x898};
    int16_t* getShortArray = NULL;
    wpiPtr->setField("m_int16Array", setShortArray, 4);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_int16Array") == true,
           "m_int16Array = true expected");
    newPiPtr->getField("m_int16Array", &getShortArray, arrayLen);
    ASSERT(arrayLen == 4, "Arraylength == 4 expected");
    ASSERT(genericCompare(setShortArray, getShortArray, arrayLen) == true,
           "shortArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    int32_t setIntArray[3] = {23, 676868, 34343};
    int32_t* newValArray = NULL;
    wpiPtr->setField("m_int32Array", setIntArray, 3);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_int32Array") == true,
           "m_int32Array = true expected");
    newPiPtr->getField("m_int32Array", &newValArray, arrayLen);
    ASSERT(arrayLen == 3, "Arraylength == 3 expected");
    ASSERT(genericCompare(setIntArray, newValArray, arrayLen) == true,
           "intArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    int64_t setLongArray[] = {3245435, 3425435};
    int64_t* getLongArray = NULL;
    wpiPtr->setField("m_longArray", setLongArray, 2);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_longArray") == true,
           "m_longArray = true expected");
    newPiPtr->getField("m_longArray", &getLongArray, arrayLen);
    ASSERT(arrayLen == 2, "Arraylength == 2 expected");
    ASSERT(genericCompare(setLongArray, getLongArray, arrayLen) == true,
           "longArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    float setFloatArray[] = {232.565f, 234323354.67f};
    float* getFloatArray = NULL;
    wpiPtr->setField("m_floatArray", setFloatArray, 2);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_floatArray") == true,
           "m_floatArray = true expected");
    newPiPtr->getField("m_floatArray", &getFloatArray, arrayLen);
    ASSERT(arrayLen == 2, "Arraylength == 2 expected");
    ASSERT(genericCompare(setFloatArray, getFloatArray, arrayLen) == true,
           "floatArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    double setDoubleArray[] = {23423432.00, 43242354315.00};
    double* getDoubleArray = NULL;
    wpiPtr->setField("m_doubleArray", setDoubleArray, 2);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_doubleArray") == true,
           "m_doubleArray = true expected");
    newPiPtr->getField("m_doubleArray", &getDoubleArray, arrayLen);
    ASSERT(arrayLen == 2, "Arraylength == 2 expected");
    ASSERT(genericCompare(setDoubleArray, getDoubleArray, arrayLen) == true,
           "doubleArray should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    bool boolVal = true;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_bool", false);
    LOG("modifyPdxInstanceAndCheckLocally setField bool done.");
    rptr->put(keyport, wpiPtr);
    LOG("modifyPdxInstanceAndCheckLocally put again complete.");
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_bool") == true, "m_bool = true expected");
    LOG("modifyPdxInstanceAndCheckLocally get again complete.");
    newPiPtr->getField("m_bool", boolVal);
    LOG("modifyPdxInstanceAndCheckLocally getField complete.");
    ASSERT(boolVal == false, "bool is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_bool", true);
    LOG("modifyPdxInstanceAndCheckLocally setField bool done.");
    rptr->put(keyport, wpiPtr);
    LOG("modifyPdxInstanceAndCheckLocally put again complete.");
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_bool") == true, "m_bool = true expected");
    LOG("modifyPdxInstanceAndCheckLocally get again complete.");
    newPiPtr->getField("m_bool", boolVal);
    LOG("modifyPdxInstanceAndCheckLocally getField complete.");
    ASSERT(boolVal == true, "bool is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == true,
           "PdxInstance should be equal");

    float fVal = 0.0f;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_float", 18389.34f);
    LOG("modifyPdxInstanceAndCheckLocally setField float done.");
    rptr->put(keyport, wpiPtr);
    LOG("modifyPdxInstanceAndCheckLocally put again complete.");
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_float") == true, "m_float = true expected");
    LOG("modifyPdxInstanceAndCheckLocally get again complete.");
    newPiPtr->getField("m_float", fVal);
    LOGINFO("modifyPdxInstanceAndCheckLocally getField complete. fval = %f",
            fVal);
    ASSERT(fVal == 18389.34f, "fval is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    double dVal = 0.0;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_double", 18389.34);
    LOG("modifyPdxInstanceAndCheckLocally setField float done.");
    rptr->put(keyport, wpiPtr);
    LOG("modifyPdxInstanceAndCheckLocally put again complete.");
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_double") == true, "m_double = true expected");
    LOG("modifyPdxInstanceAndCheckLocally get again complete.");
    newPiPtr->getField("m_double", dVal);
    LOGINFO("modifyPdxInstanceAndCheckLocally getField complete. fval = %lf",
            dVal);
    ASSERT(dVal == 18389.34, "fval is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    signed char byteVal = 0;
    signed char setSByteVal = 0x74;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_byte", setSByteVal);
    LOG("modifyPdxInstanceAndCheckLocally setField byte done.");
    rptr->put(keyport, wpiPtr);
    LOG("modifyPdxInstanceAndCheckLocally put again complete.");
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_byte") == true, "m_byte = true expected");
    LOG("modifyPdxInstanceAndCheckLocally get again complete.");
    newPiPtr->getField("m_byte", byteVal);
    LOGINFO("modifyPdxInstanceAndCheckLocally getField complete byteVal = %d ",
            byteVal);
    ASSERT(byteVal == setSByteVal, "byte is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == true,
           "PdxInstance should be equal");

    int16_t shortVal = 0;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_int16", static_cast<int16_t>(0x5678));
    LOG("modifyPdxInstanceAndCheckLocally setField short done.");
    rptr->put(keyport, wpiPtr);
    LOG("modifyPdxInstanceAndCheckLocally put again complete.");
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_int16") == true, "m_int16 = true expected");
    LOG("modifyPdxInstanceAndCheckLocally get again complete.");
    newPiPtr->getField("m_int16", shortVal);
    LOGINFO("modifyPdxInstanceAndCheckLocally getField complete shortVal = %d ",
            shortVal);
    ASSERT(shortVal == 0x5678, "short is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    int64_t longVal = 0;
    wpiPtr = pIPtr->createWriter();
    wpiPtr->setField("m_long", static_cast<int64_t>(0x56787878));
    LOG("modifyPdxInstanceAndCheckLocally setField short done.");
    rptr->put(keyport, wpiPtr);
    LOG("modifyPdxInstanceAndCheckLocally put again complete.");
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_long") == true, "m_long = true expected");
    LOG("modifyPdxInstanceAndCheckLocally get again complete.");
    newPiPtr->getField("m_long", longVal);
    LOGINFO("modifyPdxInstanceAndCheckLocally getField complete longVal = %ld ",
            longVal);
    ASSERT(longVal == 0x56787878, "long is not equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    const wchar_t* str = L"change the string";
    wpiPtr->setField("m_string", str);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_string") == true, "m_string = true expected");
    wchar_t* stringVal = NULL;
    LOG("modifyPdxInstanceAndCheckLocally get string complete.");
    newPiPtr->getField("m_string", &stringVal);
    LOGINFO("modifyPdxInstanceAndCheckLocally stringVal = %ls , str = %ls ",
            stringVal, str);
    ASSERT(wcscmp(stringVal, str) == 0, "stringVal should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    const char* str1 = "change the string";
    wpiPtr->setField("m_string", str1);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_string") == true, "m_string = true expected");
    char* getstringVal = NULL;
    LOG("modifyPdxInstanceAndCheckLocally get string complete.");
    newPiPtr->getField("m_string", &getstringVal);
    LOGINFO("modifyPdxInstanceAndCheckLocally getstringVal = %s , str1 = %s ",
            getstringVal, str1);
    ASSERT(strcmp(getstringVal, str1) == 0, "getstringVal should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    CacheableDatePtr dateVal;
    wpiPtr = pIPtr->createWriter();
    time_t timeofday = 0;
    const ACE_Time_Value currentTime = ACE_OS::gettimeofday();
    timeofday = currentTime.sec();
    CacheableDatePtr datePtr = CacheableDate::create(timeofday);
    wpiPtr->setField("m_dateTime", datePtr);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_dateTime") == true, "m_date = true expected");
    newPiPtr->getField("m_dateTime", dateVal);
    ASSERT((*(dateVal.ptr()) == *(datePtr.ptr())) == true,
           "dateObject should be equal");
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    // wchar_t* setStringArray[] = {L"one", L"two", L"eeeee"};
    wchar_t** setWideStringArray = new wchar_t*[3];
    const wchar_t* wstr1 = L"test1";
    const wchar_t* wstr2 = L"test2";
    const wchar_t* wstr3 = L"test3";
    size_t size = wcslen(wstr1);
    for (int i = 0; i < 3; i++) {
      setWideStringArray[i] = new wchar_t[size];
    }
    setWideStringArray[0] = const_cast<wchar_t*>(wstr1);
    setWideStringArray[1] = const_cast<wchar_t*>(wstr2);
    setWideStringArray[2] = const_cast<wchar_t*>(wstr3);
    wchar_t** getWideStringArray = NULL;
    wpiPtr->setField("m_stringArray", setWideStringArray, 3);
    rptr->put(keyport, wpiPtr);
    newPiPtr = dynCast<PdxInstancePtr>(rptr->get(keyport));
    ASSERT(pIPtr->hasField("m_stringArray") == true,
           "m_stringArray = true expected");
    newPiPtr->getField("m_stringArray", &getWideStringArray, arrayLen);
    ASSERT(arrayLen == 3, "Arraylength == 3 expected");
    for (int i = 0; i < arrayLen; i++) {
      LOGINFO("set string is %ls ", setWideStringArray[i]);
      LOGINFO("get string is %ls ", getWideStringArray[i]);
      ASSERT(wcscmp(setWideStringArray[i], getWideStringArray[i]) == 0,
             "All stringVals should be equal");
    }
    ASSERT((*pIPtr.ptr() == *newPiPtr.ptr()) == false,
           "PdxInstance should not be equal");

    LOG("modifyPdxInstanceAndCheckLocally complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, pdxIFPutGetTest)
  {
    LOG("pdxIFPutGetTest started ");

    try {
      Serializable::registerPdxType(Address::createDeserializable);
      LOG("Address Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("Address IllegalStateException");
    }

    try {
      Serializable::registerPdxType(PdxTests::PdxType::createDeserializable);
      LOG("PdxObject Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("PdxObject IllegalStateException");
    }

    try {
      Serializable::registerPdxType(ChildPdx::createDeserializable);
      LOG("ChildPdx Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("ChildPdx IllegalStateException");
    }

    try {
      Serializable::registerPdxType(ParentPdx::createDeserializable);
      LOG("ParentPdx Registered Successfully....");
    } catch (gemfire::IllegalStateException& /* ex*/) {
      LOG("ParentPdx IllegalStateException");
    }

    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);

    PdxTests::PdxTypePtr pdxobj(new PdxTests::PdxType());

    PdxInstanceFactoryPtr pifPtr =
        cacheHelper->getCache()->createPdxInstanceFactory("PdxTests.PdxType");
    LOG("PdxInstanceFactoryPtr created....");

    int* lengthArr = new int[2];

    lengthArr[0] = 1;
    lengthArr[1] = 2;

    pifPtr->writeBoolean("m_bool", pdxobj->getBool());
    try {
      pifPtr->writeBoolean("m_bool", pdxobj->getBool());
      FAIL(
          "calling writeBoolean on same fieldname should have thrown "
          "IllegalStateException");
    } catch (IllegalStateException&) {
      LOG("Got expected IllegalStateException for m_bool");
    }
    pifPtr->markIdentityField("m_bool");
    pifPtr->writeByte("m_byte", pdxobj->getByte());
    pifPtr->markIdentityField("m_byte");
    pifPtr->writeShort("m_int16", pdxobj->getShort());
    pifPtr->markIdentityField("m_int16");
    pifPtr->writeInt("m_int32", pdxobj->getInt());
    pifPtr->markIdentityField("m_int32");
    pifPtr->writeLong("m_long", pdxobj->getLong());
    pifPtr->markIdentityField("m_long");
    pifPtr->writeFloat("m_float", pdxobj->getFloat());
    pifPtr->markIdentityField("m_float");
    pifPtr->writeDouble("m_double", pdxobj->getDouble());
    pifPtr->markIdentityField("m_double");
    pifPtr->writeString("m_string", pdxobj->getString());
    pifPtr->markIdentityField("m_string");
    pifPtr->writeDate("m_dateTime", pdxobj->getDate());
    pifPtr->markIdentityField("m_dateTime");
    pifPtr->writeBooleanArray("m_boolArray", pdxobj->getBoolArray(), 3);
    pifPtr->markIdentityField("m_boolArray");
    pifPtr->writeByteArray("m_byteArray", pdxobj->getByteArray(), 2);
    pifPtr->markIdentityField("m_byteArray");
    pifPtr->writeShortArray("m_int16Array", pdxobj->getShortArray(), 2);
    pifPtr->markIdentityField("m_int16Array");
    pifPtr->writeIntArray("m_int32Array", pdxobj->getIntArray(), 4);
    pifPtr->markIdentityField("m_int32Array");
    pifPtr->writeLongArray("m_longArray", pdxobj->getLongArray(), 2);
    pifPtr->markIdentityField("m_longArray");
    pifPtr->writeFloatArray("m_floatArray", pdxobj->getFloatArray(), 2);
    pifPtr->markIdentityField("m_floatArray");
    pifPtr->writeDoubleArray("m_doubleArray", pdxobj->getDoubleArray(), 2);
    pifPtr->markIdentityField("m_doubleArray");
    pifPtr->writeObject("m_map", pdxobj->getHashMap());
    pifPtr->markIdentityField("m_map");
    pifPtr->writeStringArray("m_stringArray", pdxobj->getStringArray(), 2);
    pifPtr->markIdentityField("m_stringArray");
    pifPtr->writeObjectArray("m_objectArray",
                             pdxobj->getCacheableObjectArray());
    pifPtr->writeObject("m_pdxEnum", pdxobj->getEnum());
    pifPtr->markIdentityField("m_pdxEnum");
    pifPtr->writeObject("m_arraylist", pdxobj->getArrayList());
    pifPtr->markIdentityField("m_arraylist");
    pifPtr->writeObject("m_linkedlist", pdxobj->getLinkedList());
    pifPtr->markIdentityField("m_linkedlist");
    pifPtr->writeObject("m_hashtable", pdxobj->getHashTable());
    pifPtr->markIdentityField("m_hashtable");
    pifPtr->writeObject("m_vector", pdxobj->getVector());
    pifPtr->markIdentityField("m_vector");
    pifPtr->writeArrayOfByteArrays(
        "m_byteByteArray", pdxobj->getArrayOfByteArrays(), 2, lengthArr);
    pifPtr->markIdentityField("m_byteByteArray");
    pifPtr->writeWideChar("m_char", pdxobj->getChar());
    pifPtr->markIdentityField("m_char");
    pifPtr->writeWideCharArray("m_charArray", pdxobj->getCharArray(), 2);
    pifPtr->markIdentityField("m_charArray");
    pifPtr->writeObject("m_chs", pdxobj->getHashSet());
    pifPtr->markIdentityField("m_chs");
    pifPtr->writeObject("m_clhs", pdxobj->getLinkedHashSet());
    pifPtr->markIdentityField("m_clhs");
    pifPtr->writeByte("m_sbyte", pdxobj->getSByte());
    pifPtr->markIdentityField("m_sbyte");
    pifPtr->writeByteArray("m_sbyteArray", pdxobj->getSByteArray(), 2);
    pifPtr->markIdentityField("m_sbyteArray");
    pifPtr->writeShort("m_uint16", pdxobj->getUint16());
    pifPtr->markIdentityField("m_uint16");
    pifPtr->writeInt("m_uint32", pdxobj->getUInt());
    pifPtr->markIdentityField("m_uint32");
    pifPtr->writeLong("m_ulong", pdxobj->getULong());
    pifPtr->markIdentityField("m_ulong");
    pifPtr->writeShortArray("m_uint16Array", pdxobj->getUInt16Array(), 2);
    pifPtr->markIdentityField("m_uint16Array");
    pifPtr->writeIntArray("m_uint32Array", pdxobj->getUIntArray(), 4);
    pifPtr->markIdentityField("m_uint32Array");
    pifPtr->writeLongArray("m_ulongArray", pdxobj->getULongArray(), 2);
    pifPtr->markIdentityField("m_ulongArray");

    pifPtr->writeByteArray("m_byte252", pdxobj->getByte252(), 252);
    pifPtr->markIdentityField("m_byte252");
    pifPtr->writeByteArray("m_byte253", pdxobj->getByte253(), 253);
    pifPtr->markIdentityField("m_byte253");
    pifPtr->writeByteArray("m_byte65535", pdxobj->getByte65535(), 65535);
    pifPtr->markIdentityField("m_byte65535");
    pifPtr->writeByteArray("m_byte65536", pdxobj->getByte65536(), 65536);
    pifPtr->markIdentityField("m_byte65536");
    pifPtr->writeObject("m_address", pdxobj->getCacheableObjectArray());

    pifPtr->writeObjectArray(
        "", pdxobj->getCacheableObjectArrayEmptyPdxFieldName());

    PdxInstancePtr ret = pifPtr->create();
    LOG("PdxInstancePtr created....");

    LOGINFO("PdxInstance getClassName = %s ", ret->getClassName());
    ASSERT(strcmp(ret->getClassName(), "PdxTests.PdxType") == 0,
           "pdxInstance.getClassName should return PdxTests.PdxType.");

    PdxSerializablePtr psPtr = ret->getObject();
    LOG("getObject created....");

    LOG("Statistics for for (PdxTests.PdxType) PdxInstance ");
    LocalRegion* lregPtr = (dynamic_cast<LocalRegion*>(rptr.ptr()));

    LOGINFO(
        "pdxInstanceDeserializations for (PdxTests.PdxType) PdxInstance  = %d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializations());
    LOGINFO("pdxInstanceCreations for (PdxTests.PdxType) PdxInstance  = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations());
    LOGINFO(
        "pdxInstanceDeserializationTime for(PdxTests.PdxType) PdxInstance  = "
        "%d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializationTime());

    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializations() == 1,
           "pdxInstanceDeserialization should be equal to 1.");
    ASSERT(
        lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations() == 0,
        "pdxInstanceCreations should be equal to 0.");
    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializationTime() == 0,
           "pdxInstanceDeserializationTime should be equal to 0.");

    PdxTests::PdxType* obj2 = pdxobj.ptr();
    PdxTests::PdxType* obj1 = dynamic_cast<PdxTests::PdxType*>(psPtr.ptr());

    LOGINFO("Avinash Equal Starts");
    ASSERT(obj1->equals(*obj2, false) == true, "PdxObjects should be equal.");
    LOGINFO("Avinash Equal Finish");

    CacheableKeyPtr key = CacheableKey::create("pi");
    rptr->put(key, ret);
    LOG("put done....");

    PdxSerializablePtr newPiPtr = rptr->get(key);
    LOG("get done....");

    LOGINFO(
        "pdxInstanceDeserializations for (PdxTests.PdxType) PdxInstance  = %d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializations());
    LOGINFO("pdxInstanceCreations for (PdxTests.PdxType) PdxInstance  = %d ",
            lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations());
    LOGINFO(
        "pdxInstanceDeserializationTime for(PdxTests.PdxType) PdxInstance  = "
        "%d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializationTime());

    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializations() == 1,
           "pdxInstanceDeserialization should be equal to 1.");
    ASSERT(
        lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations() == 0,
        "pdxInstanceCreations should be equal to 0.");
    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializationTime() > 0,
           "pdxInstanceDeserializationTime should be greater than 0.");

    PdxTests::PdxType* obj3 = dynamic_cast<PdxTests::PdxType*>(newPiPtr.ptr());

    ASSERT(obj2->equals(*obj3, false) == true, "PdxObjects should be equal.");

    int pdxInstHashcode = ret->hashcode();
    LOGINFO("pdxinstance hash code = %d ", pdxInstHashcode);

    CacheableKeyPtr javaPdxHCKey = CacheableKey::create("javaPdxHC");
    CacheablePtr pIPtr2 = dynCast<CacheablePtr>(rptr->get(javaPdxHCKey));
    LOG("In pdxIFPutGetTest get done");
    CacheableInt32* val = dynamic_cast<CacheableInt32*>(pIPtr2.ptr());
    LOG("In pdxIFPutGetTest cast done");
    int javaPdxHC = val->value();
    LOGINFO("javaPdxHC hash code = %d ", javaPdxHC);

    ASSERT(javaPdxHC == pdxInstHashcode,
           "Pdxhashcode hashcode not matched with java pdx hash code.");

    ParentPdxPtr pp(new ParentPdx(10));
    PdxInstanceFactoryPtr if2 =
        cacheHelper->getCache()->createPdxInstanceFactory(
            "testobject::ParentPdx");
    if2->writeInt("m_parentId", pp->getParentId());
    if2->writeObject("m_enum", pp->getEnum());
    if2->writeString("m_parentName", pp->getParentName());
    if2->writeWideString("m_wideparentName", pp->getWideParentName());
    if2->writeWideStringArray("m_wideparentArrayName",
                              pp->getWideParentArrayName(), 3);
    if2->writeObject("m_childPdx", pp->getChildPdx());
    if2->writeChar("m_char", pp->getChar());
    if2->writeWideChar("m_wideChar", pp->getWideChar());
    if2->writeCharArray("m_charArray", pp->getCharArray(), 2);
    if2->writeWideCharArray("m_wideCharArray", pp->getWideCharArray(), 2);

    LOG("write set done....");
    PdxInstancePtr ip2 = if2->create();
    LOG("PdxInstancePtr created");

    LOGINFO("PdxInstance getClassName = %s ", ip2->getClassName());
    ASSERT(strcmp(ip2->getClassName(), "testobject::ParentPdx") == 0,
           "pdxInstance.getClassName should return testobject::ParentPdx.");

    CacheableKeyPtr keyport = CacheableKey::create("pp");
    rptr->put(keyport, ip2);
    LOG("put done....");

    newPiPtr = rptr->get(keyport);
    LOG("get done....");

    LOGINFO(
        "pdxInstanceDeserializations for (testobject::ParentPdx) PdxInstance  "
        "= "
        "%d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializations());
    LOGINFO(
        "pdxInstanceCreations for (testobject::ParentPdx) PdxInstance  = %d ",
        lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations());
    LOGINFO(
        "pdxInstanceDeserializationTime for(testobject::ParentPdx) PdxInstance "
        " "
        "= %d ",
        lregPtr->getCacheImpl()
            ->m_cacheStats->getPdxInstanceDeserializationTime());

    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializations() == 1,
           "pdxInstanceDeserialization should be equal to 1.");
    ASSERT(
        lregPtr->getCacheImpl()->m_cacheStats->getPdxInstanceCreations() == 0,
        "pdxInstanceCreations should be equal to 0.");
    ASSERT(lregPtr->getCacheImpl()
                   ->m_cacheStats->getPdxInstanceDeserializationTime() > 0,
           "pdxInstanceDeserializationTime should be greater than 0.");

    ParentPdxPtr pp1 = dynCast<ParentPdxPtr>(newPiPtr);
    LOG("got ParentPdxPtr....");

    ParentPdx* rawPP1 = dynamic_cast<ParentPdx*>(pp1.ptr());
    LOG("got rawPP1....");

    ParentPdx* rawPP2 = dynamic_cast<ParentPdx*>(pp.ptr());
    LOG("got rawpp2....");

    ASSERT(rawPP1->equals(*rawPP2, false) == true,
           "ParentPdx objects should be equal.");

    LOG("pdxIFPutGetTest complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, pdxInstanceWithEmptyKeys)
  {
    LOG("pdxInstanceWithEmptyKeys started ");
    RegionPtr rptr = getHelper()->getRegion(regionNames[0]);
    PdxInstanceFactoryPtr pifPtr =
        cacheHelper->getCache()->createPdxInstanceFactory("EMPTY_KEY_NAME");
    LOG("PdxInstanceFactoryPtr created....");

    bool falseValue = false;
    pifPtr->writeBoolean("", falseValue);
    PdxInstancePtr putValue = pifPtr->create();

    CacheableKeyPtr key = CacheableKey::create("pp");
    rptr->put(key, putValue);
    LOG("put done with boolean Value and Empty Key....");

    PdxInstancePtr getValue = dynCast<PdxInstancePtr>(rptr->get(key));
    LOG("get done with boolean Value and Empty Key....");

    ASSERT(*putValue.ptr() == *getValue.ptr(),
           "Boolean Value Did not match in case of Empty PdxField Key");
    bool fieldValue;
    getValue->getField("", fieldValue);
    ASSERT(fieldValue == falseValue,
           "Mismatch in the PdxInstance with Empty key");

    LOG("pdxInstanceWithEmptyKeys complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseLocator1)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

void runThinClientPdxInstance(int index) {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator)

  if (index == 0) {
    CALL_TASK(StepOne_Pooled_Locator);
    CALL_TASK(StepTwo_Pooled_Locator);

    CALL_TASK(pdxIFPutGetTest);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);

    CALL_TASK(CloseLocator1);

  } else if (index == 1) {
    CALL_TASK(StepOne_Pooled_Locator_PdxReadSerialized);
    CALL_TASK(StepTwo_Pooled_Locator_CachingEnabled_PdxReadSerialized);

    CALL_TASK(pdxPut);
    CALL_TASK(modifyPdxInstanceAndCheckLocally);
    CALL_TASK(pdxInstanceWithEmptyKeys);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);

    CALL_TASK(CloseLocator1);

  } else if (index == 2) {
    CALL_TASK(StepOne_Pooled_Locator_PdxReadSerialized);
    CALL_TASK(StepTwo_Pooled_Locator_PdxReadSerialized);

    CALL_TASK(pdxPut);
    CALL_TASK(getObject);
    CALL_TASK(verifyPdxInstanceEquals);
    CALL_TASK(verifyPdxInstanceHashcode);
    CALL_TASK(accessPdxInstance);
    CALL_TASK(modifyPdxInstance);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);

    CALL_TASK(CloseLocator1);
  }
}

DUNIT_MAIN
  {
    runThinClientPdxInstance(
        0);  // Locator, caching = false, PdxReadSerialized = false

    runThinClientPdxInstance(
        1);  // Locator, caching = true, PdxReadSerialized = true

    runThinClientPdxInstance(
        2);  // Locator, caching = false, PdxReadSerialized = true
  }
END_MAIN

#endif /* TESTTHINCLIENTPDXINSTANCE_HPP_ */
