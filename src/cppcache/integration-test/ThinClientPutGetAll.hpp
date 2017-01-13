/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef THINCLIENTPUTGETALL_HPP_
#define THINCLIENTPUTGETALL_HPP_

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>
#include <string>
#include <unordered_map>

#define ROOT_NAME "ThinClientPutGetAll"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "ThinClientHelper.hpp"
#include "testobject/VariousPdxTypes.hpp"
#include "testobject/PdxClassV1.hpp"
#include "testobject/PdxClassV2.hpp"

using namespace PdxTests;
using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

static bool isLocalServer = false;
static bool isLocator = false;
static int numberOfLocators = 0;

const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);
const char* poolName = "__TESTPOOL1_";

const char* _keys[] = {"Key-1", "Key-2", "Key-3", "Key-4"};
const char* _vals[] = {"Value-1", "Value-2", "Value-3", "Value-4"};
const char* _nvals[] = {"New Value-1", "New Value-2", "New Value-3",
                        "New Value-4"};
const char* _regionNames[] = {"DistRegionAck"};

#include "LocatorHelper.hpp"

void verifyGetAll(RegionPtr region, bool addToLocalCache, const char** _vals,
                  int startIndex, CacheablePtr callBack = NULLPTR) {
  CacheableKeyPtr keyPtr0 = CacheableKey::create(_keys[0]);
  CacheableKeyPtr keyPtr1 = CacheableKey::create(_keys[1]);
  CacheableKeyPtr keyPtr2 = CacheableKey::create("keyNotThere");

  VectorOfCacheableKey keys1;
  keys1.push_back(keyPtr0);
  keys1.push_back(keyPtr1);
  keys1.push_back(keyPtr2);

  std::unordered_map<std::string, std::string> expected;
  expected[_keys[0]] = _vals[startIndex + 0];
  expected[_keys[1]] = _vals[startIndex + 1];

  HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
  valuesMap->clear();
  region->getAll(keys1, valuesMap, NULLPTR, addToLocalCache, callBack);
  if (valuesMap->size() == keys1.size()) {
    char buf[2048];
    for (HashMapOfCacheable::Iterator iter = valuesMap->begin();
         iter != valuesMap->end(); iter++) {
      CacheableKeyPtr key = dynCast<CacheableKeyPtr>(iter.first());
      const char* actualKey = key->toString()->asChar();
      CacheablePtr mVal = iter.second();
      if (mVal != NULLPTR) {
        const char* expectedVal = expected[actualKey].c_str();
        const char* actualVal = mVal->toString()->asChar();
        sprintf(buf, "value from map %s , expected value %s ", actualVal,
                expectedVal);
        LOG(buf);
        ASSERT(strcmp(actualVal, expectedVal) == 0, "value not matched");
      } else {
        ASSERT(strcmp(actualKey, "keyNotThere") == 0,
               "keyNotThere value is not null");
      }
    }
  }
}

void verifyGetAllWithCallBackArg(RegionPtr region, bool addToLocalCache,
                                 const char** vals, int startIndex,
                                 CacheablePtr callBack) {
  verifyGetAll(region, addToLocalCache, vals, startIndex, callBack);
}

void createPooledRegion(const char* name, bool ackMode, const char* locators,
                        const char* poolname,
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

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    // start one server
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_notify_subscription.xml");
      LOG("SERVER1 started");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
  {
    // start 1st client with caching enable true and client notification true
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    createPooledRegion(_regionNames[0], USE_ACK, locatorsG, poolName, true,
                       true);
    LOG("StepOne_Pooled_Locator complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo_Pooled_Locator)
  {
    // start 1st client with caching enable true and client notification true
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    createPooledRegion(_regionNames[0], USE_ACK, locatorsG, poolName, true,
                       true);
    LOG("StepTwo_Pooled_Locator complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutAllInitialValuesFromClientOne)
  {
    // putAll from client 1
    HashMapOfCacheable map0;
    map0.clear();
    for (int i = 0; i < 2; i++) {
      map0.insert(CacheableKey::create(_keys[i]),
                  CacheableString::create(_vals[i]));
    }
    RegionPtr regPtr0 = getHelper()->getRegion(_regionNames[0]);
    regPtr0->putAll(map0);
    LOG("PutAllInitialValuesFromClientOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, GetAllInitialValuesFromClientTwo)
  {
    // getAll and validate key and value.
    RegionPtr region = getHelper()->getRegion(_regionNames[0]);
    verifyGetAll(region, true, _vals, 0);
    verifyGetAllWithCallBackArg(region, true, _vals, 0,
                                CacheableInt32::create(1000));
    LOG("GetAllInitialValuesFromClientTwo complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1, PutAllUpdatedValuesFromClientOne)
  {
    // update keys,values by putAll
    HashMapOfCacheable map0;
    map0.clear();
    for (int i = 0; i < 2; i++) {
      map0.insert(CacheableKey::create(_keys[i]),
                  CacheableString::create(_nvals[i]));
    }
    RegionPtr regPtr0 = getHelper()->getRegion(_regionNames[0]);
    regPtr0->putAll(map0);
    LOG("PutAllUpdatedValuesFromClientOne complete.");
  }
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2, GetAllUpdatedValuesFromClientTwo)
  {
    // verify getAll get the data from local cache.
    RegionPtr region = getHelper()->getRegion(_regionNames[0]);
    verifyGetAll(region, true, _vals, 0);
    verifyGetAllWithCallBackArg(region, true, _vals, 0,
                                CacheableInt32::create(1000));
    LOG("GetAllUpdatedValuesFromClientTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, GetAllAfterLocalDestroyRegionOnClientTwo)
  {
    // getAll and validate key and value after localDestroyRegion and recreation
    // of region.
    RegionPtr reg0 = getHelper()->getRegion(_regionNames[0]);
    reg0->localDestroyRegion();
    reg0 = NULLPTR;
    getHelper()->createPooledRegion(regionNames[0], USE_ACK, 0,
                                    "__TEST_POOL1__", true, true);
    reg0 = getHelper()->getRegion(_regionNames[0]);
    verifyGetAll(reg0, true, _nvals, 0);
    verifyGetAllWithCallBackArg(reg0, true, _nvals, 0,
                                CacheableInt32::create(1000));
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, GetAllAfterLocalDestroyRegionOnClientTwo_Pool)
  {
    // getAll and validate key and value after localDestroyRegion and recreation
    // of region.
    RegionPtr reg0 = getHelper()->getRegion(_regionNames[0]);
    reg0->localDestroyRegion();
    reg0 = NULLPTR;
    createPooledRegion(_regionNames[0], USE_ACK, locatorsG, poolName, true,
                       true);
    reg0 = getHelper()->getRegion(_regionNames[0]);
    verifyGetAll(reg0, true, _nvals, 0);
    verifyGetAllWithCallBackArg(reg0, true, _nvals, 0,
                                CacheableInt32::create(1000));
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putallAndGetallPdxWithCallBackArg)
  {
    LOG("putallAndGetallPdxWithCallBackArg started.");

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

    PdxTypes1Ptr p1(new PdxTypes1());
    PdxTypes2Ptr p2(new PdxTypes2());
    PdxTypes3Ptr p3(new PdxTypes3());
    PdxTypes4Ptr p4(new PdxTypes4());
    PdxTypes5Ptr p5(new PdxTypes5());
    PdxTypes6Ptr p6(new PdxTypes6());
    PdxTypes7Ptr p7(new PdxTypes7());
    PdxTypes8Ptr p8(new PdxTypes8());
    PdxTypes9Ptr p9(new PdxTypes9());
    PdxTypes10Ptr p10(new PdxTypes10());

    // putAll from client 1
    HashMapOfCacheable map0;
    map0.clear();

    map0.insert(CacheableInt32::create(21), p1);
    map0.insert(CacheableInt32::create(22), p2);
    map0.insert(CacheableInt32::create(23), p3);
    map0.insert(CacheableInt32::create(24), p4);
    map0.insert(CacheableInt32::create(25), p5);
    map0.insert(CacheableInt32::create(26), p6);
    map0.insert(CacheableInt32::create(27), p7);
    map0.insert(CacheableInt32::create(28), p8);
    map0.insert(CacheableInt32::create(29), p9);
    map0.insert(CacheableInt32::create(30), p10);

    RegionPtr regPtr0 = getHelper()->getRegion(_regionNames[0]);
    // TODO: Investigate whether callback is used
    regPtr0->putAll(map0, 15, CacheableInt32::create(1001));
    LOG("putallPdxWithCallBackArg on Pdx objects completed.");

    regPtr0->localDestroy(CacheableInt32::create(21));
    regPtr0->localDestroy(CacheableInt32::create(22));
    regPtr0->localDestroy(CacheableInt32::create(23));
    regPtr0->localDestroy(CacheableInt32::create(24));
    regPtr0->localDestroy(CacheableInt32::create(25));
    regPtr0->localDestroy(CacheableInt32::create(26));
    regPtr0->localDestroy(CacheableInt32::create(27));
    regPtr0->localDestroy(CacheableInt32::create(28));
    regPtr0->localDestroy(CacheableInt32::create(29));
    regPtr0->localDestroy(CacheableInt32::create(30));
    LOG("localDestroy on all Pdx objects completed.");

    VectorOfCacheableKey keys1;
    keys1.push_back(CacheableInt32::create(21));
    keys1.push_back(CacheableInt32::create(22));
    keys1.push_back(CacheableInt32::create(23));
    keys1.push_back(CacheableInt32::create(24));
    keys1.push_back(CacheableInt32::create(25));
    keys1.push_back(CacheableInt32::create(26));
    keys1.push_back(CacheableInt32::create(27));
    keys1.push_back(CacheableInt32::create(28));
    keys1.push_back(CacheableInt32::create(29));
    keys1.push_back(CacheableInt32::create(30));
    HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
    valuesMap->clear();
    regPtr0->getAll(keys1, valuesMap, NULLPTR, true,
                    CacheableInt32::create(1000));
    LOG("GetallPdxWithCallBackArg on Pdx objects completed.");

    ASSERT(valuesMap->size() == keys1.size(), "getAll size did not match");

    PdxTypes10Ptr pRet10 = valuesMap->operator[](CacheableInt32::create(30));
    ASSERT(p10->equals(pRet10) == true,
           "Objects of type PdxTypes10 should be equal");

    PdxTypes9Ptr pRet9 = valuesMap->operator[](CacheableInt32::create(29));
    ASSERT(p9->equals(pRet9) == true,
           "Objects of type PdxTypes9 should be equal");

    PdxTypes8Ptr pRet8 = valuesMap->operator[](CacheableInt32::create(28));
    ASSERT(p8->equals(pRet8) == true,
           "Objects of type PdxTypes8 should be equal");

    PdxTypes7Ptr pRet7 = valuesMap->operator[](CacheableInt32::create(27));
    ASSERT(p7->equals(pRet7) == true,
           "Objects of type PdxTypes7 should be equal");

    PdxTypes6Ptr pRet6 = valuesMap->operator[](CacheableInt32::create(26));
    ASSERT(p6->equals(pRet6) == true,
           "Objects of type PdxTypes6 should be equal");

    PdxTypes5Ptr pRet5 = valuesMap->operator[](CacheableInt32::create(25));
    ASSERT(p5->equals(pRet5) == true,
           "Objects of type PdxTypes5 should be equal");

    PdxTypes4Ptr pRet4 = valuesMap->operator[](CacheableInt32::create(24));
    ASSERT(p4->equals(pRet4) == true,
           "Objects of type PdxTypes4 should be equal");

    PdxTypes3Ptr pRet3 = valuesMap->operator[](CacheableInt32::create(23));
    ASSERT(p3->equals(pRet3) == true,
           "Objects of type PdxTypes3 should be equal");

    PdxTypes2Ptr pRet2 = valuesMap->operator[](CacheableInt32::create(22));
    ASSERT(p2->equals(pRet2) == true,
           "Objects of type PdxTypes2 should be equal");

    PdxTypes1Ptr pRet1 = valuesMap->operator[](CacheableInt32::create(21));
    ASSERT(p1->equals(pRet1) == true,
           "Objects of type PdxTypes1 should be equal");

    LOG("putallAndGetallPdxWithCallBackArg complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, putallAndGetallPdx)
  {
    LOG("putallAndGetallPdx started.");

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

    PdxTypes1Ptr p1(new PdxTypes1());
    PdxTypes2Ptr p2(new PdxTypes2());
    PdxTypes3Ptr p3(new PdxTypes3());
    PdxTypes4Ptr p4(new PdxTypes4());
    PdxTypes5Ptr p5(new PdxTypes5());
    PdxTypes6Ptr p6(new PdxTypes6());
    PdxTypes7Ptr p7(new PdxTypes7());
    PdxTypes8Ptr p8(new PdxTypes8());
    PdxTypes9Ptr p9(new PdxTypes9());
    PdxTypes10Ptr p10(new PdxTypes10());

    // putAll from client 1
    HashMapOfCacheable map0;
    map0.clear();

    map0.insert(CacheableInt32::create(21), p1);
    map0.insert(CacheableInt32::create(22), p2);
    map0.insert(CacheableInt32::create(23), p3);
    map0.insert(CacheableInt32::create(24), p4);
    map0.insert(CacheableInt32::create(25), p5);
    map0.insert(CacheableInt32::create(26), p6);
    map0.insert(CacheableInt32::create(27), p7);
    map0.insert(CacheableInt32::create(28), p8);
    map0.insert(CacheableInt32::create(29), p9);
    map0.insert(CacheableInt32::create(30), p10);
    RegionPtr regPtr0 = getHelper()->getRegion(_regionNames[0]);
    regPtr0->putAll(map0);
    LOG("putAll on Pdx objects completed.");

    regPtr0->localDestroy(CacheableInt32::create(21));
    regPtr0->localDestroy(CacheableInt32::create(22));
    regPtr0->localDestroy(CacheableInt32::create(23));
    regPtr0->localDestroy(CacheableInt32::create(24));
    regPtr0->localDestroy(CacheableInt32::create(25));
    regPtr0->localDestroy(CacheableInt32::create(26));
    regPtr0->localDestroy(CacheableInt32::create(27));
    regPtr0->localDestroy(CacheableInt32::create(28));
    regPtr0->localDestroy(CacheableInt32::create(29));
    regPtr0->localDestroy(CacheableInt32::create(30));
    LOG("localDestroy on all Pdx objects completed.");

    VectorOfCacheableKey keys1;
    keys1.push_back(CacheableInt32::create(21));
    keys1.push_back(CacheableInt32::create(22));
    keys1.push_back(CacheableInt32::create(23));
    keys1.push_back(CacheableInt32::create(24));
    keys1.push_back(CacheableInt32::create(25));
    keys1.push_back(CacheableInt32::create(26));
    keys1.push_back(CacheableInt32::create(27));
    keys1.push_back(CacheableInt32::create(28));
    keys1.push_back(CacheableInt32::create(29));
    keys1.push_back(CacheableInt32::create(30));
    HashMapOfCacheablePtr valuesMap(new HashMapOfCacheable());
    valuesMap->clear();
    regPtr0->getAll(keys1, valuesMap, NULLPTR, true);
    LOG("getAll on Pdx objects completed.");

    ASSERT(valuesMap->size() == keys1.size(), "getAll size did not match");

    PdxTypes10Ptr pRet10 = valuesMap->operator[](CacheableInt32::create(30));
    ASSERT(p10->equals(pRet10) == true,
           "Objects of type PdxTypes10 should be equal");

    PdxTypes9Ptr pRet9 = valuesMap->operator[](CacheableInt32::create(29));
    ASSERT(p9->equals(pRet9) == true,
           "Objects of type PdxTypes9 should be equal");

    PdxTypes8Ptr pRet8 = valuesMap->operator[](CacheableInt32::create(28));
    ASSERT(p8->equals(pRet8) == true,
           "Objects of type PdxTypes8 should be equal");

    PdxTypes7Ptr pRet7 = valuesMap->operator[](CacheableInt32::create(27));
    ASSERT(p7->equals(pRet7) == true,
           "Objects of type PdxTypes7 should be equal");

    PdxTypes6Ptr pRet6 = valuesMap->operator[](CacheableInt32::create(26));
    ASSERT(p6->equals(pRet6) == true,
           "Objects of type PdxTypes6 should be equal");

    PdxTypes5Ptr pRet5 = valuesMap->operator[](CacheableInt32::create(25));
    ASSERT(p5->equals(pRet5) == true,
           "Objects of type PdxTypes5 should be equal");

    PdxTypes4Ptr pRet4 = valuesMap->operator[](CacheableInt32::create(24));
    ASSERT(p4->equals(pRet4) == true,
           "Objects of type PdxTypes4 should be equal");

    PdxTypes3Ptr pRet3 = valuesMap->operator[](CacheableInt32::create(23));
    ASSERT(p3->equals(pRet3) == true,
           "Objects of type PdxTypes3 should be equal");

    PdxTypes2Ptr pRet2 = valuesMap->operator[](CacheableInt32::create(22));
    ASSERT(p2->equals(pRet2) == true,
           "Objects of type PdxTypes2 should be equal");

    PdxTypes1Ptr pRet1 = valuesMap->operator[](CacheableInt32::create(21));
    ASSERT(p1->equals(pRet1) == true,
           "Objects of type PdxTypes1 should be equal");

    LOG("putallAndGetallPdx complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
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

void runPutGetAll() {
  CALL_TASK(CreateLocator1);
  CALL_TASK(CreateServer1_With_Locator_XML);

  CALL_TASK(StepOne_Pooled_Locator);
  CALL_TASK(StepTwo_Pooled_Locator);

  CALL_TASK(PutAllInitialValuesFromClientOne);
  CALL_TASK(GetAllInitialValuesFromClientTwo);
  CALL_TASK(PutAllUpdatedValuesFromClientOne);
  CALL_TASK(GetAllUpdatedValuesFromClientTwo);

  CALL_TASK(GetAllAfterLocalDestroyRegionOnClientTwo_Pool);
  CALL_TASK(putallAndGetallPdx);

  // TODO: Does this task add value? Is it same code path as
  // non-WtihCallBackArg?
  //       This task has been found to intermittently error because types are
  //       still
  //       registered from  previous non-WithCallBackArg task. If this task has
  //       value
  //       then we should probably separate it into its own test.
  // CALL_TASK(putallAndGetallPdxWithCallBackArg);

  CALL_TASK(CloseCache1);

  CALL_TASK(CloseServer1);

  CALL_TASK(CloseLocator1);
}
#endif /* THINCLIENTPUTGETALL_HPP_ */
