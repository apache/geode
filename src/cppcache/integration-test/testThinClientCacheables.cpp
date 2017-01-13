/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include "BuiltinCacheableWrappers.hpp"
#include <Utils.hpp>

#include <ace/OS.h>
#include <ace/High_Res_Timer.h>

#include <string>

#define ROOT_NAME "testThinClientCacheables"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

CacheHelper* cacheHelper = NULL;
bool isLocalServer = false;

#if defined(WIN32)
// because we run out of memory on our pune windows desktops
#define DEFAULTNUMKEYS 5
#else
#define DEFAULTNUMKEYS 15
#endif
#define KEYSIZE 256
#define VALUESIZE 1024

void initClient(const bool isthinClient) {
  if (cacheHelper == NULL) {
    cacheHelper = new CacheHelper(isthinClient);
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

void createRegion(const char* name, bool ackMode,
                  bool clientNotificationEnabled = false) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, true, NULLPTR,
                                               clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}

void checkGets(int maxKeys, int8_t keyTypeId, int8_t valTypeId,
               const RegionPtr& dataReg, const RegionPtr& verifyReg) {
  for (int i = 0; i < maxKeys; i++) {
    CacheableWrapper* tmpkey =
        CacheableWrapperFactory::createInstance(keyTypeId);
    ASSERT(tmpkey != NULL, "tmpkey is NULL");
    CacheableWrapper* tmpval =
        CacheableWrapperFactory::createInstance(valTypeId);
    ASSERT(tmpval != NULL, "tmpval is NULL");
    tmpkey->initKey(i, KEYSIZE);
    CacheableKeyPtr key = dynCast<CacheableKeyPtr>(tmpkey->getCacheable());
    CacheablePtr val = dataReg->get(key);
    // also check that value is in local cache
    RegionEntryPtr entry = dataReg->getEntry(key);
    ASSERT(entry != NULLPTR, "entry is NULL");
    CacheablePtr localVal = entry->getValue();
    uint32_t keychksum = tmpkey->getCheckSum();
    CacheableInt32Ptr int32val = dynCast<CacheableInt32Ptr>(
        verifyReg->get(static_cast<int32_t>(keychksum)));
    if (int32val == NULLPTR) {
      printf("GetsTask::keychksum: %u, key: %s\n", keychksum,
             Utils::getCacheableKeyString(key)->asChar());
      FAIL("Could not find the checksum for the given key.");
    }
    uint32_t valchksum = static_cast<uint32_t>(int32val->value());
    uint32_t gotValChkSum = tmpval->getCheckSum(val);
    uint32_t gotLocalValChkSum = tmpval->getCheckSum(localVal);
    ASSERT(valchksum == gotValChkSum, "Expected valchksum == gotValChkSum");
    ASSERT(valchksum == gotLocalValChkSum,
           "Expected valchksum == gotLocalValChkSum");
    delete tmpkey;
    delete tmpval;
  }
}

const char* regionNames[] = {"DistRegionAck", "DistRegionNoAck"};

const bool USE_ACK = true;
const bool NO_ACK = false;

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) CacheHelper::initServer(1);
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
  {
    initClient(true);
    createRegion(regionNames[0], USE_ACK);
    createRegion(regionNames[1], NO_ACK);
    CacheableHelper::registerBuiltins();
    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepTwo)
  {
    initClient(true);
    createRegion(regionNames[0], USE_ACK);
    createRegion(regionNames[1], NO_ACK);
    CacheableHelper::registerBuiltins();
    LOG("StepTwo complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutsTask)
  {
    LOG("PutsTask started.");
    static int taskIndexPut = 0;

    std::vector<int8_t> keyTypes =
        CacheableWrapperFactory::getRegisteredKeyTypes();
    std::vector<int8_t> valueTypes =
        CacheableWrapperFactory::getRegisteredValueTypes();

    size_t keyTypeIndex = taskIndexPut / valueTypes.size();
    size_t valueTypeIndex = taskIndexPut % valueTypes.size();

    int8_t keyTypeId = keyTypes[keyTypeIndex];
    int8_t valTypeId = valueTypes[valueTypeIndex];

    printf("PutsTask::keyType = %s and valType = %s and taskIndexPut = %d\n",
           CacheableWrapperFactory::getTypeForId(keyTypeId).c_str(),
           CacheableWrapperFactory::getTypeForId(valTypeId).c_str(),
           taskIndexPut);

    CacheableWrapper* key = CacheableWrapperFactory::createInstance(keyTypeId);
    int maxKeys =
        (key->maxKeys() < DEFAULTNUMKEYS ? key->maxKeys() : DEFAULTNUMKEYS);
    delete key;

    RegionPtr dataReg = getHelper()->getRegion(regionNames[0]);
    RegionPtr verifyReg = getHelper()->getRegion(regionNames[1]);
    for (int i = 0; i < maxKeys; i++) {
      CacheableWrapper* tmpkey =
          CacheableWrapperFactory::createInstance(keyTypeId);
      CacheableWrapper* tmpval =
          CacheableWrapperFactory::createInstance(valTypeId);
      tmpkey->initKey(i, KEYSIZE);
      tmpval->initRandomValue(CacheableHelper::random(VALUESIZE) + 1);
      ASSERT(tmpkey->getCacheable() != NULLPTR,
             "tmpkey->getCacheable() is NULL");
      // we can have NULL values now after fix for bug #294
      if (tmpval->getCacheable() != NULLPTR) {
        dataReg->put(dynCast<CacheableKeyPtr>(tmpkey->getCacheable()),
                     tmpval->getCacheable());
      } else {
        try {
          dataReg->destroy(dynCast<CacheableKeyPtr>(tmpkey->getCacheable()));
        } catch (const EntryNotFoundException&) {
          // expected
        }
        dataReg->create(dynCast<CacheableKeyPtr>(tmpkey->getCacheable()),
                        tmpval->getCacheable());
      }
      uint32_t keychksum = tmpkey->getCheckSum();
      uint32_t valchksum = tmpval->getCheckSum();
      verifyReg->put(static_cast<int32_t>(keychksum),
                     static_cast<int32_t>(valchksum));
      // also check that value is in local cache
      RegionEntryPtr entry =
          dataReg->getEntry(dynCast<CacheableKeyPtr>(tmpkey->getCacheable()));
      CacheablePtr localVal;
      if (entry != NULLPTR) {
        localVal = entry->getValue();
      }
      uint32_t localValChkSum = tmpval->getCheckSum(localVal);
      ASSERT(valchksum == localValChkSum,
             "Expected valchksum == localValChkSum");
      delete tmpkey;
      delete tmpval;
    }
    taskIndexPut++;
    LOG("PutsTask completed.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, GetsTask)
  {
    LOG("GetsTask started.");
    static int taskIndexGet = 0;

    std::vector<int8_t> keyTypes =
        CacheableWrapperFactory::getRegisteredKeyTypes();
    std::vector<int8_t> valueTypes =
        CacheableWrapperFactory::getRegisteredValueTypes();

    size_t keyTypeIndex = taskIndexGet / valueTypes.size();
    size_t valueTypeIndex = taskIndexGet % valueTypes.size();

    int8_t keyTypeId = keyTypes[keyTypeIndex];
    int8_t valTypeId = valueTypes[valueTypeIndex];

    printf("GetsTask::keyType = %s and valType = %s and taskIndexGet = %d\n",
           CacheableWrapperFactory::getTypeForId(keyTypeId).c_str(),
           CacheableWrapperFactory::getTypeForId(valTypeId).c_str(),
           taskIndexGet);

    CacheableWrapper* key = CacheableWrapperFactory::createInstance(keyTypeId);
    int maxKeys =
        (key->maxKeys() < DEFAULTNUMKEYS ? key->maxKeys() : DEFAULTNUMKEYS);
    delete key;

    RegionPtr dataReg = getHelper()->getRegion(regionNames[0]);
    RegionPtr verifyReg = getHelper()->getRegion(regionNames[1]);
    dataReg->localInvalidateRegion();
    verifyReg->localInvalidateRegion();
    checkGets(maxKeys, keyTypeId, valTypeId, dataReg, verifyReg);

    // Also check after running a region query. This ensures that the values
    // have deserialized on server so checks serialization/deserialization
    // compatibility with java server.
    std::string queryStr = "SELECT DISTINCT iter.key, iter.value FROM /";
    queryStr += ((std::string)regionNames[0] + ".entrySet AS iter");
    dataReg->query(queryStr.c_str());
    dataReg->localInvalidateRegion();
    verifyReg->localInvalidateRegion();
    checkGets(maxKeys, keyTypeId, valTypeId, dataReg, verifyReg);

    taskIndexGet++;
    LOG("GetsTask completed.");
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

DUNIT_MAIN
  {
    CacheableHelper::registerBuiltins();
    CALL_TASK(CreateServer1);
    CALL_TASK(StepOne);
    CALL_TASK(StepTwo);
    size_t totKeyTypes =
        CacheableWrapperFactory::getRegisteredKeyTypes().size();
    size_t totValTypes =
        CacheableWrapperFactory::getRegisteredValueTypes().size();
    for (int i = 0; i < totKeyTypes; i++) {
      for (int j = 0; j < totValTypes; j++) {
        CALL_TASK(PutsTask);
        CALL_TASK(GetsTask);
      }
    }
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseServer1);
  }
END_MAIN
