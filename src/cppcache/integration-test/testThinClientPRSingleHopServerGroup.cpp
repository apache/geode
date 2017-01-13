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
#include <gfcpp/statistics/StatisticsFactory.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>

#include <ace/ACE.h>

#include <string>

#define ROOT_NAME "testThinClientPRSingleHopServerGroup"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

// Include these 2 headers for access to CacheImpl for test hooks.
#include "CacheImplHelper.hpp"
#include "testUtils.hpp"

#include "ThinClientHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s2p1
#define CLIENT3 s1p2
#define SERVER1 s2p2

bool isLocalServer = false;

static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);

const char* group1 = "ABC";
const char* group2 = "BC";
const char* group3 = "C";

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator1)
  {
    initClient(true);
    getHelper()->createPoolWithLocators("__TEST_POOL1__", locatorsG, false, -1,
                                        -1, -1, false, group1);
    getHelper()->createRegionAndAttachPool(regionNames[0], USE_ACK,
                                           "__TEST_POOL1__", false);
    LOG("StepOne_Pooled_Locator1 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, StepOne_Pooled_Locator2)
  {
    initClient(true);
    getHelper()->createPoolWithLocators("__TEST_POOL2__", locatorsG, false, -1,
                                        -1, -1, false, group2);
    getHelper()->createRegionAndAttachPool(regionNames[0], USE_ACK,
                                           "__TEST_POOL2__", false);
    LOG("StepOne_Pooled_Locator2 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, StepOne_Pooled_Locator3)
  {
    initClient(true);
    getHelper()->createPoolWithLocators("__TEST_POOL3__", locatorsG, false, -1,
                                        -1, -1, false, group3);
    getHelper()->createRegionAndAttachPool(regionNames[0], USE_ACK,
                                           "__TEST_POOL3__", false);
    LOG("StepOne_Pooled_Locator3 complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CheckPrSingleHopForIntKeysTask_CLIENT1)
  {
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT1 started.");
    int failureCount = 0;
    int nonSingleHopCount = 0, metadatarefreshCount = 0;

    RegionPtr dataReg = getHelper()->getRegion(regionNames[0]);

    for (int i = 0; i < 1000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGINFO("CPPTEST: Putting key %d with hashcode %d", i,
                static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->put(keyPtr, static_cast<int32_t>(keyPtr->hashcode()));
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGINFO("CheckPrSingleHopForIntKeysTask_CLIENT1: networkhop %d ",
                networkhop);
        if (networkhop) {
          failureCount++;
        }
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT1: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");

        StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
        StatisticsType* type = factory->findType("RegionStatistics");
        if (type) {
          Statistics* rStats = factory->findFirstStatisticsByType(type);
          if (rStats) {
            nonSingleHopCount = rStats->getInt((char*)"nonSingleHopCount");
            metadatarefreshCount =
                rStats->getInt((char*)"metaDataRefreshCount");
          }
        }
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT1: nonSingleHopCount is %d & "
            "metadatarefreshCount is %d failureCount = %d",
            nonSingleHopCount, metadatarefreshCount, failureCount);
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL("Put caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL("Put caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: Put caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("Put caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: Put caused random exception");
        cleanProc();
        FAIL("Put caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    // relaxed this limit as it takes time
    ASSERT(failureCount < 70, "Count should be less then 70");
    ASSERT(nonSingleHopCount < 70, "nonSingleHopCount should be less than 70");
    ASSERT(metadatarefreshCount < 70,
           "metadatarefreshCount should be less than 70");
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT1 put completed.");

    for (int i = 0; i < 1000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGINFO("CPPTEST: getting key %d with hashcode %d", i,
                static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->get(keyPtr);
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGINFO("CheckPrSingleHopForIntKeysTask_CLIENT1: networkhop %d ",
                networkhop);
        ASSERT(!networkhop, "It is networkhop operation.");
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT1: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: get caused extra hop.");
        FAIL("get caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: get caused extra hop.");
        FAIL("get caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: get caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("get caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: get caused random exception");
        cleanProc();
        FAIL("get caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT1 get completed.");

    for (int i = 1000; i < 2000; i++) {
      VectorOfCacheableKey keys;
      for (int j = i; j < i + 5; j++) {
        keys.push_back(CacheableInt32::create(j));
      }

      HashMapOfCacheablePtr values(new HashMapOfCacheable());
      HashMapOfExceptionPtr exceptions(new HashMapOfException());

      try {
        dataReg->getAll(keys, values, exceptions, false);
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGINFO("CheckPrSingleHopForIntKeysTask_CLIENT1: networkhop %d ",
                networkhop);
        ASSERT(!networkhop, "It is networkhop operation.");
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT1: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: getAll caused extra hop.");
        FAIL("getAll caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: getAll caused extra hop.");
        FAIL("getAll caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: getALL caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("getAll caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: getAll caused random exception");
        cleanProc();
        FAIL("getAll caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT1 getAll completed.");

    for (int i = 0; i < 1000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGINFO("CPPTEST: destroying key %d with hashcode %d", i,
                static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->destroy(keyPtr);
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGINFO("CheckPrSingleHopForIntKeysTask_CLIENT1: networkhop %d ",
                networkhop);
        ASSERT(!networkhop, "It is networkhop operation.");
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT1: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: destroy caused extra hop.");
        FAIL("destroy caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: destroy caused extra hop.");
        FAIL("destroy caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: destroy caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("destroy caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: destroy caused random exception");
        cleanProc();
        FAIL("destroy caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT1 destroy completed.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CheckPrSingleHopForIntKeysTask_CLIENT2)
  {
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT2 started.");
    int failureCount = 0;
    int nonSingleHopCount = 0, metadatarefreshCount = 0;

    RegionPtr dataReg = getHelper()->getRegion(regionNames[0]);

    for (int i = 0; i < 1000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGINFO("CPPTEST: Putting key %d with hashcode %d", i,
                static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->put(keyPtr, static_cast<int32_t>(keyPtr->hashcode()));
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGINFO("CheckPrSingleHopForIntKeysTask_CLIENT2: networkhop %d ",
                networkhop);
        if (networkhop) {
          failureCount++;
        }
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT2: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");

        StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
        StatisticsType* type = factory->findType("RegionStatistics");
        if (type) {
          Statistics* rStats = factory->findFirstStatisticsByType(type);
          if (rStats) {
            nonSingleHopCount = rStats->getInt((char*)"nonSingleHopCount");
            metadatarefreshCount =
                rStats->getInt((char*)"metaDataRefreshCount");
          }
        }
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT2: nonSingleHopCount is %d & "
            "metadatarefreshCount is %d ",
            nonSingleHopCount, metadatarefreshCount);
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL("Put caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL("Put caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: Put caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("Put caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: Put caused random exception");
        cleanProc();
        FAIL("Put caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    ASSERT(failureCount > 0, "Count should be greater than 1");
    ASSERT(nonSingleHopCount > 0, "nonSingleHopCount should be greater than 1");
    ASSERT(metadatarefreshCount > 0,
           "metadatarefreshCount should be greater than 1");
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT2 put completed.");

    for (int i = 0; i < 1000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGINFO("CPPTEST: getting key %d with hashcode %d", i,
                static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->get(keyPtr);
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT2: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: get caused extra hop.");
        FAIL("get caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: get caused extra hop.");
        FAIL("get caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: get caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("get caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: get caused random exception");
        cleanProc();
        FAIL("get caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT2 get completed.");

    for (int i = 1000; i < 2000; i++) {
      VectorOfCacheableKey keys;
      for (int j = i; j < i + 5; j++) {
        keys.push_back(CacheableInt32::create(j));
      }

      HashMapOfCacheablePtr values(new HashMapOfCacheable());
      HashMapOfExceptionPtr exceptions(new HashMapOfException());

      try {
        dataReg->getAll(keys, values, exceptions, false);
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOG("after gatall ");
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT2: serverGroupFlag is %d "
            "getall size = %d",
            serverGroupFlag, values->size());
        ASSERT(values->size() == 5, "getall size should be 5 ");
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: getAll caused extra hop.");
        FAIL("getAll caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: getAll caused extra hop.");
        FAIL("getAll caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: getALL caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("getAll caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: getAll caused random exception");
        cleanProc();
        FAIL("getAll caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT2 getAll completed.");

    for (int i = 0; i < 1000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGINFO("CPPTEST: destroying key %d with hashcode %d", i,
                static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->destroy(keyPtr);
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT2: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: destroy caused extra hop.");
        FAIL("destroy caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: destroy caused extra hop.");
        FAIL("destroy caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: destroy caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("destroy caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: destroy caused random exception");
        cleanProc();
        FAIL("destroy caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT2 destroy completed.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, CheckPrSingleHopForIntKeysTask_CLIENT3)
  {
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT3 started.");
    int failureCount = 0;
    int nonSingleHopCount = 0, metadatarefreshCount = 0;

    RegionPtr dataReg = getHelper()->getRegion(regionNames[0]);

    for (int i = 0; i < 1000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGINFO("CPPTEST: Putting key %d with hashcode %d", i,
                static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->put(keyPtr, static_cast<int32_t>(keyPtr->hashcode()));
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGINFO("CheckPrSingleHopForIntKeysTask_CLIENT3: networkhop %d ",
                networkhop);
        if (networkhop) {
          failureCount++;
        }
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT3: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");

        StatisticsFactory* factory = StatisticsFactory::getExistingInstance();
        StatisticsType* type = factory->findType("RegionStatistics");
        if (type) {
          Statistics* rStats = factory->findFirstStatisticsByType(type);
          if (rStats) {
            nonSingleHopCount = rStats->getInt((char*)"nonSingleHopCount");
            metadatarefreshCount =
                rStats->getInt((char*)"metaDataRefreshCount");
          }
        }
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT3: nonSingleHopCount is %d & "
            "metadatarefreshCount is %d ",
            nonSingleHopCount, metadatarefreshCount);
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL("Put caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL("Put caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: Put caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("Put caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: Put caused random exception");
        cleanProc();
        FAIL("Put caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    ASSERT(failureCount > 0, "Count should be greater than 1");
    ASSERT(nonSingleHopCount > 0, "nonSingleHopCount should be greater than 1");
    ASSERT(metadatarefreshCount > 0,
           "metadatarefreshCount should be greater than 1");
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT3 put completed.");

    for (int i = 0; i < 1000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGINFO("CPPTEST: getting key %d with hashcode %d", i,
                static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->get(keyPtr);
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT3: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: get caused extra hop.");
        FAIL("get caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: get caused extra hop.");
        FAIL("get caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: get caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("get caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: get caused random exception");
        cleanProc();
        FAIL("get caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT3 get completed.");

    for (int i = 1000; i < 2000; i++) {
      VectorOfCacheableKey keys;
      for (int j = i; j < i + 5; j++) {
        keys.push_back(CacheableInt32::create(j));
      }

      HashMapOfCacheablePtr values(new HashMapOfCacheable());
      HashMapOfExceptionPtr exceptions(new HashMapOfException());

      try {
        dataReg->getAll(keys, values, exceptions, false);
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT3: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: getAll caused extra hop.");
        FAIL("getAll caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: getAll caused extra hop.");
        FAIL("getAll caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: getALL caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("getAll caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: getAll caused random exception");
        cleanProc();
        FAIL("getAll caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT3 getAll completed.");

    for (int i = 0; i < 1000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGINFO("CPPTEST: destroying key %d with hashcode %d", i,
                static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->destroy(keyPtr);
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask_CLIENT3: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: destroy caused extra hop.");
        FAIL("destroy caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: destroy caused extra hop.");
        FAIL("destroy caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: destroy caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("destroy caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: destroy caused random exception");
        cleanProc();
        FAIL("destroy caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    LOG("CheckPrSingleHopForIntKeysTask_CLIENT3 destroy completed.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, CloseCache2)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, CloseCache3)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServers_With_Locator_PR)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }

    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }

    if (isLocalServer) {
      CacheHelper::closeServer(3);
      LOG("SERVER3 stopped");
    }

    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServers_With_Locator_PR)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");

    // starting servers
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver1_partitioned_servergroup.xml",
                              locatorsG);
    }

    // starting servers
    if (isLocalServer) {
      CacheHelper::initServer(2, "cacheserver2_partitioned_servergroup.xml",
                              locatorsG);
    }

    // starting servers
    if (isLocalServer) {
      CacheHelper::initServer(3, "cacheserver3_partitioned_servergroup.xml",
                              locatorsG);
    }
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    // server-group applicable only with locators, so no test case with pool
    // endpoints.

    CALL_TASK(CreateServers_With_Locator_PR);

    CALL_TASK(StepOne_Pooled_Locator1);
    CALL_TASK(StepOne_Pooled_Locator2);
    CALL_TASK(StepOne_Pooled_Locator3);

    CALL_TASK(CheckPrSingleHopForIntKeysTask_CLIENT1);
    CALL_TASK(CheckPrSingleHopForIntKeysTask_CLIENT2);
    CALL_TASK(CheckPrSingleHopForIntKeysTask_CLIENT3);

    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseCache3);

    CALL_TASK(CloseServers_With_Locator_PR);
  }
END_MAIN
