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

#define ROOT_NAME "testThinClientPutAllPRSingleHop"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

// Include these 2 headers for access to CacheImpl for test hooks.
#include "CacheImplHelper.hpp"
#include "testUtils.hpp"

#include "ThinClientHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define SERVER1 s2p1
#define SERVER2 s1p2
#define SERVER3 s2p2

bool isLocalServer = false;

static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);

#if defined(WIN32)
// because we run out of memory on our pune windows desktops
#define DEFAULTNUMKEYS 5
#else
#define DEFAULTNUMKEYS 15
#endif
#define KEYSIZE 256
#define VALUESIZE 1024

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver1_partitioned.xml");
    }
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
  {
    if (isLocalServer) {
      CacheHelper::initServer(2, "cacheserver2_partitioned.xml");
    }
    LOG("SERVER2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER3, CreateServer3)
  {
    if (isLocalServer) {
      CacheHelper::initServer(3, "cacheserver3_partitioned.xml");
    }
    LOG("SERVER3 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Endpoint)
  {
    initClient(true);

    // getHelper()->createPoolWithEPs("__TEST_POOL1__", endpoints);
    getHelper()->createRegionAndAttachPool(
        regionNames[0], USE_ACK, "__TEST_POOL1__",
        true /*false:: caching disabled originally*/);
    getHelper()->createRegionAndAttachPool(regionNames[1], NO_ACK,
                                           "__TEST_POOL1__", true);

    LOG("StepOne_Pooled_EndPoint complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_EndpointTL)
  {
    initClient(true);

    RegionPtr regPtr = getHelper()->createPooledRegionStickySingleHop(
        regionNames[0], USE_ACK, NULL, "__TEST_POOL1__", false, false);
    ASSERT(regPtr != NULLPTR, "Failed to create region.");
    regPtr = getHelper()->createPooledRegionStickySingleHop(
        regionNames[1], NO_ACK, NULL, "__TEST_POOL1__", false, false);
    ASSERT(regPtr != NULLPTR, "Failed to create region.");

    LOG("StepOne_Pooled_EndPointTL complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, WarmUpTask)
  {
    LOG("WarmUpTask started.");
    int failureCount = 0;
    int nonSingleHopCount = 0, metadatarefreshCount = 0;
    RegionPtr dataReg = getHelper()->getRegion(regionNames[0]);

    // This is to get MetaDataService going.
    for (int i = 3000; i < 8000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));
      try {
        LOGINFO("CPPTEST: put item %d", i);
        dataReg->put(keyPtr, static_cast<int32_t>(keyPtr->hashcode()));
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGINFO("WarmUpTask: networkhop is %d ", networkhop);
        if (networkhop) {
          failureCount++;
        }
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
            "WarmUpTask: nonSingleHopCount is %d & metadatarefreshCount is %d "
            "failureCount = %d",
            nonSingleHopCount, metadatarefreshCount, failureCount);
        LOGINFO("CPPTEST: put success ");
      } catch (CacheServerException&) {
        // This is actually a success situation!
        // bool singlehop = TestUtils::getCacheImpl(getHelper(
        // )->cachePtr)->getAndResetSingleHopFlag();
        // if (!singlehop) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL("Put caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
        //}
        // LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while putting key %s with
        // hashcode %d", logmsg, (int32_t)keyPtr->hashcode());
      } catch (CacheWriterException&) {
        // This is actually a success situation! Once bug #521 is fixed.
        // bool singlehop = TestUtils::getCacheImpl(getHelper(
        // )->cachePtr)->getAndResetSingleHopFlag();
        // if (!singlehop) {
        LOGERROR("CPPTEST: Put caused extra hop.");
        FAIL("Put caused extra hop.");
        throw IllegalStateException("TEST FAIL DUE TO EXTRA HOP");
        //}
        // LOGINFO("CPPTEST: SINGLEHOP SUCCEEDED while putting key %s with
        // hashcode %d", logmsg, (int32_t)keyPtr->hashcode());
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: Unexpected %s: %s", ex.getName(), ex.getMessage());
        FAIL(ex.getMessage());
      } catch (...) {
        LOGERROR("CPPTEST: Put caused random exception in WarmUpTask");
        cleanProc();
        FAIL("Put caused unexpected exception");
        throw IllegalStateException("TEST FAIL");
      }
    }
    // it takes time to fetch prmetadata so relaxing this limit
    ASSERT(failureCount < 100, "Count should be less than 100");
    ASSERT(nonSingleHopCount < 100,
           "nonSingleHopCount should be less than 100");
    ASSERT(metadatarefreshCount < 100,
           "metadatarefreshCount should be less than 100");

    SLEEP(20000);

    LOG("WarmUpTask completed.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CheckPrSingleHopForIntKeysTask)
  {
    LOG("CheckPrSingleHopForIntKeysTask started.");

    RegionPtr dataReg = getHelper()->getRegion(regionNames[0]);

    LOG("CheckPrSingleHopForIntKeysTask get completed.");

    {
      LOGINFO("Iteration putALL Start ");
      try {
        HashMapOfCacheable valMap;

        for (int j = 1000; j < 25000; j++) {
          CacheableKeyPtr keyPtr =
              dynCast<CacheableKeyPtr>(CacheableInt32::create(j));
          CacheablePtr valPtr =
              dynCast<CacheablePtr>(CacheableInt32::create(keyPtr->hashcode()));
          LOGINFO("CPPTEST: putALL CASE:: getting key %d with hashcode %d", j,
                  static_cast<int32_t>(keyPtr->hashcode()));
          valMap.insert(keyPtr, valPtr);
        }
        LOGINFO("TEST-1");
        ACE_Time_Value startTime = ACE_OS::gettimeofday();
        dataReg->putAll(valMap);
        ACE_Time_Value interval = ACE_OS::gettimeofday() - startTime;

        LOGINFO("Time taken to execute putAll SH sec = %d and MSec = %d ",
                interval.sec(), interval.usec());
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGINFO("CheckPrSingleHopForIntKeysTask2: putALL OP :: networkhop %d ",
                networkhop);
        ASSERT(networkhop == false, "PutAll : Should not cause network hop");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: putAll caused extra hop.");
        FAIL("putAll caused extra hop.");
        throw IllegalStateException("putAll :: TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: putAll caused extra hop.");
        FAIL("putAll caused extra hop.");
        throw IllegalStateException("putAll::TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: putAll caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("putAll caused unexpected exception");
        throw IllegalStateException("putAll::TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: putAll caused random exception");
        cleanProc();
        FAIL("putAll caused unexpected exception");
        throw IllegalStateException("putAll::TEST FAIL");
      }
    }
    int poolconn = TestUtils::getCacheImpl(getHelper()->cachePtr)
                       ->getPoolSize("__TEST_POOL1__");
    LOGINFO("CheckPrSingleHopForIntKeysTask: poolconn is %d ", poolconn);
    LOG("CheckPrSingleHopForIntKeysTask get completed.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CheckPrSingleHopRemoveAllForIntKeysTask)
  {
    LOG("CheckPrSingleHopRemoveAllForIntKeysTask started.");

    RegionPtr dataReg = getHelper()->getRegion(regionNames[0]);

    LOG("CheckPrSingleHopForIntKeysTask get completed.");

    {
      LOGINFO("Iteration removeAll Start ");
      try {
        HashMapOfCacheable valMap;
        VectorOfCacheableKey keys;
        for (int j = 1000; j < 25000; j++) {
          CacheableKeyPtr keyPtr =
              dynCast<CacheableKeyPtr>(CacheableInt32::create(j));
          CacheablePtr valPtr =
              dynCast<CacheablePtr>(CacheableInt32::create(keyPtr->hashcode()));
          LOGINFO("CPPTEST: removeall CASE:: getting key %d with hashcode %d",
                  j, static_cast<int32_t>(keyPtr->hashcode()));
          valMap.insert(keyPtr, valPtr);
          keys.push_back(keyPtr);
        }
        LOGINFO("TEST-1");
        ACE_Time_Value startTime = ACE_OS::gettimeofday();
        dataReg->putAll(valMap);
        ACE_Time_Value interval = ACE_OS::gettimeofday() - startTime;

        LOGINFO("Time taken to execute putAll SH sec = %d and MSec = %d ",
                interval.sec(), interval.usec());
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGINFO("CheckPrSingleHopForIntKeysTask2: putALL OP :: networkhop %d ",
                networkhop);
        ASSERT(networkhop == false, "PutAll : Should not cause network hop");

        LOGINFO("RemoveALL test");
        startTime = ACE_OS::gettimeofday();
        dataReg->removeAll(keys);
        interval = ACE_OS::gettimeofday() - startTime;

        LOGINFO("Time taken to execute removeAll SH sec = %d and MSec = %d ",
                interval.sec(), interval.usec());
        networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                         ->getAndResetNetworkHopFlag();
        LOGINFO(
            "CheckPrSingleHopForIntKeysTask2: removeall OP :: networkhop %d ",
            networkhop);
        ASSERT(networkhop == false, "RemoveAll : Should not cause network hop");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: removeall caused extra hop.");
        FAIL("removeall caused extra hop.");
        throw IllegalStateException("removeall :: TEST FAIL DUE TO EXTRA HOP");
      } catch (CacheWriterException&) {
        LOGERROR("CPPTEST: removeall caused extra hop.");
        FAIL("removeall caused extra hop.");
        throw IllegalStateException("removeall::TEST FAIL DUE TO EXTRA HOP");
      } catch (Exception& ex) {
        LOGERROR("CPPTEST: removeall caused unexpected %s: %s", ex.getName(),
                 ex.getMessage());
        cleanProc();
        FAIL("putAll caused unexpected exception");
        throw IllegalStateException("removeall::TEST FAIL");
      } catch (...) {
        LOGERROR("CPPTEST: removeall caused random exception");
        cleanProc();
        FAIL("removeall caused unexpected exception");
        throw IllegalStateException("removeall::TEST FAIL");
      }
    }
    int poolconn = TestUtils::getCacheImpl(getHelper()->cachePtr)
                       ->getPoolSize("__TEST_POOL1__");
    LOGINFO("CheckPrSingleHopRemoveAllForIntKeysTask: poolconn is %d ",
            poolconn);
    LOG("CheckPrSingleHopRemoveAllForIntKeysTask get completed.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  {
    PoolPtr pool = PoolManager::find("__TEST_POOL1__");
    if (pool->getThreadLocalConnections()) {
      LOG("releaseThreadLocalConnection1 doing...");
      pool->releaseThreadLocalConnection();
      LOG("releaseThreadLocalConnection1 done");
    }
    cleanProc();
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

DUNIT_TASK_DEFINITION(SERVER2, CloseServer2)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER3, CloseServer3)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(3);
      LOG("SERVER3 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CacheableHelper::registerBuiltins(true);

    // First pool with endpoints
    /////////////////////////////////////////////
    /*TODO:: change k < 1 to its original k < 2*/
    for (int k = 0; k < 1; k++) {
      CALL_TASK(CreateServer1);
      CALL_TASK(CreateServer2);
      CALL_TASK(CreateServer3);

      if (k == 0) {
        CALL_TASK(StepOne_Pooled_Endpoint);
      } else {
        CALL_TASK(StepOne_Pooled_EndpointTL);  // StickySingleHop Case
      }

      CALL_TASK(WarmUpTask);  //

      CALL_TASK(CheckPrSingleHopForIntKeysTask);  //

      CALL_TASK(CloseCache1);

      CALL_TASK(CloseServer1);
      CALL_TASK(CloseServer2);
      CALL_TASK(CloseServer3);
    }
  }
END_MAIN

DUNIT_MAIN
  {
    CacheableHelper::registerBuiltins(true);

    // First pool with endpoints
    /////////////////////////////////////////////
    /*TODO:: change k < 1 to its original k < 2*/
    for (int k = 0; k < 1; k++) {
      CALL_TASK(CreateServer1);
      CALL_TASK(CreateServer2);
      CALL_TASK(CreateServer3);

      if (k == 0) {
        CALL_TASK(StepOne_Pooled_Endpoint);
      } else {
        CALL_TASK(StepOne_Pooled_EndpointTL);  // StickySingleHop Case
      }

      CALL_TASK(WarmUpTask);  //

      CALL_TASK(CheckPrSingleHopRemoveAllForIntKeysTask);  //

      CALL_TASK(CloseCache1);

      CALL_TASK(CloseServer1);
      CALL_TASK(CloseServer2);
      CALL_TASK(CloseServer3);
    }
  }
END_MAIN
