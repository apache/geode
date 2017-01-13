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
#include <gfcpp/FixedPartitionResolver.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>

#include <string>

#include "CacheHelper.hpp"

// Include these 2 headers for access to CacheImpl for test hooks.
#include "CacheImplHelper.hpp"
#include "testUtils.hpp"

#include "ThinClientHelper.hpp"

const char* partitionRegionNames[] = {"R1", "R2", "R3"};
const char* partitionRegionName;

using namespace gemfire;
class CustomFixedPartitionResolver1 : public FixedPartitionResolver {
 public:
  CustomFixedPartitionResolver1() {}
  ~CustomFixedPartitionResolver1() {}
  const char* getName() {
    LOG("CustomFixedPartitionResolver1::getName()");
    return "CustomFixedPartitionResolver1";
  }

  CacheableKeyPtr getRoutingObject(const EntryEvent& opDetails) {
    LOG("CustomFixedPartitionResolver1::getRoutingObject()");
    int32_t key = atoi(opDetails.getKey()->toString()->asChar());
    int32_t newKey = key + 5;
    return CacheableKey::create(newKey);
  }

  const char* getPartitionName(const EntryEvent& opDetails) {
    LOG("CustomFixedPartitionResolver1::getPartitionName()");
    int32_t key = atoi(opDetails.getKey()->toString()->asChar());
    int32_t newkey = key % 6;
    if (newkey == 0) {
      return "P1";
    } else if (newkey == 1) {
      return "P2";
    } else if (newkey == 2) {
      return "P3";
    } else if (newkey == 3) {
      return "P4";
    } else if (newkey == 4) {
      return "P5";
    } else if (newkey == 5) {
      return "P6";
    } else {
      return "Invalid";
    }
  }
};
FixedPartitionResolverPtr cptr1(new CustomFixedPartitionResolver1());

class CustomFixedPartitionResolver2 : public FixedPartitionResolver {
 public:
  CustomFixedPartitionResolver2() {}
  ~CustomFixedPartitionResolver2() {}
  const char* getName() {
    LOG("CustomFixedPartitionResolver2::getName()");
    return "CustomFixedPartitionResolver2";
  }

  CacheableKeyPtr getRoutingObject(const EntryEvent& opDetails) {
    LOG("CustomFixedPartitionResolver2::getRoutingObject()");
    int32_t key = atoi(opDetails.getKey()->toString()->asChar());
    int32_t newKey = key + 4;
    return CacheableKey::create(newKey /*key*/);
  }

  const char* getPartitionName(const EntryEvent& opDetails) {
    LOG("CustomFixedPartitionResolver2::getPartitionName()");
    int32_t key = atoi(opDetails.getKey()->toString()->asChar());
    int32_t newkey = key % 6;
    if (newkey == 0) {
      return "P1";
    } else if (newkey == 1) {
      return "P2";
    } else if (newkey == 2) {
      return "P3";
    } else if (newkey == 3) {
      return "P4";
    } else if (newkey == 4) {
      return "P5";
    } else if (newkey == 5) {
      return "P6";
    } else {
      return "Invalid";
    }
  }
};
FixedPartitionResolverPtr cptr2(new CustomFixedPartitionResolver2());

class CustomFixedPartitionResolver3 : public FixedPartitionResolver {
 public:
  CustomFixedPartitionResolver3() {}
  ~CustomFixedPartitionResolver3() {}
  const char* getName() {
    LOG("CustomFixedPartitionResolver3::getName()");
    return "CustomFixedPartitionResolver3";
  }

  CacheableKeyPtr getRoutingObject(const EntryEvent& opDetails) {
    LOG("CustomFixedPartitionResolver3::getRoutingObject()");
    int32_t key = atoi(opDetails.getKey()->toString()->asChar());
    int32_t newKey = key % 5;
    return CacheableKey::create(newKey /*key*/);
  }

  const char* getPartitionName(const EntryEvent& opDetails) {
    LOG("CustomFixedPartitionResolver3::getPartitionName()");
    int32_t key = atoi(opDetails.getKey()->toString()->asChar());
    int32_t newkey = key % 3;
    if (newkey == 0) {
      return "P1";
    } else if (newkey == 1) {
      return "P2";
    } else if (newkey == 2) {
      return "P3";
    } else {
      return "Invalid";
    }
  }
};
FixedPartitionResolverPtr cptr3(new CustomFixedPartitionResolver3());

#define CLIENT1 s1p1
#define SERVER1 s2p1
#define SERVER2 s1p2
#define SERVER3 s2p2

bool isLocalServer = false;

static bool isLocator = false;
const char* locatorsG = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);

std::vector<char*> storeEndPoints(const char* points) {
  std::vector<char*> endpointNames;
  if (points != NULL) {
    char* ep = strdup(points);
    char* token = strtok(ep, ",");
    while (token) {
      endpointNames.push_back(token);
      token = strtok(NULL, ",");
    }
  }
  ASSERT(endpointNames.size() == 3, "There should be 3 end points");
  return endpointNames;
}

DUNIT_TASK_DEFINITION(CLIENT1, SetRegion1)
  { partitionRegionName = partitionRegionNames[0]; }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetRegion2)
  { partitionRegionName = partitionRegionNames[1]; }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, SetRegion3)
  { partitionRegionName = partitionRegionNames[2]; }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) CacheHelper::initServer(1, "cacheserver1_fpr.xml");
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
  {
    if (isLocalServer) CacheHelper::initServer(2, "cacheserver2_fpr.xml");
    LOG("SERVER2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER3, CreateServer3)
  {
    if (isLocalServer) CacheHelper::initServer(3, "cacheserver3_fpr.xml");
    LOG("SERVER3 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, StepOne_Pooled_Locator)
  {
    initClient(true);

    getHelper()->createPoolWithLocators("__TEST_POOL1__", locatorsG);
    getHelper()->createRegionAndAttachPool2(partitionRegionNames[0], USE_ACK,
                                            "__TEST_POOL1__", cptr1);
    getHelper()->createRegionAndAttachPool2(partitionRegionNames[1], USE_ACK,
                                            "__TEST_POOL1__", cptr2);
    getHelper()->createRegionAndAttachPool2(partitionRegionNames[2], USE_ACK,
                                            "__TEST_POOL1__", cptr3);

    LOG("StepOne_Pooled_Locator complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CheckPrSingleHopForIntKeysTask_REGION)
  {
    LOG("CheckPrSingleHopForIntKeysTask_REGION started.");
    int failureCount = 0;

    LOGDEBUG("CheckPrSingleHopForIntKeysTask_REGION create region  = %s ",
             partitionRegionName);
    RegionPtr dataReg = getHelper()->getRegion(partitionRegionName);

    for (int i = 0; i < 3000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGDEBUG("CPPTEST: Putting key %d with hashcode %d", i,
                 static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->put(keyPtr, static_cast<int32_t>(keyPtr->hashcode()));
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGDEBUG("CheckPrSingleHopForIntKeysTask_REGION: networkhop %d ",
                 networkhop);
        if (networkhop) {
          failureCount++;
        }
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGDEBUG(
            "CheckPrSingleHopForIntKeysTask_REGION: serverGroupFlag is %d "
            "failureCount = %d",
            serverGroupFlag, failureCount);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");
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
    ASSERT(failureCount < 70, "Count should be less than 70");
    LOG("CheckPrSingleHopForIntKeysTask_REGION put completed.");

    for (int i = 0; i < 1000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGDEBUG("CPPTEST: getting key %d with hashcode %d", i,
                 static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->get(keyPtr);
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGDEBUG("CheckPrSingleHopForIntKeysTask_REGION: networkhop %d ",
                 networkhop);
        ASSERT(!networkhop, "It is networkhop operation.");
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGDEBUG(
            "CheckPrSingleHopForIntKeysTask_REGION: serverGroupFlag is %d ",
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
    LOG("CheckPrSingleHopForIntKeysTask_REGION get completed.");

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
        ASSERT(values->size() == 5, "number of value size should be 5");
        LOGDEBUG("CheckPrSingleHopForIntKeysTask_REGION: networkhop %d ",
                 networkhop);
        ASSERT(!networkhop, "It is networkhop operation.");
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGDEBUG(
            "CheckPrSingleHopForIntKeysTask_REGION: serverGroupFlag is %d ",
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

      try {
        dataReg->getAll(keys, values, exceptions, false,
                        CacheableInt32::create(1000));
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        ASSERT(values->size() == 5, "number of value size should be 5");
        LOGDEBUG("CheckPrSingleHopForIntKeysTask_REGION: networkhop %d ",
                 networkhop);
        ASSERT(!networkhop, "It is networkhop operation.");
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGDEBUG(
            "CheckPrSingleHopForIntKeysTask_REGION: serverGroupFlag is %d ",
            serverGroupFlag);
        ASSERT(serverGroupFlag != 2,
               "serverGroupFlag should not be equal to 2");
      } catch (CacheServerException&) {
        LOGERROR("CPPTEST: getAllwithCallBackArg caused extra hop.");
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
    LOG("CheckPrSingleHopForIntKeysTask_REGION getAll completed.");

    for (int i = 0; i < 1000; i++) {
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));

      try {
        LOGDEBUG("CPPTEST: destroying key %d with hashcode %d", i,
                 static_cast<int32_t>(keyPtr->hashcode()));
        dataReg->destroy(keyPtr);
        bool networkhop = TestUtils::getCacheImpl(getHelper()->cachePtr)
                              ->getAndResetNetworkHopFlag();
        LOGDEBUG("CheckPrSingleHopForIntKeysTask_REGION: networkhop %d ",
                 networkhop);
        ASSERT(!networkhop, "It is networkhop operation.");
        int8 serverGroupFlag = TestUtils::getCacheImpl(getHelper()->cachePtr)
                                   ->getAndResetServerGroupFlag();
        LOGDEBUG(
            "CheckPrSingleHopForIntKeysTask_REGION: serverGroupFlag is %d ",
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
    LOG("CheckPrSingleHopForIntKeysTask_REGION destroy completed.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
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

DUNIT_TASK_DEFINITION(SERVER1, CreateLocator1)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
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

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1_With_Locator_PR)
  {
    // starting servers
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver1_fpr.xml", locatorsG);
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2_With_Locator_PR)
  {
    // starting servers
    if (isLocalServer) {
      CacheHelper::initServer(2, "cacheserver2_fpr.xml", locatorsG);
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER3, CreateServer3_With_Locator_PR)
  {
    // starting servers
    if (isLocalServer) {
      CacheHelper::initServer(3, "cacheserver3_fpr.xml", locatorsG);
    }
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(CreateLocator1);

    CALL_TASK(CreateServer1_With_Locator_PR);
    CALL_TASK(CreateServer2_With_Locator_PR);
    CALL_TASK(CreateServer3_With_Locator_PR);

    CALL_TASK(StepOne_Pooled_Locator);

    CALL_TASK(SetRegion1);
    CALL_TASK(CheckPrSingleHopForIntKeysTask_REGION);

    CALL_TASK(SetRegion2);
    CALL_TASK(CheckPrSingleHopForIntKeysTask_REGION);

    CALL_TASK(SetRegion3);
    CALL_TASK(CheckPrSingleHopForIntKeysTask_REGION);

    CALL_TASK(CloseCache1);

    CALL_TASK(CloseServer1);
    CALL_TASK(CloseServer2);
    CALL_TASK(CloseServer3);

    CALL_TASK(CloseLocator1);
  }
END_MAIN
