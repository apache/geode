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
#include <gfcpp/PartitionResolver.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>

#include <string>

#define ROOT_NAME "testThinClientPartitionResolver"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"

// Include these 2 headers for access to CacheImpl for test hooks.
#include "CacheImplHelper.hpp"
#include "testUtils.hpp"

#include "ThinClientHelper.hpp"

using namespace gemfire;

class CustomPartitionResolver : public PartitionResolver {
 public:
  bool called;

  CustomPartitionResolver() : called(false) {}
  ~CustomPartitionResolver() {}
  const char *getName() {
    LOG("CustomPartitionResolver::getName()");
    return "CustomPartitionResolver";
  }

  CacheableKeyPtr getRoutingObject(const EntryEvent &opDetails) {
    called = true;
    LOG("CustomPartitionResolver::getRoutingObject()");
    int32_t key = atoi(opDetails.getKey()->toString()->asChar());
    int32_t newKey = key + 5;
    return CacheableKey::create(newKey);
  }
};
CustomPartitionResolver *cpr = new CustomPartitionResolver();
PartitionResolverPtr cptr(cpr);

#define CLIENT1 s1p1
#define SERVER1 s2p1
#define SERVER2 s1p2

bool isLocalServer = false;
const char *endPoints = CacheHelper::getTcrEndpoints(isLocalServer, 3);

std::vector<char *> storeEndPoints(const char *points) {
  std::vector<char *> endpointNames;
  if (points != NULL) {
    char *ep = strdup(points);
    char *token = strtok(ep, ",");
    while (token) {
      endpointNames.push_back(token);
      token = strtok(NULL, ",");
    }
  }
  ASSERT(endpointNames.size() == 3, "There should be 3 end points");
  return endpointNames;
}

std::vector<char *> endpointNames = storeEndPoints(endPoints);

DUNIT_TASK_DEFINITION(SERVER1, CreateServer1)
  {
    if (isLocalServer) CacheHelper::initServer(1, "cacheserver1_pr.xml");
    LOG("SERVER1 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER2, CreateServer2)
  {
    if (isLocalServer) CacheHelper::initServer(2, "cacheserver2_pr.xml");
    LOG("SERVER2 started");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CreatePoolAndRegions)
  {
    initClient(true);

    char endpoints[1024] = {0};
    sprintf(endpoints, "%s,%s,%s", endpointNames.at(0), endpointNames.at(1),
            endpointNames.at(2));

    getHelper()->createPoolWithLocators("__TEST_POOL1__", NULL);
    getHelper()->createRegionAndAttachPool2(regionNames[0], USE_ACK,
                                            "__TEST_POOL1__", cptr);
    // getHelper()->createRegionAndAttachPool2(regionNames[1], NO_ACK,
    // "__TEST_POOL1__",cptr);

    LOG("CreatePoolAndRegions complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutThroughPartitionResolver)
  {
    LOG("PutThroughPartitionResolver started.");

    for (int i = 0; i < 100; i++) {
      // RegionPtr dataReg = getHelper()->getRegion("LocalRegion");
      RegionPtr dataReg = getHelper()->getRegion(regionNames[0]);
      CacheableKeyPtr keyPtr =
          dynCast<CacheableKeyPtr>(CacheableInt32::create(i));
      dataReg->put(keyPtr, static_cast<int32_t>(keyPtr->hashcode()));
    }
    SLEEP(5000);
    ASSERT(cpr->called, "Partition resolver not called");
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

DUNIT_MAIN
  {
    CacheableHelper::registerBuiltins(true);

    // Need multiple servers to test PartitionResolver
    CALL_TASK(CreateServer1);
    CALL_TASK(CreateServer2);

    CALL_TASK(CreatePoolAndRegions);

    CALL_TASK(PutThroughPartitionResolver);

    CALL_TASK(CloseCache1);

    CALL_TASK(CloseServer1);
    CALL_TASK(CloseServer2);
  }
END_MAIN
