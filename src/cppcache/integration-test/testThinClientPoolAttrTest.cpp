/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include "testUtils.hpp"

#include "gfcpp/GemfireCppCache.hpp"

/* This is to test
1) If Connections are left idle ,they timed out to min connections.
2) To validate PoolAttributes.
3) Create two pools with same name(in parallel threads).It should fail.
*/

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define LOCATOR1 s2p1
#define SERVER s2p2

bool isLocalServer = false;
bool isLocator = false;

const char* locHostPort = CacheHelper::getLocatorHostPort(isLocator, isLocalServer, 1);
const char* poolRegNames[] = {"PoolRegion1"};
const char* poolName = "__TEST_POOL1__";
const char* poolName1 = "clientPool";

const char* serverGroup = "ServerGroup1";
CachePtr cachePtr;

class putThread : public ACE_Task_Base {
 private:
  RegionPtr regPtr;

 public:
  explicit putThread(const char* name) : regPtr(getHelper()->getRegion(name)) {}

  int svc(void) {
    // TODO: No. of connection should be = minConnection

    for (int i = 0; i < 10000; i++) {
      try {
        regPtr->put(keys[i % 5], vals[i % 6]);
      } catch (const Exception&) {
        // ignore
      } catch (...) {
        // ignore
      }
      // TODO: Check no. of connection > minConnetion
    }
    // LOG(" Incremented 100 times by thread.");
    return 0;
  }
  void start() { activate(); }
  void stop() { wait(); }
};

void doAttrTestingAndCreatePool(const char* poolName) {
  PoolFactoryPtr poolFacPtr = PoolManager::createFactory();
  poolFacPtr->setFreeConnectionTimeout(10000);
  poolFacPtr->setLoadConditioningInterval(60000);
  poolFacPtr->setSocketBufferSize(1024);
  poolFacPtr->setReadTimeout(10000);
  poolFacPtr->setMinConnections(4);
  poolFacPtr->setMaxConnections(8);
  poolFacPtr->setIdleTimeout(5000);
  poolFacPtr->setRetryAttempts(5);
  poolFacPtr->setPingInterval(120000);
  poolFacPtr->setUpdateLocatorListInterval(122000);
  poolFacPtr->setStatisticInterval(120000);
  poolFacPtr->setServerGroup(serverGroup);
  poolFacPtr->setSubscriptionEnabled(true);
  poolFacPtr->setSubscriptionRedundancy(1);
  poolFacPtr->setSubscriptionMessageTrackingTimeout(500000);
  poolFacPtr->setSubscriptionAckInterval(120000);
  poolFacPtr->addLocator("localhost", CacheHelper::staticLocatorHostPort1);
  // poolFacPtr->setMultiuserSecurityMode(true);
  poolFacPtr->setPRSingleHopEnabled(false);

  PoolPtr pptr = poolFacPtr->create(poolName);

  // Validate the attributes
  ASSERT(pptr->getFreeConnectionTimeout() == 10000,
         "FreeConnectionTimeout Should have been 10000");
  ASSERT(pptr->getLoadConditioningInterval() == 60000,
         "LoadConditioningInterval Should have been 60000");
  ASSERT(pptr->getSocketBufferSize() == 1024,
         "SocketBufferSize Should have been 1024");
  ASSERT(pptr->getReadTimeout() == 10000, "ReadTimeout Should have been 10000");
  ASSERT(pptr->getMinConnections() == 4, "MinConnections Should have been 4");
  ASSERT(pptr->getMaxConnections() == 8, "MaxConnections Should have been 8");
  ASSERT(pptr->getIdleTimeout() == 5000, "IdleTimeout Should have been 5000");
  ASSERT(pptr->getRetryAttempts() == 5, "RetryAttempts Should have been 5");
  ASSERT(pptr->getPingInterval() == 120000,
         "PingInterval Should have been 120000");
  ASSERT(pptr->getUpdateLocatorListInterval() == 122000,
         "UpdateLocatorListInterval Should have been 122000");
  ASSERT(pptr->getStatisticInterval() == 120000,
         "StatisticInterval Should have been 120000");
  ASSERT(strcmp(pptr->getServerGroup(), "ServerGroup1") == 0,
         "ServerGroup Should have been ServerGroup1");
  ASSERT(pptr->getSubscriptionEnabled() == true,
         "SubscriptionEnabled Should have been true");
  ASSERT(pptr->getSubscriptionRedundancy() == 1,
         "SubscriptionRedundancy Should have been 1");
  ASSERT(pptr->getSubscriptionMessageTrackingTimeout() == 500000,
         "SubscriptionMessageTrackingTimeout Should have been 500000");
  ASSERT(pptr->getSubscriptionAckInterval() == 120000,
         "SubscriptionAckInterval Should have been 120000");
  // ASSERT(pptr->getMultiuserSecurityMode()==true,"SetMultiuserSecurityMode
  // Should have been true");
  ASSERT(pptr->getPRSingleHopEnabled() == false,
         "PRSingleHopEnabled should have been false");
}

void doAttrTesting(const char* poolName1) {
  // PoolFactoryPtr poolFacPtr = cachePtr->getPoolFactory();
  PoolPtr pptr = PoolManager::find(poolName1);
  // PoolPtr pptr = poolFacPtr->find(poolName1);

  ASSERT(strcmp(pptr->getName(), "clientPool") == 0,
         "Pool name should have been clientPool");
  ASSERT(pptr->getFreeConnectionTimeout() == 10000,
         "FreeConnectionTimeout Should have been 10000");
  ASSERT(pptr->getLoadConditioningInterval() == 1,
         "LoadConditioningInterval Should have been 1");
  ASSERT(pptr->getSocketBufferSize() == 1024,
         "SocketBufferSize Should have been 1024");
  ASSERT(pptr->getReadTimeout() == 10, "ReadTimeout Should have been 10");
  ASSERT(pptr->getMinConnections() == 2, "MinConnections Should have been 2");
  ASSERT(pptr->getMaxConnections() == 5, "MaxConnections Should have been 5");
  ASSERT(pptr->getIdleTimeout() == 5, "IdleTimeout Should have been 5");
  ASSERT(pptr->getRetryAttempts() == 5, "RetryAttempts Should have been 5");
  ASSERT(pptr->getPingInterval() == 1, "PingInterval Should have been 1");
  ASSERT(pptr->getUpdateLocatorListInterval() == 25000,
         "UpdateLocatorListInterval Should have been 25000");
  ASSERT(pptr->getStatisticInterval() == 1,
         "StatisticInterval Should have been 1");
  ASSERT(strcmp(pptr->getServerGroup(), "ServerGroup1") == 0,
         "ServerGroup Should have been ServerGroup1");
  ASSERT(pptr->getSubscriptionEnabled() == true,
         "SubscriptionEnabled Should have been true");
  ASSERT(pptr->getSubscriptionRedundancy() == 1,
         "SubscriptionRedundancy Should have been 1");
  ASSERT(pptr->getSubscriptionMessageTrackingTimeout() == 5,
         "SubscriptionMessageTrackingTimeout Should have been 5");
  ASSERT(pptr->getSubscriptionAckInterval() == 1,
         "SubscriptionAckInterval Should have been 1");
  ASSERT(pptr->getPRSingleHopEnabled() == false,
         "PRSingleHopEnabled should have been false");
}

DUNIT_TASK(LOCATOR1, StartLocator1)
  {
    // starting locator
    if (isLocator) CacheHelper::initLocator(1);
    LOG("Locator1 started");
  }
END_TASK(StartLocator1)

DUNIT_TASK(SERVER, StartS12)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver1_pool.xml", locHostPort);
    }
    if (isLocalServer) {
      CacheHelper::initServer(2, "cacheserver2_pool.xml", locHostPort);
    }
  }
END_TASK(StartS12)

DUNIT_TASK(CLIENT1, StartC1)
  {
    PropertiesPtr props = Properties::create();
    props->insert("redundancy-monitor-interval", "120");
    props->insert("statistic-sampling-enabled", "false");
    props->insert("statistic-sample-rate", "120");

    initClient(true, props);

    doAttrTestingAndCreatePool(poolName);

    // Do PoolCreation testing , create another pool with same name
    PoolFactoryPtr poolFacPtr = PoolManager::createFactory();
    try {
      PoolPtr pptr = poolFacPtr->create(poolName);
      FAIL("Pool creation with same name should fail");
    } catch (IllegalStateException&) {
      LOG("OK:Pool creation with same name should fail");
    } catch (...) {
      FAIL("Pool creation with same name should fail");
    }

    createRegionAndAttachPool(poolRegNames[0], USE_ACK, poolName);
    LOG("Clnt1Init complete.");
  }
END_TASK(StartC1)

DUNIT_TASK(CLIENT2, StartC2)
  {
    PropertiesPtr props = Properties::create();
    std::string path = "cacheserver_pool_client.xml";
    std::string duplicateFile;
    CacheHelper::createDuplicateXMLFile(duplicateFile, path);

    props->insert("cache-xml-file", duplicateFile.c_str());

    try {
      LOG(" starts client");
      initClient(true, props);
      LOG(" started client");
      ASSERT(PoolManager::find("clientPoolMultiUser")
                     ->getMultiuserAuthentication() == true,
             "MultiUser secure mode should be true for Pool");
    } catch (const Exception& excp) {
      LOG("Exception during client 2 XML creation");
      LOG(excp.getMessage());
    }
    doAttrTesting(poolName1);
  }
END_TASK(StartC2)

// Test min-max connection.
DUNIT_TASK(CLIENT1, ClientOp)
  {
    // Wait for load conditioning thread to reduce pool connections to
    // min.
    SLEEP(5000);
    // Check current # connections they should be == min
    char number[20] ATTR_UNUSED = {0};
    std::string poolName =
        getHelper()->getRegion(poolRegNames[0])->getAttributes()->getPoolName();
    int level = TestUtils::getCacheImpl(getHelper()->cachePtr)
                    ->getPoolSize(poolName.c_str());
    int min = PoolManager::find(poolName.c_str())->getMinConnections();
    char logmsg[100] = {0};
    sprintf(logmsg, "Pool level not equal to min level. Expected %d, actual %d",
            min, level);
    ASSERT(level == min, logmsg);

    putThread* threads[25];
    for (int thdIdx = 0; thdIdx < 10; thdIdx++) {
      threads[thdIdx] = new putThread(poolRegNames[0]);
      threads[thdIdx]->start();
    }

    SLEEP(5000);  // wait for threads to become active

    // Check current # connections they should be == max
    level = TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getPoolSize(poolName.c_str());
    int max = PoolManager::find(poolName.c_str())->getMaxConnections();
    sprintf(logmsg, "Pool level not equal to max level. Expected %d, actual %d",
            max, level);
    ASSERT(level == max, logmsg);

    for (int thdIdx = 0; thdIdx < 10; thdIdx++) {
      threads[thdIdx]->stop();
    }

    // Milli second sleep: IdleTimeout is 5 sec, load conditioning
    // interval is 1 min
    LOG("Waiting 25 sec for idle timeout to kick in");
    SLEEP(25000);

    level = TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getPoolSize(poolName.c_str());
    min = PoolManager::find(poolName.c_str())->getMinConnections();
    sprintf(logmsg,
            "Pool level not equal to min level after idle timeout. "
            "Expected %d, actual %d",
            min, level);
    ASSERT(level == min, logmsg);

    LOG("Waiting 1 minute for load conditioning to kick in");
    SLEEP(60000);

    level = TestUtils::getCacheImpl(getHelper()->cachePtr)
                ->getPoolSize(poolName.c_str());
    sprintf(logmsg,
            "Pool level not equal to min level after load "
            "conditioning. Expected %d, actual %d",
            min, level);
    ASSERT(level == min, logmsg);
  }
END_TASK(ClientOp)

DUNIT_TASK(CLIENT1, StopC1)
  {
    cleanProc();
    LOG("Clnt1Down complete: Keepalive = True");
  }
END_TASK(StopC1)

DUNIT_TASK(CLIENT2, StopC2)
  {
    cleanProc();
    LOG("Clnt1Down complete: Keepalive = True");
  }
END_TASK(StopC2)

DUNIT_TASK(SERVER, CloseServers)
  {
    // stop servers
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
    if (isLocalServer) {
      CacheHelper::closeServer(2);
      LOG("SERVER2 stopped");
    }
  }
END_TASK(CloseServers)

DUNIT_TASK(LOCATOR1, CloseLocator1)
  {
    // stop locator
    if (isLocator) {
      CacheHelper::closeLocator(1);
      LOG("Locator1 stopped");
    }
  }
END_TASK(CloseLocator1)
