/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"

#include <gfcpp/GemfireCppCache.hpp>

#include <string>
#include <vector>

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1
#define SERVER2 s2p2

#include "CacheHelper.hpp"

static bool isLocalServer = false;
static bool isLocator = false;
static int numberOfLocators = 1;
const char* endPoints = (const char*)NULL;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);

#include "LocatorHelper.hpp"

using namespace gemfire;
using namespace test;
using namespace std;

#define SLIST vector<string>

bool findString(string& item, CacheableStringArrayPtr array) {
  for (int size = 0; size < array->length(); size++) {
    if (strcmp(item.c_str(), array->operator[](size)->asChar()) == 0) {
      return true;
    }
  }

  return false;
}

bool checkStringArray(SLIST& first, CacheableStringArrayPtr second) {
  if (second == NULLPTR && first.size() > 0) return false;

  if (second == NULLPTR && first.size() == 0) return true;

  if (first.size() != second->length()) return false;

  for (size_t size = 0; size < first.size(); size++) {
    if (!findString(first[size], second)) {
      return false;
    }
  }

  return true;
}

bool checkPoolAttribs(PoolPtr pool, SLIST& locators, SLIST& servers,
                      int freeConnectionTimeout, int loadConditioningInterval,
                      int minConnections, int maxConnections, int retryAttempts,
                      int idleTimeout, int pingInterval, const char* name,
                      int readTimeout, const char* serverGroup,
                      int socketBufferSize, bool subscriptionEnabled,
                      int subscriptionMessageTrackingTimeout,
                      int subscriptionAckInterval, int subscriptionRedundancy,
                      int statisticInterval, int threadLocalConnections,
                      bool prSingleHopEnabled, int updateLocatorListInterval) {
  char logmsg[500] = {0};

  if (pool == NULLPTR) {
    LOG("checkPoolAttribs: PoolPtr is NULL");
    return false;
  }

  test::cout << "Checking pool " << pool->getName() << test::endl;

  if (strcmp(pool->getName(), name)) {
    sprintf(logmsg, "checkPoolAttribs: Pool name expected [%s], actual [%s]",
            name, pool->getName() == NULL ? "null" : pool->getName());
    LOG(logmsg);
    return false;
  }
  if (!checkStringArray(locators, pool->getLocators())) {
    LOG("checkPoolAttribs: locators mismatch");
    return false;
  }
  if (servers.size() > 0 && !checkStringArray(servers, pool->getServers())) {
    LOG("checkPoolAttribs: servers mismatch");
    return false;
  }
  if (freeConnectionTimeout != pool->getFreeConnectionTimeout()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool freeConnectionTimeout expected [%d], "
            "actual [%d]",
            freeConnectionTimeout, pool->getFreeConnectionTimeout());
    LOG(logmsg);
    return false;
  }
  if (loadConditioningInterval != pool->getLoadConditioningInterval()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool loadConditioningInterval expected [%d], "
            "actual [%d]",
            loadConditioningInterval, pool->getLoadConditioningInterval());
    LOG(logmsg);
    return false;
  }
  if (minConnections != pool->getMinConnections()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool minConnections expected [%d], actual [%d]",
            minConnections, pool->getMinConnections());
    LOG(logmsg);
    return false;
  }
  if (maxConnections != pool->getMaxConnections()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool maxConnections expected [%d], actual [%d]",
            maxConnections, pool->getMaxConnections());
    LOG(logmsg);
    return false;
  }
  if (retryAttempts != pool->getRetryAttempts()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool retryAttempts expected [%d], actual [%d]",
            retryAttempts, pool->getRetryAttempts());
    LOG(logmsg);
    return false;
  }
  if (idleTimeout != pool->getIdleTimeout()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool idleTimeout expected [%d], actual [%ld]",
            idleTimeout, pool->getIdleTimeout());
    LOG(logmsg);
    return false;
  }
  if (pingInterval != pool->getPingInterval()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool pingInterval expected [%d], actual [%ld]",
            pingInterval, pool->getPingInterval());
    LOG(logmsg);
    return false;
  }
  if (readTimeout != pool->getReadTimeout()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool readTimeout expected [%d], actual [%d]",
            readTimeout, pool->getReadTimeout());
    LOG(logmsg);
    return false;
  }
  if (strcmp(serverGroup, pool->getServerGroup())) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool serverGroup expected [%s], actual [%s]",
            serverGroup,
            pool->getServerGroup() == NULL ? "null" : pool->getServerGroup());
    LOG(logmsg);
    return false;
  }
  if (socketBufferSize != pool->getSocketBufferSize()) {
    sprintf(
        logmsg,
        "checkPoolAttribs: Pool socketBufferSize expected [%d], actual [%d]",
        socketBufferSize, pool->getSocketBufferSize());
    LOG(logmsg);
    return false;
  }
  if (subscriptionEnabled != pool->getSubscriptionEnabled()) {
    sprintf(
        logmsg,
        "checkPoolAttribs: Pool subscriptionEnabled expected [%s], actual [%s]",
        subscriptionEnabled ? "true" : "false",
        pool->getSubscriptionEnabled() ? "true" : "false");
    LOG(logmsg);
    return false;
  }
  if (subscriptionMessageTrackingTimeout !=
      pool->getSubscriptionMessageTrackingTimeout()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool subscriptionMessageTrackingTimeout "
            "expected [%d], actual [%d]",
            subscriptionMessageTrackingTimeout,
            pool->getSubscriptionMessageTrackingTimeout());
    LOG(logmsg);
    return false;
  }
  if (subscriptionAckInterval != pool->getSubscriptionAckInterval()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool subscriptionAckInterval expected [%d], "
            "actual [%d]",
            subscriptionAckInterval, pool->getSubscriptionAckInterval());
    LOG(logmsg);
    return false;
  }
  if (subscriptionRedundancy != pool->getSubscriptionRedundancy()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool subscriptionRedundancy expected [%d], "
            "actual [%d]",
            subscriptionRedundancy, pool->getSubscriptionRedundancy());
    LOG(logmsg);
    return false;
  }
  if (statisticInterval != pool->getStatisticInterval()) {
    sprintf(
        logmsg,
        "checkPoolAttribs: Pool statisticInterval expected [%d], actual [%d]",
        statisticInterval, pool->getStatisticInterval());
    LOG(logmsg);
    return false;
  }
  if (prSingleHopEnabled != pool->getPRSingleHopEnabled()) {
    sprintf(
        logmsg,
        "checkPoolAttribs: Pool prSingleHopEnabled expected [%d], actual [%d]",
        prSingleHopEnabled, pool->getPRSingleHopEnabled());
    LOG(logmsg);
    return false;
  }
  if (updateLocatorListInterval != pool->getUpdateLocatorListInterval()) {
    sprintf(logmsg,
            "checkPoolAttribs: Pool updateLocatorListInterval expected [%d], "
            "actual [%ld]",
            updateLocatorListInterval, pool->getUpdateLocatorListInterval());
    LOG(logmsg);
    return false;
  }
  return true;
}

int testXmlCacheCreationWithPools() {
  char* host_name = (char*)"XML_CACHE_CREATION_TEST";
  CacheFactoryPtr cacheFactory;
  CachePtr cptr;

  test::cout << "create DistributedSytem with name=" << host_name << test::endl;
  try {
    cacheFactory = CacheFactory::createCacheFactory();
  } catch (Exception& ex) {
    ex.showMessage();
    ex.printStackTrace();
    return -1;
  }

  test::cout
      << "Create cache with the configurations provided in valid_cache_pool.xml"
      << test::endl;

  try {
    std::string filePath = "valid_cache_pool.xml";
    std::string duplicateFile;
    CacheHelper::createDuplicateXMLFile(duplicateFile, filePath);
    cptr = cacheFactory->set("cache-xml-file", duplicateFile.c_str())->create();
    if (cptr->getPdxIgnoreUnreadFields() != true) {
      test::cout << "getPdxIgnoreUnreadFields should return true."
                 << test::endl;
      return -1;
    } else {
      test::cout << "getPdxIgnoreUnreadFields returned true." << test::endl;
    }
  } catch (Exception& ex) {
    ex.showMessage();
    ex.printStackTrace();
    return -1;
  } catch (...) {
    LOGINFO("unknown exception");
    return -1;
  }

  VectorOfRegion vrp;
  test::cout << "Test if number of root regions are correct" << test::endl;
  cptr->rootRegions(vrp);
  test::cout << "  vrp.size=" << vrp.size() << test::endl;

  if (vrp.size() != 2) {
    test::cout << "Number of root regions does not match" << test::endl;
    return -1;
  }

  test::cout << "Root regions in Cache :" << test::endl;
  for (int32_t i = 0; i < vrp.size(); i++) {
    test::cout << "vc[" << i << "].m_regionPtr=" << vrp.at(i).ptr()
               << test::endl;
    test::cout << "vc[" << i << "]=" << vrp.at(i)->getName() << test::endl;
  }
  RegionPtr regPtr1 = vrp.at(0);

  VectorOfRegion vr;
  test::cout << "Test if the number of sub regions with the root region Root1 "
                "are correct"
             << test::endl;
  regPtr1->subregions(true, vr);
  test::cout << "  vr.size=" << vr.size() << test::endl;
  if (vr.size() != 1) {
    test::cout << "Number of Subregions does not match" << test::endl;
    return -1;
  }

  test::cout << "get subregions from the root region :" << vrp.at(0)->getName()
             << test::endl;
  for (int32_t i = 0; i < vr.size(); i++) {
    test::cout << "vc[" << i << "].m_regionPtr=" << vr.at(i).ptr()
               << test::endl;
    test::cout << "vc[" << i << "]=" << vr.at(i)->getName() << test::endl;
  }

  RegionPtr subRegPtr = vr.at(0);
  vr.clear();

  RegionPtr regPtr2 = vrp.at(1);

  test::cout << "Test if the number of sub regions with the root region Root2 "
                "are correct"
             << test::endl;
  regPtr2->subregions(true, vr);
  test::cout << "  vr.size=" << vr.size() << test::endl;
  if (vr.size() != 0) {
    test::cout << "Number of Subregions does not match" << test::endl;
    return -1;
  }

  vr.clear();
  vrp.clear();

  test::cout << "Test the attributes of region" << test::endl;

  const char* poolNameReg1 = regPtr1->getAttributes()->getPoolName();
  const char* poolNameSubReg = subRegPtr->getAttributes()->getPoolName();
  const char* poolNameReg2 = regPtr2->getAttributes()->getPoolName();

  if (strcmp(poolNameReg1, "test_pool_1")) {
    test::cout << "Wrong pool name for region 1" << test::endl;
    return -1;
  }
  if (strcmp(poolNameReg2, "test_pool_2")) {
    test::cout << "Wrong pool name for region 2" << test::endl;
    return -1;
  }
  if (strcmp(poolNameSubReg, "test_pool_2")) {
    test::cout << "Wrong pool name for sub region" << test::endl;
    return -1;
  }

  PoolPtr poolOfReg1 = PoolManager::find(poolNameReg1);
  PoolPtr poolOfSubReg = PoolManager::find(poolNameSubReg);
  PoolPtr poolOfReg2 = PoolManager::find(poolNameReg2);
  SLIST locators;
  SLIST servers;
  SLIST emptylist;

  locators.clear();
  servers.clear();
  emptylist.clear();
  char tmp[128];
  sprintf(tmp, "localhost:%d", CacheHelper::staticLocatorHostPort1);

  locators.push_back(string(tmp));
  sprintf(tmp, "localhost:%d", CacheHelper::staticHostPort1);
  servers.push_back(string(tmp));
  sprintf(tmp, "localhost:%d", CacheHelper::staticHostPort2);
  servers.push_back(string(tmp));

  // THIS MUST MATCH WITH THE CLIENT CACHE XML LOADED

  bool check1 =
      checkPoolAttribs(poolOfReg1, locators, emptylist, 12345, 23456, 3, 7, 3,
                       5555, 12345, "test_pool_1", 23456, "ServerGroup1", 32768,
                       true, 900123, 567, 0, 10123, 5, true, 250001);

  bool check2 =
      checkPoolAttribs(poolOfReg2, emptylist, servers, 23456, 34567, 2, 8, 5,
                       6666, 23456, "test_pool_2", 34567, "ServerGroup2", 65536,
                       false, 800222, 678, 1, 20345, 3, false, 5000);
  bool check3 =
      checkPoolAttribs(poolOfSubReg, emptylist, servers, 23456, 34567, 2, 8, 5,
                       6666, 23456, "test_pool_2", 34567, "ServerGroup2", 65536,
                       false, 800222, 678, 1, 20345, 3, false, 5000);

  if (!cptr->isClosed()) {
    cptr->close();
    cptr = NULLPTR;
  }

  if (!check1 || !check2 || !check3) {
    test::cout << "Property check failed" << test::endl;
    return -1;
  }
  ////////////////////////////testing of cache.xml completed///////////////////

  try {
    test::cout << "Testing invalid pool xml 1" << test::endl;
    std::string filePath = "invalid_cache_pool.xml";
    std::string duplicateFile;
    CacheHelper::createDuplicateXMLFile(duplicateFile, filePath);
    cptr = cacheFactory->set("cache-xml-file", duplicateFile.c_str())->create();
    return -1;
  } catch (Exception& ex) {
    test::cout << "EXPECTED EXCEPTION" << test::endl;
    ex.showMessage();
    ex.printStackTrace();
  }

  try {
    test::cout << "Testing invalid pool xml 2" << test::endl;
    std::string filePath = "invalid_cache_pool2.xml";
    std::string duplicateFile;
    CacheHelper::createDuplicateXMLFile(duplicateFile, filePath);
    cptr = cacheFactory->set("cache-xml-file", duplicateFile.c_str())->create();
    return -1;
  } catch (Exception& ex) {
    test::cout << "EXPECTED EXCEPTION" << test::endl;
    ex.showMessage();
    ex.printStackTrace();
  }

  try {
    test::cout << "Testing invalid pool xml 3" << test::endl;
    std::string filePath = "invalid_cache_pool3.xml";
    std::string duplicateFile;
    CacheHelper::createDuplicateXMLFile(duplicateFile, filePath);
    cptr = cacheFactory->set("cache-xml-file", duplicateFile.c_str())->create();
    return -1;
  } catch (Exception& ex) {
    test::cout << "EXPECTED EXCEPTION" << test::endl;
    ex.showMessage();
    ex.printStackTrace();
  }

  try {
    test::cout << "Testing invalid pool xml 4" << test::endl;
    std::string filePath = "invalid_cache_pool4.xml";
    std::string duplicateFile;
    CacheHelper::createDuplicateXMLFile(duplicateFile, filePath);
    cptr = cacheFactory->set("cache-xml-file", duplicateFile.c_str())->create();
    return -1;
  } catch (Exception& ex) {
    test::cout << "EXPECTED EXCEPTION" << test::endl;
    ex.showMessage();
    ex.printStackTrace();
  }

  test::cout << "disconnecting..." << test::endl;
  try {
    test::cout << "just before disconnecting..." << test::endl;
    if (cptr != NULLPTR) cptr->close();
  } catch (Exception& ex) {
    ex.showMessage();
    ex.printStackTrace();
    return -1;
  }
  test::cout << "done with test" << test::endl;
  test::cout << "Test successful!" << test::endl;
  return 0;
}

int testXmlDeclarativeCacheCreation() {
  char* host_name = (char*)"XML_DECLARATIVE_CACHE_CREATION_TEST";
  CacheFactoryPtr cacheFactory;
  CachePtr cptr;

  char* path = ACE_OS::getenv("TESTSRC");
  std::string directory(path);

  test::cout << "create DistributedSytem with name=" << host_name << test::endl;
  try {
    cacheFactory = CacheFactory::createCacheFactory();
  } catch (Exception& ex) {
    ex.showMessage();
    ex.printStackTrace();
    return -1;
  }

  try {
    std::string filePath = directory + "/valid_declarative_cache_creation.xml";
    cptr = cacheFactory->set("cache-xml-file", filePath.c_str())->create();

  } catch (Exception& ex) {
    ex.showMessage();
    ex.printStackTrace();
    return -1;
  } catch (...) {
    LOGINFO("unknown exception");
    return -1;
  }

  VectorOfRegion vrp;
  test::cout << "Test if number of root regions are correct" << test::endl;
  cptr->rootRegions(vrp);
  test::cout << "  vrp.size=" << vrp.size() << test::endl;

  if (vrp.size() != 1) {
    test::cout << "Number of root regions does not match" << test::endl;
    return -1;
  }

  test::cout << "Root regions in Cache :" << test::endl;
  for (int32_t i = 0; i < vrp.size(); i++) {
    test::cout << "vc[" << i << "].m_reaPtr=" << vrp.at(i).ptr() << test::endl;
    test::cout << "vc[" << i << "]=" << vrp.at(i)->getName() << test::endl;
  }
  RegionPtr regPtr1 = vrp.at(0);

  RegionAttributesPtr raPtr = regPtr1->getAttributes();
  RegionAttributes* regAttr = raPtr.ptr();
  test::cout << "Test Attributes of root region Root1 " << test::endl;
  test::cout << "Region name " << regPtr1->getName() << test::endl;

  if (regAttr->getCacheLoader() == NULLPTR) {
    test::cout << "Cache Loader not initialized." << test::endl;
    return -1;
  }

  if (regAttr->getCacheListener() == NULLPTR) {
    test::cout << "Cache Listener not initialized." << test::endl;
    return -1;
  }

  if (regAttr->getCacheWriter() == NULLPTR) {
    test::cout << "Cache Writer not initialized." << test::endl;
    return -1;
  }

  test::cout << "Attributes of Root1 are correctly set" << test::endl;

  if (!cptr->isClosed()) {
    cptr->close();
    cptr = NULLPTR;
  }

  return 0;
}

DUNIT_TASK_DEFINITION(CLIENT1, ValidXmlTestPools)
  {
    CacheHelper::initLocator(1);
    char tmp[128];
    sprintf(tmp, "localhost:%d", CacheHelper::staticLocatorHostPort1);
    CacheHelper::initServer(1, "cacheserver1_pool.xml", tmp);
    CacheHelper::initServer(2, "cacheserver2_pool.xml", tmp);

    int res = testXmlCacheCreationWithPools();

    CacheHelper::closeServer(1);
    CacheHelper::closeServer(2);

    CacheHelper::closeLocator(1);

    if (res != 0) {
      FAIL("Pool Test Failed.");
    }
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, ValidXmlTestDeclarativeCacheCreation)
  {
    int res = testXmlDeclarativeCacheCreation();
    if (res != 0) {
      FAIL("DeclarativeCacheCreation Test Failed.");
    }
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(ValidXmlTestPools);
    CALL_TASK(ValidXmlTestDeclarativeCacheCreation);
  }
END_MAIN
