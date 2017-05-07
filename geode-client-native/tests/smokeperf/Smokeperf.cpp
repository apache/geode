/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
 * @file    Smokeperf.cpp
 * @since   1.0
 * @version 1.0
 * @see
 *
 */

// ----------------------------------------------------------------------------

#include "Smokeperf.hpp"
#include "SmokeTasks.hpp"

#include <ace/Time_Value.h>
#include <time.h>
#include "fwklib/GsRandom.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/RegionHelper.hpp"
#include "fwklib/FwkExport.hpp"
#include "fwklib/PoolHelper.hpp"
#include "security/CredentialGenerator.hpp"
#include <gfcpp/SystemProperties.hpp>

namespace smoketest {
std::string REGIONSBB("Regions");
std::string CLIENTSBB("ClientsBb");
std::string READYCLIENTS("ReadyClients");
std::string DURABLEBB( "DURABLEBB" );
}
using namespace smoketest;
using namespace gemfire;
using namespace testobject;
using namespace gemfire::testframework;
using namespace gemfire::testframework::smokeperf;
using namespace gemfire::testframework::security;

Smokeperf * g_test = NULL;
PerfStat *PutGetTask::perfstat[10] = {0};
// ----------------------------------------------------------------------------

TESTTASK initialize(const char * initArgs) {
  int32_t result = FWK_SUCCESS;
  if (g_test == NULL) {
    FWKINFO( "Initializing Smokeperf library." );
    try {
      g_test = new Smokeperf(initArgs);
    } catch (const FwkException &ex) {
      FWKSEVERE( "initialize: caught exception: " << ex.getMessage() );
      result = FWK_SEVERE;
    }
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK finalize() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Finalizing Smokeperf library." );
  if (g_test != NULL) {
    g_test->cacheFinalize();
    delete g_test;
    g_test = NULL;
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doCloseCache() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Closing cache, disconnecting from distributed system." );
  if (g_test != NULL) {
    g_test->cacheFinalize();
  }
  return result;
}
void Smokeperf::getClientSecurityParams(PropertiesPtr prop,
    std::string credentials) {

  std::string securityParams = getStringValue("securityParams");

  // no security means security param is not applicable
  if (securityParams.empty()) {
    FWKINFO("security is DISABLED :");
    return;
  }

  FWKDEBUG("security param is : " << securityParams);

  std::string bb("GFE_BB");
  std::string key("securityScheme");
  std::string sc = bbGetString(bb, key);
  if (sc.empty()) {
    sc = getStringValue(key.c_str());
    if (!sc.empty()) {
      bbSet(bb, key, sc);
    }
  }
  FWKINFO("security scheme : " << sc);

  if (prop == NULLPTR)
    prop = Properties::create();

  CredentialGeneratorPtr cg = CredentialGenerator::create(sc);
  cg->getAuthInit(prop);

  if (securityParams == "valid" || securityParams == "VALID") {
    FWKINFO("getting valid credentials ");
    cg->getValidCredentials(prop);
  } else if (securityParams == "invalid" || securityParams == "INVALID") {
    FWKDEBUG("getting invalid credentials ");
    cg->getInvalidCredentials(prop);
  } else {
    FWKDEBUG("getting credentials for list of operations ");
    opCodeList opCodes;
#if defined(_SOLARIS) && (__cplusplus < 201103L)
    int n = 0;
    while (!securityParams.empty() && (std::count(opCodes.begin(),
            opCodes.end(), strToOpCode(securityParams), n), n == 0)) {
#else
    while (!securityParams.empty() && std::count(opCodes.begin(),
            opCodes.end(), strToOpCode(securityParams)) == 0) {
#endif

      opCodes.push_back(strToOpCode(securityParams));

      securityParams = getStringValue("securityParams");
      FWKINFO("next security params " << securityParams);
    }
    cg->getAllowedCredentialsForOps(opCodes, prop);
    FWKDEBUG("securities properties entries " << prop->getSize());
  }
}

// ----------------------------------------------------------------------------

void Smokeperf::checkTest(const char * taskId) {
  SpinLockGuard guard(m_lck);
  setTask(taskId);
  if (m_cache == NULLPTR) {

    PropertiesPtr pp = Properties::create();
    std::string authInit;
    getClientSecurityParams(pp, getStringValue("credentials"));

    int32_t heapLruLimit = getIntValue("heapLruLimit");
    if (heapLruLimit > 0)
      pp->insert("heap-lru-limit", heapLruLimit);

    bool conflate = getBoolValue("conflate");
    if (conflate) {
      std::string conflateEvents = getStringValue("conflateEvents");
      pp->insert("conflate-events", conflateEvents.c_str());
    }

    CacheAttributesPtr cAttrs = NULLPTR;

    int32_t redundancyLevel = getIntValue("redundancyLevel");
    if (redundancyLevel > 0) {
      std::string tag = getStringValue("TAG");
      std::string label = "EndPoints";
      if (!tag.empty()) {
        label += "_";
        label += tag;
      }

      std::string endPts;
      std::string bb("GFE_BB");
      std::string cnt("EP_COUNT");
      int32_t epCount = (int32_t) bbGet(bb, cnt);
      for (int32_t i = 1; i <= epCount; i++) {
        std::string key = label + "_";
        key.append(FwkStrCvt(i).toString());
        std::string ep = bbGetString(bb, key);
        if (!endPts.empty())
          endPts.append(",");
        endPts.append(ep);
      }
      if (!endPts.empty()) {
        CacheAttributesFactory aFact;
        aFact.setEndpoints(endPts.c_str());
        aFact.setRedundancyLevel(redundancyLevel);
        cAttrs = aFact.createCacheAttributes();
      }
    }

    bool statEnable = getBoolValue("statEnable");
    if (!statEnable) {
      pp->insert("statistic-sampling-enabled", "false");
    }

    bool disableShufflingEP = getBoolValue("disableShufflingEP");
    if (disableShufflingEP) {
      pp->insert("disable-shuffling-of-endpoints", true);
    }
    int32_t connectionPoolSize = getIntValue("connectionPoolSize");
    if (connectionPoolSize > 0)
      pp->insert("connection-pool-size", connectionPoolSize);

    bool isDC = getBoolValue("isDurable");
    if (isDC) {
      int32_t timeout = getIntValue("durableTimeout");
      char name[32] = { '\0' };
      ACE_OS::sprintf(name, "ClientName_%d", getClientId());
      FWKINFO( "checktest durableID = " << name);
      pp->insert("durable-client-id", name);
      if (timeout > 0)
        pp->insert("durable-timeout", timeout);
    }

    cacheInitialize(pp);

    // Smokeperf specific initialization
    // none
  }
}

// ----------------------------------------------------------------------------

TESTTASK doCreateRegion(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreateRegion called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->createRegion();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreateRegion caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doPuts(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPuts called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->puts();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPuts caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doPutDataTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPutDataTask called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->putDataTask();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPutDataTask caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doGets(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGets called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->gets();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGets caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doGetDataTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGetDataTask called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->getDataTask();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGetDataTask caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doPopulateRegion(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopulateRegion called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->populateRegion();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopulateRegion caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doRegisterInterestList(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterInterestList called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->registerInterestList();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterInterestList caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doRegisterRegexList(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterRegexList called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->registerRegexList();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterRegexList caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doUnRegisterRegexList(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterRegexList called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->unregisterRegexList();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterRegexList caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doRegisterAllKeys(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterAllKeys called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->registerAllKeys();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterAllKeys caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doOpenStatistic(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doOpenStatistic called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->openStatistic();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doOpenStatistic caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doCloseStatistic() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCloseStatistic called for task." );
  if (g_test != NULL) {
    result = g_test->closeStatistic();
  }
  return result;
}
TESTTASK doGenerateTrimSpec(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGenerateTrimSpec called for task: " << taskId );
  try {
    //g_test->checkTest( taskId );
	result = g_test->trimSpecData();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGenerateTrimSpec caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doCreatePool(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreatePool called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->createPools();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreatePool caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doCyclePoolTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCyclePoolTask called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->cyclePoolTask();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCyclePoolTask caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doCycleBridgeConnectionTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCycleBridgeConnectionTask called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->cycleBridgeConnectionTask();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCycleBridgeConnectionTask caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doMixPutGetDataTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doMixPutGetDataTask called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->mixPutGetDataTask();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doMixPutGetDataTask caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doQueryRegionDataTask( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doQueryRegionDataTask called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->queries();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doQueryRegionDataTask caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doRegisterCQs( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterCQs called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->registerCQs();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterCQs caught exception: " << ex.getMessage() );
  }

  return result;
}
TESTTASK doPutBatchObj( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPutBatchObj called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->putBatchObj();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPutBatchObj caught exception: " << ex.getMessage() );
  }

  return result;
}

TESTTASK doCycleDurableBridgeConnectionTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCycleDurableBridgeConnectionTask called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->cycleDurableBridgeConnection(taskId);
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCycleDurableBridgeConnectionTask caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doCycleDurableClientTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCycleDurableClientTask called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->cycleDurableClientTask(taskId);
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCycleDurableClientTask caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doCreateEntryMapTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreateEntryMapTask called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->createEntryMapTask();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreateEntryMapTask caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doPutAllEntryMapTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPutAllEntryMapTask called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->putAllEntryMapTask();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPutAllEntryMapTask caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doUpdateDeltaData(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doUpdateDeltaData called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->updateDelta();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doUpdateDeltaData caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doCreateData(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreateData called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->createData();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreateData caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doCreateOrPutBulkDataTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreateOrPutBulkDataTask called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->createOrPutBulkDataTask();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreateOrPutBulkDataTask caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doGetBulkDataTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGetBulkDataTask called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->getBulkDataTask();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGetBulkDataTask caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doUpdateScaleDeltaData(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doUpdateScaleDeltaData called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->scaleupdateDelta();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doUpdateScaleDeltaData caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doGetFunctionExecutionData(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGetFunctionExecutionData called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->getFunctionExecutionData();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGetFunctionExecutionData caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doPutFunctionExecutionData(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPutFunctionExecutionData called for task: " << taskId );
  try {
    g_test->checkTest(taskId);
    result = g_test->putFunctionExecutionData();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPutFunctionExecutionData caught exception: " << ex.getMessage() );
  }
  return result;
}
TESTTASK doGenerateScaleTrimSpec(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doGenerateTrimSpec called for task: " << taskId );
  try {
    //g_test->checkTest( taskId );
	result = g_test->trimScaleSpecData();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doGenerateTrimSpec caught exception: " << ex.getMessage() );
  }

  return result;
}
// Factory functions for listeners, loaders, etc.
// ----------------------------------------------------------------------------

TEST_EXPORT CacheListener * createSmokeperfCacheListener() {
  return new SmokeperfCacheListener();
}

// ----------------------------------------------------------------------------
TEST_EXPORT CacheLoader * createCacheLoader() {
  return new perfCacheLoader();
}

TEST_EXPORT CacheListener * createLatencyListener() {
  return new LatencyListener(PutGetTask::perfstat[0]);
}
TEST_EXPORT CacheListener * createDurableCacheListener() {
  return new DurableCacheListener();
}
//listener------------------------------------------------------------------------------------
DurableCacheListener::DurableCacheListener():m_ops(0)
{
  char name[32] = { '\0' };
  ACE_OS::sprintf(name, "ClientName_%d", g_test->getClientId());
  m_clntName = name;
  //FWKINFO("DurableCacheListener: created for client: " << m_clntName);
}

void DurableCacheListener::check(const EntryEvent& event)
{
  //FWKINFO("DurableCacheListener::check # ");
  CacheableKeyPtr key = event.getKey();
  CacheableBytesPtr value = dynCast<CacheableBytesPtr>(event.getNewValue());
  m_ops++;
  return;
}

void DurableCacheListener::dumpToBB()
{
  // Dump Things in Black Board
  FWKINFO("DurableCacheListener: dumpToBB called");

  //Increment Count
  std::string oper_cnt_key = m_clntName + std::string("_Count");
  int64_t cur_cnt = g_test->bbGet(DURABLEBB,oper_cnt_key);
  g_test->bbSet(DURABLEBB,oper_cnt_key,cur_cnt+m_ops);
  FWKINFO( "Current count for " << oper_cnt_key << " is " <<  cur_cnt+m_ops );
}

TEST_EXPORT CacheListener * createConflationTestCacheListener() {
  return new ConflationTestCacheListener(g_test);
}
void ConflationTestCacheListener::afterCreate(const EntryEvent& event) {
  m_numAfterCreate++;
}
void ConflationTestCacheListener::afterUpdate(const EntryEvent& event) {
  m_numAfterUpdate++;
  std::string keyIsOldValue = std::string("isOldValue");
  std::string key1 = m_test->bbGetString(m_bb, keyIsOldValue);
  if ((event.getOldValue() == NULLPTR) && (key1 == "false"))
    m_test->bbSet(m_bb, keyIsOldValue, "true");
}
void ConflationTestCacheListener::afterInvalidate(const EntryEvent& event) {
  m_numAfterInvalidate++;
}
void ConflationTestCacheListener::afterDestroy(const EntryEvent& event) {
  m_numAfterDestroy++;
}
void ConflationTestCacheListener::dumpToBB(const RegionPtr& regPtr) {
  char name[32] = { '\0' };
  sprintf(name, "%d", g_test->getClientId());
  std::string key1 = std::string("AFTER_CREATE_COUNT_") + std::string(name)
      + std::string("_") + std::string(regPtr->getName());
  std::string key2 = std::string("AFTER_UPDATE_COUNT_") + std::string(name)
      + std::string("_") + std::string(regPtr->getName());
  std::string key3 = std::string("AFTER_INVALIDATE_COUNT_") + std::string(name)
      + std::string("_") + std::string(regPtr->getName());
  std::string key4 = std::string("AFTER_DESTROY_COUNT_") + std::string(name)
      + std::string("_") + std::string(regPtr->getName());
  m_test->bbSet(m_bb, key1, m_numAfterCreate);
  m_test->bbSet(m_bb, key2, m_numAfterUpdate);
  m_test->bbSet(m_bb, key3, m_numAfterInvalidate);
  m_test->bbSet(m_bb, key4, m_numAfterDestroy);

}
// ========================================================================

void Smokeperf::clearKeys() {
  if (m_KeysA != NULL) {
    for (int32_t i = 0; i < m_MaxKeys; i++) {
      m_KeysA[i] = NULLPTR;
    }
    delete[] m_KeysA;
    m_KeysA = NULL;
    m_MaxKeys = 0;
  }
}
//-------------------------------------------------------------------------
int32_t Smokeperf::initBatchKeys(bool useDefault) {
  int32_t low = 0;
  int32_t numKeys = getIntValue("distinctKeys"); // check distince keys first
  if (numKeys <= 0) {
    if (useDefault) {
      numKeys = 5000;
    } else {
      return numKeys;
    }
  }
  int32_t batchSize = getIntValue("BatchSize");
  batchSize = (batchSize <= 0) ? 500 : batchSize;
  int32_t high= 0;
  FWKINFO("numKeys: " << numKeys << " low: " << low);
  clearKeys();
  m_MaxKeys = numKeys;
  int32_t batches = numKeys/batchSize;
  char buf[128] =  {'\0'};
  m_KeysA = new CacheableKeyPtr[m_MaxKeys];
  high = batchSize;
  FWKINFO("m_MaxKeys: " << m_MaxKeys << " low: " << low << " high: " << high);
  for(int32_t i = 0; i < batches; i++) {
    for (int32_t j = low; j < high; j++) {
      sprintf(buf, "_%d_%d",i, j);
      m_KeysA[j] = CacheableKey::create(buf);
    }
    low += batchSize;
    high += batchSize;
    FWKINFO("low: " << low << " high: " << high);
  }
  for (int j = 0; j < m_MaxKeys; j++) {
    std::swap(m_KeysA[GsRandom::random(numKeys)], m_KeysA[numKeys - 1]);
    numKeys--;
  }

  return m_MaxKeys;
}
// ========================================================================

int32_t Smokeperf::initKeys(bool useDefault, bool useAllClientID) {
  static char keyType = 'i';
  std::string typ = getStringValue("keyType"); // int is only value to use
  char newType = typ.empty() ? 'i' : typ[0];

  int32_t low = getIntValue("keyIndexBegin");
  low = (low > 0) ? low : 0;
  int32_t numKeys = getIntValue("distinctKeys"); // check distince keys first
  if (numKeys <= 0) {
    if (useDefault) {
      numKeys = 5000;
    } else {
      return numKeys;
    }
  }
  int32_t numClients = getIntValue("clientCount");
  int32_t id = 0;
  if (numClients > 0) {
    id = g_test->getClientId();
    if (id < 0)
      id = -id;
    numKeys = numKeys / numClients;
  }
  if (numKeys < 1)
    FWKEXCEPTION("Smokeperf::initKeys:Key is less than 0 for each client. Provide max number of distinctKeys");
  int32_t high = numKeys + low;
  FWKINFO("numKeys: " << numKeys << " low: " << low);
  if ((newType == keyType) && (numKeys == m_MaxKeys)
      && (m_KeyIndexBegin == low)) {
    return numKeys;
  }
  clearKeys();
  m_MaxKeys = numKeys;
  m_KeyIndexBegin = low;
  keyType = newType;
  if (keyType == 's') {
    int32_t keySize = getIntValue("keySize");
    keySize = (keySize > 0) ? keySize : 10;
    std::string keyBase(keySize, 'A');
    initStrKeys(low, high, keyBase, id, useAllClientID);
  } else {
    initIntKeys(low, high);
  }

  for (int j = 0; j < m_MaxKeys; j++) {
    std::swap(m_KeysA[GsRandom::random(numKeys)], m_KeysA[numKeys - 1]);
    numKeys--;
  }

  return m_MaxKeys;
}

// ========================================================================

void Smokeperf::initStrKeys(int32_t low, int32_t high,
    const std::string & keyBase, uint32_t clientId, bool useAllClientID) {
  m_KeysA = new CacheableKeyPtr[m_MaxKeys];
  const char * const base = keyBase.c_str();

  char buf[128];
  int32_t numClients = getIntValue("clientCount");
  int32_t id = clientId;
  for (int32_t i = low; i < high; i++) {
    if (useAllClientID) {
      id = GsRandom::random((uint32_t) 1, (uint32_t) (numClients + 1));
    }
    sprintf(buf, "%s%d%010d", base, id, i);
    m_KeysA[i - low] = CacheableKey::create(buf);
  }
}

// ========================================================================

void Smokeperf::initIntKeys(int32_t low, int32_t high) {
  m_KeysA = new CacheableKeyPtr[m_MaxKeys];
  FWKINFO("m_MaxKeys: " << m_MaxKeys << " low: " << low << " high: " << high);

  for (int32_t i = low; i < high; i++) {
    m_KeysA[i - low] = CacheableKey::create(i);
  }
}

// ========================================================================

int32_t Smokeperf::initValues(int32_t numKeys, int32_t siz, bool useDefault) {
  if (siz == 0) {
    siz = getIntValue("valueSizes");
  }
  if (siz <= 0) {
    if (useDefault) {
      siz = 55;
    } else {
      return siz;
    }
  }
  return siz;
}

// ----------------------------------------------------------------------------

int32_t Smokeperf::createRegion() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::createRegion()" );

  try {
    RegionHelper help(g_test);
    if (!m_isObjectRegistered) {
      Serializable::registerType(PSTObject::createDeserializable);
      Serializable::registerType(FastAssetAccount::createDeserializable);
      Serializable::registerType(FastAsset::createDeserializable);
      Serializable::registerType(BatchObject::createDeserializable);
      Serializable::registerType(DeltaFastAssetAccount::createDeserializable);
      Serializable::registerType(DeltaPSTObject::createDeserializable);
      m_isObjectRegistered = true;
    }
    RegionPtr region = help.createRootRegion(m_cache);

    std::string key(region->getName());
    bbIncrement(REGIONSBB, key);
    FWKINFO( "Smokeperf::createRegion Created region " << region->getName() << std::endl);
    result = FWK_SUCCESS;

  } catch (Exception e) {
    FWKEXCEPTION( "Smokeperf::createRegion FAILED -- caught exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::createRegion FAILED -- caught test exception: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::createRegion FAILED -- caught unknown exception." );
  }

  return result;
}

// ----------------------------------------------------------------------------

#ifndef WIN32
#include <unistd.h>
#endif

// ----------------------------------------------------------------------------

int32_t Smokeperf::gets() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::gets()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue("clientCount");

    std::string label = RegionHelper::regionTag(region->getAttributes());
    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0)
      timedInterval = 5;

    resetValue("distinctKeys");
    initKeys(false, true);

    int32_t valSize = getIntValue("valueSizes");

    // Loop over threads
    resetValue("numThreads");
    int32_t numThreads = getIntValue("numThreads");
    resetValue("isMainWorkLoad");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    while (numThreads > 0) { // thread loop

      // And we do the real work now
      m_PerfSuite.setName(label.c_str());
      m_PerfSuite.setAction("Gets");
      m_PerfSuite.setNumKeys(m_MaxKeys);
      m_PerfSuite.setNumClients(numClients);
      m_PerfSuite.setValueSize(valSize);
      m_PerfSuite.setNumThreads(numThreads);
      FWKINFO( "Doing " << m_PerfSuite.asString() << " test." );

      GetsTask * gets = new GetsTask(region, m_KeysA, m_MaxKeys, mainworkLoad);
      //      FWKINFO( "Running warmup task." );

      bool checked = checkReady(numClients);
      FWKINFO( "Running timed task." );
      setTrimTime("get");
      if (!clnt->timeInterval(gets, timedInterval, numThreads, 300
          + timedInterval)) {
        clearKeys();
        FWKEXCEPTION( "In TestTask_gets()  Timed run timed out." );
      }
      setTrimTime("get", true);
      if (clnt->getTaskStatus() == FWK_SEVERE)
        FWKEXCEPTION( "Exception during get task");
      m_PerfSuite.addRecord(gets->getIters(), (uint32_t) clnt->getTotalMicros());
      // real work complete for this pass thru the loop
      if (checked) {
        bbDecrement(CLIENTSBB, READYCLIENTS);
      }

      numThreads = getIntValue("numThreads");
      if (numThreads > 0) {
        perf::sleepSeconds(3);
      }
      delete gets;
    } // thread loop
    result = FWK_SUCCESS;
  } catch (Exception e) {
    FWKEXCEPTION( "Smokeperf::gets Caught Exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::gets Caught FwkException: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::gets Caught unknown exception." );
  }
  clearKeys();
  perf::sleepSeconds(3);
  FWKINFO( "Smokeperf::gets() complete." );
  return result;
}
void Smokeperf::setTrimTime(std::string op, bool endTime) {
  ACE_Time_Value startTime;
  ACE_UINT64 tusec;
  std::string bb("Trim_BB");
  std::string trTime;
  std::string TemptrTime;
  int timediff = 30;
  if(op.compare("creates") == 0)
	  timediff = 10;
  if (endTime) {
    startTime = ACE_OS::gettimeofday() - ACE_Time_Value(timediff);
    startTime.to_usec(tusec);
    trTime = op + std::string("_") + std::string("EndTime");
    TemptrTime = op + std::string("_") + std::string("TempEndTime");
  } else {
    startTime = ACE_OS::gettimeofday() + ACE_Time_Value(timediff);
    startTime.to_usec(tusec);
    trTime = op + std::string("_") + std::string("StartTime");
    TemptrTime = op + std::string("_") + std::string("TempStartTime");
  }
  std::string trim_Time = bbGetString(bb, TemptrTime);
  char buf[200] = { 0 };
  std::string timeFormat;
  int64_t time1 = (timeval(startTime).tv_sec);
  ACE_OS::strftime(buf, 200, "%Y/%m/%d %H:%M:%S", ACE_OS::localtime(
      (const time_t*) &time1));
  timeFormat = buf;
  sprintf(buf, ".%03d", (int) ((const timeval*) startTime)->tv_usec / 1000);
  timeFormat += buf;
  ACE_OS::strftime(buf, 200, " %Z", ACE_OS::localtime((const time_t*) &time1));
  std::string shortTZ = " ";
  for (uint32_t i = 0; i < strlen(buf); i++) {
    if (isupper(buf[i]))
      shortTZ += buf[i];
  }
  timeFormat += shortTZ;
  char buf1[1024];
  sprintf(buf1, " (%llu) ", tusec / 1000);
  timeFormat += buf1;

  if (!trim_Time.empty()) {
    uint64_t stTime;
    sscanf(trim_Time.c_str(), "%llu", &stTime);
    if (((tusec > stTime) && !endTime) || ((tusec < stTime) && endTime)) {
      bbSet(bb, trTime, timeFormat.c_str());
      bbSet(bb, TemptrTime, tusec);
    }
  } else {
    bbSet(bb, trTime, timeFormat.c_str());
    bbSet(bb, TemptrTime, tusec);
  }
}

int32_t Smokeperf::trimSpecData() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "In Smokeperf::trimSpecData()" );
  try {
    FILE* trimfile = ACE_OS::fopen("trim.spec", "a+");
    if (trimfile == (FILE *) 0) {
      FWKSEVERE( "Could not openfile trim.spec");
      exit(-1);
    }
    std::string msg;

    if (!bbGetString("Trim_BB", "creates_EndTime").empty()) {
        msg = "trimspec creates" + std::string(" end=") + bbGetString("Trim_BB",
            "creates_EndTime") + std::string("\n;\n");
        ACE_OS::fwrite(msg.c_str(), msg.size(), 1, trimfile);
    }
    trimrecord(trimfile);
    //else {
    //  result = FWK_SEVERE;
    //}
    //ACE_OS::fwrite( msg.c_str(), msg.size(), 1, trimfile );
    ACE_OS::fflush(trimfile);
    ACE_OS::fclose(trimfile);
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::trimSpecData() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::trimSpecData() Caught unknown exception." );
  }
  return result;
}


//----------------------------------------------------------------------------
void Smokeperf::trimrecord(FILE* trimfile)
{
	  if (!bbGetString("Trim_BB", "reg_EndTime").empty()) {
	      std::string msg = "trimspec registerInterests" + std::string(" end=")
	          + bbGetString("Trim_BB", "reg_EndTime") + std::string("\n;\n");
	      ACE_OS::fwrite(msg.c_str(), msg.size(), 1, trimfile);
	    }
	    if (!bbGetString("Trim_BB", "put_StartTime").empty() && !bbGetString(
	        "Trim_BB", "put_EndTime").empty()) {
	      std::string msg = "trimspec puts start=" + bbGetString("Trim_BB",
	          "put_StartTime") + std::string(" end=") + bbGetString("Trim_BB",
	          "put_EndTime") + std::string("\n;\n") + "trimspec operations start="
	            + bbGetString("Trim_BB", "put_StartTime") + std::string(" end=")
	            + bbGetString("Trim_BB", "put_EndTime") + std::string("\n;\n");

	      ACE_OS::fwrite(msg.c_str(), msg.size(), 1, trimfile);
	    }
	    if (!bbGetString("Trim_BB", "connects_StartTime").empty() && !bbGetString(
	        "Trim_BB", "connects_EndTime").empty()) {
	      std::string msg = "trimspec connects start=" + bbGetString("Trim_BB",
	          "connects_StartTime") + std::string(" end=") + bbGetString("Trim_BB",
	          "connects_EndTime") + std::string("\n;\n")
	          + "trimspec operations start=" + bbGetString("Trim_BB",
	          "connects_StartTime") + std::string(" end=") + bbGetString("Trim_BB",
	          "connects_EndTime") + std::string("\n;\n");
	      ACE_OS::fwrite(msg.c_str(), msg.size(), 1, trimfile);
	    }
	    if (!bbGetString("Trim_BB", "get_StartTime").empty() && !bbGetString(
	        "Trim_BB", "get_EndTime").empty()) {
	      std::string msg = "trimspec gets start=" + bbGetString("Trim_BB",
	          "get_StartTime") + std::string(" end=") + bbGetString("Trim_BB",
	          "get_EndTime") + std::string("\n;\n") + "trimspec operations start="
	          + bbGetString("Trim_BB", "get_StartTime") + std::string(" end=")
	          + bbGetString("Trim_BB", "get_EndTime") + std::string("\n;\n");
	      ACE_OS::fwrite(msg.c_str(), msg.size(), 1, trimfile);
	    }
	    if (!bbGetString("Trim_BB", "putgets_StartTime").empty() && !bbGetString(
	        "Trim_BB", "putgets_EndTime").empty()) {
	      std::string msg = "trimspec putgets start=" + bbGetString("Trim_BB",
	          "putgets_StartTime") + std::string(" end=") + bbGetString("Trim_BB",
	          "putgets_EndTime") + std::string("\n;\n")
	          + "trimspec operations start=" + bbGetString("Trim_BB",
	          "putgets_StartTime") + std::string(" end=") + bbGetString("Trim_BB",
	          "putgets_EndTime") + std::string("\n;\n");
	      ACE_OS::fwrite(msg.c_str(), msg.size(), 1, trimfile);
	    }
	    if (!bbGetString("Trim_BB", "queries_StartTime").empty() && !bbGetString(
	            "Trim_BB", "queries_EndTime").empty()) {
	          std::string msg = "trimspec queries start=" + bbGetString("Trim_BB",
	              "queries_StartTime") + std::string(" end=") + bbGetString("Trim_BB",
	              "queries_EndTime") + std::string("\n;\n") + "trimspec operations start="
	              + bbGetString("Trim_BB", "queries_StartTime") + std::string(" end=")
	              + bbGetString("Trim_BB", "queries_EndTime") + std::string("\n;\n");
	          ACE_OS::fwrite(msg.c_str(), msg.size(), 1, trimfile);
	    }
	    if (!bbGetString("Trim_BB", "updates_StartTime").empty() && !bbGetString(
	            "Trim_BB", "updates_EndTime").empty()) {
	          std::string msg = "trimspec updates start=" + bbGetString("Trim_BB",
	              "updates_StartTime") + std::string(" end=") + bbGetString("Trim_BB",
	              "updates_EndTime") + std::string("\n;\n") + "trimspec operations start="
	              + bbGetString("Trim_BB", "updates_StartTime") + std::string(" end=")
	              + bbGetString("Trim_BB", "updates_EndTime") + std::string("\n;\n");
	          ACE_OS::fwrite(msg.c_str(), msg.size(), 1, trimfile);
	   }
}


int32_t Smokeperf::puts() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::puts()" );

  try {
	bool encodeKey = true;
	bool encodeTimestamp = false;
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue("clientCount");
    std::string label = RegionHelper::regionTag(region->getAttributes());

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    // Loop over key set sizes
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    resetValue("ObjectType");
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    std::string objectname = getStringValue("ObjectType");
    resetValue("isMainWorkLoad");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    resetValue("distinctKeys");
    resetValue("BatchSize");
    resetValue("opsSecond");
    int32_t numKeys = initKeys(false, true);
    int32_t opsSec = getIntValue( "opsSecond" );
    opsSec = ( opsSec < 1 ) ? 0 : opsSec;
    PutTask * put = NULL;
    while (numKeys > 0) { // keys loop
      // Loop over value sizes
      resetValue("valueSizes");
      //uint32_t valSize = getIntValue("valueSizes" );
      int32_t valSize = initValues(numKeys, 0, false);
      while (valSize > 0) { // value loop
        // Loop over threads
        resetValue("numThreads");
        int32_t numThreads = getIntValue("numThreads");
        while (numThreads > 0) { // thread loop

          // And we do the real work now
          if(opsSec > 0){
        	put = new MeteredPutTask(region, m_KeysA, m_MaxKeys, valSize,
                        objectname, encodeKey, encodeTimestamp, mainworkLoad,opsSec);
          }else {
            put = new PutTask(region, m_KeysA, m_MaxKeys, valSize,
                  objectname, encodeKey, encodeTimestamp, mainworkLoad);
          }
          bool checked = checkReady(numClients);
          FWKINFO( "Running timed task." );
          setTrimTime("put");
          if (!clnt->timeInterval(put, timedInterval, numThreads, 10
              * timedInterval)) {
            clearKeys();
            FWKEXCEPTION( "In doPuts()  Timed run timed out." );
          }
          setTrimTime("put", true);
          if(clnt->getTaskStatus() == FWK_SEVERE)
            FWKEXCEPTION( "Exception during put task");
          if (checked) {
            bbDecrement(CLIENTSBB, READYCLIENTS);
          }

          numThreads = getIntValue("numThreads");
          if (numThreads > 0) {
            perf::sleepSeconds(3); // Put a marker of inactivity in the stats
          }
          delete put;
        } // thread loop

        valSize = initValues(numKeys, 0, false);
        if (valSize > 0) {
          perf::sleepSeconds(3); // Put a marker of inactivity in the stats
        }
      } // value loop

      numKeys = initKeys(false, true);
      if (numKeys > 0) {
        perf::sleepSeconds(3); // Put a marker of inactivity in the stats
      }
    } // keys loop


    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Smokeperf::puts() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Smokeperf::puts() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::puts() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::puts() Caught unknown exception." );
  }
  clearKeys();
  perf::sleepSeconds(3); // Put a marker of inactivity in the stats
  FWKINFO( "Smokeperf::puts() complete." );
  return result;
}
//----------------------------------------------------------------------------------------------
int32_t Smokeperf::populateRegion() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::populateRegion()" );

  try {
    bool encodeKey = true;
	bool encodeTimestamp = false;
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();
    resetValue("distinctKeys");
    resetValue("valueSizes");
    initKeys();
    uint32_t size = getIntValue("valueSizes");
    resetValue("ObjectType");
    std::string objectname = getStringValue("ObjectType");
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    resetValue("numThreads");
    resetValue("AssetAccountSize");
    resetValue("AssetMaxVal");
    resetValue("isMainWorkLoad");
    //int32_t numThreads = getIntValue( "numThreads" );
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    int32_t assetAccountSize = getIntValue("AssetAccountSize");
    if(assetAccountSize < 0)
      assetAccountSize = 0;
    int32_t assetMaxVal = getIntValue("AssetMaxVal");
    if(assetMaxVal < 0)
      assetMaxVal = 0;
    CreateTask creates(region, m_KeysA, m_MaxKeys, size, objectname, encodeKey,
        encodeTimestamp, mainworkLoad,assetAccountSize,assetMaxVal);
    FWKINFO( "Populating region." );
    if (!clnt->runIterations(&creates, m_MaxKeys, 1, 0)) {
      clearKeys();
      FWKEXCEPTION( "In populateRegion()  Population timed out." );
    }
    setTrimTime("creates", true);
    FWKINFO( "Added " << creates.getIters() << " entries." );
    result = FWK_SUCCESS;
  } catch (std::exception e) {
    FWKEXCEPTION( "Smokeperf::populateRegion() Caught std::exception: " << e.what() );
  } catch (Exception e) {
    FWKEXCEPTION( "Smokeperf::populateRegion() Caught Exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::populateRegion() Caught FwkException: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::populateRegion() Caught unknown exception." );
  }
  clearKeys();
  FWKINFO( "Smokeperf::populateRegion() complete." );
  return result;
}
//---------------------------------------------
int32_t Smokeperf::registerInterestList() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::registerInterestList()" );

  try {
    static char keyType = 'i';
    std::string typ = getStringValue("keyType"); // int is only value to use
    char newType = typ.empty() ? 's' : typ[0];

    RegionPtr region = getRegionPtr();
    resetValue("distinctKeys");
    resetValue("keyIndexBegin");
    resetValue("registerKeys");
    int32_t numKeys = getIntValue("distinctKeys"); // check distince keys first
    int32_t numClients = getIntValue("clientCount");
    int32_t id = 0;
    if (numClients > 0) {
      id = g_test->getClientId();
      if (id < 0)
        id = -id;
      numKeys = numKeys / numClients;
    }
    if (numKeys < 1)
      FWKEXCEPTION("Key is less than 0 for each client. Provide max number of distinctKeys");
    if (numKeys <= 0) {
      return result;
    }
    int32_t low = getIntValue("keyIndexBegin");
    low = (low > 0) ? low : 0;
    int32_t numOfRegisterKeys = getIntValue("registerKeys");
    int32_t high = numOfRegisterKeys + low;

    clearKeys();
    m_MaxKeys = numOfRegisterKeys;
    m_KeyIndexBegin = low;
    keyType = newType;
    VectorOfCacheableKey registerKeyList;
    if (keyType == 'i') {
      initIntKeys(low, high);
    } else {
      int32_t keySize = getIntValue("keySize");
      keySize = (keySize > 0) ? keySize : 10;
      std::string keyBase(keySize, 'A');
      initStrKeys(low, high, keyBase, id);
    }

    for (int j = low; j < high; j++) {
      if (m_KeysA[j - low] != NULLPTR) {
        registerKeyList.push_back(m_KeysA[j - low]);
      } else {
        FWKINFO("Smokeperf::registerInterestList key is NULL");
      }
    }
    resetValue("getInitialValues");
    bool isGetInitialValues = getBoolValue("getInitialValues");
    FWKINFO("Smokeperf::registerInterestList region name is " << region->getName()
        << "; getInitialValues is " << isGetInitialValues);
    region->registerKeys(registerKeyList, false, isGetInitialValues);
    result = FWK_SUCCESS;

  } catch (Exception& e) {
    FWKEXCEPTION( "Smokeperf::registerInterestList() Caught Exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::registerInterestList() Caught FwkException: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::registerInterestList() Caught unknown exception." );
  }
  FWKINFO( "Smokeperf::registerInterestList() complete." );
  return result;
}
// ----------------------------------------------------------------------------

int32_t Smokeperf::registerRegexList() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::registerRegexList()" );
  try {
    RegionPtr region = getRegionPtr();
    std::string registerRegex = getStringValue("registerRegex");
    FWKINFO("Smokeperf::registerRegexList region name is " << region->getName() << "regex is: " << registerRegex.c_str());
    resetValue("getInitialValues");
    bool isGetInitialValues = getBoolValue("getInitialValues");
    region->registerRegex(registerRegex.c_str(), false, NULLPTR,
        isGetInitialValues);
    result = FWK_SUCCESS;

  } catch (Exception& e) {
    FWKEXCEPTION( "Smokeperf::registerRegexList() Caught Exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::registerRegexList() Caught FwkException: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::registerRegexList() Caught unknown exception." );
  }
  FWKINFO( "Smokeperf::registerRegexList() complete." );
  return result;

}
// ----------------------------------------------------------------------------

int32_t Smokeperf::unregisterRegexList() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::unregisterRegexList()" );
  try {
    RegionPtr region = getRegionPtr();
    std::string unregisterRegex = getStringValue("unregisterRegex");
    FWKINFO("Smokeperf::unregisterRegexList region name is " << region->getName() << "regex is: " << unregisterRegex.c_str());
    region->unregisterRegex(unregisterRegex.c_str());
    result = FWK_SUCCESS;

  } catch (Exception& e) {
    FWKEXCEPTION( "Smokeperf::unregisterRegexList() Caught Exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::unregisterRegexList() Caught FwkException: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::unregisterRegexList() Caught unknown exception." );
  }
  FWKINFO( "Smokeperf::unregisterRegexList() complete." );
  return result;

}

//-----------------------------------------------------------------------------
int32_t Smokeperf::registerAllKeys() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::registerAllKeys()" );

  try {
    RegionPtr region = getRegionPtr();
    resetValue("getInitialValues");
    bool isGetInitialValues = getBoolValue("getInitialValues");
    resetValue("isDurableReg");
    bool isDurable = getBoolValue("isDurableReg");
    FWKINFO("Smokeperf::registerAllKeys region name is " << region->getName()
        << "; getInitialValues is " << isGetInitialValues);
    region->registerAllKeys(isDurable, NULLPTR, isGetInitialValues);
    setTrimTime("reg", true);
    result = FWK_SUCCESS;

  } catch (Exception& e) {
    FWKEXCEPTION( "Smokeperf::registerAllKeys() Caught Exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::registerAllKeys() Caught FwkException: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::registerAllKeys() Caught unknown exception." );
  }
  FWKINFO( "Smokeperf::registerAllKeys() complete." );
  return result;

}

// ----------------------------------------------------------------------------

RegionPtr Smokeperf::getRegionPtr(const char * reg) {
  RegionPtr region;
  std::string name;

  if (reg == NULL) {
    name = getStringValue("regionName");
    if (name.empty()) {
      try {
        RegionHelper help(g_test);
        name = help.regionName();
        if (name.empty()) {
          name = help.specName();
        }
      } catch (...) {
      }
    }
  }
  try {
    if (name.empty()) { // just get a random root region
      VectorOfRegion rootRegionVector;
      m_cache->rootRegions(rootRegionVector);
      int32_t size = rootRegionVector.size();

      if (size == 0) {
        FWKEXCEPTION( "In Smokeperf::getRegionPtr()  No regions exist." );
      }

      FWKINFO( "Getting a random root region." );
      region = rootRegionVector.at(GsRandom::random(size));
    } else {
      FWKINFO( "Getting region: " << name );
      if (m_cache == NULLPTR) {
        FWKEXCEPTION( "Failed to get region: " << name << "  cache ptr is null." );
      }
      region = m_cache->getRegion(name.c_str());
      if (region == NULLPTR) {
        FWKEXCEPTION( "Failed to get region: " << name );
      }
    }
  } catch (CacheClosedException e) {
    FWKEXCEPTION( "In Smokeperf::getRegionPtr()  CacheFactory::getInstance encountered "
        "CacheClosedException: " << e.getMessage() );
  } catch (EntryNotFoundException e) {
    FWKEXCEPTION( "In Smokeperf::getRegionPtr()  CacheFactory::getInstance encountered "
        "EntryNotFoundException: " << e.getMessage() );
  } catch (IllegalArgumentException e) {
    FWKEXCEPTION( "In Smokeperf::getRegionPtr()  CacheFactory::getInstance encountered "
        "IllegalArgumentException: " << e.getMessage() );
  }
  return region;
}

bool Smokeperf::checkReady(int32_t numClients) {
  if (numClients > 0) {
    FWKINFO( "Check whether all clients are ready to run" );
    bbIncrement(CLIENTSBB, READYCLIENTS);
    int64_t readyClients = 0;
    while (readyClients < numClients) {
      readyClients = bbGet(CLIENTSBB, READYCLIENTS);
      perf::sleepMillis(3);
    }
    FWKINFO( "All Clients are ready to go !!" );
    return true;
  }
  FWKINFO( "All Clients are ready to go !!" );
  return false;
}
int32_t Smokeperf::openStatistic() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::openStatistic()");
  try {
    TestClient * clnt = TestClient::getTestClient();
    resetValue("numThreads");
    int32_t numThreads = getIntValue("numThreads");
    numThreads = (numThreads < 0) ? 1:numThreads;
    InitPerfStat *initStat = new InitPerfStat();
    if (!clnt->timeInterval(initStat, 0, numThreads, 0)) {
      FWKEXCEPTION( "In openStatistic() Timed run timed out." );
    }
    FWKINFO( "Opened statistics");
    result = FWK_SUCCESS;
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::openStatistic() Caught unknown exception." );
  }
  FWKINFO( "Smokeperf::openStatistic() complete." );
  return result;
}
int32_t Smokeperf::closeStatistic() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::closeStatistic");
  try {
    for (int i = 0; i < 10; i++) {
      delete PutGetTask::perfstat[i];
      PutGetTask::perfstat[i] = NULL;
    }
    FWKINFO( "Closed statistics");
    result = FWK_SUCCESS;
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::closeStatistic() Caught exception.");
  }
  return result;
}

int32_t Smokeperf::createPools() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::createPool()" );
  try {
    PoolHelper help(g_test);
    PoolPtr pool = help.createPool();
    FWKINFO( "Smokeperf::createPool Created Pool " << pool->getName() << std::endl);
    result = FWK_SUCCESS;
  } catch (Exception e) {
    FWKEXCEPTION( "Smokeperf::createPool FAILED -- caught exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::createPool FAILED -- caught test exception: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::createPool FAILED -- caught unknown exception. " );
  }
  return result;
}

int32_t Smokeperf::cyclePoolTask() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::cyclePoolTask" );
  try {
    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    resetValue("isMainWorkLoad");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    int32_t sleepMs = getTimeValue("sleepMs");
    ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value(timedInterval);
    ACE_Time_Value now;
    int64_t startTime;
    PoolHelper help(g_test);
    setTrimTime("connects");
    while (now < end) {
      startTime = PutGetTask::perfstat[0]->startConnect();
      PoolPtr pool = help.createPoolForPerf();
      pool->destroy();
      PutGetTask::perfstat[0]->endConnect(startTime, mainworkLoad);
      perf::sleepMillis(sleepMs);
      now = ACE_OS::gettimeofday();
    }
    setTrimTime("connects", true);
    result = FWK_SUCCESS;
  } catch (Exception e) {
    FWKEXCEPTION( "Smokeperf::cyclePoolTask FAILED -- caught exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::cyclePoolTask FAILED -- caught test exception: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::cyclePoolTask FAILED -- caught unknown exception. " );
  }
  return result;

}

int32_t Smokeperf::cycleBridgeConnectionTask() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::cycleBridgeConnectionTask" );
  const std::string name = getStringValue("regionName");
  if (name.empty()) {
    FWKEXCEPTION( "Region name not specified in test." );
  }
  RegionHelper help(g_test);
  resetValue("isMainWorkLoad");
  bool mainworkLoad = getBoolValue("isMainWorkLoad");
  try {
    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    int32_t sleepMs = getTimeValue("sleepMs");
    ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value(timedInterval);
    ACE_Time_Value now;
    int64_t startTime;
    setTrimTime("connects");
    while (now < end) {
      startTime = PutGetTask::perfstat[0]->startConnect();
      CacheLoaderPtr cl(new perfCacheLoader());
      RegionPtr regionPtr = help.createRootRegion(m_cache);
      regionPtr->localDestroyRegion();
      PutGetTask::perfstat[0]->endConnect(startTime, mainworkLoad);
      perf::sleepMillis(sleepMs);
      now = ACE_OS::gettimeofday();
    }
    setTrimTime("connects", true);
    result = FWK_SUCCESS;
  } catch (Exception e) {
    FWKEXCEPTION( "Smokeperf::cycleBridgeConnectionTask FAILED -- caught exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::cycleBridgeConnectionTask FAILED -- caught test exception: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::cycleBridgeConnectionTask FAILED -- caught unknown exception. " );
  }
  return result;
}
//----------------------------------------------------------------------------------------
int32_t Smokeperf::mixPutGetDataTask() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::mixPutGetDataTask()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue("clientCount");
    std::string label = RegionHelper::regionTag(region->getAttributes());

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    // Loop over key set sizes
    bool encodeKey = true;
    bool encodeTimestamp = false;
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    resetValue("ObjectType");
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    std::string objectname = getStringValue("ObjectType");
    int32_t putPercentage = getIntValue("putPercentage");
    resetValue("isMainWorkLoad");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    resetValue("distinctKeys");
    int32_t numKeys = initKeys(false, true);
    while (numKeys > 0) { // keys loop
      // Loop over value sizes
      resetValue("valueSizes");
      //uint32_t valSize = getIntValue("valueSizes" );
      int32_t valSize = initValues(numKeys, 0, false);
      while (valSize > 0) { // value loop
        // Loop over threads
        resetValue("numThreads");
        int32_t numThreads = getIntValue("numThreads");
        while (numThreads > 0) { // thread loop

          PutGetMixTask * putGet = new PutGetMixTask(region, m_KeysA,
              m_MaxKeys, valSize, objectname, encodeKey, encodeTimestamp,
              mainworkLoad, putPercentage);

          bool checked = checkReady(numClients);
          FWKINFO( "Running timed task." );
          setTrimTime("putgets");
          if (!clnt->timeInterval(putGet, timedInterval, numThreads, 10
              * timedInterval)) {
            clearKeys();
            FWKEXCEPTION( "In mixPutGetDataTask() Timed run timed out." );
          }
          setTrimTime("putgets", true);
          if (clnt->getTaskStatus() == FWK_SEVERE)
            FWKEXCEPTION( "Exception during putGet task");

          if (checked) {
            bbDecrement(CLIENTSBB, READYCLIENTS);
          }

          numThreads = getIntValue("numThreads");
          if (numThreads > 0) {
            perf::sleepSeconds(3); // Put a marker of inactivity in the stats
          }
          delete putGet;
        } // thread loop

        valSize = initValues(numKeys, 0, false);
        if (valSize > 0) {
          perf::sleepSeconds(3); // Put a marker of inactivity in the stats
        }
      } // value loop

      numKeys = initKeys(false, true);
      if (numKeys > 0) {
        perf::sleepSeconds(3); // Put a marker of inactivity in the stats
      }
    } // keys loop


    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Smokeperf::mixPutGetDataTask() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Smokeperf::mixPutGetDataTask() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::mixPutGetDataTask() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::mixPutGetDataTask() Caught unknown exception." );
    //perf::sleepSeconds( 120 );
  }
  clearKeys();
  perf::sleepSeconds(3); // Put a marker of inactivity in the stats
  FWKINFO( "Smokeperf::mixPutGetDataTask() complete." );
  return result;
}

int32_t Smokeperf::queries() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::queries()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }

    // Loop over key set sizes
    resetValue("query");
    std::string queryStr = getStringValue("query"); // set the query string in xml
    if(queryStr.empty())
      queryStr = "select distinct * from " + std::string(region->getFullPath());
    resetValue("numThreads");
    int32_t numThreads = getIntValue("numThreads");
    while (numThreads > 0) { // thread loop
      RegionQueryTask * query = new RegionQueryTask(region,queryStr);
      setTrimTime("queries");
      if (!clnt->timeInterval(query, timedInterval, numThreads, 10 * timedInterval))
      {
        FWKEXCEPTION( "In Queries()  Timed run timed out." );
      }
      setTrimTime("queries", true);
      numThreads = getIntValue("numThreads");
      if (numThreads > 0) {
        perf::sleepSeconds(3);
      }
      delete query;
    } // thread loop
    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Smokeperf::queries() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Smokeperf::queries() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::queries() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::queries() Caught unknown exception." );
  }
  FWKINFO( "Smokeperf::queries() complete." );
  return result;
}

int32_t Smokeperf::registerCQs() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::registerCQs()" );

  try {
    RegionPtr region = getRegionPtr();
    int32_t numCQ = getIntValue("numCQs");
    numCQ = (numCQ <= 0) ? 1 : numCQ;
    char cqname[100];
    for(int32_t i=0;i < numCQ; i++) {
      sprintf(cqname,"cq%d",i);
      std::string query = getQuery(i);
      PoolPtr pool = PoolManager::find("_Test_Pool1");
      QueryServicePtr qs= pool->getQueryService();
      CqAttributesFactory cqFac;
      CqListenerPtr cqLstner(new CQLatencyListener(PutGetTask::perfstat[0]));
      cqFac.addCqListener(cqLstner);
      CqAttributesPtr cqAttr = cqFac.create();
      FWKINFO("Registering CQ named " << cqname <<" with query: "<< query );
      CqQueryPtr qry = qs->newCq(cqname, query.c_str(), cqAttr);
      bool isexecuteWithInitialResults = getBoolValue("executeWithInitialResults");
      if(isexecuteWithInitialResults)
    	  SelectResultsPtr results = qry->executeWithInitialResults(300);
      else
          qry->execute();
      FWKINFO("Successfully executed CQ named "<< cqname);
    }

    result = FWK_SUCCESS;
  } catch (Exception & e) {
      FWKEXCEPTION( "Smokeperf::registerCQs() Caught Exception: " << e.getMessage() );
    } catch (FwkException & e) {
      FWKEXCEPTION( "Smokeperf::registerCQs() Caught FwkException: " << e.getMessage() );
    } catch (std::exception & e) {
      FWKEXCEPTION( "Smokeperf::registerCQs() Caught std::exception: " << e.what() );
    } catch (...) {
      FWKEXCEPTION( "Smokeperf::registerCQs() Caught unknown exception." );
    }
    FWKINFO( "Smokeperf::registerCQs() complete." );
    return result;
}

std::string Smokeperf::getQuery(int i){
  RegionPtr region = getRegionPtr();
  int32_t strBatchSize = getIntValue("BatchSize");
  int32_t maxkeys = getIntValue("distinctKeys");
  if ((maxkeys % strBatchSize) != 0)
    FWKEXCEPTION( "Keys does not evenly divide");
  int32_t batches = maxkeys/strBatchSize;
  int32_t batchNum = (i +1) % batches;
  char buf[100];
  sprintf(buf,"%d",batchNum);
  std::string query ="SELECT * FROM " + std::string(region->getFullPath()) + " obj WHERE obj.batch = " + std::string(buf);
  return query;
}

//----------------------------------------------------------------------------
int32_t Smokeperf::putBatchObj() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::putBatchObj()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue("clientCount");
    std::string label = RegionHelper::regionTag(region->getAttributes());

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    // Loop over key set sizes
    bool encodeKey = true;
    bool encodeTimestamp = false;
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    resetValue("ObjectType");
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    std::string objectname = getStringValue("ObjectType");
    resetValue("isMainWorkLoad");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    resetValue("distinctKeys");
    resetValue("BatchSize");
    int32_t batchsize = getIntValue("BatchSize");
    int32_t numKeys = 0;
    if(batchsize > 0)
      numKeys = initBatchKeys(false);
    else
      numKeys = initKeys(false, true);
    while (numKeys > 0) { // keys loop
      // Loop over value sizes
      resetValue("valueSizes");
      //uint32_t valSize = getIntValue("valueSizes" );
      int32_t valSize = initValues(numKeys, 0, false);
      while (valSize > 0) { // value loop
        // Loop over threads
        resetValue("numThreads");
        int32_t numThreads = getIntValue("numThreads");
        while (numThreads > 0) { // thread loop

          PutBatchObjectTask * puts = new PutBatchObjectTask(region, m_KeysA, m_MaxKeys, valSize,
              objectname, encodeKey, encodeTimestamp, mainworkLoad,batchsize,valSize);
          bool checked = checkReady(numClients);
          bool isCreate = getBoolValue("isCreate");
          if (isCreate) {
            FWKINFO( "Creating entries." );
            if (!clnt->runIterations(puts, m_MaxKeys, 1, 0)) {
              clearKeys();
              FWKEXCEPTION( "In doPuts()  Warmup timed out." );
            }
          } else {
            FWKINFO( "Running timed task." );
            setTrimTime("put");
            if (!clnt->timeInterval(puts, timedInterval, numThreads, 10
                * timedInterval)) {
              clearKeys();
              FWKEXCEPTION( "In doPuts()  Timed run timed out." );
            }
            setTrimTime("put", true);
          }
          if (checked) {
            bbDecrement(CLIENTSBB, READYCLIENTS);
          }

          numThreads = getIntValue("numThreads");
          if (numThreads > 0) {
            perf::sleepSeconds(3); // Put a marker of inactivity in the stats
          }
          delete puts;
        } // thread loop

        valSize = initValues(numKeys, 0, false);
        if (valSize > 0) {
          perf::sleepSeconds(3); // Put a marker of inactivity in the stats
        }
      } // value loop

      int32_t batchsize = getIntValue("BatchSize");
      if (batchsize > 0)
        numKeys = initBatchKeys(false);
      else
        numKeys = initKeys(false, true);
      if (numKeys > 0) {
        perf::sleepSeconds(3); // Put a marker of inactivity in the stats
      }
    } // keys loop

    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Smokeperf::putBatchObj() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Smokeperf::putBatchObj() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::putBatchObj() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::putBatchObj() Caught unknown exception." );
  }
  clearKeys();
  perf::sleepSeconds(3); // Put a marker of inactivity in the stats
  FWKINFO( "Smokeperf::putBatchObj() complete." );
  return result;
}

int32_t Smokeperf::cycleDurableBridgeConnection(const char * taskId) {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::cycleDurableBridgeConnection()" );
  resetValue("isMainWorkLoad");
  bool mainworkLoad = getBoolValue("isMainWorkLoad");
  try {
    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    int32_t sleepMs = getTimeValue("sleepMs");
    bool keepalive = getBoolValue("keepAlive");
    ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value(timedInterval);
    ACE_Time_Value now;
    int64_t startTime;
    setTrimTime("connects");
    while (now < end) {
      startTime = PutGetTask::perfstat[0]->startConnect();
      result = doCreateRegion(taskId);
      bool isDurable = g_test->getBoolValue("isDurableReg");
      RegionPtr region = getRegionPtr();
      region->registerRegex(".*", isDurable);
      m_cache->close(keepalive);
      region = NULLPTR;
      m_cache = NULLPTR;
      //FrameworkTest::cacheFinalize();
      PutGetTask::perfstat[0]->endConnect(startTime, mainworkLoad);
      perf::sleepMillis(sleepMs);
      now = ACE_OS::gettimeofday();
    }
    setTrimTime("connects", true);
    result = FWK_SUCCESS;
  } catch (Exception e) {
    FWKEXCEPTION( "Smokeperf::cycleDurableBridgeConnection FAILED -- caught exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::cycleDurableBridgeConnection FAILED -- caught test exception: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::cycleDurableBridgeConnection FAILED -- caught unknown exception. " );
  }
  return result;
}

int32_t Smokeperf::cycleDurableClientTask(const char * taskId) {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::cycleDurableClientTask()" );
  //resetValue("isMainWorkLoad");
  //bool mainworkLoad = getBoolValue("isMainWorkLoad");
  try {
    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    bool isDurable = g_test->getBoolValue("isDurableReg");
    std::string poolName = getStringValue("poolName");
    if(poolName.empty())
      poolName = "_Test_Pool1";
    char oper_cnt_key[32] = { '\0' };
    PoolHelper help(g_test);
    ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value(timedInterval);
    ACE_Time_Value now;
    int64_t startTime;
    setTrimTime("connects");
    while (now < end) {
      startTime = PutGetTask::perfstat[0]->startConnect();
      g_test->checkTest( taskId );
      PoolPtr pool = help.createPool();
      result = doCreateRegion(taskId);
      //m_cache->readyForEvents();
      RegionPtr region = getRegionPtr();
      region->registerRegex(".*", isDurable);
      m_cache->readyForEvents();
      PutGetTask::perfstat[0]->endConnect(startTime, false);
      ACE_OS::sprintf(oper_cnt_key, "ClientName_%d_Count", getClientId());
      int64_t cur_cnt = g_test->bbGet(DURABLEBB,oper_cnt_key);
      PutGetTask::perfstat[0]->incUpdateEvents((int)cur_cnt);
      PutGetTask::perfstat[0]->setOpTime((long)PutGetTask::perfstat[0]->getConnectTime());
      PutGetTask::perfstat[0]->setOps((int)cur_cnt + PutGetTask::perfstat[0]->getOps());
      m_cache->close(true);
      //pool->destroy();
      region = NULLPTR;
      m_cache = NULLPTR;
      pool = NULLPTR;
      perf::sleepMillis(10000);
      now = ACE_OS::gettimeofday();
    }
    setTrimTime("connects", true);
    result = FWK_SUCCESS;
  } catch (Exception e) {
    FWKEXCEPTION( "Smokeperf::cycleDurableClientTask FAILED -- caught exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::cycleDurableClientTask FAILED -- caught test exception: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::cycleDurableClientTask FAILED -- caught unknown exception. " );
  }
  return result;
}

int32_t Smokeperf::createEntryMapTask() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::createEntryMapTask()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    std::string label = RegionHelper::regionTag(region->getAttributes());

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    // Loop over key set sizes
    bool encodeKey = true;
    bool encodeTimestamp = false;
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    resetValue("ObjectType");
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    std::string objectname = getStringValue("ObjectType");
    resetValue("isMainWorkLoad");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    resetValue("distinctKeys");
    int32_t numKeys = initKeys(false, true);
    resetValue("valueSizes");
    int32_t valSize = initValues(numKeys, 0, false);
    resetValue("numThreads");
    int32_t numThreads = getIntValue("numThreads");
    CreatePutAllMap * createMap = new CreatePutAllMap(region, m_KeysA,
        m_MaxKeys, valSize, objectname, maps,encodeKey, encodeTimestamp,
        mainworkLoad);

    FWKINFO( "Running timed task." );
    if (!clnt->runIterations( createMap, m_MaxKeys, numThreads, 0 )) {
      clearKeys();
      FWKEXCEPTION( "In createEntryMapTask()  Timed run timed out." );
    }
    delete createMap;
    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Smokeperf::createEntryMapTask() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Smokeperf::createEntryMapTask() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::createEntryMapTask() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::createEntryMapTask() Caught unknown exception." );
  }
  clearKeys();
  perf::sleepSeconds(3); // Put a marker of inactivity in the stats
  FWKINFO( "Smokeperf::createEntryMapTask() complete." );
  return result;
}

int32_t Smokeperf::putAllEntryMapTask() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::putAllEntryMapTask()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    std::string label = RegionHelper::regionTag(region->getAttributes());

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    // Loop over key set sizes
    bool encodeKey = true;
    bool encodeTimestamp = false;
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    resetValue("ObjectType");
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    std::string objectname = getStringValue("ObjectType");
    resetValue("isMainWorkLoad");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    resetValue("distinctKeys");
    int32_t numKeys = initKeys(false, true);
    resetValue("valueSizes");
    int32_t valSize = initValues(numKeys, 0, false);
    resetValue("numThreads");
    int32_t numThreads = getIntValue("numThreads");
    CreatePutAllMap * createMap = new CreatePutAllMap(region, m_KeysA,
        m_MaxKeys, valSize, objectname, maps,encodeKey, encodeTimestamp,
        mainworkLoad);

    FWKINFO( "Running timed task for CreatePutAllMap. " );
    if (!clnt->runIterations( createMap, m_MaxKeys, numThreads, 0 )) {
      clearKeys();
      FWKEXCEPTION( "CreatePutAllMap()  Timed run timed out." );
    }
    delete createMap;
    PutAll * putall = new PutAll(region, m_KeysA,
        m_MaxKeys, valSize, objectname, maps,encodeKey, encodeTimestamp,
        mainworkLoad);

    FWKINFO( "Running timed task for putAllEntryMapTask." );
    setTrimTime("put");
    if (!clnt->timeInterval(putall, timedInterval, numThreads, 10
        * timedInterval)) {
      clearKeys();
      FWKEXCEPTION( "In putAllEntryMapTask()  Timed run timed out." );
    }
    setTrimTime("put",true);
    delete putall;
    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Smokeperf::putAllEntryMapTask() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Smokeperf::putAllEntryMapTask() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::putAllEntryMapTask() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::putAllEntryMapTask() Caught unknown exception." );
  }
  clearKeys();
  perf::sleepSeconds(3); // Put a marker of inactivity in the stats
  FWKINFO( "Smokeperf::putAllEntryMapTask() complete." );
  return result;
}
//----------------------------------------------------------------------------------------
//----------------------------------------------------------------------------
int32_t Smokeperf::updateDelta() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::updateDelta()" );

  try {
	bool encodeKey = true;
	bool encodeTimestamp = false;
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue("clientCount");
    std::string label = RegionHelper::regionTag(region->getAttributes());

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    resetValue("distinctKeys");
    resetValue("ObjectType");
    std::string objectname = getStringValue("ObjectType");
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    resetValue("numThreads");
    resetValue("AssetAccountSize");
    resetValue("AssetMaxVal");
    resetValue("isMainWorkLoad");
    //int32_t numThreads = getIntValue( "numThreads" );
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    int32_t assetAccountSize = getIntValue("AssetAccountSize");
    if(assetAccountSize < 0)
      assetAccountSize = 0;
    int32_t assetMaxVal = getIntValue("AssetMaxVal");
      if(assetMaxVal < 0)
        assetMaxVal = 0;
    int32_t numKeys = initKeys(false, true);
    while (numKeys > 0) { // keys loop
      // Loop over value sizes
      resetValue("valueSizes");
      //uint32_t valSize = getIntValue("valueSizes" );
      int32_t valSize = initValues(numKeys, 0, false);
      while (valSize > 0) { // value loop
        // Loop over threads
        resetValue("numThreads");
        int32_t numThreads = getIntValue("numThreads");
        while (numThreads > 0) { // thread loop

          // And we do the real work now
          UpdateDeltaTask * put = new UpdateDeltaTask(region, m_KeysA, m_MaxKeys, valSize, objectname, encodeKey,
              encodeTimestamp, mainworkLoad,assetAccountSize,assetMaxVal);
          bool checked = checkReady(numClients);
          FWKINFO( "Running timed task." );
          setTrimTime("updates");
          if (!clnt->timeInterval(put, timedInterval, numThreads, 10
              * timedInterval)) {
            clearKeys();
            FWKEXCEPTION( "In updateDelta()  Timed run timed out." );
          }
          setTrimTime("updates", true);
          if(clnt->getTaskStatus() == FWK_SEVERE)
            FWKEXCEPTION( "Exception during updateDelta task");
          if (checked) {
            bbDecrement(CLIENTSBB, READYCLIENTS);
          }

          numThreads = getIntValue("numThreads");
          if (numThreads > 0) {
            perf::sleepSeconds(3); // Put a marker of inactivity in the stats
          }
          delete put;
        } // thread loop

        valSize = initValues(numKeys, 0, false);
        if (valSize > 0) {
          perf::sleepSeconds(3); // Put a marker of inactivity in the stats
        }
      } // value loop

      numKeys = initKeys(false, true);
      if (numKeys > 0) {
        perf::sleepSeconds(3); // Put a marker of inactivity in the stats
      }
    } // keys loop


    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Smokeperf::updateDelta() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Smokeperf::updateDelta() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::updateDelta() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::updateDelta() Caught unknown exception." );
  }
  clearKeys();
  perf::sleepSeconds(3); // Put a marker of inactivity in the stats
  FWKINFO( "Smokeperf::updateDelta() complete." );
  return result;
}

//---------------------------------------------------------------------------------
//------------------------------------Functions for scale perf----------------------------------------------------------

int32_t Smokeperf::trimScaleSpecData() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "In Smokeperf::trimScaleSpecData()" );
  try {
    FILE* trimfile = ACE_OS::fopen("trim.spec", "a+");
    if (trimfile == (FILE *) 0) {
      FWKSEVERE( "Could not openfile trim.spec");
      exit(-1);
    }
    std::string msg;
    resetValue("isCreatePerf");
    bool isCreate = getBoolValue("isCreatePerf");
    if(isCreate){
      if (!bbGetString("Trim_BB", "creates_StartTime").empty() && !bbGetString(
            "Trim_BB", "creates_EndTime").empty()) {
          std::string msg = "trimspec creates start=" + bbGetString("Trim_BB",
              "creates_StartTime") + std::string(" end=") + bbGetString("Trim_BB",
              "creates_EndTime") + std::string("\n;");
          FWKINFO( "In Smokeperf::trimSpecData() --3");
          ACE_OS::fwrite(msg.c_str(), msg.size(), 1, trimfile);
      }
    }
    else {
      if (!bbGetString("Trim_BB", "creates_EndTime").empty()) {
        msg = "trimspec creates" + std::string(" end=") + bbGetString("Trim_BB",
            "creates_EndTime") + std::string("\n;\n");
        ACE_OS::fwrite(msg.c_str(), msg.size(), 1, trimfile);
      }
    }
    trimrecord(trimfile);
    //else {
    //  result = FWK_SEVERE;
    //}
    //ACE_OS::fwrite( msg.c_str(), msg.size(), 1, trimfile );
    ACE_OS::fflush(trimfile);
    ACE_OS::fclose(trimfile);
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::trimSpecData() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::trimSpecData() Caught unknown exception." );
  }
  return result;
}


int32_t Smokeperf::createData() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::createData()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();
    resetValue("distinctKeys");
    resetValue("valueSizes");
    int32_t Keys = getIntValue("distinctKeys");
    int32_t numClients = getIntValue("clientCount");
    int32_t numKeyperClient = Keys/numClients;
    uint32_t size = getIntValue("valueSizes");
    resetValue("ObjectType");
    std::string objectname = getStringValue("ObjectType");
    bool encodeKey = true;
    bool encodeTimestamp = false;
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    resetValue("numThreads");
    resetValue("AssetAccountSize");
    resetValue("AssetMaxVal");
    resetValue("isMainWorkLoad");
    int32_t numThreads = getIntValue( "numThreads" );
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    int32_t assetAccountSize = getIntValue("AssetAccountSize");
    if(assetAccountSize < 0)
      assetAccountSize = 0;
    int32_t assetMaxVal = getIntValue("AssetMaxVal");
    if(assetMaxVal < 0)
      assetMaxVal = 0;
    setTrimTime("creates");
    resetValue("BatchSize");
    int32_t batchsize = getIntValue("BatchSize");
    if(batchsize > 0){
      CreateBatchTask creates(region, numKeyperClient, size,
    	        	objectname, encodeKey, encodeTimestamp, mainworkLoad,g_test,batchsize,size);
      FWKINFO( "creating data." );
      setTrimTime("creates",true);
      if (!clnt->runIterations(&creates, numKeyperClient/numThreads, numThreads, 0)) {
        clearKeys();
        FWKEXCEPTION( "In createData()  Population timed out." );
      }
      setTrimTime("creates", true);
      FWKINFO( "Added " << creates.getIters() << " entries." );
    }
    else {
      CreateDataTask creates(region, numKeyperClient, size, objectname, encodeKey,
          encodeTimestamp, mainworkLoad,g_test,assetAccountSize,assetMaxVal);
      FWKINFO( "creating data." );
      setTrimTime("creates",true);
      if (!clnt->runIterations(&creates, numKeyperClient/numThreads, numThreads, 0)) {
        clearKeys();
        FWKEXCEPTION( "In createData()  Population timed out." );
      }
      setTrimTime("creates", true);
      FWKINFO( "Added " << creates.getIters() << " entries." );
    }
    result = FWK_SUCCESS;
  } catch (std::exception e) {
    FWKEXCEPTION( "Smokeperf::createData() Caught std::exception: " << e.what() );
  } catch (Exception e) {
    FWKEXCEPTION( "Smokeperf::createData() Caught Exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::createData() Caught FwkException: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::createData() Caught unknown exception." );
  }
  FWKINFO( "Smokeperf::createData() complete." );
  return result;
}

int32_t Smokeperf::getDataTask() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::getDataTask()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue("clientCount");

    std::string label = RegionHelper::regionTag(region->getAttributes());
    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0)
      timedInterval = 5;

    resetValue("distinctKeys");
    int32_t maxKey = getIntValue("distinctKeys");
    // Loop over threads
    resetValue("numThreads");
    int32_t numThreads = getIntValue("numThreads");
    resetValue("isMainWorkLoad");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    while (numThreads > 0) { // thread loop



    	GetDataTask gets(region, maxKey, mainworkLoad);
      //      FWKINFO( "Running warmup task." );

      bool checked = checkReady(numClients);
      FWKINFO( "Running timed task." );
      setTrimTime("get");
      if (!clnt->timeInterval(&gets, timedInterval, numThreads, timedInterval)) {
        clearKeys();
        FWKEXCEPTION( "In getDataTask()  Timed run timed out." );
      }
      setTrimTime("get", true);
      if (clnt->getTaskStatus() == FWK_SEVERE)
        FWKEXCEPTION( "Exception during get task");
      if (checked) {
        bbDecrement(CLIENTSBB, READYCLIENTS);
      }

      numThreads = getIntValue("numThreads");
      if (numThreads > 0) {
        perf::sleepSeconds(3);
      }
   } // thread loop
    result = FWK_SUCCESS;
  } catch (Exception e) {
    FWKEXCEPTION( "Smokeperf::getDataTask Caught Exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "Smokeperf::getDataTask Caught FwkException: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::getDataTask Caught unknown exception." );
  }
  FWKINFO( "Smokeperf::getDataTask() complete." );
  return result;
}

//----------------------------------------------------------------------------
int32_t Smokeperf::putDataTask() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::putDataTask()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue("clientCount");
    std::string label = RegionHelper::regionTag(region->getAttributes());

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    bool encodeKey = true;
    bool encodeTimestamp = false;
    resetValue("distinctKeys");
    int32_t numKeys = getIntValue("distinctKeys");

    resetValue("valueSizes");
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    int32_t valSize = getIntValue("valueSizes");
    int32_t numThreads = getIntValue("numThreads");
    resetValue("ObjectType");
    std::string objectname = getStringValue("ObjectType");
    resetValue("isMainWorkLoad");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    int32_t assetAccountSize = getIntValue("AssetAccountSize");
    if(assetAccountSize < 0)
       assetAccountSize = 0;
    int32_t assetMaxVal = getIntValue("AssetMaxVal");
    if(assetMaxVal < 0)
      assetMaxVal = 0;
    resetValue("BatchSize");
    int32_t batchsize = getIntValue("BatchSize");
    bool checked;
        while (numThreads > 0) { // thread loop

          // And we do the real work now
        if(batchsize > 0){
          PutBatchTask puts(region, numKeys, valSize,
        	              objectname, encodeKey, encodeTimestamp, mainworkLoad,batchsize,valSize);
          checked = checkReady(numClients);
          FWKINFO( "Running timed task." );
          setTrimTime("put");
          if (!clnt->timeInterval(&puts, timedInterval, numThreads, 10 * timedInterval)) {
              clearKeys();
              FWKEXCEPTION( "In putDataTask()  Timed run timed out." );
          }
          setTrimTime("put", true);
        }else {
          PutDataTask puts(region, numKeys, valSize,
                 objectname, encodeKey, encodeTimestamp, mainworkLoad,g_test,assetAccountSize,assetMaxVal);
          checked = checkReady(numClients);
          FWKINFO( "Running timed task." );
          setTrimTime("put");
          if (!clnt->timeInterval(&puts, timedInterval, numThreads, 10 * timedInterval)) {
            clearKeys();
            FWKEXCEPTION( "In putDataTask()  Timed run timed out." );
          }
          setTrimTime("put", true);
        }
          if(clnt->getTaskStatus() == FWK_SEVERE)
                    FWKEXCEPTION( "Exception during put task");
          if (checked) {
            bbDecrement(CLIENTSBB, READYCLIENTS);
          }
          numThreads = getIntValue("numThreads");
          if (numThreads > 0) {
            perf::sleepSeconds(3); // Put a marker of inactivity in the stats
          }

        } // thread loop

    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Smokeperf::putDataTask() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Smokeperf::putDataTask() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::putDataTask() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::putDataTask() Caught unknown exception." );
  }
  FWKINFO( "Smokeperf::putDataTask() complete." );
  return result;
}

int32_t Smokeperf::createOrPutBulkDataTask() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::createOrPutBulkDataTask()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue("clientCount");
    std::string label = RegionHelper::regionTag(region->getAttributes());

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    bool encodeKey = true;
    bool encodeTimestamp = false;
    resetValue("distinctKeys");
    int32_t numKeys = getIntValue("distinctKeys");

    int32_t opsSec = getIntValue( "opsSecond" );
    opsSec = ( opsSec < 1 ) ? 0 : opsSec;
    resetValue("valueSizes");
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    int32_t valSize = getIntValue("valueSizes");
    int32_t numThreads = getIntValue("numThreads");
    resetValue("ObjectType");
    std::string objectname = getStringValue("ObjectType");
    resetValue("isMainWorkLoad");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    int32_t assetAccountSize = getIntValue("AssetAccountSize");
    if(assetAccountSize < 0)
       assetAccountSize = 0;
    int32_t assetMaxVal = getIntValue("AssetMaxVal");
      if(assetMaxVal < 0)
         assetMaxVal = 0;
      bool checked = false;
        while (numThreads > 0) { // thread loop

          // And we do the real work now
        	resetValue("isCreateAll");
        	bool isCreateAll = getBoolValue("isCreateAll");
        	if(!isCreateAll){
        		putAllData *puts = new putAllData(region, numKeys, valSize,
        		          objectname, maps,encodeKey, encodeTimestamp, g_test,mainworkLoad,assetAccountSize,assetMaxVal);
        		checked = checkReady(numClients);
        		FWKINFO( "Running timed task." );
        		setTrimTime("puts");
        		if (!clnt->runIterations(puts, timedInterval, numThreads, 10 * timedInterval)) {
        		  clearKeys();
        		  FWKEXCEPTION( "In createOrPutBulkDataTask()  Timed run timed out." );
        		}
        		setTrimTime("puts", true);
        		delete puts;
        	}else {
        	  createAllData *create = new createAllData(region, numKeys, valSize,
                 objectname, maps,encodeKey, encodeTimestamp, g_test,mainworkLoad,assetAccountSize,assetMaxVal);
               checked = checkReady(numClients);
               FWKINFO( "Running timed task." );
               if (!clnt->runIterations(create, numKeys/numThreads, numThreads, 0)) {
            	   clearKeys();
            	   FWKEXCEPTION( "In createBulkDataTask()  Timed run timed out." );
               }
               setTrimTime("creates", true);
               delete create;
          }
          if(clnt->getTaskStatus() == FWK_SEVERE)
            FWKEXCEPTION( "Exception during put task");
          if (checked) {
            bbDecrement(CLIENTSBB, READYCLIENTS);
          }
          numThreads = getIntValue("numThreads");
          if (numThreads > 0) {
            perf::sleepSeconds(3); // Put a marker of inactivity in the stats
          }

        } // thread loop

    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Smokeperf::createOrPutBulkDataTask() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Smokeperf::createOrPutBulkDataTask() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::createOrPutBulkDataTask() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::createOrPutBulkDataTask() Caught unknown exception." );
  }
  FWKINFO( "Smokeperf::createOrPutBulkDataTask() complete." );
  return result;
}

int32_t Smokeperf::getBulkDataTask() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::getBulkDataTask()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue("clientCount");
    std::string label = RegionHelper::regionTag(region->getAttributes());

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    bool encodeKey = true;
    bool encodeTimestamp = false;
    resetValue("distinctKeys");
    int32_t numKeys = getIntValue("distinctKeys");

    int32_t opsSec = getIntValue( "opsSecond" );
    opsSec = ( opsSec < 1 ) ? 0 : opsSec;
    resetValue("valueSizes");
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    int32_t valSize = getIntValue("valueSizes");
    int32_t numThreads = getIntValue("numThreads");
    resetValue("ObjectType");
    std::string objectname = getStringValue("ObjectType");
    resetValue("isMainWorkLoad");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    int32_t assetAccountSize = getIntValue("AssetAccountSize");
    if(assetAccountSize < 0)
       assetAccountSize = 0;
    int32_t assetMaxVal = getIntValue("AssetMaxVal");
      if(assetMaxVal < 0)
         assetMaxVal = 0;
        while (numThreads > 0) { // thread loop

          // And we do the real work now
        	getAllData *getall = new getAllData(region, numKeys, valSize,
                 objectname, maps,encodeKey, encodeTimestamp, g_test,mainworkLoad,assetAccountSize,assetMaxVal);
          bool checked = checkReady(numClients);
          FWKINFO( "Running timed task." );
          setTrimTime("gets");
          if (!clnt->timeInterval(getall, timedInterval, numThreads, 10 * timedInterval)) {
            clearKeys();
            FWKEXCEPTION( "In getBulkDataTask()  Timed run timed out." );
          }
          setTrimTime("gets", true);
          if(clnt->getTaskStatus() == FWK_SEVERE)
            FWKEXCEPTION( "Exception during put task");
          if (checked) {
            bbDecrement(CLIENTSBB, READYCLIENTS);
          }
          numThreads = getIntValue("numThreads");
          if (numThreads > 0) {
            perf::sleepSeconds(3); // Put a marker of inactivity in the stats
          }
          delete getall;
        } // thread loop

    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Smokeperf::getBulkDataTask() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Smokeperf::getBulkDataTask() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::getBulkDataTask() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::getBulkDataTask() Caught unknown exception." );
  }
  FWKINFO( "Smokeperf::getBulkDataTask() complete." );
  return result;
}

int32_t Smokeperf::scaleupdateDelta() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Smokeperf::scaleupdateDelta()" );

  try {
	bool encodeKey = true;
	bool encodeTimestamp = false;
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();

    int32_t numClients = getIntValue("clientCount");
    std::string label = RegionHelper::regionTag(region->getAttributes());

    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    resetValue("distinctKeys");
    resetValue("ObjectType");
    std::string objectname = getStringValue("ObjectType");
    resetValue("encodeKey");
    resetValue("encodeTimestamp");
    resetValue("numThreads");
    resetValue("AssetAccountSize");
    resetValue("AssetMaxVal");
    resetValue("isMainWorkLoad");
    //int32_t numThreads = getIntValue( "numThreads" );
    encodeKey = getBoolValue("encodeKey");
    encodeTimestamp = getBoolValue("encodeTimestamp");
    bool mainworkLoad = getBoolValue("isMainWorkLoad");
    int32_t assetAccountSize = getIntValue("AssetAccountSize");
    if(assetAccountSize < 0)
      assetAccountSize = 0;
    int32_t assetMaxVal = getIntValue("AssetMaxVal");
      if(assetMaxVal < 0)
        assetMaxVal = 0;
    int32_t numKeys = getIntValue("distinctKeys");
    int32_t valSize = getIntValue("valueSizes");
    resetValue("numThreads");
    int32_t numThreads = getIntValue("numThreads");
        while (numThreads > 0) { // thread loop

          // And we do the real work now
        	UpdateScaleDeltaTask * put = new UpdateScaleDeltaTask(region, numKeys, valSize, objectname, encodeKey,
              encodeTimestamp, mainworkLoad,assetAccountSize,assetMaxVal);
          bool checked = checkReady(numClients);
          FWKINFO( "Running timed task." );
          setTrimTime("updates");
          if (!clnt->timeInterval(put, timedInterval, numThreads, 10
              * timedInterval)) {
            clearKeys();
            FWKEXCEPTION( "In scaleupdateDelta()  Timed run timed out." );
          }
          setTrimTime("updates", true);
          if(clnt->getTaskStatus() == FWK_SEVERE)
            FWKEXCEPTION( "Exception during scaleupdateDelta task");
          if (checked) {
            bbDecrement(CLIENTSBB, READYCLIENTS);
          }

          numThreads = getIntValue("numThreads");
          if (numThreads > 0) {
            perf::sleepSeconds(3); // Put a marker of inactivity in the stats
          }
          delete put;
        } // thread loop




    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "Smokeperf::scaleupdateDelta() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "Smokeperf::scaleupdateDelta() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "Smokeperf::scaleupdateDelta() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "Smokeperf::scaleupdateDelta() Caught unknown exception." );
  }
  perf::sleepSeconds(3); // Put a marker of inactivity in the stats
  FWKINFO( "Smokeperf::scaleupdateDelta() complete." );
  return result;
}

int32_t Smokeperf::getFunctionExecutionData()
{
	int32_t result = FWK_SEVERE;
	FWKINFO("Smokeperf::getFunctionExecutionData called.");
	FWKINFO("Smokeperf::putFunctionExecutionData called.");
		RegionPtr region = getRegionPtr();
		TestClient * clnt = TestClient::getTestClient();
		resetValue("distinctKeys");
		int32_t numKeys = getIntValue("distinctKeys");
		resetValue("isMainWorkLoad");
		bool mainworkLoad = getBoolValue("isMainWorkLoad");
		int32_t timedInterval = getTimeValue("timedInterval");
		if (timedInterval <= 0) {
		  timedInterval = 5;
		}
	try
	{
		GetFEDataTask dooperation(region, numKeys,mainworkLoad,g_test);
	  resetValue("numThreads");
	  int32_t numThreads = getIntValue("numThreads");
	  if ( !clnt->timeInterval( &dooperation, timedInterval, numThreads, 10* timedInterval ) ) {
	    FWKEXCEPTION( "In getFunctionExecutionData()  doOps timed out." );
	  }
	  if(clnt->getTaskStatus() == FWK_SEVERE)
	     FWKEXCEPTION( "Exception in getFunctionExecutionData task");
	  result = FWK_SUCCESS;
	}
	catch (Exception &e)
	{
	  FWKEXCEPTION("Caught exception during Smokeperf::getFunctionExecutionData: " << e.getMessage());
	}
	FWKINFO("Done in Smokeperf::getFunctionExecutionData");
	return result;
}

int32_t Smokeperf::putFunctionExecutionData()
{
	int32_t result = FWK_SEVERE;
	FWKINFO("Smokeperf::putFunctionExecutionData called.");
	RegionPtr region = getRegionPtr();
	    TestClient * clnt = TestClient::getTestClient();

	    int32_t timedInterval = getTimeValue("timedInterval");
	    if (timedInterval <= 0) {
	      timedInterval = 5;
	    }
	    bool encodeKey = true;
	    bool encodeTimestamp = false;
	    resetValue("distinctKeys");
	    int32_t numKeys = getIntValue("distinctKeys");

	    resetValue("valueSizes");
	    resetValue("encodeKey");
	    resetValue("encodeTimestamp");
	    encodeKey = getBoolValue("encodeKey");
	    encodeTimestamp = getBoolValue("encodeTimestamp");
	    int32_t valSize = getIntValue("valueSizes");
	    resetValue("ObjectType");
	    std::string objectname = getStringValue("ObjectType");
	    resetValue("isMainWorkLoad");
	    bool mainworkLoad = getBoolValue("isMainWorkLoad");
	    int32_t assetAccountSize = getIntValue("AssetAccountSize");
	    if(assetAccountSize < 0)
	       assetAccountSize = 0;
	    int32_t assetMaxVal = getIntValue("AssetMaxVal");
	      if(assetMaxVal < 0)
	         assetMaxVal = 0;
	try
	{
		PutFEDataTask dooperation(region, numKeys, valSize,
				objectname, encodeKey, encodeTimestamp, mainworkLoad,g_test,assetAccountSize,assetMaxVal);
	  resetValue("numThreads");
	  int32_t numThreads = getIntValue("numThreads");
	  if ( !clnt->timeInterval( &dooperation, timedInterval, numThreads, 10* timedInterval ) ) {
	    FWKEXCEPTION( "In putFunctionExecutionData()  doOps timed out." );
	  }
	  if(clnt->getTaskStatus() == FWK_SEVERE)
	     FWKEXCEPTION( "Exception in putFunctionExecutionData task");
	  result = FWK_SUCCESS;
	}
	catch (Exception &e)
	{
	  FWKEXCEPTION("Caught exception during Smokeperf::putFunctionExecutionData: " << e.getMessage());
	}
	FWKINFO("Done in Smokeperf::putFunctionExecutionData");
	return result;
}
