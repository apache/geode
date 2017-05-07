/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
 * @file    delta.cpp
 * @since   1.0
 * @version 1.0
 * @see
 *
 */

// ----------------------------------------------------------------------------
#include "DeltaTest.hpp"
#include <ace/Time_Value.h>
#include <time.h>
#include "fwklib/FrameworkTest.cpp"
#include "fwklib/GsRandom.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/RegionHelper.hpp"
#include "fwklib/FwkExport.hpp"
#include "fwklib/PoolHelper.hpp"
#include "security/CredentialGenerator.hpp"
#include <gfcpp/SystemProperties.hpp>
#include "fwklib/PaceMeter.hpp"

namespace fwkdeltatest {
  std::string REGIONSBB("Regions");
  std::string CLIENTSBB("ClientsBb");
  std::string READYCLIENTS("ReadyClients");
  std::string DURABLEBB( "DURABLEBB" );
}
using namespace fwkdeltatest;
using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::deltatest;
using namespace gemfire::testframework::security;

DeltaTest * g_test = NULL;
// ----------------------------------------------------------------------------

TESTTASK initialize(const char * initArgs) {
  int32_t result = FWK_SUCCESS;
  if (g_test == NULL) {
    FWKINFO( "Initializing DeltaTest library." );
    try {
      g_test = new DeltaTest(initArgs);
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
  FWKINFO( "Finalizing DeltaTest library." );
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

void DeltaTest::getClientSecurityParams(PropertiesPtr prop,
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

void DeltaTest::checkTest(const char * taskId,bool ispool) {
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

    //CacheAttributesPtr cAttrs = NULLPTR;
    setCacheLevelEp(ispool);

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
TESTTASK doCreatePool(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreatePool doCreatePool called for task: " << taskId );
  try {
    g_test->checkTest(taskId,true);
    result = g_test->createPools();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreatePool caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doValidateDeltaTest( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doValidateDeltaTest called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->validateDeltaTest();

  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doValidateDeltaTest caught exception: " << ex.getMessage() );
  }
  return result;
}

TESTTASK doEntryOperation(const char * taskId) {
  int32_t result=FWK_SUCCESS;
  FWKINFO("doEntryOperation called for task:" << taskId );
  try {
    g_test->checkTest(taskId);
    result=g_test->doEntryOperation();
  } catch(FwkException ex) {
    result=FWK_SEVERE;
    FWKSEVERE("doEntryOperation caught exception :" << ex.getMessage());
  }
  return result;
}
// ========================================================================
TEST_EXPORT CacheListener * createDeltaValidationCacheListener() {
  return new DeltaClientValidationListener(g_test);
}

DeltaClientValidationListener::DeltaClientValidationListener(const FrameworkTest * test):m_numAfterCreate(0),
m_numAfterUpdate(0),
m_numAfterInvalidate(0),
m_numAfterDestroy(0),
m_test( test )
{
  FWKINFO("DeltaClientValidationListener: created");
}

void DeltaClientValidationListener::afterCreate(const EntryEvent& event) {

  m_numAfterCreate++;
  CacheableKeyPtr key = event.getKey();
  DeltaTestImplPtr value = dynCast<DeltaTestImplPtr> (event.getNewValue());
  if (value == NULLPTR) {
    FWKEXCEPTION("value in afterCreate cannot be null: key = "
        << key->toString()->asChar());
    return;
  }
  if (value->getIntVar() != 0 && value->getFromDeltaCounter() != 0) {
    validateIncrementByOne(key, value);
  }
  CacheableInt32Ptr mapValue = CacheableInt32::create(value->getIntVar());
  CacheableInt32Ptr fromDeltaValue=CacheableInt32::create((int32_t)value->getFromDeltaCounter());
  m_latestValues.insert(key, mapValue);
  m_ValidateMap.insert(key,fromDeltaValue);
}

void DeltaClientValidationListener::afterUpdate(const EntryEvent& event) {
  m_numAfterUpdate++;
  CacheableKeyPtr key = event.getKey();
  DeltaTestImplPtr oldValue = dynCast<DeltaTestImplPtr> (event.getOldValue());
  DeltaTestImplPtr newValue = dynCast<DeltaTestImplPtr> (event.getNewValue());
    if (newValue == NULLPTR) {
      FWKEXCEPTION("newValue in afterUpdate cannot be null: key = " << key->toString()->asChar());
    }
    if (oldValue == NULLPTR) {
      validateIncrementByOne(key, newValue);
    } else {
      HashMapOfCacheable::Iterator item = m_latestValues.find(key);
      int32_t mapValue = dynCast<CacheableInt32Ptr>(item.second())->value();
      long diff = newValue->getIntVar() - mapValue;
      if (diff != 1) {
        FWKEXCEPTION("difference expected in newValue and oldValue is less than 1" << newValue->getIntVar() << mapValue);
        return;
      }
    }
    CacheableInt32Ptr mapValue = CacheableInt32::create(newValue->getIntVar());
    CacheableInt32Ptr fromDeltaValue=CacheableInt32::create((int32_t)newValue->getFromDeltaCounter());
    m_latestValues.update(key, mapValue);
    HashMapOfCacheable::Iterator vitem = m_ValidateMap.find(key);
    int32_t vmapValue = dynCast<CacheableInt32Ptr>(vitem.second())->value();
    if(((event.getRegion()->getAttributes()->getConcurrencyChecksEnabled() == true ) && (newValue->getFromDeltaCounter() == 0)
    		  && ((int32_t)m_test->bbGet("ToDeltaBB",key->toString()->asChar())) > 0) ||( vmapValue >= newValue->getFromDeltaCounter())){
           
        fromDeltaValue=CacheableInt32::create(vmapValue +1);
      }
      m_ValidateMap.update(key,fromDeltaValue);

 }

void DeltaClientValidationListener::afterInvalidate(const EntryEvent& event) {
  m_numAfterInvalidate++;
  CacheableKeyPtr key = event.getKey();
  DeltaTestImplPtr oldValue = dynCast<DeltaTestImplPtr> (event.getOldValue());
  if (oldValue==NULLPTR) {
    FWKEXCEPTION("oldValue in afterInvalidate canot be null : key = " << key->toString()->asChar());
    return;
  }
  CacheableInt32Ptr mapValue=CacheableInt32::create(oldValue->getIntVar());
  m_latestValues.insert(key,mapValue);
}

void DeltaClientValidationListener::afterDestroy(const EntryEvent& event) {
  m_numAfterDestroy++;
  CacheableKeyPtr key = event.getKey();
  DeltaTestImplPtr oldValue = dynCast<DeltaTestImplPtr> (event.getOldValue());
  if(oldValue!=NULLPTR) {
    CacheableInt32Ptr mapValue=CacheableInt32::create(oldValue->getIntVar());
    {
      ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
      m_latestValues.erase(key);
      m_ValidateMap.erase(key);
    }
  }
}

void DeltaClientValidationListener::validateIncrementByOne(CacheableKeyPtr key,DeltaTestImplPtr newValue) {
  HashMapOfCacheable::Iterator item = m_latestValues.find(key);
  int32_t oldValue = dynCast<CacheableInt32Ptr>(item.second())->value();
  if (oldValue == 0) {
    FWKEXCEPTION("oldValue in latestValues cannot be null: key = " << key->toString( )->asChar( )
        << " & newVal = " << newValue->toString()->asChar( ));
    return;
  }
  int32_t diff = newValue->getIntVar() - oldValue;
  if (diff != 1) {
    FWKEXCEPTION(
        "difference expected in newValue and oldValue is 1, but is was "
            << diff << " for key = " << key->toString()->asChar()
            << " & newVal = " << newValue->toString()->asChar());
    return;
  }
}

void DeltaClientValidationListener::dumpToBB(const RegionPtr& regPtr)
{
  char name[32] = {'\0'};
  sprintf(name,"%d",g_test->getClientId());
  std::string key1 = std::string( "AFTER_CREATE_COUNT_") + std::string(name) + std::string("_") + std::string(regPtr->getName());
  std::string key2 = std::string( "AFTER_UPDATE_COUNT_" ) + std::string(name) + std::string("_") + std::string(regPtr->getName());
  std::string key3 = std::string( "AFTER_INVALIDATE_COUNT_" ) + std::string(name) + std::string("_") + std::string(regPtr->getName());
  std::string key4 = std::string( "AFTER_DESTROY_COUNT_" ) + std::string(name) + std::string("_") + std::string(regPtr->getName());
  m_test->bbSet("DeltaBB",key1,m_numAfterCreate);
  m_test->bbSet("DeltaBB",key2,m_numAfterUpdate);
  m_test->bbSet("DeltaBB",key3,m_numAfterInvalidate);
  m_test->bbSet("DeltaBB",key4,m_numAfterDestroy);
}

HashMapOfCacheable DeltaClientValidationListener:: getMap()
{
	return m_ValidateMap;
}

//--------------------------------------------------------------------------
void DeltaTest::clearKeys() {
  if (m_KeysA != NULL) {
    for (int32_t i = 0; i < m_MaxKeys; i++) {
      m_KeysA[i] = NULLPTR;
    }
    delete[] m_KeysA;
    m_KeysA = NULL;
    m_MaxKeys = 0;
  }
}
// ========================================================================

int32_t DeltaTest::initKeys(bool useDefault, bool useAllClientID) {
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
    FWKEXCEPTION("DeltaTest::initKeys:Key is less than 0 for each client. Provide max number of distinctKeys");
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

void DeltaTest::initStrKeys(int32_t low, int32_t high,
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

void DeltaTest::initIntKeys(int32_t low, int32_t high) {
  m_KeysA = new CacheableKeyPtr[m_MaxKeys];
  FWKINFO("m_MaxKeys: " << m_MaxKeys << " low: " << low << " high: " << high);

  for (int32_t i = low; i < high; i++) {
    m_KeysA[i - low] = CacheableKey::create(i);
  }
}
// ----------------------------------------------------------------------------

int32_t DeltaTest::createRegion() {
  int32_t result = FWK_SEVERE;
  try {

    createPool();
    RegionHelper help(g_test);

    if (!m_isObjectRegistered) {
      Serializable::registerType( DeltaTestImpl::create );
      Serializable::registerType( TestObject1::create );
      m_isObjectRegistered = true;
    }
    RegionPtr region = help.createRootRegion(m_cache);

    std::string key(region->getName());
    bbIncrement(REGIONSBB, key);
    FWKINFO( "DeltaTest::createRegion Created region " << region->getName() << std::endl);
    result = FWK_SUCCESS;

  } catch (Exception e) {
    FWKEXCEPTION( "DeltaTest::createRegion FAILED -- caught exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "DeltaTest::createRegion FAILED -- caught test exception: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "DeltaTest::createRegion FAILED -- caught unknown exception." );
  }

  return result;
}

// ----------------------------------------------------------------------------

#ifndef WIN32
#include <unistd.h>
#endif

//-----------------------------------------------------------------------------
int32_t DeltaTest::registerAllKeys() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In DeltaTest::registerAllKeys()" );

  try {
    RegionPtr region = getRegionPtr();
    resetValue("getInitialValues");
    bool isGetInitialValues = getBoolValue("getInitialValues");
    FWKINFO("DeltaTest::registerAllKeys region name is " << region->getName()
        << "; getInitialValues is " << isGetInitialValues);
    region->registerAllKeys(false, NULLPTR, isGetInitialValues);
    result = FWK_SUCCESS;

  } catch (Exception& e) {
    FWKEXCEPTION( "DeltaTest::registerAllKeys() Caught Exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "DeltaTest::registerAllKeys() Caught FwkException: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "DeltaTest::registerAllKeys() Caught unknown exception." );
  }
  FWKINFO( "DeltaTest::registerAllKeys() complete." );
  return result;

}

// ----------------------------------------------------------------------------

RegionPtr DeltaTest::getRegionPtr(const char * reg) {
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
        FWKEXCEPTION( "In DeltaTest::getRegionPtr()  No regions exist." );
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
    FWKEXCEPTION( "In DeltaTest::getRegionPtr()  CacheFactory::getInstance encountered "
        "CacheClosedException: " << e.getMessage() );
  } catch (EntryNotFoundException e) {
    FWKEXCEPTION( "In DeltaTest::getRegionPtr()  CacheFactory::getInstance encountered "
        "EntryNotFoundException: " << e.getMessage() );
  } catch (IllegalArgumentException e) {
    FWKEXCEPTION( "In DeltaTest::getRegionPtr()  CacheFactory::getInstance encountered "
        "IllegalArgumentException: " << e.getMessage() );
  }
  return region;
}

bool DeltaTest::checkReady(int32_t numClients) {
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
int32_t DeltaTest::createPools() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In DeltaTest::createPool()" );
  try {
    PoolHelper help(g_test);
    PoolPtr pool = help.createPool();
    FWKINFO( "DeltaTest::createPool Created Pool " << pool->getName() << std::endl);
    result = FWK_SUCCESS;
  } catch (Exception e) {
    FWKEXCEPTION( "DeltaTest::createPool FAILED -- caught exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "DeltaTest::createPool FAILED -- caught test exception: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "DeltaTest::createPool FAILED -- caught unknown exception. " );
  }
  return result;
}
//----------------------------------------------------------------------------
int32_t DeltaTest::puts() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In DeltaTest::puts()" );

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
    resetValue("distinctKeys");
    resetValue("BatchSize");
    int32_t numKeys = initKeys(false, true);
    while (numKeys > 0) { // keys loop
        // Loop over threads
      resetValue("numThreads");
      int32_t numThreads = getIntValue("numThreads");
      while (numThreads > 0) { // thread loop
        //And we do the  real work now
        PutTask * put = new PutTask(region, numKeys, g_test);

        bool checked = checkReady(numClients);
        FWKINFO( "Running timed task." );
        if (!clnt->timeInterval(put, timedInterval, numThreads, 10
            * timedInterval)) {
          clearKeys();
          FWKEXCEPTION( "In doPuts()  Timed run timed out." );
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
        put->dumpToBB();
        FWKINFO( "Updated " << put->getIters() << " entries." );
        delete put;
      } // thread loop
      numKeys = initKeys(false, true);
      if (numKeys > 0) {
        perf::sleepSeconds(3); // Put a marker of inactivity in the stats
      }
    } // keys loop

    result = FWK_SUCCESS;
  } catch (Exception & e) {
    FWKEXCEPTION( "DeltaTest::puts() Caught Exception: " << e.getMessage() );
  } catch (FwkException & e) {
    FWKEXCEPTION( "DeltaTest::puts() Caught FwkException: " << e.getMessage() );
  } catch (std::exception & e) {
    FWKEXCEPTION( "DeltaTest::puts() Caught std::exception: " << e.what() );
  } catch (...) {
    FWKEXCEPTION( "DeltaTest::puts() Caught unknown exception." );
  }
  clearKeys();
  perf::sleepSeconds(3); // Put a marker of inactivity in the stats
  FWKINFO( "DeltaTest::puts() complete." );
  return result;
}
//----------------------------------------------------------------------------------------------
int32_t DeltaTest::populateRegion() {
  int32_t result = FWK_SEVERE;
  FWKINFO( "In DeltaTest::populateRegion()" );

  try {
    RegionPtr region = getRegionPtr();
    int32_t opsSec = getIntValue( "opsSecond" );
    opsSec = ( opsSec < 1 ) ? 0 : opsSec;
    TestClient * clnt = TestClient::getTestClient();
    resetValue("distinctKeys");
    int32_t numKeys = getIntValue("distinctKeys");
    resetValue("numThreads");
    //int32_t numThreads = getIntValue("numThreads");
    PaceMeter meter( opsSec );
    CreateTask creates(region, numKeys, g_test);
    FWKINFO( "Populating region." );
    if (!clnt->runIterations(&creates, numKeys, 1, 0)) {
      clearKeys();
      FWKEXCEPTION( "In populateRegion()  Population timed out." );
    }
    //maps->insert();
    //bbSet("MAPVAL",)
    creates.dumpToBB();
    meter.checkPace();

    FWKINFO( "Added " << creates.getIters() << " entries." );
    result = FWK_SUCCESS;
  } catch (std::exception e) {
    FWKEXCEPTION( "DeltaTest::populateRegion() Caught std::exception: " << e.what() );
  } catch (Exception e) {
    FWKEXCEPTION( "DeltaTest::populateRegion() Caught Exception: " << e.getMessage() );
  } catch (FwkException& e) {
    FWKEXCEPTION( "DeltaTest::populateRegion() Caught FwkException: " << e.getMessage() );
  } catch (...) {
    FWKEXCEPTION( "DeltaTest::populateRegion() Caught unknown exception." );
  }
  clearKeys();
  FWKINFO( "DeltaTest::populateRegion() complete." );
  return result;
}
//-------------------------------------------------------------------------------------------------
int32_t DeltaTest::doEntryOperation() {
  int32_t fwkResult = FWK_SUCCESS;
  try {
    RegionPtr region = getRegionPtr();

    int32_t opsSec = getIntValue( "opsSecond" );
    opsSec = ( opsSec < 1 ) ? 0 : opsSec;

    TestClient * clnt = TestClient::getTestClient();
    FWKINFO( "doEntryOperations called." );
    resetValue("distinctKeys");
    int32_t numKeys = getIntValue("distinctKeys");
    resetValue("numThreads");
    int32_t numThreads = getIntValue("numThreads");
    int32_t timedInterval = getTimeValue("timedInterval");
    if (timedInterval <= 0) {
      timedInterval = 5;
    }
    int32_t numClients = getIntValue("clientCount");
    std::string opcode;
    if (region == NULLPTR) {
      fwkResult = FWK_SEVERE;
      FWKSEVERE( "DeltaTest::doEntryOperations(): No region to perform operations on." );
    }
    PaceMeter meter( opsSec );
    while (numThreads > 0) { // thread loop
    // And we do the real work now
      //PaceMeter meter( opsSec );
      EntryTask * entrytask = new EntryTask(region, numKeys, g_test);
      bool checked = checkReady(numClients);
      FWKINFO( "Running timed task." );
      if (!clnt->timeInterval(entrytask, timedInterval, numThreads, 10
	                                                * timedInterval)) {
        clearKeys();
        FWKEXCEPTION( "In doEntryOperation()  Timed run timed out." );
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
        entrytask->dumpToBB();
        delete entrytask;
       // meter.checkPace();
      } // thread loop
   meter.checkPace();
   } catch ( TimeoutException &e ) {
       fwkResult = FWK_SEVERE;
       FWKSEVERE( "Caught unexpected timeout exception during entry " << " operation: " << e.getMessage() << " continuing with test." );
   } catch ( Exception &e ) {
       fwkResult = FWK_SEVERE;
       FWKEXCEPTION( "Caught unexpected exception during entry  operation: " << e.getMessage() << " exiting task." );
   }
   return fwkResult;
}

int32_t DeltaTest::validateDeltaTest()
{
  int32_t result = FWK_SEVERE;
  try {
    RegionPtr region = getRegionPtr();
    region->localDestroyRegion(); //to dump the evnt count to BB
    char name[32] = {'\0'};
    resetValue("distinctKeys");
    resetValue("numThreads");
    //int32_t numThreads=g_test->getIntValue("numThreads");
    sprintf(name,"%d",g_test->getClientId());
    std::string rgnName = std::string(region->getName());
    std::string afterCreateKey = std::string( "AFTER_CREATE_COUNT_" ) + std::string(name) + std::string("_") + rgnName;
    std::string afterUpdateKey = std::string( "AFTER_UPDATE_COUNT_" ) + std::string(name) + std::string("_") + rgnName;
    std::string afterInvalidateKey = std::string( "AFTER_INVALIDATE_COUNT_" ) + std::string(name) + std::string("_") + rgnName;
    std::string afterDestroyKey = std::string( "AFTER_DESTROY_COUNT_" ) + std::string(name) + std::string("_") + rgnName;

    std::string bb( "DeltaBB" );

    int32_t eventAfterCreate = ( int32_t )bbGet( bb, afterCreateKey );
    int32_t eventAfterUpdate = ( int32_t )bbGet( bb, afterUpdateKey );
    int32_t eventAfterInvalidate = ( int32_t )bbGet( bb, afterInvalidateKey );
    int32_t eventAfterDestroy = (int32_t) bbGet(bb, afterDestroyKey);
    int32_t expectedAfterCreateEvent = (int32_t) bbGet(bb, "CREATECOUNT");
    int32_t expectedAfterUpdateEvent = (int32_t) bbGet(bb, "UPDATECOUNT");
    int32_t expectedAfterInvalidateEvent = (int32_t) bbGet(bb,"INVALIDATECOUNT");
    int32_t expectedAfterDestroyEvent = (int32_t) bbGet(bb, "DESTROYCOUNT");
    //int32_t toDeltaMapCount = (int32_t) bbGet("MAPCOUNT","size");


    DeltaClientValidationListenerPtr cs= dynCast<DeltaClientValidationListenerPtr>(region->getAttributes()->getCacheListener());
    HashMapOfCacheable map=cs->getMap();
    if(region->getAttributes()->getConcurrencyChecksEnabled() == true){
       for(HashMapOfCacheable::Iterator item=map.begin();item!=map.end();item++) {
          CacheableKeyPtr key=item.first();
          int32_t fromDeltaCount = dynCast<CacheableInt32Ptr>(item.second())->value();
          int32_t toDeltaCount=(int32_t)bbGet("ToDeltaBB",key->toString()->asChar());
          FWKINFO("toDeltaCount = " << toDeltaCount << " fromDeltaCount = " << fromDeltaCount << " for key " <<  key->toString()->asChar());
          if(toDeltaCount != fromDeltaCount) {
            FWKEXCEPTION("key" << key->toString()->asChar() << " has " <<toDeltaCount - fromDeltaCount << " mis match toDeltaCounter and fromDeltaCounter");
          }
        }
        result = FWK_SUCCESS;
    }
    else {
    	if(expectedAfterCreateEvent == eventAfterCreate && expectedAfterUpdateEvent == eventAfterUpdate
    	        && expectedAfterInvalidateEvent == eventAfterInvalidate && expectedAfterDestroyEvent == eventAfterDestroy){
          for(HashMapOfCacheable::Iterator item=map.begin();item!=map.end();item++) {
            CacheableKeyPtr key=item.first();
            int32_t fromDeltaCount = dynCast<CacheableInt32Ptr>(item.second())->value();
            int32_t toDeltaCount=(int32_t)bbGet("ToDeltaBB",key->toString()->asChar());
            FWKINFO("toDeltaCount = " << toDeltaCount << " fromDeltaCount = " << fromDeltaCount << " for key " <<  key->toString()->asChar());
            if(toDeltaCount != fromDeltaCount) {
             FWKEXCEPTION("key" << key->toString()->asChar() <<" has " << toDeltaCount - fromDeltaCount << " mis match toDeltaCounter and fromDeltaCounter");
            }
          }
    	    result = FWK_SUCCESS;
    	}
    	else {
    	      FWKINFO("afterCreate = " << eventAfterCreate << "expectedAfterCreate = " << expectedAfterCreateEvent << " afterUpdate = " << eventAfterUpdate << "expectedAfterUpdate = " << expectedAfterUpdateEvent << "afterInvalidate = " << eventAfterInvalidate << "expectedAfterInvalidate= " << expectedAfterInvalidateEvent << "afterDestroy= " << eventAfterDestroy << "expectedAfterDestroy = " << expectedAfterDestroyEvent);
    	      FWKEXCEPTION("Validation Failed: expected count is not equal to DeltaBB value");
    	    }
    }
  } catch ( Exception e ) {
    FWKEXCEPTION("DeltaTest::validateDeltaTest Caught Exception: " << e.getMessage());
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "DeltaTest::validateDeltaTest Caught FwkException: " << e.getMessage() );
  }
  return result;
}

uint32_t EntryTask::doTask(int32_t id) {
  char buf[128];
  uint32_t count = m_MyOffset->value();
  int32_t localcnt=0;
  uint32_t loop=m_Loop;
  uint32_t idx;
  while (m_Run && loop--) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
    idx = count % m_MaxKeys;
    sprintf(buf, "%s%d%010d", "AAAAAA", localcnt, idx);
    CacheableKeyPtr key = CacheableKey::create(buf);
    std::string opcode = m_test->getStringValue("entryOps");
    if(opcode.empty()) opcode = "no-op";
      if(opcode == "put") {
        DeltaTestImplPtr newVal = NULLPTR;
        if(m_Region->containsKey(key)){
          DeltaTestImplPtr oldVal = dynCast<DeltaTestImplPtr> (m_Region->get(key));
          if (oldVal == NULLPTR) {
        	  DeltaTestImplPtr newVal(new DeltaTestImpl(1, CacheableString::create(buf)));
            m_Region->put(key, newVal);
          }
    	  else {
    		newVal = new  DeltaTestImpl(oldVal);
    	    newVal->setIntVar(oldVal->getIntVar() + 1);
    	    m_Region->put(key, newVal);
    	  }
          m_update++;
     	  m_test->bbSet("ToDeltaBB",key->toString()->asChar(),newVal->getToDeltaCounter());
      	}
    	else {
    		DeltaTestImplPtr newVal(new DeltaTestImpl(1, CacheableString::create(buf)));
    	  m_Region->create(key, newVal);
    	  m_create++;
    	}
      }
      else if(opcode == "destroy") {
        DeltaTestImplPtr oldVal = NULLPTR;
        if(m_Region->containsKey(key)){
          if((oldVal = dynCast<DeltaTestImplPtr>(m_Region->get(key)))== NULLPTR) {
            if(m_isDestroy){
              m_Region->destroy(key);
            }
    	  }
    	  else {
    	     m_Region->destroy(key);
    	  }
    	  m_destroy++;
    	}
     }
     else if(opcode == "invalidate") {
       DeltaTestImplPtr oldVal;
       if(m_Region->containsKey(key)){
         if((oldVal = dynCast<DeltaTestImplPtr> (m_Region->get(key)))!= NULLPTR) {
            m_Region->invalidate(key);
        	m_invalidate++;
   	      }
   	    }
     }
      count++;
   }
   return (count - m_MyOffset->value());
  }


