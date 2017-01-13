/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    Security.cpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#include "Security.hpp"
//#include "PerfTasks.hpp"
//#include "SockIpc.hpp"

#include <ace/Time_Value.h>
#include "fwklib/GsRandom.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/QueryHelper.hpp"
#include "fwklib/RegionHelper.hpp"
#include "fwklib/PaceMeter.hpp"

#include "fwklib/FwkExport.hpp"

#include "security/CredentialGenerator.hpp"

namespace FwkSecurity {
std::string REGIONSBB("Regions");
std::string CLIENTSBB("ClientsBb");
std::string READYCLIENTS("ReadyClients");
}

using namespace FwkSecurity;
using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::security;

Security *g_test = NULL;

// ----------------------------------------------------------------------------

TESTTASK initialize(const char *initArgs) {
  int32_t result = FWK_SUCCESS;
  if (g_test == NULL) {
    FWKINFO("Initializing Security library.");
    try {
      g_test = new Security(initArgs);
    } catch (const FwkException &ex) {
      FWKSEVERE("initialize: caught exception: " << ex.getMessage());
      result = FWK_SEVERE;
    }
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK finalize() {
  int32_t result = FWK_SUCCESS;
  FWKINFO("Finalizing Security library.");
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
  FWKINFO("Closing cache, disconnecting from distributed system.");
  if (g_test != NULL) {
    g_test->cacheFinalize();
  }
  return result;
}

void Security::getClientSecurityParams(PropertiesPtr prop,
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

  if (prop == NULLPTR) prop = Properties::create();

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
    while (!securityParams.empty() &&
           (std::count(opCodes.begin(), opCodes.end(),
                       strToOpCode(securityParams), n),
            n == 0)) {
#else
    while (!securityParams.empty() &&
           std::count(opCodes.begin(), opCodes.end(),
                      strToOpCode(securityParams)) == 0) {
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

void Security::checkTest(const char *taskId) {
  SpinLockGuard guard(m_lck);
  setTask(taskId);
  if (m_cache == NULLPTR || m_cache->isClosed()) {
    PropertiesPtr pp = Properties::create();

    getClientSecurityParams(pp, getStringValue("credentials"));

    int32_t heapLruLimit = getIntValue("heapLruLimit");
    if (heapLruLimit > 0) pp->insert("heap-lru-limit", heapLruLimit);

    cacheInitialize(pp);
    // Security specific initialization
    // none
  }
}

// ----------------------------------------------------------------------------

TESTTASK doCreateRegion(const char *taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO("doCreateRegion called for task: " << taskId);
  try {
    g_test->checkTest(taskId);
    result = g_test->createRegion();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE("doCreateRegion caught exception: " << ex.getMessage());
  }

  return result;
}

TESTTASK doCheckValues(const char *taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO("doCheckValues called for task: " << taskId);
  try {
    g_test->checkTest(taskId);
    result = g_test->checkValues();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE("doCheckValues caught exception: " << ex.getMessage());
  }

  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doEntryOperations(const char *taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO("doEntryOperations called for task: " << taskId);

  try {
    g_test->checkTest(taskId);
    result = g_test->doEntryOperations();
  } catch (const FwkException &ex) {
    result = FWK_SEVERE;
    FWKSEVERE("doEntryOperations caught exception: " << ex.getMessage());
  }
  return result;
}
/*
TESTTASK doPopulateRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPopulateRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->populateRegion();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPopulateRegion caught exception: " << ex.getMessage() );
  }

  return result;
}
*/

TESTTASK doRegisterInterestList(const char *taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO("doRegisterInterestList called for task: " << taskId);
  try {
    g_test->checkTest(taskId);
    result = g_test->registerInterestList();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE("doRegisterInterestList caught exception: " << ex.getMessage());
  }

  return result;
}

TESTTASK doRegisterRegexList(const char *taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO("doRegisterRegexList called for task: " << taskId);
  try {
    g_test->checkTest(taskId);
    result = g_test->registerRegexList();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE("doRegisterRegexList caught exception: " << ex.getMessage());
  }

  return result;
}
TESTTASK doUnRegisterRegexList(const char *taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO("doRegisterRegexList called for task: " << taskId);
  try {
    g_test->checkTest(taskId);
    result = g_test->unregisterRegexList();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE("doRegisterRegexList caught exception: " << ex.getMessage());
  }

  return result;
}
TESTTASK doRegisterAllKeys(const char *taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO("doRegisterAllKeys called for task: " << taskId);
  try {
    g_test->checkTest(taskId);
    result = g_test->registerAllKeys();
  } catch (FwkException ex) {
    result = FWK_SEVERE;
    FWKSEVERE("doRegisterAllKeys caught exception: " << ex.getMessage());
  }

  return result;
}
/*

TESTTASK doVerifyInterestList( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerifyInterestList called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->verifyInterestList();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerifyInterestList caught exception: " << ex.getMessage() );
  }

  return result;
}
*/
/*
TESTTASK doServerKeys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doServerKeys called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->doServerKeys();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doServerKeys caught exception: " << ex.getMessage() );
  }

  return result;
}

int32_t Security::doServerKeys()
{
  int32_t fwkResult = FWK_SUCCESS;

  FWKINFO( "doServerKeys called." );

  int32_t expected = getIntValue( "expectedCount" );

  RegionPtr regionPtr = getRegionPtr();
  if ( regionPtr == NULL ) {
    FWKEXCEPTION( "Security::doServerKeys(): No region to perform operations
on." );
  }

  VectorOfCacheableKey keysVec;
  try {
    regionPtr->serverKeys( keysVec );
  } catch( Exception & e ) {
    FWKEXCEPTION( "Exception thrown by serverKeys(): " << e.getMessage() );
  }

  int32_t keys = keysVec.size();

  if ( keys != expected ) {
    FWKEXCEPTION( "doServerKeys expected " << expected << " keys on server, but
server reports " << keys << " keys." );
  }

  return fwkResult;
}

*/
// ========================================================================

void Security::clearKeys() {
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

int32_t Security::initKeys(bool useDefault) {
  int32_t low = getIntValue("keyIndexBegin");
  low = (low > 0) ? low : 0;
  int32_t numKeys = getIntValue("distinctKeys");  // check distince keys first
  if (numKeys <= 0) {
    if (useDefault) {
      numKeys = 5000;
    } else {
      return numKeys;
    }
  }
  int32_t high = numKeys + low;
  FWKINFO("numKeys: " << numKeys << " low: " << low);
  if ((numKeys == m_MaxKeys) && (m_KeyIndexBegin == low)) {
    return numKeys;
  }

  clearKeys();
  m_MaxKeys = numKeys;
  m_KeyIndexBegin = low;
  int32_t keySize = getIntValue("keySize");
  keySize = (keySize > 0) ? keySize : 10;
  std::string keyBase(keySize, 'A');
  initStrKeys(low, high, keyBase);

  for (int j = 0; j < m_MaxKeys; j++) {
    std::swap(m_KeysA[GsRandom::random(numKeys)], m_KeysA[numKeys - 1]);
    numKeys--;
  }

  return m_MaxKeys;
}

// ========================================================================

void Security::initStrKeys(int32_t low, int32_t high,
                           const std::string &keyBase) {
  m_KeysA = new CacheableStringPtr[m_MaxKeys];
  const char *const base = keyBase.c_str();

  char buf[128];
  for (int32_t i = low; i < high; i++) {
    sprintf(buf, "%s%010d", base, i);
    m_KeysA[i - low] = CacheableString::create(buf);
  }
}

// ========================================================================

int32_t Security::initValues(int32_t numKeys, int32_t siz, bool useDefault) {
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

  if (numKeys <= 0) {
    numKeys = 5000;
  }

  if (m_CValue != NULL) {
    for (int32_t i = 0; i < m_MaxValues; i++) {
      m_CValue[i] = NULLPTR;
    }
    delete[] m_CValue;
  }

  m_MaxValues = numKeys;

  m_CValue = new CacheableBytesPtr[m_MaxValues];

  char *buf = new char[siz];
  memset(buf, 'V', siz);
  int32_t rsiz = (siz <= 20) ? siz : 20;
  for (int32_t i = 0; i < m_MaxValues; i++) {
    GsRandom::getAlphanumericString(rsiz, buf);
    m_CValue[i] = CacheableBytes::create(
        reinterpret_cast<const unsigned char *>(buf), siz);
  }
  return siz;
}

// ----------------------------------------------------------------------------

int32_t Security::createRegion() {
  int32_t result = FWK_SEVERE;
  FWKINFO("In Security::createRegion()");

  try {
    createPool();
    RegionHelper help(g_test);
    RegionPtr region = help.createRootRegion(m_cache);

    std::string key(region->getName());
    bbIncrement(REGIONSBB, key);
    FWKINFO("Security::createRegion Created region " << region->getName()
                                                     << std::endl);
    result = FWK_SUCCESS;

  } catch (Exception e) {
    FWKSEVERE("Security::createRegion FAILED -- caught exception: "
              << e.getMessage());
  } catch (FwkException &e) {
    FWKSEVERE("Security::createRegion FAILED -- caught test exception: "
              << e.getMessage());
  } catch (...) {
    FWKSEVERE("Security::createRegion FAILED -- caught unknown exception.");
  }

  return result;
}

// ----------------------------------------------------------------------------
/*
int32_t Security::verifyInterestList()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Security::verifyInterestList()" );

  try {
    int32_t countUpdate = 0;
    RegionPtr region = getRegionPtr();
    int32_t numOfRegisterKeys = getIntValue( "registerKeys");
    int32_t payload = getIntValue( "valueSizes");

    VectorOfCacheableKey keys;
    region->keys(keys);
    CacheableBytesPtr valuePtr;
    CacheableKeyPtr keyPtr;
    uint32_t valueSize;
    for(int32_t i = 0; i < (int32_t) keys.size(); i++)
    {
      keyPtr = keys.at(i);
      valuePtr = dynCast<CacheableBytesPtr>( region->get(keyPtr) );
      valueSize = valuePtr->length();

      if( (int32_t)valueSize == payload )
      {
        ++countUpdate;
      }
    }
    if( countUpdate == numOfRegisterKeys){
      FWKINFO( "Security::verifyInterestList update interest list count " <<
countUpdate <<
                 " is equal to number of register keys " << numOfRegisterKeys);
      result = FWK_SUCCESS;
    }
    else{
      FWKSEVERE( "Security::verifyInterestList update interest list count " <<
countUpdate << " is not equal to number of register keys " <<
numOfRegisterKeys);
      return result;
    }

  } catch ( Exception e ) {
    FWKSEVERE( "Security::verifyInterestList Caught Exception: " <<
e.getMessage() );
  } catch ( FwkException& e ) {
    FWKSEVERE( "Security::verifyInterestList Caught FwkException: " <<
e.getMessage() );
  } catch ( ... ) {
    FWKSEVERE( "Security::verifyInterestList Caught unknown exception." );
  }
  FWKINFO( "Security::verifyInterestList complete." );
  return result;
}
//-----------------------------------------------------------------------------------------
/ *
int32_t Security::populateRegion()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In Security::populateRegion()" );

  try {
    RegionPtr region = getRegionPtr();
    TestClient * clnt = TestClient::getTestClient();
    resetValue( "distinctKeys" );
    initValues( initKeys());

    PutsTask puts( region, m_KeysA, m_MaxKeys, m_CValue );
    FWKINFO( "Populating region." );
    if ( !clnt->runIterations( &puts, m_MaxKeys, 1, 0 ) ) {
      FWKEXCEPTION( "In populateRegion()  Population timed out." );
    }
    FWKINFO( "Added " << puts.getIters() << " entries." );
    result = FWK_SUCCESS;
  } catch ( std::exception e ) {
    FWKSEVERE( "Security::populateRegion() Caught std::exception: " << e.what()
);
  } catch ( Exception e ) {
    FWKSEVERE( "Security::populateRegion() Caught Exception: " << e.getMessage()
);
  } catch ( FwkException& e ) {
    FWKSEVERE( "Security::populateRegion() Caught FwkException: " <<
e.getMessage() );
  } catch ( ... ) {
    FWKSEVERE( "Security::populateRegion() Caught unknown exception." );
  }
  FWKINFO( "Security::populateRegion() complete." );
  return result;
}
*/
//-----------------------------------------------------------------------------
int32_t Security::registerInterestList() {
  int32_t result = FWK_SEVERE;
  FWKINFO("In Security::registerInterestList()");

  try {
    RegionPtr region = getRegionPtr();
    int32_t numKeys = getIntValue("distinctKeys");  // check distince keys first
    if (numKeys <= 0) {
      // FWKSEVERE( "Failed to initialize keys with numKeys :" <<  numKeys);
      return result;
    }
    int32_t low = getIntValue("keyIndexBegin");
    low = (low > 0) ? low : 0;
    int32_t numOfRegisterKeys = getIntValue("registerKeys");
    int32_t high = numOfRegisterKeys + low;

    clearKeys();
    m_MaxKeys = numOfRegisterKeys;
    m_KeyIndexBegin = low;
    VectorOfCacheableKey registerKeyList;
    int32_t keySize = getIntValue("keySize");
    keySize = (keySize > 0) ? keySize : 10;
    std::string keyBase(keySize, 'A');
    initStrKeys(low, high, keyBase);

    for (int j = low; j < high; j++) {
      if (m_KeysA[j - low] != NULLPTR) {
        registerKeyList.push_back(m_KeysA[j - low]);
      } else
        FWKINFO("Security::registerInterestList key is NULL");
    }
    FWKINFO("Security::registerInterestList region name is "
            << region->getName());
    region->registerKeys(registerKeyList);
    result = FWK_SUCCESS;

  } catch (Exception &e) {
    FWKEXCEPTION("Security::registerInterestList() Caught Exception: "
                 << e.getMessage());
  } catch (FwkException &e) {
    FWKEXCEPTION("Security::registerInterestList() Caught FwkException: "
                 << e.getMessage());
  } catch (...) {
    FWKEXCEPTION("Security::registerInterestList() Caught unknown exception.");
  }
  FWKINFO("Security::registerInterestList() complete.");
  return result;
}

// ----------------------------------------------------------------------------

int32_t Security::registerRegexList() {
  int32_t result = FWK_SEVERE;
  FWKINFO("In Security::registerRegexList()");
  try {
    RegionPtr region = getRegionPtr();
    std::string registerRegex = getStringValue("registerRegex");
    FWKINFO("Security::registerRegexList region name is "
            << region->getName() << "regex is: " << registerRegex.c_str());
    region->registerRegex(registerRegex.c_str());
    result = FWK_SUCCESS;

  } catch (Exception &e) {
    FWKSEVERE(
        "Security::registerRegexList() Caught Exception: " << e.getMessage());
  } catch (FwkException &e) {
    FWKSEVERE("Security::registerRegexList() Caught FwkException: "
              << e.getMessage());
  } catch (...) {
    FWKSEVERE("Security::registerRegexList() Caught unknown exception.");
  }
  FWKINFO("Security::registerRegexList() complete.");
  return result;
}
// ----------------------------------------------------------------------------

int32_t Security::unregisterRegexList() {
  int32_t result = FWK_SEVERE;
  FWKINFO("In Security::unregisterRegexList()");
  try {
    RegionPtr region = getRegionPtr();
    std::string unregisterRegex = getStringValue("unregisterRegex");
    FWKINFO("Security::unregisterRegexList region name is "
            << region->getName() << "regex is: " << unregisterRegex.c_str());
    region->unregisterRegex(unregisterRegex.c_str());
    result = FWK_SUCCESS;

  } catch (Exception &e) {
    FWKSEVERE(
        "Security::unregisterRegexList() Caught Exception: " << e.getMessage());
  } catch (FwkException &e) {
    FWKSEVERE("Security::unregisterRegexList() Caught FwkException: "
              << e.getMessage());
  } catch (...) {
    FWKSEVERE("Security::unregisterRegexList() Caught unknown exception.");
  }
  FWKINFO("Security::unregisterRegexList() complete.");
  return result;
}

//-----------------------------------------------------------------------------
int32_t Security::registerAllKeys() {
  int32_t result = FWK_SEVERE;
  FWKINFO("In Security::registerAllKeys()");

  try {
    RegionPtr region = getRegionPtr();
    FWKINFO("Security::registerAllKeys region name is " << region->getName());
    region->registerAllKeys();
    result = FWK_SUCCESS;

  } catch (Exception &e) {
    FWKSEVERE(
        "Security::registerAllKeys() Caught Exception: " << e.getMessage());
  } catch (FwkException &e) {
    FWKSEVERE(
        "Security::registerAllKeys() Caught FwkException: " << e.getMessage());
  } catch (...) {
    FWKSEVERE("Security::registerAllKeys() Caught unknown exception.");
  }
  FWKINFO("Security::registerAllKeys() complete.");
  return result;
}

//-----------------------------------------------------------------------------
int32_t Security::checkValues() {
  int32_t result = FWK_SEVERE;
  FWKINFO("In Security::checkValues()");
  try {
    RegionPtr region = getRegionPtr();

    VectorOfCacheable vals;
    region->values(vals);
    int32_t creates = 0;
    int32_t updates = 0;
    int32_t unknowns = 0;
    for (int32_t i = 0; i < vals.size(); i++) {
      CacheableBytesPtr valStr = dynCast<CacheableBytesPtr>(vals.at(i));
      if (strncmp("Create", reinterpret_cast<const char *>(valStr->value()),
                  6) == 0) {
        creates++;
      } else if (strncmp("Update",
                         reinterpret_cast<const char *>(valStr->value()),
                         6) == 0) {
        updates++;
      } else {
        unknowns++;
      }
    }
    FWKINFO("Security::checkValues Found "
            << creates << " values from creates, " << updates
            << " values from updates, and " << unknowns << " unknown values.");
    result = FWK_SUCCESS;
  } catch (Exception e) {
    FWKSEVERE(
        "Security::checkValues FAILED -- caught exception: " << e.getMessage());
  } catch (FwkException &e) {
    FWKSEVERE("Security::checkValues FAILED -- caught test exception: "
              << e.getMessage());
  } catch (...) {
    FWKSEVERE("Security::checkValues FAILED -- caught unknown exception.");
  }

  return result;
}

// ----------------------------------------------------------------------------

RegionPtr Security::getRegionPtr(const char *reg) {
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
    if (name.empty()) {  // just get a random root region
      VectorOfRegion rootRegionVector;
      m_cache->rootRegions(rootRegionVector);
      int32_t size = rootRegionVector.size();

      if (size == 0) {
        FWKEXCEPTION("In Security::getRegionPtr()  No regions exist.");
      }

      FWKINFO("Getting a random root region.");
      region = rootRegionVector.at(GsRandom::random(size));
    } else {
      FWKINFO("Getting region: " << name);
      if (m_cache == NULLPTR) {
        FWKEXCEPTION("Failed to get region: " << name
                                              << "  cache ptr is null.");
      }
      region = m_cache->getRegion(name.c_str());
      if (region == NULLPTR) {
        FWKEXCEPTION("Failed to get region: " << name);
      }
    }
  } catch (CacheClosedException e) {
    FWKEXCEPTION(
        "In Security::getRegionPtr()  CacheFactory::getInstance encountered "
        "CacheClosedException: "
        << e.getMessage());
  } catch (EntryNotFoundException e) {
    FWKEXCEPTION(
        "In Security::getRegionPtr()  CacheFactory::getInstance encountered "
        "EntryNotFoundException: "
        << e.getMessage());
  } catch (IllegalArgumentException e) {
    FWKEXCEPTION(
        "In Security::getRegionPtr()  CacheFactory::getInstance encountered "
        "IllegalArgumentException: "
        << e.getMessage());
  }
  return region;
}

bool Security::checkReady(int32_t numClients) {
  if (numClients > 0) {
    FWKINFO("Check whether all clients are ready to run");
    bbIncrement(CLIENTSBB, READYCLIENTS);
    int64_t readyClients = 0;
    while (readyClients < numClients) {
      readyClients = bbGet(CLIENTSBB, READYCLIENTS);
      perf::sleepMillis(3);
    }
    FWKINFO("All Clients are ready to go !!");
    return true;
  }
  FWKINFO("All Clients are ready to go !!");
  return false;
}
//----------------------------------------------------------------------------
CacheablePtr Security::getUserObject(const std::string &objType) {
  CacheablePtr usrObj = NULLPTR;
  resetValue("entryCount");
  int numOfKeys =
      getIntValue("entryCount");  // number of key should be multiple of 20
  resetValue("valueSizes");
  int objSize = getIntValue("valueSizes");
  QueryHelper *qh = &QueryHelper::getHelper();
  int numSet = 0;
  int setSize = 0;
  if (objType == "Portfolio") {
    setSize = qh->getPortfolioSetSize();
    numSet = numOfKeys / setSize;
    usrObj = new Portfolio(GsRandom::random(setSize), objSize);
  } else if (objType == "Position") {
    setSize = qh->getPositionSetSize();
    numSet = numOfKeys / setSize;
    int numSecIds = sizeof(secIds) / sizeof(char *);
    usrObj = new Position(secIds[setSize % numSecIds], setSize * 100);
  }
  return usrObj;
}
// ----------------------------------------------------------------------------
CacheableStringPtr Security::getKey(int32_t max) {
  CacheableStringPtr keyPtr;
  char buf[32];
  resetValue("objectType");
  const std::string objectType = getStringValue("objectType");
  QueryHelper *qh = &QueryHelper::getHelper();
  int32_t numSet = 0;
  int32_t setSize = 0;
  initValues(initKeys());

  if (objectType == "Portfolio") {
    setSize = qh->getPortfolioSetSize();
    numSet = max / setSize;
    sprintf(buf, "port%d-%d", GsRandom::random(numSet),
            GsRandom::random(setSize));
    keyPtr = CacheableString::create(buf);
  } else if (objectType == "Position") {
    setSize = qh->getPositionSetSize();
    numSet = max / setSize;
    sprintf(buf, "pos%d-%d", GsRandom::random(numSet),
            GsRandom::random(setSize));
    keyPtr = CacheableString::create(buf);
  } else {
    keyPtr = m_KeysA[GsRandom::random(m_MaxKeys)];
  }
  return keyPtr;
}
// ----------------------------------------------------------------------------
bool allowQuery(queryCategory category, bool haveLargeResultset,
                bool islargeSetQuery, bool isUnsupportedPRQuery) {
  if (category == unsupported) {
    return false;
  } else if (haveLargeResultset != islargeSetQuery) {
    return false;
  } else if (isUnsupportedPRQuery &&
             ((category == multiRegion) || (category == nestedQueries))) {
    return false;
  } else {
    return true;
  }
}
// ----------------------------------------------------------------------------
void Security::runQuery(int32_t &queryCnt) {
  try {
    resetValue("entryCount");
    int numOfKeys = getIntValue("entryCount");
    QueryHelper *qh = &QueryHelper::getHelper();
    int setSize = qh->getPortfolioSetSize();
    if (numOfKeys < setSize) {
      setSize = numOfKeys;
    }
    int32_t i = GsRandom::random(static_cast<uint32_t>(0),
                                 static_cast<uint32_t>(QueryStrings::RSsize()));

    ACE_Time_Value startTime, endTime;
    QueryServicePtr qs = m_cache->getQueryService();
    QueryPtr qry;
    SelectResultsPtr results;
    resetValue("largeSetQuery");
    resetValue("unsupportedPRQuery");
    bool islargeSetQuery = g_test->getBoolValue("largeSetQuery");
    bool isUnsupportedPRQuery = g_test->getBoolValue("unsupportedPRQuery");
    if (allowQuery(resultsetQueries[i].category,
                   resultsetQueries[i].haveLargeResultset, islargeSetQuery,
                   isUnsupportedPRQuery)) {
      FWKINFO(" running resultsetQueries["
              << i << "] query : " << resultsetQueries[i].query());
      qry = qs->newQuery(resultsetQueries[i].query());
      startTime = ACE_OS::gettimeofday();
      results = qry->execute(600);
      endTime = ACE_OS::gettimeofday() - startTime;
      FWKINFO(" Time Taken to execute the reselt set query : "
              << resultsetQueries[i].query() << ": is " << endTime.sec() << "."
              << endTime.usec() << " sec");
      queryCnt++;
    }
    i = GsRandom::random(static_cast<uint32_t>(0),
                         static_cast<uint32_t>(QueryStrings::SSsize()));
    if (allowQuery(structsetQueries[i].category,
                   structsetQueries[i].haveLargeResultset, islargeSetQuery,
                   isUnsupportedPRQuery)) {
      FWKINFO(" running structsetQueries["
              << i << "] query : " << structsetQueries[i].query());
      qry = qs->newQuery(structsetQueries[i].query());
      startTime = ACE_OS::gettimeofday();
      results = qry->execute(600);
      endTime = ACE_OS::gettimeofday() - startTime;
      FWKINFO(" Time Taken to execute the struct set query : "
              << structsetQueries[i].query() << ": is " << endTime.sec() << "."
              << endTime.usec() << " sec");
      queryCnt++;
    }
  } catch (Exception e) {
    FWKEXCEPTION(
        "CacheServerTest::runQuery Caught Exception: " << e.getMessage());
  } catch (FwkException &e) {
    FWKEXCEPTION(
        "CacheServerTest::runQuery Caught FwkException: " << e.getMessage());
  } catch (...) {
    FWKEXCEPTION("CacheServerTest::runQuery Caught unknown exception.");
  }
}
//----------------------------------------------------------------------------
int32_t Security::doEntryOperations() {
  int32_t fwkResult = FWK_SUCCESS;

  FWKINFO("doEntryOperations called.");

  int32_t opsSec = getIntValue("opsSecond");
  opsSec = (opsSec < 1) ? 0 : opsSec;

  int32_t entryCount = getIntValue("entryCount");
  entryCount = (entryCount < 1) ? 10000 : entryCount;

  int32_t secondsToRun = getTimeValue("workTime");
  secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;

  int32_t valSize = getIntValue("valueSizes");
  valSize = ((valSize < 0) ? 32 : valSize);

  ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value(secondsToRun);
  ACE_Time_Value now;

  CacheableStringPtr keyPtr;
  CacheablePtr valuePtr;
  CacheablePtr tmpValue;
  char *valBuf = new char[valSize + 1];
  memset(valBuf, 'A', valSize);
  valBuf[valSize] = 0;

  std::string opcode;
  std::string objectType;

  int32_t creates = 0, puts = 0, gets = 0, dests = 0, invals = 0, query = 0;
  RegionPtr regionPtr = getRegionPtr();
  if (regionPtr == NULLPTR) {
    fwkResult = FWK_SEVERE;
    FWKSEVERE(
        "CacheServerTest::doEntryOperations(): No region to perform operations "
        "on.");
    now = end;  // Do not do the loop
  }

  FWKINFO("doEntryOperations will work for " << secondsToRun << " using "
                                             << valSize << " byte values.");

  PaceMeter meter(opsSec);
  objectType = getStringValue("objectType");
  while (now < end) {
    try {
      opcode = getStringValue("entryOps");
      if (opcode.empty()) {
        opcode = "no-op";
      }
      if (opcode == "add") {
        keyPtr = getKey(entryCount);
        if (!objectType.empty()) {
          tmpValue = getUserObject(objectType);
        } else {
          tmpValue = CacheableBytes::create(
              reinterpret_cast<const unsigned char *>(valBuf),
              static_cast<int32_t>(strlen(valBuf)));
        }
        regionPtr->create(keyPtr, tmpValue);
        creates++;
      } else {
        keyPtr = getKey(entryCount);
        if (opcode == "update") {
          if (!objectType.empty()) {
            tmpValue = getUserObject(objectType);
          } else {
            int32_t keyVal = atoi(keyPtr->toString());
            tmpValue = CacheableBytes::create(
                reinterpret_cast<const unsigned char *>(valBuf),
                static_cast<int32_t>(strlen(valBuf)));
            int32_t *val =
                (int32_t *)(dynCast<CacheableBytesPtr>(tmpValue)->value());
            *val = (*val == keyVal) ? keyVal + 1
                                    : keyVal;  // alternate the value so that it
                                               // can be validated later.
            int64_t *adjNow =
                (int64_t *)(dynCast<CacheableBytesPtr>(tmpValue)->value() + 4);
            *adjNow = getAdjustedNowMicros();
          }
          regionPtr->put(keyPtr, tmpValue);
          puts++;
        } else if (opcode == "invalidate") {
          regionPtr->invalidate(keyPtr);
          invals++;
        } else if (opcode == "destroy") {
          regionPtr->destroy(keyPtr);
          dests++;
        } else if (opcode == "read") {
          valuePtr = regionPtr->get(keyPtr);
          gets++;
        } else if (opcode == "read+localdestroy") {
          valuePtr = regionPtr->get(keyPtr);
          gets++;
          regionPtr->localDestroy(keyPtr);
          dests++;
        } else if (opcode == "query") {
          runQuery(query);
        } else {
          FWKSEVERE("Invalid operation specified: " << opcode);
        }
      }
    } catch (TimeoutException &e) {
      fwkResult = FWK_SEVERE;
      FWKSEVERE("Caught unexpected timeout exception during entry "
                << opcode << " operation: " << e.getMessage()
                << " continuing with test.");
    } catch (EntryExistsException &ignore) {
      ignore.getMessage();
    } catch (EntryNotFoundException &ignore) {
      ignore.getMessage();
    } catch (EntryDestroyedException &ignore) {
      ignore.getMessage();
    } catch (Exception &e) {
      end = 0;
      fwkResult = FWK_SEVERE;
      FWKSEVERE("Caught unexpected exception during entry "
                << opcode << " operation: " << e.getMessage()
                << " exiting task.");
    }
    meter.checkPace();
    now = ACE_OS::gettimeofday();
  }
  keyPtr = NULLPTR;
  valuePtr = NULLPTR;
  delete[] valBuf;

  FWKINFO("doEntryOperations did "
          << creates << " creates, " << puts << " puts, " << gets << " gets, "
          << invals << " invalidates, " << dests << " destroys, " << query
          << " query.");
  return fwkResult;
}
