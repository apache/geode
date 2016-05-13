/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    DurableClientTest.cpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#include "DurableClientTest.hpp"

#include <ace/Time_Value.h>
#include "fwklib/GsRandom.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/RegionHelper.hpp"

#include "fwklib/FwkExport.hpp"
#include <gfcpp/SystemProperties.hpp>


using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::durable;

static DurableClientTest * g_test = NULL;

static DurablePerfListener* g_perflistener = NULL;

static DurableCacheListener* g_listener = NULL;

//Signal Handling for listener close.
static void lisner_close( int signo ) {
  if( ( signo == SIGQUIT || signo == SIGTERM )&& g_listener!= NULL ) {
    FWKINFO("Client is crashed : dumping all data to BB");
    g_listener->dumpToBB();

    //Set a Null Signal Handler
    ACE_Sig_Action sa(NULL);
    sa.register_action(signo);
  }
  ACE_OS::kill(ACE_OS::getpid(),signo);
}
static void registerSignalHandlers() {
  FWKINFO( "Registering Signal Handle." );
  ACE_Sig_Action sa(lisner_close);
  sa.register_action(SIGQUIT);
  sa.register_action(SIGTERM);
}

// ----------------------------------------------------------------------------
TESTTASK initialize( const char * initArgs ) {
  int32_t result = FWK_SUCCESS;
  if ( g_test == NULL ) {
    FWKINFO( "Initializing DurableClientTest library." );
    try {
      registerSignalHandlers();
      g_test = new DurableClientTest( initArgs );
    } catch( const FwkException &ex ) {
      result = FWK_SEVERE;
      FWKSEVERE( "initialize: caught exception: " << ex.getMessage() );
    }
  }
  return result;
}

// ----------------------------------------------------------------------------
TESTTASK finalize() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Finalizing DurableClientTest library." );
  if ( g_test != NULL ) {
    g_test->cacheFinalize();
    delete g_test;
    g_test = NULL;
  }
  return result;
}

//listener------------------------------------------------------------------------------------
DurableCacheListener::DurableCacheListener():m_ops(0),m_result(true)
{
  bool isFeeder = g_test->getBoolValue( "isFeeder");
  if(isFeeder) {
      m_clntName = "Feeder";
  }
  else {
    char name[32] = {'\0'};
    ACE_OS::sprintf(name,"ClientName_%d",g_test->getClientId());
    m_clntName = name;
  }
  std::string cntIndexKey = m_clntName + std::string("_IDX");
  m_prevValue = static_cast<int32_t>(g_test->bbGet(DURABLEBB,cntIndexKey));

  FWKINFO("DurableCacheListener: created for client: " << m_clntName << ",Previous Index: " << m_prevValue);
}

void DurableCacheListener::check(const EntryEvent& event)
{
    CacheableKeyPtr key = event.getKey();
    CacheableInt32Ptr value = dynCast<CacheableInt32Ptr>(event.getNewValue());
    if(value->value() <= m_prevValue ) {  // Duplicate event, Ignore it
      //FWKINFO("Duplicate Event recieved, Value : " << value->value());
    }
    else if(value->value() == m_prevValue + 1 ) { // Desired
      m_ops++;
      m_prevValue = value->value();

      //FWKINFO("Event recieved, Value : " << value->value());
    }
    else { // Event missing
      FWKINFO("Missed Event # " << (value->value() - m_prevValue - 1) << " [ " << m_prevValue + 1 << " - " << value->value() - 1 << "  ]");

      m_ops++;
      m_prevValue = value->value();

      //Store Result with error msg
      if(m_result) {
        m_result = false;
        char buf[128] = {'\0'};
        sprintf(buf, "First missed event with value: %d ",m_prevValue + 1);
        m_err = std::string(buf);
      }
    }
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

  //Set current index
  std::string cntIndexKey = m_clntName + std::string("_IDX");
  g_test->bbSet(DURABLEBB,cntIndexKey,m_prevValue);
  FWKINFO( "Current Index for " << m_clntName << " is " <<  m_prevValue );

  //Store Error Message
  if(!m_result && !m_err.empty()) {
    std::string cntErrKey = m_clntName + std::string("_ErrMsg");
    std::string cntErrVal = g_test->bbGetString(DURABLEBB,cntErrKey);

    if(cntErrVal.empty()) {
      g_test->bbSet(DURABLEBB,cntErrKey,m_err);
    }
  }
}

// ----------------------------------------------------------------------------
TEST_EXPORT CacheListener * createDurableCacheListener() {
  FWKINFO( "createDurableCacheListener called " );

  g_listener = new DurableCacheListener();

  return g_listener;
}

// ----------------------------------------------------------------------------
TEST_EXPORT CacheListener * createDurablePerfListener() {
  FWKINFO( "createDurablePerfListener called " );

  g_perflistener = new DurablePerfListener();

  return g_perflistener;
}

//Dummy Task to restart client ----------------------------------------------------------------------------
TESTTASK doDummyTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doDummyTask called for task: " << taskId );
  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doIncrementalPuts( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doIncrementalPuts called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->incrementalPuts();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doIncrementalPuts caught exception: " << ex.getMessage() );
  }

  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doLogDurablePerformance() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Logging Durable performance." );
  if ( g_perflistener != NULL ) {
    g_perflistener->logPerformance();
  }
  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doCloseCache() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Closing cache, disconnecting from distributed system." );
  if ( g_test != NULL ) {
    g_test->cacheFinalize();
  }
  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doDurableCloseCache(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Closing cache, disconnecting from distributed system." );
  if ( g_test != NULL ) {
    g_test->setTask( taskId );
    g_test->durableCacheFinalize(taskId);
  }
  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doCreateRegion( const char * taskId ) {

  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreateRegion called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->createRegion();
    // static bool isReady = false;
    //bool isReady = false;
    /*
	  if(result == FWK_SUCCESS && !g_test->isReady) {
	    g_test->isReady = true;
      result = g_test->callReadyForEvents( taskId );
    }*/
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreateRegion caught exception: " << ex.getMessage() );
  }

  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doRegisterAllKeys( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterAllKeys called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->registerAllKeys();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterAllKeys caught exception: " << ex.getMessage() );
  }

  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doRegisterInterestList( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterInterestList called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->registerInterestList();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterInterestList caught exception: " << ex.getMessage() );
  }

  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doRegisterRegexList( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doRegisterRegexList called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result = g_test->registerRegexList();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRegisterRegexList caught exception: " << ex.getMessage() );
  }

  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doCloseCacheAndReInitialize(const char * taskId) {
  int32_t result = FWK_SUCCESS;

  FWKINFO( "doCloseCacheAndReInitialize called for task: " << taskId );

  try {
    g_test->setTask( taskId );
    g_test->isReady = false;
    result = g_test->closeNormalAndRestart(taskId);
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCloseCacheAndReInitialize caught exception: " << ex.getMessage() );
  }

  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doRestartClientAndRegInt(const char * taskId) {
  int32_t result = FWK_SUCCESS;

  FWKINFO( "doRestartClientAndRegInt called for task: " << taskId );

  try {
    g_test->setTask( taskId );
    result = g_test->restartClientAndRegInt(taskId);
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doRestartClientAndRegInt caught exception: " << ex.getMessage() );
  }

  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doVerifyEventCount( const char * taskId) {
  int32_t result = FWK_SUCCESS;
  if ( g_test != NULL ) {
    g_test->setTask( taskId );
    result = g_test->durableClientVerify(taskId);
  }
  return result;
}

// ----------------------------------------------------------------------------
int32_t DurableClientTest::durableCacheFinalize(const char * taskId ) {
  int32_t result = FWK_SUCCESS;

  if (m_cache != NULLPTR) {
    try {
      //destroyAllRegions();
      bool keepalive = getBoolValue("keepAlive");
      bool isDC = getBoolValue("isDurable");

      if(isDC) {
       FWKINFO("durableCacheFinalize: keepalive is " << keepalive);
       m_cache->close(keepalive);
      }
      else {
       m_cache->close();
      }
    } catch( CacheClosedException ignore ) {
    } catch( Exception & e ) {
      FWKEXCEPTION( "Caught an unexpected Exception during cache close: " << e.getMessage() );
    } catch( ... ) {
      FWKEXCEPTION( "Caught an unexpected unknown exception during cache close." );
    }
  }
  m_cache = NULLPTR;
  FWKINFO( "Cache closed." );

  FrameworkTest::cacheFinalize();

  return result;
}

// ----------------------------------------------------------------------------
int32_t DurableClientTest::durableClientVerify(const char * taskId ) {
  int32_t result = FWK_SUCCESS;

  FWKINFO( " Verify Durable Client Called ");

  try {
    int32_t id = getClientId();
    FWKINFO( " durableClientVerify: id  = " << id);

    char name[32] = {'\0'};
    ACE_OS::sprintf(name,"ClientName_%d",g_test->getClientId());
    std::string oper_cnt_key = std::string(name) + std::string("_Count");
    std::string feeder_cnt_key = std::string("Feeder") + std::string("_Count");
    int64_t cur_cnt = bbGet(DURABLEBB,oper_cnt_key);
    int64_t feeder_cnt = bbGet(DURABLEBB,feeder_cnt_key);

    FWKINFO( "doVerifyEventCount: Feeder: " << feeder_cnt << " Actual : " << cur_cnt );

    resetValue("missedEvents");
    bool missedEvents = getBoolValue("missedEvents");
    if(!missedEvents && (cur_cnt != feeder_cnt)) {
      std::string cntErrMsgKey = std::string(name) + std::string("_ErrMsg");
      std::string errMsg = bbGetString(DURABLEBB,cntErrMsgKey);
      result = FWK_SEVERE;
      FWKSEVERE( "VERIFY FAILED : #Missed = " <<  feeder_cnt - cur_cnt << " : " << errMsg );
    }
    else if(missedEvents && cur_cnt > feeder_cnt ) {
      result = FWK_SEVERE;
      FWKSEVERE("VERIFY FAILED:: few events should be missed");
    }
  }
  catch (const Exception& ex) {
    FWKEXCEPTION("doVerifyEventCount caught exception: " << ex.getMessage());
  }
  catch (const FwkException& ex) {
    FWKEXCEPTION( "doVerifyEventCount caught FwkException: " << ex.getMessage() );
  }

  LOGINFO("doVerityEventCount complete.");
  return result;
}

// ----------------------------------------------------------------------------
void DurableClientTest::checkTest( const char * taskId ) {
  SpinLockGuard guard( m_lck );
  setTask( taskId );
  if (m_cache == NULLPTR) {
    PropertiesPtr pp = Properties::create();

    int32_t heapLruLimit = getIntValue( "heapLruLimit" );
    if( heapLruLimit > 0 )
      pp->insert("heap-lru-limit",heapLruLimit);

    bool conflate = getBoolValue( "conflate" );
    if(conflate) {
      std::string conflateEvents = getStringValue( "conflateEvents" );
      pp->insert("conflate-events",conflateEvents.c_str());
    }

    bool isDC = getBoolValue("isDurable");
    if(isDC)
    {
      int32_t timeout = getIntValue("durableTimeout");
      bool isFeeder = getBoolValue( "isFeeder");
      std::string durableId;

      if(isFeeder) {
        durableId =  std::string("Feeder");
      }
      else {
        char name[32] = {'\0'};
        ACE_OS::sprintf(name,"ClientName_%d",getClientId());
        durableId =  std::string(name);
      }

      FWKINFO( "checktest durableID = " << durableId);
      pp->insert( "durable-client-id", durableId.c_str() );
      if(timeout > 0)
        pp->insert( "durable-timeout", timeout );
    }
    //CacheAttributesPtr cAttrs = NULLPTR;
    setCacheLevelEp();
    cacheInitialize( pp );

  }
}

// ----------------------------------------------------------------------------
int32_t DurableClientTest::callReadyForEvents( const char * taskId ) {
  int32_t  result = FWK_SEVERE;

  FWKINFO(" callReadyForEvents called for task:"<< taskId);
  try {
    bool isDC = getBoolValue("isDurable");
    if(isDC ) {
     m_cache->readyForEvents();
    }
    result = FWK_SUCCESS;
  } catch ( Exception& e ) {
    FWKEXCEPTION( "DurableClientTest::callReadyForEvents() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "DurableClientTest::callReadyForEvents() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "DurableClientTest::callReadyForEvents() Caught unknown exception." );
  }

  return result;
}
//----------------------------------------------------------------------------

TEST_EXPORT CacheListener * createConflationTestCacheListener() {
  return new ConflationTestCacheListener( g_test );
}
void ConflationTestCacheListener::afterCreate( const EntryEvent& event )
{
  m_numAfterCreate++;
}
void ConflationTestCacheListener::afterUpdate( const EntryEvent& event )
{
   m_numAfterUpdate++;
}
void ConflationTestCacheListener::afterInvalidate( const EntryEvent& event )
{
   m_numAfterInvalidate++;
}
void ConflationTestCacheListener::afterDestroy( const EntryEvent& event )
{
   m_numAfterDestroy++;
}
void ConflationTestCacheListener::dumpToBB(const RegionPtr& regPtr)
{
  char name[32] = {'\0'};
  sprintf(name,"%d",g_test->getClientId());
  std::string bb("ConflationCacheListener");
  std::string key1 = std::string( "AFTER_CREATE_COUNT_") + std::string(name) + std::string("_") + std::string(regPtr->getName());
  std::string key2 = std::string( "AFTER_UPDATE_COUNT_" ) + std::string(name) + std::string("_") + std::string(regPtr->getName());
  std::string key3 = std::string( "AFTER_INVALIDATE_COUNT_") + std::string(name) + std::string("_") + std::string(regPtr->getName());
  std::string key4 = std::string( "AFTER_DESTROY_COUNT_" ) + std::string(name) + std::string("_") + std::string(regPtr->getName());
  m_test->bbSet(bb,key1,m_numAfterCreate);
  m_test->bbSet(bb,key2,m_numAfterUpdate);
  m_test->bbSet(bb,key3,m_numAfterInvalidate);
  m_test->bbSet(bb,key4,m_numAfterDestroy);

}
// ----------------------------------------------------------------------------
int32_t DurableClientTest::closeNormalAndRestart( const char * taskId ) {

  int32_t  result = durableCacheFinalize(taskId);

  if(result == FWK_SUCCESS) {
    result = restartClientAndRegInt(taskId);
  } else {
    FWKEXCEPTION( "Cache Close Failed");
  }

  return result;
}

// ----------------------------------------------------------------------------
int32_t DurableClientTest::restartClientAndRegInt( const char * taskId ) {

  int32_t  result = FWK_SUCCESS;

  // Do sleep  based on restartTime ,
  int32_t sleepSec = getIntValue("restartTime");
  if(sleepSec > 0 ) {
    perf::sleepSeconds(sleepSec);
  }
  result = doCreateRegion(taskId);

  if(result == FWK_SUCCESS) {
     FWKINFO("Client Created Successfully.");
     // do registerIntrest based on string given All , List , Regex
     std::string regType = getStringValue("registerType");
     if(regType == std::string("All") ){
       result = registerAllKeys();
     } else if(regType == std::string("List")) {
       result = registerInterestList();
     } else if(regType == std::string("Regex")) {
       result = registerRegexList();
     } else {
       FWKEXCEPTION("Incorrect Reg Type given : " << regType );
     }
  } else {
    FWKEXCEPTION( "Create Client Failed");
    return result;
  }

  if(!g_test->isReady){
     g_test->isReady=true;
  }
  // Sleep for updateReceiveTime to recieve entries,
  int32_t waitTime = getIntValue("updateReceiveTime");
  if(waitTime > 0 ) {
    perf::sleepSeconds(waitTime);
  }

  return result;
}

int32_t DurableClientTest::createRegion()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In DurableClientTest::createRegion()" );

  try {
    createPool();
    RegionHelper help( this );
    //bool isDC = getBoolValue("isDurable");
    RegionPtr region = help.createRootRegion( m_cache);

    std::string key( region->getName() );
    bbIncrement( REGIONSBB, key );
    FWKINFO( "DurableClientTest::createRegion Created region " << region->getName());
    result = FWK_SUCCESS;

  } catch ( Exception e ) {
    FWKEXCEPTION( "DurableClientTest::createRegion FAILED -- caught exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "DurableClientTest::createRegion FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "DurableClientTest::createRegion FAILED -- caught unknown exception." );
  }

  return result;
}

int32_t DurableClientTest::registerAllKeys()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In DurableClientTest::registerAllKeys()" );

  try {
  RegionPtr region = getRegionPtr();
  FWKINFO("DurableClientTest::registerAllKeys region name is " << region->getName());
  bool isDurable = g_test->getBoolValue("isDurableReg");
  region->registerAllKeys(isDurable);
  const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
  if(strlen(durableClientId) > 0) {
      m_cache->readyForEvents();
     g_test->isReady=true;
    }
  result = FWK_SUCCESS;

  } catch ( Exception& e ) {
    FWKEXCEPTION( "DurableClientTest::registerAllKeys() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "DurableClientTest::registerAllKeys() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "DurableClientTest::registerAllKeys() Caught unknown exception." );
  }
  FWKINFO( "DurableClientTest::registerAllKeys() complete." );
  return result;

}

int32_t DurableClientTest::registerInterestList()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In DurableClientTest::registerInterestList()" );

  try {
  static char keyType = 'i';
  std::string typ = getStringValue( "keyType" ); // int is only value to use
  char newType = typ.empty() ? 's' : typ[0];

  RegionPtr region = getRegionPtr();
  resetValue("distinctKeys");
  int32_t numKeys = getIntValue( "distinctKeys" );  // check distince keys first
  if (numKeys <= 0) {
    FWKSEVERE( "Failed to initialize keys with numKeys :" <<  numKeys);
    return  result;
  }
  int32_t low = getIntValue( "keyIndexBegin" );
  low = (low > 0) ? low : 0;
  int32_t numOfRegisterKeys = getIntValue( "registerKeys");
  int32_t high = numOfRegisterKeys + low;

  clearKeys();
  m_MaxKeys = numOfRegisterKeys;
  keyType = newType;
  VectorOfCacheableKey registerKeyList;
  if ( keyType == 'i' ) {
   initIntKeys(low, high);
  } else {
    int32_t keySize = getIntValue( "keySize" );
    keySize = (keySize > 0) ? keySize : 10;
    std::string keyBase(keySize, 'A');
    initStrKeys(low, high, keyBase);
  }

  for (int j = low; j < high; j++) {
    if (m_KeysA[j - low] != NULLPTR) {
      registerKeyList.push_back(m_KeysA[j - low]);
    }
    else {
      FWKINFO("DurableClientTest::registerInterestList key is NULL");
    }
  }
  FWKINFO(" DurableClientTest::registerInterestList region name is " << region->getName());
  resetValue("getInitialValues");
  bool isGetInitialValues = getBoolValue("getInitialValues");
  bool isReceiveValues = true;
  bool checkReceiveVal = getBoolValue("checkReceiveVal");
  if(checkReceiveVal){
    resetValue("receiveValue");
    isReceiveValues = getBoolValue("receiveValue");
  }
  resetValue("isDurableReg");
  bool isDurableReg = getBoolValue( "isDurableReg" );
  region->registerKeys(registerKeyList, isDurableReg,isGetInitialValues,isReceiveValues);
  const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
  if(strlen(durableClientId) > 0) {
      m_cache->readyForEvents();
     g_test->isReady=true;
    }
  result = FWK_SUCCESS;

  } catch ( Exception& e ) {
    FWKEXCEPTION( "DurableClientTest::registerInterestList() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "DurableClientTest::registerInterestList() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "DurableClientTest::registerInterestList() Caught unknown exception." );
  }
  FWKINFO( "DurableClientTest::registerInterestList() complete." );
  return result;

}

// ----------------------------------------------------------------------------

int32_t DurableClientTest::registerRegexList()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In DurableClientTest::registerRegexList()" );
  try {
  RegionPtr region = getRegionPtr();
  std::string registerRegex = getStringValue("registerRegex");
  FWKINFO("DurableClientTest::registerRegexList region name is " << region->getName() << "regex is: " << registerRegex.c_str());
  resetValue("getInitialValues");
  bool isGetInitialValues = getBoolValue("getInitialValues");
  bool isReceiveValues = true;
  bool checkReceiveVal = getBoolValue("checkReceiveVal");
  if (checkReceiveVal) {
    resetValue("receiveValue");
    isReceiveValues = getBoolValue("receiveValue");
  }
  resetValue("isDurableReg");
  bool isDurableReg = getBoolValue( "isDurableReg" );
  region->registerRegex(registerRegex.c_str(),isDurableReg,NULLPTR,isGetInitialValues,isReceiveValues);
  const char *durableClientId = DistributedSystem::getSystemProperties()->durableClientId();
  if(strlen(durableClientId) > 0) {
      m_cache->readyForEvents();
     g_test->isReady=true;
    }
  result = FWK_SUCCESS;

  } catch ( Exception& e ) {
    FWKEXCEPTION( "DurableClientTest::registerRegexList() Caught Exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "DurableClientTest::registerRegexList() Caught FwkException: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "DurableClientTest::registerRegexList() Caught unknown exception." );
  }
  FWKINFO( "DurableClientTest::registerRegexList() complete." );
  return result;

}

RegionPtr DurableClientTest::getRegionPtr( const char * reg )
{
  RegionPtr region;
  std::string name;

  if ( reg == NULL ) {
    name = getStringValue( "regionName" );
    if ( name.empty() ) {
      try {
        RegionHelper help( g_test );
        name = help.regionName();
        if ( name.empty() ) {
          name = help.specName();
        }
      } catch( ... ) {}
    }
  }
  try {
    if ( name.empty() ) { // just get a random root region
      VectorOfRegion rootRegionVector;
      m_cache->rootRegions( rootRegionVector );
      int32_t size = rootRegionVector.size();

      if ( size == 0 ) {
        FWKEXCEPTION( "In DurableClientTest::getRegionPtr()  No regions exist." );
      }

      FWKINFO( "Getting a random root region." );
      region = rootRegionVector.at( GsRandom::random( size ) );
    }
    else {
      FWKINFO( "Getting region: " << name );
      if (m_cache == NULLPTR) {
        FWKEXCEPTION( "Failed to get region: " << name << "  cache ptr is null." );
      }
      region = m_cache->getRegion( name.c_str() );
      if (region == NULLPTR) {
        FWKEXCEPTION( "Failed to get region: " << name );
      }
    }
  } catch( CacheClosedException e ) {
    FWKEXCEPTION( "In DurableClientTest::getRegionPtr()  CacheFactory::getInstance encountered "
      "CacheClosedException: " << e.getMessage() );
  } catch( EntryNotFoundException e ) {
    FWKEXCEPTION( "In DurableClientTest::getRegionPtr()  CacheFactory::getInstance encountered "
      "EntryNotFoundException: " << e.getMessage() );
  } catch( IllegalArgumentException e ) {
    FWKEXCEPTION( "In DurableClientTest::getRegionPtr()  CacheFactory::getInstance encountered "
      "IllegalArgumentException: " << e.getMessage() );
  }
  return region;
}

void DurableClientTest::clearKeys() {
  if ( m_KeysA != NULL ) {
    for ( int32_t i = 0; i < m_MaxKeys; i++ ) {
      m_KeysA[i] = NULLPTR;
    }
    delete [] m_KeysA;
    m_KeysA = NULL;
    m_MaxKeys = 0;
  }
}

// ========================================================================

void DurableClientTest::initStrKeys(int32_t low, int32_t high, const std::string & keyBase) {
  m_KeysA = new CacheableKeyPtr[m_MaxKeys];
  const char * const base = keyBase.c_str();

  char buf[128];
  for ( int32_t i = low; i < high; i++ ) {
    sprintf( buf, "%s%010d", base, i);
    m_KeysA[i - low] = CacheableKey::create( buf );
  }
}

// ========================================================================

void DurableClientTest::initIntKeys(int32_t low, int32_t high) {
  m_KeysA = new CacheableKeyPtr[m_MaxKeys];
  FWKINFO("m_MaxKeys: " << m_MaxKeys << " low: " << low << " high: " << high);

  for ( int32_t i = low; i < high; i++ ) {
    m_KeysA[i - low] = CacheableKey::create( i );

  }
}

// ----------------------------------------------------------------------------
// This is Global incremental put.only single thread will iterate over all keys until
// max value is reached. keys will be string type of keysize.
//for first put valueStart should be either 1 or not defined.
int32_t DurableClientTest::incrementalPuts()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In DurableClientTest::incrementalPuts()" );

  try {
    RegionPtr region = getRegionPtr();

    char buf[128];
    std::string keyTyp = getStringValue( "keyType" ); // int is only value to use
    int32_t keySize = getIntValue( "keySize" );
    int32_t valStart = getIntValue( "valueStart"); // optional ( 1)
    int32_t valEnd = getIntValue( "valueEnd");

    resetValue("distinctKeys");
    int32_t numKeys = getIntValue( "distinctKeys" );  // check distince keys first
    if ( numKeys <= 0  ) {
      FWKSEVERE( "Error in reading  distinctKeys ");
      return result;
    }

    //validate above tags
    char typ = keyTyp[0] == 'i' ? 'i' :'s';
    int32_t curVal = (valStart > 1) ? valStart : 1;
    if ( valEnd <= 0) {
      FWKSEVERE( "Error in reading  valueEnd ");
      return result;
    }

    int32_t keyIndex = 0;
    while (true )
    {
      if(keyIndex == numKeys) {
        keyIndex = 1;
      } else {
        keyIndex++;
      }

      if(typ == 'i') {
        CacheableInt32Ptr key = CacheableInt32::create(keyIndex);
        CacheableInt32Ptr value = CacheableInt32::create(curVal);

        region->put(key, value);
      } else {
        keySize = (keySize > 0) ? keySize : 10;
        std::string keyBase(keySize, 'A');
        const char * const base = keyBase.c_str();

        sprintf( buf, "%s%010d", base, keyIndex);
        CacheableStringPtr key = CacheableString::create(buf);
        CacheableInt32Ptr value = CacheableInt32::create(curVal);

        region->put(key, value);
      }

      if(++curVal > valEnd ) {
        break;
      }
      //Log for Put
      if(curVal % 10000 == 1) {
        FWKINFO( "DurableClientTest::Putting..." );
      }
    }

    result = FWK_SUCCESS;
  } catch ( Exception & e ) {
    FWKEXCEPTION( "DurableClientTest::incrementalPuts() Caught Exception: " << e.getMessage() );
  } catch ( FwkException & e ) {
    FWKEXCEPTION( "DurableClientTest::incrementalPuts() Caught FwkException: " << e.getMessage() );
  } catch ( std::exception & e ) {
    FWKEXCEPTION( "DurableClientTest::incrementalPuts() Caught std::exception: " << e.what() );
  } catch ( ... ) {
    FWKEXCEPTION( "DurableClientTest::incrementalPuts() Caught unknown exception." );
  }

  FWKINFO( "DurableClientTest::incrementalPuts() complete." );
  return result;
}
