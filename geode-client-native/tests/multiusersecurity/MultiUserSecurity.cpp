/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    MultiUserSecurity.cpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------

#include "MultiUserSecurity.hpp"

#include <ace/Time_Value.h>
#include "fwklib/GsRandom.hpp"

#include "fwklib/FwkLog.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/RegionHelper.hpp"

#include "fwklib/FwkExport.hpp"
#include "fwklib/PoolHelper.hpp"
#include "fwklib/QueryHelper.hpp"
#include "fwklib/PaceMeter.hpp"
#include "functionExecution/FunctionExecution.hpp"

#include "security/CredentialGenerator.hpp"
#include "security/XmlAuthzCredentialGenerator.hpp"
#include "security/PkcsCredentialGenerator.hpp"
#include "security/PkcsAuthInit.hpp"
#include <gfcpp/SystemProperties.hpp>

#include <vector>
#include <map>

#define QUERY_RESPONSE_TIMEOUT 600
#define FE_TIMEOUT 15
#define PUTALL_TIMEOUT 60

namespace FwkMultiUserTests {
  std::string REGIONSBB( "Regions" );
  std::string CLIENTSBB( "ClientsBb" );
  std::string READYCLIENTS( "ReadyClients" );
}

using namespace FwkMultiUserTests;
using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::multiusersecurity;
using namespace gemfire::testframework::security;


MultiUser * g_test = NULL;
PerClientList userlist;
readerList readerRole;
writerList writerRole;
adminList adminRole;
queryList queryRole;
static int totalOperation = 0;
static int notAuthzCount = 0;
bool doAddOperation = true;
static std::map<std::string,RegionPtr>proxyRegionMap;
static std::map<std::string,RegionServicePtr>authCacheMap;
static std::map<std::string,std::map<std::string,int > >operationMap;
static std::map<std::string,std::map<std::string,int > >exceptionMap;
static std::map<std::string,std::vector<std::string> >userToRolesMap;
static std::map<std::string,std::map<std::string,int> >UserMap;
static std::map<std::string,std::map<std::string,int> >UserlatestValMap;
//----------------------------------------------------------------------------

TESTTASK initialize( const char * initArgs ) {
  int32_t result = FWK_SUCCESS;
  if ( g_test == NULL ) {
    FWKINFO( "Initializing MultiUserSecurity library." );
    try {
      g_test = new MultiUser( initArgs );
    } catch( const FwkException &ex ) {
      FWKSEVERE( "initialize: caught exception: " << ex.getMessage() );
      result = FWK_SEVERE;
    }
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK finalize() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Finalizing MultiUserSecurity library." );
  if ( g_test != NULL ) {
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
  if ( g_test != NULL ) {
    userlist.clear();
    g_test->cacheFinalize();
  }
  return result;
}

void MultiUser::getClientSecurityParams(PropertiesPtr prop, std::string credentials) {

  std::string securityParams = getStringValue("securityParams");
  std::string bb ( "GFE_BB" );
  std::string key( "securityScheme" );
  std::string sc = bbGetString(bb,key);
  if( sc.empty() ) {
    sc = getStringValue( key.c_str() );
    if( !sc.empty() ) {
      bbSet(bb, key, sc);
    }
  }
  FWKINFO("security scheme : " << sc);

}

// ----------------------------------------------------------------------------

void MultiUser::checkTest( const char * taskId ) {
  SpinLockGuard guard( m_lck );
  setTask( taskId );
  if (m_cache == NULLPTR) {
    PropertiesPtr pp = Properties::create();
    std::string authInit;

    getClientSecurityParams(pp, getStringValue("credentials"));

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
    cacheInitialize(pp);
    bool isTimeOutInMillis = m_cache->getDistributedSystem()->getSystemProperties()->readTimeoutUnitInMillis();
    if(isTimeOutInMillis){
      #define QUERY_RESPONSE_TIMEOUT 600*1000
      #define FE_TIMEOUT 15*1000
      #define PUTALL_TIMEOUT 60*1000
    }
    
  }
  // MultiUser specific initialization
  // none
 }

// -----------------------------------------------------------------------------

TESTTASK doCreatePool( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreatePool called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->createPools();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreatePool caught exception: " << ex.getMessage() );
  }

  return result;
}
//-----------------------------------------------------------------------------


TESTTASK doValidateEntryOperationsForPerUser(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doValidateEntryOperationsForPerUser called for task: " << taskId );
  try {
    g_test->checkTest( taskId);
    result = g_test->validateEntryOperationsForPerUser();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doValidateEntryOperationsForPerUser caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doCreateRegion( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doCreateRegion called for task: " << taskId );
  std::string multiusermode = "true";
  try {
    g_test->checkTest( taskId );
    result = g_test->createRegion();
  } catch ( FwkException ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doCreateRegion caught exception: " << ex.getMessage() );
  }
  return result;
}
// ----------------------------------------------------------------------------

TESTTASK doFeedTask(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doFeedTask called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    g_test->doFeed();
   } catch (const FwkException &ex ) {
     result = FWK_SEVERE;
     FWKSEVERE( "doFeed caught exception: " << ex.getMessage() );
   }
   return result;
}
// -----------------------------------------------------------------------------

TESTTASK doEntryOperationsForMU(const char * taskId) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doEntryOperationsForMU called for task: " << taskId );
  try {
    g_test->checkTest( taskId );
    result=g_test->entryOperationsForMU();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doEntryOperationsForMU caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------
TESTTASK doCloseCacheAndReInitialize(const char * taskId){
  int32_t result=FWK_SUCCESS;
  FWKINFO( "doCloseCacheAndReInitialize called for task: " << taskId );
  try{
    g_test->setTask( taskId );
    result = g_test->closeCacheAndReInitialize(taskId);
  } catch(const FwkException &ex){
      result=FWK_SEVERE;
      FWKSEVERE("doCloseCacheAndReInitialize caught exception: " << ex.getMessage());
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doCqForMU(const char * taskId){
  int result=FWK_SUCCESS;
  FWKINFO( "doCqForMU called for task: " << taskId );
  try{
    g_test->checkTest(taskId);
    result = g_test->cqForMU();
  }catch(const FwkException &ex){
    result=FWK_SEVERE;
    FWKSEVERE("doCqForMU caught exception: " << ex.getMessage());
  }
  return result;
}
// ----------------------------------------------------------------------------

TESTTASK doValidateCqOperationsForPerUser(const char * taskId){
  int32_t result=FWK_SUCCESS;
  FWKINFO("doValidateCqOperationsForPerUser called for task : " << taskId );
  try{
    g_test->checkTest(taskId);
    g_test->validateCqOperationsForPerUser();
  }catch(const FwkException &ex) {
    result=FWK_SEVERE;
    FWKSEVERE("doValidateCqOperationsForPerUser caught exception: " << ex.getMessage());
  }
  return result;
}
// --------------------------------------------------------------------------------

int32_t MultiUser::closeCacheAndReInitialize(const char * taskId)
{
  int32_t result = FWK_SUCCESS;
  try {
    if (m_cache != NULLPTR) {
      destroyAllRegions();
      bool keepalive = getBoolValue("keepAlive");
      bool isDC = getBoolValue("isDurable");
      if(isDC) {
        FWKINFO("keepalive is " << keepalive);
        m_cache->close(keepalive);
      }
      else {
        m_cache->close();
      }
      m_cache = NULLPTR;
      FWKINFO( "Cache closed." );
    }
    userlist.clear();
    FrameworkTest::cacheFinalize();
    result=doCreateRegion(taskId);
    bool isCq = getBoolValue( "cq" );
    if( result == FWK_SUCCESS && isCq ){
      result=doCqForMU(taskId);
    }
  } catch( CacheClosedException ignore ) {
  } catch( Exception & e ) {
     FWKEXCEPTION( "Caught an unexpected Exception during cache close: " << e.getMessage() );
  }
  return result;
}

int32_t MultiUser::createRegion()
{
  FWKINFO( "In MultiUser::createRegion()" );

  int32_t result = FWK_SEVERE;
  try {
    PropertiesPtr pp = Properties::create();
    createPool();
    RegionHelper help( g_test );
    RegionPtr region = help.createRootRegion(m_cache );
    std::string key( region->getName() );
    bbIncrement( REGIONSBB, key );
    FWKINFO( "MultiUser::createRegion Created region " << region->getName() << std::endl);
    resetValue( "isDurable" );
    bool isDC = getBoolValue("isDurable");
    if(isDC ) {
      m_cache->readyForEvents();
    }
    result = FWK_SUCCESS;
    if(region != NULLPTR) {
      RegionAttributesPtr regAttr = region->getAttributes();
      std::string poolName = regAttr->getPoolName();
      if(!poolName.empty()) {
    	PoolPtr pool = PoolManager::find(poolName.c_str());
        if(pool->getMultiuserAuthentication()) {
          createMultiUserCacheAndRegion(pool,region);
        }
      }
    }
  } catch ( Exception e ) {
    FWKEXCEPTION( "MultiUser::createRegion FAILED -- caught exception: " << e.getMessage() );
  } catch ( FwkException& e ) {
    FWKEXCEPTION( "MultiUser::createRegion FAILED -- caught test exception: " << e.getMessage() );
  } catch ( ... ) {
    FWKEXCEPTION( "MultiUser::createRegion FAILED -- caught unknown exception." );
  }
  return result;
}

// ------------------------------------------------------------------------------
// multiUser initialization
void MultiUser::createMultiUserCacheAndRegion(PoolPtr pool,RegionPtr region){
  std::string bb ( "GFE_BB" );
  std::string key( "securityScheme" );
  std::string sc = bbGetString(bb,key);
  std::string user=getStringValue(sc.c_str());
  CredentialGeneratorPtr cg = CredentialGenerator::create( sc );
  const char* userCreds = user.c_str();
  FWKINFO("testScheme is " << sc);
  std::string userName="";
  std::string regionName=region->getName();
  int32_t numOfMU = getIntValue( "MultiUsers" );
  char str[15];

 //populating the vector with no. of multiusers.
  for(int32_t i=1;i<=numOfMU;i++){
    sprintf(str, "%s%d", userCreds,i);
    userlist.push_back(str);
  }

  if(sc == "LDAP" || sc == "DUMMY"){
    for(uint32_t i =0 ;i < userlist.size();i++) {
      PropertiesPtr userProp = Properties::create();
      userName=userlist.at(i);
      cg->getAuthInit(userProp);
      userProp->insert("security-username",userName.c_str());
      userProp->insert("security-password",userName.c_str());
      RegionServicePtr mu_cache= m_cache->createAuthenticatedView(userProp, pool->getName());
      authCacheMap[userName] = mu_cache;
      RegionPtr region=mu_cache->getRegion(regionName.c_str());
      proxyRegionMap[userName]=region;
      std::map<std::string,int>opMAP;
      std::map<std::string,int>expMAP;
      operationMap[userName]=opMAP;
      exceptionMap[userName]=expMAP;
      switch(i){
      case 0:
      case 1:
       	setAdminRole(userName);
       	break;
      case 2:
      case 3:
      case 4:
    	setReaderRole(userName);
       	break;
      case 5:
      case 6:
      case 7:
       	setWriterRole(userName);
       	break;
      case 8:
      case 9:
       	setQueryRole(userName);
       	break;
      default:
       	break;
     };
   }
 }
 else if(sc == "PKCS"){
   std::string keyStoreAlias="";
   for(uint32_t i =0 ;i < userlist.size();i++){
     PropertiesPtr userProp = Properties::create();
     PKCSAuthInitInternal* pkcs = new PKCSAuthInitInternal();
     if (pkcs == NULL) {
       FWKEXCEPTION("NULL PKCS Credential Generator");
     }
     cg->getAuthInit(userProp);
     PKCSCredentialGenerator * pkcs1 = dynamic_cast<PKCSCredentialGenerator*>(cg.ptr());
     userName=userlist.at(i);
     pkcs1->insertKeyStorePath(userProp , userName.c_str());
     userProp->insert("security-alias",userName.c_str());
     userProp->insert("security-keystorepass","gemfire");
     //mu_cache = pool->createSecureUserCache(pkcs->getCredentials(userProp,"0:0"));
     RegionServicePtr mu_cache= m_cache->createAuthenticatedView(pkcs->getCredentials(userProp,"0:0"), pool->getName());
     authCacheMap[userName] = mu_cache;
     RegionPtr region=mu_cache->getRegion(regionName.c_str());
     proxyRegionMap[userName]=region;
     std::map<std::string,int>opMAP;
     std::map<std::string,int>expMAP;
     operationMap[userName]=opMAP;
     exceptionMap[userName]=expMAP;
     FWKINFO("inserted  PKCS security-alias" << (userProp)->find("security-alias")->asChar()
                 << " password "
                 << ((userProp)->find("security-keystorepass") != NULLPTR ? (userProp)->find("security-keystorepass")->asChar():"not set") );
     switch(i){
     case 0:
     case 1:
       setAdminRole(userName);
       break;
     case 2:
     case 3:
     case 4:
       setReaderRole(userName);
       break;
     case 5:
     case 6:
     case 7:
       setWriterRole(userName);
       break;
     case 8:
     case 9:
       setQueryRole(userName);
       break;
     default:
       break;
    };
   }
 }
}

// ----------------------------------------------------------------------------

void MultiUser::insertKeyStorePath(const char *username,PropertiesPtr userProps)
{
  char keystoreFilePath[1024] ;
  char *tempPath = NULL;
  tempPath = ACE_OS::getenv("TESTSRC");
  std::string path = "";
  if(tempPath == NULL ){
    tempPath =  ACE_OS::getenv("BUILDDIR");
    path = std::string(tempPath) + "/framework/data";
  }else {
    path =  std::string(tempPath);
  }
  sprintf(keystoreFilePath,"%s/keystore/%s.keystore" , path.c_str(), username);
  userProps->insert("security-keystorepath",keystoreFilePath);
}
// ----------------------------------------------------------------------------
int32_t MultiUser::createPools()
{
  int32_t result = FWK_SEVERE;
  FWKINFO( "In MultiUser::createPool()" );
  try{
    PoolHelper help( g_test );
    PoolPtr pool = help.createPool();
    FWKINFO( "MultiUser::createPool Created Pool " << pool->getName() << std::endl << "multiUserMode set as " << pool->getMultiuserAuthentication());
    result = FWK_SUCCESS;
  } catch( Exception e ) {
    FWKEXCEPTION( "MultiUser::createPool FAILED -- caught exception: " << e.getMessage() );
  } catch( FwkException& e ){
    FWKEXCEPTION( "MultiUser::createPool FAILED -- caught test exception: " << e.getMessage() );
  } catch( ... ) {
    FWKEXCEPTION( "MultiUser::createPool FAILED -- caught unknown exception. " );
  }
  return result;
}
// ----------------------------------------------------------------------------------------
 int32_t MultiUser::doFeed() {
   FWKINFO("MultiUser::Inside doFeed()");
   int32_t result = FWK_SEVERE;
   int32_t entryCount = getIntValue( "entryCount" );
   entryCount = ( entryCount < 1 ) ? 10000 : entryCount;
   try {
     char keybuf[100];
     int32_t cnt = 1;
     std::string userName=userlist.at(0);
     FWKINFO("In doFeed user is " << userName);
     RegionPtr region=proxyRegionMap[userName];
     while(cnt < entryCount){
       sprintf(keybuf,"%u",cnt);
       CacheableKeyPtr key = CacheableKey::create(keybuf);
       CacheableInt32Ptr value = CacheableInt32::create(cnt);
       region->put(key,value);
       cnt++;
     }
     result = FWK_SUCCESS;
   } catch ( std::exception e ) {
     FWKEXCEPTION( "MultiUser::doFeed() Caught std::exception: " << e.what() );
   } catch ( Exception e ) {
     FWKEXCEPTION( "MultiUser::doFeed() Caught Exception: " << e.getMessage() );
   } catch ( FwkException& e ) {
     FWKEXCEPTION( "MultiUser::doFeed() Caught FwkException: " << e.getMessage() );
   } catch ( ... ) {
     FWKEXCEPTION( "MultiUser::doFeed() Caught unknown exception." );
   }
   FWKINFO( "MultiUser::doFeed() complete." );
   return result;
}
//-----------------------------------------------------------------------------------------

 int32_t MultiUser::entryOperationsForMU(){
   int32_t result=FWK_SEVERE;
   int32_t opsSec = getIntValue( "opsSecond" );
   opsSec = ( opsSec < 1 ) ? 0 : opsSec;
   int32_t entryCount = getIntValue( "entryCount" );
   entryCount = ( entryCount < 1 ) ? 10000 : entryCount;
   int32_t secondsToRun = getTimeValue( "workTime" );
   secondsToRun = ( secondsToRun < 1 ) ? 30 : secondsToRun;
   ACE_Time_Value end = ACE_OS::gettimeofday() + ACE_Time_Value( secondsToRun );
   ACE_Time_Value now;
   CacheableKeyPtr keyPtr;
   //CacheableStringPtr valPtr;
   CacheableInt32Ptr valPtr;
   CacheablePtr value;
   CqQueryPtr qry;
   int32_t creates = 0, puts = 0, destroy = 0, putAll=0;
   std::string opcode;
   char keybuf[100];
   char cqName[32] = {'\0'};
   PaceMeter meter( opsSec );

   while ( now < end ) {
     int32_t userSize = userlist.size();
     std::string userName = userlist.at( GsRandom::random( userSize ) );
     RegionPtr m_Region=proxyRegionMap[userName];
     RegionServicePtr mu_cache = authCacheMap[userName];
     doAddOperation = true;
     opcode = getStringValue( "entryOps" );
     try{
       if ( opcode.empty() ) opcode = "no-op";
       int32_t cnt=1;
       FWKINFO("The opcode is " << opcode << " for user " << userName);
       if ( opcode == "create" ) {
	 sprintf( keybuf, "%u", ( uint32_t )GsRandom::random( entryCount ) );
         keyPtr = CacheableKey::create(keybuf);
         valPtr=CacheableInt32::create(cnt);
         cnt++;
         m_Region->create( keyPtr, valPtr );
         creates++;
         result = FWK_SUCCESS;
      }
      else {
        sprintf( keybuf, "%u", ( uint32_t )GsRandom::random( entryCount ) );
        keyPtr = CacheableKey::create(keybuf);
        if ( opcode == "update" ) {
         valPtr=CacheableInt32::create(cnt);
         cnt++;
          m_Region->put( keyPtr, valPtr );
          puts++;
          result = FWK_SUCCESS;
        }
	else if ( opcode == "invalidate" ) {
	  m_Region->invalidate( keyPtr );
        }
        else if ( opcode == "destroy" ) {
          m_Region->destroy( keyPtr );
          result = FWK_SUCCESS;
          destroy++;
        }
        else if ( opcode == "get" ) {
          value =m_Region->get( keyPtr );
        }
        else if ( opcode == "getServerKeys" ) {
          VectorOfCacheableKey keysVec;
          m_Region->serverKeys( keysVec );
        }
        else if ( opcode == "read+localdestroy" ) {
	  value = m_Region->get( keyPtr );
	  m_Region->localDestroy( keyPtr );
	}
	else if ( opcode == "query" ) {
	  QueryPtr qry;
	  SelectResultsPtr results;
	  QueryServicePtr qs = mu_cache->getQueryService();
	  qry = qs->newQuery("select distinct * from /Portfolios where FALSE");
	  results = qry->execute(QUERY_RESPONSE_TIMEOUT );
	}
	else if( opcode == "cq" ) {
	  ACE_OS::sprintf(cqName,"cq-%s",userName.c_str());
	  QueryServicePtr qs = mu_cache->getQueryService();
	  CqAttributesFactory cqFac;
          FWKINFO("CqName = " << cqName);
	  CqListenerPtr cqLstner(new MyCqListener(0));
	  cqFac.addCqListener(cqLstner);
	  CqAttributesPtr cqAttr = cqFac.create();
	  qry = qs->newCq(cqName,"select * from /Portfolios where TRUE", cqAttr);
	  qry->execute();
	  qry->stop();
          qry->execute();
	  qry->close();
	  result = FWK_SUCCESS;
	}
	else if( opcode == "executefunction" ) {
	  bool isFailOver=getBoolValue("isFailover");
	  bool getResult = true;
          int32_t randomNo=GsRandom::random(3);
          ExecutionPtr exc = NULLPTR;
          CacheableVectorPtr filterObj=CacheableVector::create();
          filterObj->push_back(keyPtr);          
          char funcArr[100];
          if(isFailOver || randomNo == 0){
            CacheablePtr args1 = CacheableKey::create("addKey");
            filterObj->push_back(args1);
            CacheablePtr args=filterObj;
            std::string func="RegionOperationsFunction";
            sprintf(funcArr,"%s",func.c_str());
            FWKINFO("RegionOperationsFunction user is " << userName);
            exc = FunctionService::onRegion(m_Region);
            exc->withFilter(filterObj)->withArgs(args)->execute(funcArr,getResult, FE_TIMEOUT, true, true)->getResult();
          }
          else if(randomNo == 1){
            CacheablePtr args=filterObj;
            std::string func="ServerOperationsFunction";
            sprintf(funcArr,"%s",func.c_str());
            FWKINFO("ServerOperationsFunction user is " << userName);
            //exc = m_Region->getCache()->getFunctionService()->onServer();
            exc = FunctionService::onServer(mu_cache);
            exc->withArgs(args)->execute(funcArr,getResult, FE_TIMEOUT, true, true)->getResult();
          }
          else if(randomNo == 2) {
            CacheablePtr args=filterObj;
            FWKINFO("Inside  ServersOperationsResultFunction ");
            std::string func="ServerOperationsFunction";
            sprintf(funcArr,"%s",func.c_str());
            //exc = m_Region->getCache()->getFunctionService()->onServers();
            exc = FunctionService::onServers(mu_cache);
            exc->withArgs(args)->execute(funcArr,getResult, FE_TIMEOUT, true, true)->getResult();
          }
        }
        else if ( opcode == "putAll" ) {
	  HashMapOfCacheable map0;
	  map0.clear();
	  for (uint32_t i=0; i<200; i++) {
	    sprintf( keybuf, "%u", i );
	    keyPtr = CacheableKey::create( keybuf );
	    valPtr=CacheableInt32::create(cnt);
	    cnt++;
	    map0.insert(keyPtr, valPtr);
	  }
	  m_Region->putAll(map0,PUTALL_TIMEOUT);
	  putAll++;
	  result = FWK_SUCCESS;
	}
        else {
          FWKEXCEPTION( "Invalid operation specified: " << opcode );
        }
        updateOperationMap(opcode,userName);
     }
   }catch(const gemfire::NotAuthorizedException &){
   FWKINFO(" Got Expected NotAuthorizedException for operation " << opcode);
   if(opcode == "cq"){
     if(qry->isStopped()){
       FWKINFO("Cq with same name still exists,hence closing it");
       qry->close();
     }
   }
   updateExceptionMap(opcode,userName);
 }catch ( TimeoutException &e ) {
  result = FWK_SEVERE;
  FWKSEVERE( "Caught unexpected timeout exception during entry " << opcode
   << " operation: " << e.getMessage() << " continuing with test." );
 }catch ( EntryExistsException &ignore ) {
   ignore.getMessage();
   doAddOperation = false;
   updateOperationMap(opcode,userName);
 } catch ( EntryNotFoundException &ignore ) {
   ignore.getMessage();
   doAddOperation = false;
   updateOperationMap(opcode,userName);
 } catch ( EntryDestroyedException &ignore ) {
   ignore.getMessage();
   doAddOperation = false;
   updateOperationMap(opcode,userName);
 } catch (CqExistsException &ignore) {
    ignore.getMessage();
    FWKINFO("Inside CqExists Exception.");
 }
catch ( Exception &e ) {
   end = 0;
   result = FWK_SEVERE;
   FWKEXCEPTION( "Caught unexpected exception during entry " << opcode
     << " operation: " << e.getMessage() << " : "<< e.getName() <<" exiting task." );
 }
 meter.checkPace();
 now = ACE_OS::gettimeofday();
}
FWKINFO( "doEntryOperations did " << creates << " creates, " << puts << " puts, " << destroy << " destroys, " << putAll << " putAll");
FWKINFO("doEntryOperation completed");
return result;
}

 // -------------------------------------------------------------------------------------------
 int32_t MultiUser::cqForMU(){
   int32_t result=FWK_SUCCESS;
   FWKINFO("Inside cqForMU task ");
   try{
     CqQueryPtr qry;
     RegionPtr m_Region=proxyRegionMap[userlist.at(0)];
     RegionServicePtr mu_cache = authCacheMap[userlist.at(0)];
     char cqName[32] = {'\0'};
     for(uint32_t i =0 ;i < userlist.size();i++){
       std::string userName=userlist.at(i);
       char name[32] = {'\0'};
       sprintf(name,"%d",g_test->getClientId());
       ACE_OS::sprintf(cqName,"cq-%s",userName.c_str());
       std::string key = std::string( "CQLISTENER_") + std::string(name) + std::string( "_") + std::string(cqName);
       FWKINFO("The key is " << key);
       QueryServicePtr qs = mu_cache->getQueryService();
       CqAttributesFactory cqFac;
       CqListenerPtr cqLstner(new MyCqListener(i));
       cqFac.addCqListener(cqLstner);
       CqAttributesPtr cqAttr = cqFac.create();
       qry = qs->newCq(cqName,"select * from /Portfolios where TRUE",cqAttr);
       qry->execute();
       bbIncrement( "CQListenerBB", key );
     }
   }catch ( Exception e ) {
        FWKEXCEPTION( "MultiUser::cqForMU FAILED -- caught exception: " << e.getMessage() );
   } catch ( FwkException& e ) {
        FWKEXCEPTION( "MultiUser::cqForMU FAILED -- caught test exception: " << e.getMessage() );
   }
   return result;
 }

 //--------------------------------------------------------------------------------------------
 int32_t MultiUser::validateEntryOperationsForPerUser(){
   int32_t result=FWK_SUCCESS;
   FWKINFO("Inside validation task");
   bool opFound=false;
   for(uint32_t i=0;i<userlist.size();i++) {
     std::map<std::string,int>validateOpMap;
     std::map<std::string,int>validateExpMap;
     std::string userName=userlist.at(i);
     validateOpMap=operationMap[userName];
     validateExpMap=exceptionMap[userName];
     std::map<std::string,int>::iterator validateOpMapItr;
     for(validateOpMapItr=validateOpMap.begin();validateOpMapItr!=validateOpMap.end();validateOpMapItr++){
       std::string operation=validateOpMapItr->first;
       int totalopCnt=validateOpMap[operation];
       int notAuthCnt=validateExpMap[operation];
       FWKINFO("Operation is " << operation << " for user " << userName);
       std::vector<std::string>vectorList=userToRolesMap[userName];
       for(uint32_t i =0 ;i < vectorList.size();i++){
         if(vectorList.at(i)==operation) {
           opFound=true;
	   break;
	 }
         else
	   opFound=false;
       }
       if(opFound) {
         if((totalopCnt != 0 )&& (notAuthCnt == 0)) {
           FWKINFO("Task passed sucessfully with total operation = " << totalopCnt << " for user " << userName);
         }
         else {
	   FWKEXCEPTION("Task failed for user " << userName << " NotAuthorizedException found for operation = " << notAuthCnt << " while expected was 0 ");
	 }
       }
       else {
	 if(totalopCnt == notAuthCnt) {
	   FWKINFO("Task passed sucessfully and got the expected number of notAuth exception = " << notAuthCnt << " with total number of operation = " << totalopCnt);
          }
          else {
	    FWKEXCEPTION("Task failed ,Expected NotAuthorizedException cnt to be = " << totalopCnt << " but found " << notAuthCnt);
       }
     }
   }
 }
 return result;
}

 // -----------------------------------------------------------------------------------------------------

 int32_t MultiUser::validateCqOperationsForPerUser(){
   int32_t result=FWK_SUCCESS;
   FWKINFO("Inside validation task");
   try {
     RegionPtr m_Region=proxyRegionMap[userlist.at(0)];
     RegionServicePtr mu_cache = authCacheMap[userlist.at(0)];
     QueryServicePtr qs=mu_cache->getQueryService();
     char buf[1024];
     char cqName[32] = {'\0'};
     for(uint32_t i =0 ;i < userlist.size();i++){
       std::string userName=userlist.at(i);
       ACE_OS::sprintf(cqName,"cq-%s",userName.c_str());
       CqQueryPtr cqy=qs->getCq(cqName);
       CqStatisticsPtr cqStats = cqy->getStatistics();
       CqAttributesPtr cqAttr = cqy->getCqAttributes();
       VectorOfCqListener vl;
       cqAttr->getCqListeners(vl);
       sprintf(buf, "number of listeners for cq[%s] is %d", cqy->getName(), vl.size());
       FWKINFO(buf);
       MyCqListener*  ml = dynamic_cast<MyCqListener*>(vl[0].ptr());
       sprintf(buf, "MyCount for cq[%s] Listener: numInserts[%d], numDeletes[%d], numUpdates[%d], numEvents[%d]",
             cqy->getName(), ml->getNumInserts(), ml->getNumDeletes(), ml->getNumUpdates(), ml->getNumEvents());
       FWKINFO(buf);
       sprintf(buf, "cq[%s] From CqStatistics : numInserts[%d], numDeletes[%d], numUpdates[%d], numEvents[%d]",
    	        cqy->getName(), cqStats->numInserts(), cqStats->numDeletes(), cqStats->numUpdates(), cqStats->numEvents());
       FWKINFO(buf);
       if(ml->getNumInserts()==cqStats->numInserts() && ml->getNumUpdates()== cqStats->numUpdates() && ml->getNumDeletes()== cqStats->numDeletes()
               && ml->getNumEvents()==cqStats->numEvents() )
         result = FWK_SUCCESS;
       else
         FWKEXCEPTION(" accumulative event count incorrect");
     }
   } catch ( Exception e ) {
     FWKEXCEPTION( "MultiUser::validateCqOperationsForPerUser() FAILED -- caught exception: " << e.getMessage() );
   } catch ( FwkException& e ) {
     FWKEXCEPTION( "MultiUser::validateCqOperationsForPerUser() FAILED -- caught test exception: " << e.getMessage() );
   } catch ( ... ) {
     FWKEXCEPTION( "MultiUser::validateCqOperationsForPerUser() FAILED -- caught unknown exception." );
   }
   return result;
 }
//---------------------------------------------------------------------------------------------
void MultiUser::updateOperationMap(std::string opcode, std::string userName){
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
  std::map<std::string,int>opMap=operationMap[userName];
  totalOperation = opMap[opcode];
     if(totalOperation == 0)
       opMap[opcode]= 1;
     else
       opMap[opcode] = ++totalOperation;
   FWKINFO("SP OpMap:opcode is "<<opcode <<" users is "<< userName);
   operationMap[userName]=opMap;
}
//---------------------------------------------------------------------------------------------
void MultiUser::updateExceptionMap(std::string opcode, std::string userName){
  ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
  updateOperationMap(opcode,userName);
  std::map<std::string,int>expMap=exceptionMap[userName];
  notAuthzCount = expMap[opcode];
   if(notAuthzCount == 0)
     expMap[opcode] = 1;
   else
     expMap[opcode] = ++notAuthzCount;
   FWKINFO("SP ExpMap:opcode is "<<opcode <<" users is "<< userName);
   FWKINFO("Inside updateExceptionMap notAuthcount is = " << notAuthzCount << " for opcode = " << opcode << " for user " << userName);
   exceptionMap[userName]=expMap;
}
//---------------------------------------------------------------------------------------------

void MultiUser::setAdminRole(std::string userName){
  FWKINFO("Setting Admin role for " << userName);
  adminRole.push_back("create");
  adminRole.push_back("update");
  adminRole.push_back("get");
  adminRole.push_back("getServerKeys");
  adminRole.push_back("putAll");
  adminRole.push_back("destroy");
  adminRole.push_back("executefunction");
  adminRole.push_back("query");
  adminRole.push_back("cq");
  userToRolesMap[userName]=adminRole;
}

void MultiUser::setReaderRole(std::string userName){
  FWKINFO("Setting Reader role for " << userName);
  readerRole.push_back("get");
  readerRole.push_back("getServerKeys");
  readerRole.push_back("cq");
  userToRolesMap[userName]=readerRole;
}

void MultiUser::setWriterRole(std::string userName){
  FWKINFO("Setting Writer role for " << userName);
  writerRole.push_back("create");
  writerRole.push_back("update");
  writerRole.push_back("putAll");
  writerRole.push_back("destroy");
  writerRole.push_back("executefunction");
  userToRolesMap[userName]=writerRole;
}

void MultiUser::setQueryRole(std::string userName){
  FWKINFO("Setting Query role for " << userName);
  queryRole.push_back("query");
  queryRole.push_back("cq");
  userToRolesMap[userName]=queryRole;
}
// ------------------------------------------------------------------------------------------------
