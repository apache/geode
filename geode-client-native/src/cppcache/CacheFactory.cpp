/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
*/

#include "CacheFactory.hpp"
#include "impl/CppCacheLibrary.hpp"
#include "Cache.hpp"
#include "impl/CacheImpl.hpp"
#include "SystemProperties.hpp"
#include "PoolManager.hpp"
#include "impl/PoolAttributes.hpp"
#include "impl/CacheConfig.hpp"
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Guard_T.h>
#include <map>
#include <string>
#include "impl/DistributedSystemImpl.hpp"
#include "impl/SerializationRegistry.hpp"
#include "impl/PdxInstantiator.hpp"
#include "impl/PdxEnumInstantiator.hpp"
#include "impl/PdxType.hpp"
#include "impl/PdxTypeRegistry.hpp"

#include "version.h"

#define DEFAULT_DS_NAME "default_GemfireDS"
#define DEFAULT_CACHE_NAME "default_GemfireCache"
#define DEFAULT_SERVER_PORT 40404
#define DEFAULT_SERVER_HOST "localhost"

extern ACE_Recursive_Thread_Mutex* g_disconnectLock;

bool Cache_CreatedFromCacheFactory = false;

namespace gemfire
{
  ACE_Recursive_Thread_Mutex g_cfLock;

typedef std::map< std::string, CachePtr > StringToCachePtrMap;

void* CacheFactory::m_cacheMap=(void*)NULL;

CacheFactoryPtr CacheFactory::default_CacheFactory = NULLPTR;

PoolPtr CacheFactory::createOrGetDefaultPool()
{
  ACE_Guard< ACE_Recursive_Thread_Mutex > connectGuard( *g_disconnectLock );

  CachePtr cache = CacheFactory::getAnyInstance();

  if (cache != NULLPTR && cache->isClosed() == false && cache->m_cacheImpl->getDefaultPool() != NULLPTR)
    return cache->m_cacheImpl->getDefaultPool();

    PoolPtr pool = PoolManager::find(DEFAULT_POOL_NAME);
    
    //if default_poolFactory is null then we are not using latest API....
    if(pool == NULLPTR && Cache_CreatedFromCacheFactory)
    {
      if(default_CacheFactory != NULLPTR)
        pool = default_CacheFactory->determineDefaultPool(cache);
      default_CacheFactory = NULLPTR;
    }

    return pool;
}

CacheFactoryPtr CacheFactory::createCacheFactory(const PropertiesPtr& configPtr)
{
  //need to create PoolFactory instance
  CacheFactoryPtr cf(new CacheFactory(configPtr));
  return cf;
}

void CacheFactory::init()
{
  if(m_cacheMap==(void*)NULL)
  {
     m_cacheMap = (void*) new StringToCachePtrMap();
  }
  if(!(StringToCachePtrMap*)m_cacheMap)
    throw OutOfMemoryException("CacheFactory::create: ");
}

CachePtr CacheFactory::create( const char* name, DistributedSystemPtr& system,
    const CacheAttributesPtr& attrs )
{
  ACE_Guard< ACE_Recursive_Thread_Mutex > connectGuard( *g_disconnectLock );
  CachePtr cptr;
  CacheFactory::create_( name, system, "", cptr,false, false );
  cptr->m_cacheImpl->setAttributes( attrs );
  try {
    std::string file = system->getSystemProperties()->cacheXMLFile();
    if ( file != "" ) {
      cptr->initializeDeclarativeCache(file.c_str());
    } else {
      cptr->m_cacheImpl->initServices( );
    }
  } catch ( gemfire::RegionExistsException& ) {
    LOGWARN("Attempt to create existing regions declaratively");
  }
  catch(gemfire::Exception&) {
    if(!cptr->isClosed()) {
      cptr->close();
      cptr = NULLPTR;
    }
    throw;
  }
  catch(...) {
    if(!cptr->isClosed()) {
      cptr->close();
      cptr = NULLPTR;
    }
    throw gemfire::UnknownException("Exception thrown in CacheFactory::create");
  }
  return cptr;
}

CachePtr CacheFactory::create(const char* name, DistributedSystemPtr& system,
    const char* cacheXml, const CacheAttributesPtr& attrs , bool ignorePdxUnreadFields, bool readPdxSerialized)
{
  ACE_Guard< ACE_Recursive_Thread_Mutex > connectGuard( *g_disconnectLock );

  CachePtr cptr;
  CacheFactory::create_( name, system, "", cptr, ignorePdxUnreadFields,  readPdxSerialized );
  cptr->m_cacheImpl->setAttributes( attrs );
  try {
    if(cacheXml != 0 && strlen(cacheXml) > 0) {
      cptr->initializeDeclarativeCache(cacheXml);
    } else {
      std::string file = system->getSystemProperties()->cacheXMLFile();
      if ( file != "" ) {
        cptr->initializeDeclarativeCache(file.c_str());
      } else {
        cptr->m_cacheImpl->initServices( );
      }
    }
  } catch ( gemfire::RegionExistsException& ) {
    LOGWARN("Attempt to create existing regions declaratively");
  }
  catch(gemfire::Exception&) {
    if(!cptr->isClosed()) {
      cptr->close();
      cptr = NULLPTR;
    }
    throw;
  }
  catch(...) {
    if(!cptr->isClosed()) {
      cptr->close();
      cptr = NULLPTR;
    }
    throw gemfire::UnknownException("Exception thrown in CacheFactory::create");
  }

//Comment the original parsing code.
// handleXML( cptr, cachexml, system );
  return cptr;
}


void CacheFactory::create_(const char* name, DistributedSystemPtr& system, const char* id_data, CachePtr& cptr, bool ignorePdxUnreadFields, bool readPdxSerialized)
{
  //if(!DistributedSystem::isConnected())
    //DistributedSystem::connect(DEFAULT_DS_NAME);

  CppCacheLibrary::initLib();

  cptr = NULLPTR;
  if(!(StringToCachePtrMap*)m_cacheMap)
    throw IllegalArgumentException("CacheFactory::create: cache map is not initialized");
  if(system == NULLPTR)
    throw IllegalArgumentException("CacheFactory::create: system uninitialized");
  if (name == NULL) {
    throw IllegalArgumentException("CacheFactory::create: name is NULL");
  }
  if (name[0] == '\0') {
    name = "NativeCache";
  }

  CachePtr cp = NULLPTR;
  basicGetInstance(system, true, cp);
  if ((cp == NULLPTR) || (cp->isClosed()==true)) {
     Cache* cep = new Cache(name, system, id_data, ignorePdxUnreadFields, readPdxSerialized );
     if(!cep)
     {
        throw OutOfMemoryException("Out of Memory");
     }
     cptr = cep;
     std::string key(system->getName());
     if (cp != NULLPTR) {
       ACE_Guard<ACE_Recursive_Thread_Mutex> guard(g_cfLock);
       ((StringToCachePtrMap*)m_cacheMap)->erase(
           ((StringToCachePtrMap*)m_cacheMap)->find(key));
     }
     std::pair<std::string, CachePtr> pc(key, cptr);
     ACE_Guard<ACE_Recursive_Thread_Mutex> guard(g_cfLock);
     ((StringToCachePtrMap*)m_cacheMap)->insert(pc);
     return;
  }
  throw CacheExistsException("an open cache exists with the specified system");
}

CachePtr CacheFactory::getInstance(const DistributedSystemPtr& system)
{
    CachePtr cptr;
    CppCacheLibrary::initLib();
    if (system == NULLPTR) {
      throw IllegalArgumentException("CacheFactory::getInstance: system uninitialized");
    }
    GfErrType err =    basicGetInstance(system, false, cptr);
    GfErrTypeToException("CacheFactory::getInstance", err);
    return cptr;
}

CachePtr CacheFactory::getInstanceCloseOk(const DistributedSystemPtr& system )
{
    CachePtr cptr;
    CppCacheLibrary::initLib();
    if (system == NULLPTR) {
      throw IllegalArgumentException("CacheFactory::getInstanceClosedOK: system uninitialized");
    }
    GfErrType err =    basicGetInstance(system, true, cptr);
    GfErrTypeToException("CacheFactory::getInstanceCloseOk", err);
    return cptr;
}

CachePtr CacheFactory::getAnyInstance( )
{
  return getAnyInstance(true);
//suds    throw EntryNotFoundException("CacheFactory::getAnyInstance: not found");
}

CachePtr CacheFactory::getAnyInstance(bool throwException)
{
  CachePtr cptr;
  CppCacheLibrary::initLib();
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(g_cfLock);
  if(((StringToCachePtrMap*)m_cacheMap)->empty()==true)
  {
    if(throwException)
      throw EntryNotFoundException("CacheFactory::getAnyInstance: not found, no cache created yet");
    else
      return NULLPTR;
     //makes no sense to throw this, because i may have never created a cache
    // return NULL;
  }
  for(StringToCachePtrMap::iterator p = ((StringToCachePtrMap*)m_cacheMap)->begin(); p!= ((StringToCachePtrMap*)m_cacheMap)->end(); ++p)
  {
     if(!(p->second->isClosed()))
     {
        cptr = p->second;
        return cptr;
     }
  }
  return NULLPTR;
}

const char* CacheFactory::getVersion()
{
  return GEMFIRE_VERSION;
}

const char* CacheFactory::getProductDescription( )
{
    return GEMFIRE_PRODUCTNAME " " GEMFIRE_VERSION " (" GEMFIRE_BITS ") " GEMFIRE_BUILDID " " GEMFIRE_BUILDDATE;
}

CacheFactory::CacheFactory()
{
  ignorePdxUnreadFields = false;
  pdxReadSerialized = false;
  dsProp = NULLPTR;
  pf = NULLPTR;
}

CacheFactory::CacheFactory(const PropertiesPtr dsProps)
{
  ignorePdxUnreadFields = false;
  pdxReadSerialized = false;
  this->dsProp = dsProps;
  this->pf = NULLPTR;
}

CachePtr CacheFactory::create()
{
	//bool pdxIgnoreUnreadFields = false;
	//bool pdxReadSerialized = false;

	ACE_Guard< ACE_Recursive_Thread_Mutex > connectGuard( *g_disconnectLock );
  DistributedSystemPtr dsPtr = NULLPTR;

  //should we compare deafult DS properties here??
  if(DistributedSystem::isConnected())
    dsPtr = DistributedSystem::getInstance();
  else
  {
    dsPtr = DistributedSystem::connect(DEFAULT_DS_NAME, dsProp);
    LOGFINE("CacheFactory called DistributedSystem::connect");
  }

  CachePtr cache = NULLPTR;

  cache = getAnyInstance(false);
  
  if(cache == NULLPTR)
  {
    CacheFactoryPtr cacheFac(this); 
    default_CacheFactory = cacheFac;
    Cache_CreatedFromCacheFactory = true;
    cache = create(DEFAULT_CACHE_NAME ,dsPtr, dsPtr->getSystemProperties()->cacheXMLFile() , NULLPTR, ignorePdxUnreadFields, pdxReadSerialized);
    //if(cache->m_cacheImpl->getDefaultPool() == NULLPTR)
      //determineDefaultPool(cache);
  }
  else
  {
    if(cache->m_cacheImpl->getDefaultPool() != NULLPTR)
    {
      //we already choose or created deafult pool
      determineDefaultPool(cache);
    }
    else
    {
      //not yet created, create from first cacheFactory instance 
      if(default_CacheFactory != NULLPTR)
      {
        default_CacheFactory->determineDefaultPool(cache);
        default_CacheFactory = NULLPTR;
       }
       determineDefaultPool(cache);
     }
    }

  SerializationRegistry::addType(GemfireTypeIdsImpl::PDX, PdxInstantiator::createDeserializable);
  SerializationRegistry::addType(GemfireTypeIds::CacheableEnum, PdxEnumInstantiator::createDeserializable);
  SerializationRegistry::addType(GemfireTypeIds::PdxType, PdxType::CreateDeserializable);
  PdxTypeRegistry::setPdxIgnoreUnreadFields(cache->getPdxIgnoreUnreadFields());
  PdxTypeRegistry::setPdxReadSerialized(cache->getPdxReadSerialized());

  return cache;
}

PoolPtr CacheFactory::determineDefaultPool(CachePtr cachePtr)
 {
    PoolPtr pool = NULLPTR;
   HashMapOfPools allPools = PoolManager::getAll();
   int currPoolSize = allPools.size();

   //means user has not set any pool attributes 
   if(this->pf == NULLPTR)
   {
    this->pf = getPoolFactory();
    if(currPoolSize == 0)
    {     
      if(!this->pf->m_addedServerOrLocator)
      {
        this->pf->addServer(DEFAULT_SERVER_HOST, DEFAULT_SERVER_PORT); 
      }
 
      pool = this->pf->create(DEFAULT_POOL_NAME);
      //creatubg default pool so setting this as default pool
      LOGINFO("Set default pool with localhost:40404");
      cachePtr->m_cacheImpl->setDefaultPool(pool);
      return pool;
    }
    else if(currPoolSize == 1)
    {
      pool = allPools.begin().second();
      LOGINFO("Set default pool from existing pool.");
      cachePtr->m_cacheImpl->setDefaultPool(pool);
      return pool;
    }
    else
    {
      //can't set anything as deafult pool
      return NULLPTR;
    }
  }
  else
  {
    PoolPtr defaulPool = cachePtr->m_cacheImpl->getDefaultPool(); 
 
    if(!this->pf->m_addedServerOrLocator)
    {
      this->pf->addServer(DEFAULT_SERVER_HOST, DEFAULT_SERVER_PORT); 
    }

    if(defaulPool != NULLPTR)
    {
      //once default pool is created, we will not create
      if (*(defaulPool->m_attrs) == *(this->pf->m_attrs))
        return defaulPool;
      else
      {
        throw IllegalStateException("Existing cache's default pool was not compatible");
      }
    }  

    pool = NULLPTR;

    //return any existing pool if it matches
    for ( HashMapOfPools::Iterator iter = allPools.begin( );
                    iter != allPools.end( );++iter ) {
        PoolPtr currPool( iter.second( ) );
        if (*(currPool->m_attrs) == *(this->pf->m_attrs))
        {
          return currPool;
        }
    }

    //defaul pool is null
    GF_DEV_ASSERT(defaulPool == NULLPTR);
    
    if(defaulPool == NULLPTR)
    {
      pool = this->pf->create(DEFAULT_POOL_NAME);
      LOGINFO("Created default pool");
      //creating default so setting this as defaul pool
      cachePtr->m_cacheImpl->setDefaultPool(pool);
    }

    return pool;
  }
 }
 
PoolFactoryPtr CacheFactory::getPoolFactory()
{
  if (this->pf == NULLPTR)
  {
    this->pf = PoolManager::createFactory();
  }
  return this->pf;
}

CacheFactory::~CacheFactory()
{
}
void CacheFactory::cleanup()
{
  if(m_cacheMap!=NULL)
  {
    if(((StringToCachePtrMap*)m_cacheMap)->empty()==true)
       ((StringToCachePtrMap*)m_cacheMap)->clear();
     delete ((StringToCachePtrMap*)m_cacheMap);
     m_cacheMap=NULL;
  }
}

GfErrType CacheFactory::basicGetInstance(const DistributedSystemPtr& system,
    const bool closeOk, CachePtr& cptr)
{
    GfErrType err = GF_NOERR;
    if (system == NULLPTR) {
       return GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION;
    }
    cptr = NULLPTR;
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(g_cfLock);
    if(((StringToCachePtrMap*)m_cacheMap)->empty()==true)
    {
       return GF_CACHE_ENTRY_NOT_FOUND ;
    }
    std::string key(system->getName());
    StringToCachePtrMap::iterator p = ((StringToCachePtrMap*)m_cacheMap)->find(key);
    if(p!= ((StringToCachePtrMap*)m_cacheMap)->end())
    {
       if((closeOk==true) ||  (!(p->second->isClosed())))
       {
          cptr = p->second;
       }
       else
       {
          return GF_CACHE_ENTRY_NOT_FOUND ;
       }
    }
    else
    {
       return GF_CACHE_ENTRY_NOT_FOUND ;
    }
    return err;
}

void CacheFactory::handleXML( CachePtr& cachePtr, const char* cachexml, DistributedSystemPtr& system )
{
  CacheConfig config( cachexml );

  RegionConfigMapT regionMap = config.getRegionList();
  RegionConfigMapT::const_iterator iter = regionMap.begin();
  while (  iter != regionMap.end() ) {
    std::string regionName = (*iter).first;
    RegionConfigPtr regConfPtr = (*iter).second;

    AttributesFactory af;
    af.setScope( regConfPtr->scope() );
    af.setLruEntriesLimit( regConfPtr->getLruEntriesLimit() );
    af.setConcurrencyLevel( regConfPtr->getConcurrency() );
    af.setInitialCapacity( regConfPtr->entries() );
    af.setCachingEnabled( regConfPtr->getCaching() );

    RegionAttributesPtr regAttrsPtr;
    regAttrsPtr = af.createRegionAttributes( );

    cachePtr->createRegion( regionName.c_str(), regAttrsPtr );
    ++iter;
  }
}

CacheFactoryPtr CacheFactory::set(const char* name, const char* value){
  if(this->dsProp == NULLPTR)
    this->dsProp = Properties::create();
  this->dsProp->insert(name, value);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}

CacheFactoryPtr CacheFactory::setFreeConnectionTimeout(int connectionTimeout){
  getPoolFactory()->setFreeConnectionTimeout(connectionTimeout);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setLoadConditioningInterval(int loadConditioningInterval){
  getPoolFactory()->setLoadConditioningInterval(loadConditioningInterval);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setSocketBufferSize(int bufferSize){
  getPoolFactory()->setSocketBufferSize(bufferSize);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setThreadLocalConnections(bool threadLocalConnections){
  getPoolFactory()->setThreadLocalConnections( threadLocalConnections );
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setReadTimeout(int timeout){
  getPoolFactory()->setReadTimeout(timeout);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setMinConnections(int minConnections){
  getPoolFactory()->setMinConnections(minConnections);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setMaxConnections(int maxConnections){
  getPoolFactory()->setMaxConnections(maxConnections);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setIdleTimeout(long idleTimeout){
  getPoolFactory()->setIdleTimeout(idleTimeout);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setRetryAttempts(int retryAttempts){
  getPoolFactory()->setRetryAttempts(retryAttempts);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setPingInterval(long pingInterval){
  getPoolFactory()->setPingInterval(pingInterval);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setUpdateLocatorListInterval(long updateLocatorListInterval){
  getPoolFactory()->setUpdateLocatorListInterval(updateLocatorListInterval);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setStatisticInterval(int statisticInterval){
  getPoolFactory()->setStatisticInterval(statisticInterval);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setServerGroup(const char* group){
  getPoolFactory()->setServerGroup(group);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::addLocator(const char* host, int port){
  getPoolFactory()->addLocator(host,port);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::addServer(const char* host, int port){
  getPoolFactory()->addServer(host,port);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setSubscriptionEnabled(bool enabled){
  getPoolFactory()->setSubscriptionEnabled(enabled);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setSubscriptionRedundancy(int redundancy){
  getPoolFactory()->setSubscriptionRedundancy(redundancy);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setSubscriptionMessageTrackingTimeout(int messageTrackingTimeout){
  getPoolFactory()->setSubscriptionMessageTrackingTimeout(messageTrackingTimeout);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setSubscriptionAckInterval(int ackInterval){
  getPoolFactory()->setSubscriptionAckInterval(ackInterval);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}
CacheFactoryPtr CacheFactory::setMultiuserAuthentication(bool multiuserAuthentication){
  getPoolFactory()->setMultiuserAuthentication(multiuserAuthentication);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}

CacheFactoryPtr CacheFactory::setPRSingleHopEnabled(bool enabled) {
  getPoolFactory()->setPRSingleHopEnabled(enabled);
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}

CacheFactoryPtr CacheFactory::setPdxIgnoreUnreadFields(bool ignore) {
  ignorePdxUnreadFields = ignore;
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}

CacheFactoryPtr CacheFactory::setPdxReadSerialized(bool prs) {
  pdxReadSerialized = prs;
  CacheFactoryPtr cfPtr(this);
  return cfPtr;
}

}
