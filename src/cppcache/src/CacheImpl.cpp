/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
* This product is protected by U.S. and international copyright
* and intellectual property laws. Pivotal products are covered by
* more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "CacheImpl.hpp"
#include <string.h>
#include <gfcpp/CacheStatistics.hpp>
#include "Utils.hpp"
#include "LocalRegion.hpp"
#include "ExpiryTaskManager.hpp"
#include <gfcpp/PersistenceManager.hpp>
#include "RegionExpiryHandler.hpp"
#include "TcrMessage.hpp"
#include "ThinClientRegion.hpp"
#include "ThinClientHARegion.hpp"
#include "ThinClientPoolRegion.hpp"
#include "ThinClientPoolDM.hpp"
#include <gfcpp/PoolManager.hpp>
#include <gfcpp/SystemProperties.hpp>
#include "Version.hpp"
#include "ClientProxyMembershipID.hpp"
#include "AutoDelete.hpp"
#include <string>
#include "ace/OS.h"
#include <gfcpp/PoolManager.hpp>
#include <gfcpp/RegionAttributes.hpp>
#include "ThinClientPoolHADM.hpp"
#include "InternalCacheTransactionManager2PCImpl.hpp"
#include "PdxTypeRegistry.hpp"

using namespace gemfire;

ExpiryTaskManager* CacheImpl::expiryTaskManager = NULL;
CacheImpl* CacheImpl::s_instance = NULL;
volatile bool CacheImpl::s_networkhop = false;
volatile int CacheImpl::s_blacklistBucketTimeout = 0;
ACE_Recursive_Thread_Mutex CacheImpl::s_nwHopLock;
volatile int8 CacheImpl::s_serverGroupFlag = 0;
MemberListForVersionStampPtr CacheImpl::s_versionStampMemIdList = NULLPTR;

#define DEFAULT_LRU_MAXIMUM_ENTRIES 100000

ExpiryTaskManager* getCacheImplExpiryTaskManager() {
  return CacheImpl::expiryTaskManager;
}

CacheImpl::CacheImpl(Cache* c, const char* name, DistributedSystemPtr sys,
                     const char* id_data, bool iPUF, bool readPdxSerialized)
    : m_defaultPool(NULLPTR),
      m_ignorePdxUnreadFields(iPUF),
      m_readPdxSerialized(readPdxSerialized),
      m_closed(false),
      m_initialized(false),
      m_distributedSystem(sys),
      m_implementee(c),
      m_cond(m_mutex),
      m_attributes(NULLPTR),
      m_evictionControllerPtr(NULL),
      m_tcrConnectionManager(NULL),
      m_remoteQueryServicePtr(NULLPTR),
      m_destroyPending(false),
      m_initDone(false),
      m_adminRegion(NULLPTR) {
  m_cacheTXManager = InternalCacheTransactionManager2PCPtr(
      new InternalCacheTransactionManager2PCImpl(c));

  m_name = Utils::copyString(name);

  if (!DistributedSystem::isConnected()) {
    throw IllegalArgumentException("DistributedSystem is not up");
  }
  m_regions = new MapOfRegionWithLock();
  SystemProperties* prop = DistributedSystem::getSystemProperties();
  if (prop && prop->heapLRULimitEnabled()) {
    m_evictionControllerPtr = new EvictionController(
        prop->heapLRULimit(), prop->heapLRUDelta(), this);
    m_evictionControllerPtr->start();
    LOGINFO("Heap LRU eviction controller thread started");
  }
  /*
  else {
    LOGFINE("Eviction controller is NULL");
  }
  */

  ClientProxyMembershipID::init(sys->getName());

  m_cacheStats = new CachePerfStats;

  s_instance = this;
  m_initialized = true;
}

CacheImpl::CacheImpl(Cache* c, const char* name, DistributedSystemPtr sys,
                     bool iPUF, bool readPdxSerialized)
    : m_defaultPool(NULLPTR),
      m_ignorePdxUnreadFields(iPUF),
      m_readPdxSerialized(readPdxSerialized),
      m_closed(false),
      m_initialized(false),
      m_distributedSystem(sys),
      m_implementee(c),
      m_cond(m_mutex),
      m_attributes(NULLPTR),
      m_evictionControllerPtr(NULL),
      m_tcrConnectionManager(NULL),
      m_remoteQueryServicePtr(NULLPTR),
      m_destroyPending(false),
      m_initDone(false),
      m_adminRegion(NULLPTR) {
  m_cacheTXManager = InternalCacheTransactionManager2PCPtr(
      new InternalCacheTransactionManager2PCImpl(c));

  m_name = Utils::copyString(name);
  if (!DistributedSystem::isConnected()) {
    throw IllegalArgumentException("DistributedSystem is not connected");
  }
  m_regions = new MapOfRegionWithLock();
  SystemProperties* prop = DistributedSystem::getSystemProperties();
  if (prop && prop->heapLRULimitEnabled()) {
    m_evictionControllerPtr = new EvictionController(
        prop->heapLRULimit(), prop->heapLRUDelta(), this);
    m_evictionControllerPtr->start();
    LOGINFO("Heap LRU eviction controller thread started");
  }
  /*
  else {
    LOGFINE("Eviction controller is NULL");
  }
  */

  ClientProxyMembershipID::init(sys->getName());

  m_cacheStats = new CachePerfStats;

  s_instance = this;
  m_initialized = true;
}

void CacheImpl::initServices() {
  m_tcrConnectionManager = new TcrConnectionManager(this);
  PdxTypeRegistry::init();
  CacheImpl::s_versionStampMemIdList =
      MemberListForVersionStampPtr(new MemberListForVersionStamp());
  if (!m_initDone && m_attributes != NULLPTR && m_attributes->getEndpoints()) {
    if (PoolManager::getAll().size() > 0 && getCacheMode()) {
      LOGWARN(
          "At least one pool has been created so ignoring cache level "
          "redundancy setting");
    }
    m_tcrConnectionManager->init();
    m_remoteQueryServicePtr = new RemoteQueryService(this);
    // StartAdminRegion
    SystemProperties* prop = DistributedSystem::getSystemProperties();
    if (prop && prop->statisticsEnabled()) {
      m_adminRegion = new AdminRegion(this);
    }
    m_initDone = true;
  }
}

int CacheImpl::blackListBucketTimeouts() { return s_blacklistBucketTimeout; }

void CacheImpl::setBlackListBucketTimeouts() { s_blacklistBucketTimeout += 1; }

bool CacheImpl::getAndResetNetworkHopFlag() {
  ACE_Guard<ACE_Recursive_Thread_Mutex> _lock(s_nwHopLock);
  bool networkhop = CacheImpl::s_networkhop;
  CacheImpl::s_networkhop = false;
  // This log should only appear in tests
  LOGDEBUG("networkhop flag = %d", networkhop);
  return networkhop;
}

int8 CacheImpl::getAndResetServerGroupFlag() {
  int8 serverGroupFlag = CacheImpl::s_serverGroupFlag;
  CacheImpl::s_serverGroupFlag = 0;
  return serverGroupFlag;
}

void CacheImpl::netDown() {
  m_tcrConnectionManager->netDown();

  const HashMapOfPools& pools = PoolManager::getAll();
  PoolPtr currPool = NULLPTR;
  for (HashMapOfPools::Iterator itr = pools.begin(); itr != pools.end();
       itr++) {
    currPool = itr.second();
    try {
      ThinClientPoolHADMPtr poolHADM = dynCast<ThinClientPoolHADMPtr>(currPool);
      poolHADM->netDown();
    } catch (const ClassCastException&) {
      // Expect a ClassCastException for non-HA PoolDMs.
      continue;
    }
  }
}

void CacheImpl::revive() { m_tcrConnectionManager->revive(); }

CacheImpl::RegionKind CacheImpl::getRegionKind(
    const RegionAttributesPtr& rattrs) const {
  RegionKind regionKind = CPP_REGION;
  const char* endpoints = NULL;

  if (m_attributes != NULLPTR &&
      (endpoints = m_attributes->getEndpoints()) != NULL &&
      (m_attributes->getRedundancyLevel() > 0 ||
       m_tcrConnectionManager->isDurable())) {
    regionKind = THINCLIENT_HA_REGION;
  } else if (endpoints != NULL && rattrs->getEndpoints() == NULL) {
    rattrs->setEndpoints(endpoints);
  }

  if ((endpoints = rattrs->getEndpoints()) != NULL) {
    if (strcmp(endpoints, "none") == 0) {
      regionKind = CPP_REGION;
    } else if (regionKind != THINCLIENT_HA_REGION) {
      regionKind = THINCLIENT_REGION;
    }
  } else if (rattrs->getPoolName()) {
    PoolPtr pPtr = PoolManager::find(rattrs->getPoolName());
    if ((pPtr != NULLPTR && (pPtr->getSubscriptionRedundancy() > 0 ||
                             pPtr->getSubscriptionEnabled())) ||
        m_tcrConnectionManager->isDurable()) {
      regionKind = THINCLIENT_HA_REGION;  // As of now ThinClinetHARegion deals
                                          // with Pool as well.
    } else {
      regionKind = THINCLIENT_POOL_REGION;
    }
  }

  return regionKind;
}

int CacheImpl::removeRegion(const char* name) {
  TryReadGuard guardCacheDestroy(m_destroyCacheMutex, m_destroyPending);
  if (m_destroyPending) {
    return 0;
  }

  MapOfRegionGuard guard(m_regions->mutex());
  return m_regions->unbind(name);
}

QueryServicePtr CacheImpl::getQueryService(bool noInit) {
  if (m_defaultPool != NULLPTR) {
    if (m_defaultPool->isDestroyed()) {
      throw IllegalStateException("Pool has been destroyed.");
    }
    return m_defaultPool->getQueryService();
  }

  if (m_remoteQueryServicePtr == NULLPTR) {
    m_tcrConnectionManager->init();
    m_remoteQueryServicePtr = new RemoteQueryService(this);
  }
  if (!noInit) {
    m_remoteQueryServicePtr->init();
  }
  return m_remoteQueryServicePtr;
}

QueryServicePtr CacheImpl::getQueryService(const char* poolName) {
  if (poolName == NULL || strlen(poolName) == 0) {
    throw IllegalArgumentException("PoolName is NULL or not defined..");
  }
  PoolPtr pool = PoolManager::find(poolName);

  if (pool != NULLPTR) {
    if (pool->isDestroyed()) {
      throw IllegalStateException("Pool has been destroyed.");
    }
    return pool->getQueryService();
  } else {
    throw IllegalArgumentException("Pool not found..");
  }
}

CacheImpl::~CacheImpl() {
  if (!m_closed) {
    close();
  }

  if (m_regions != NULL) {
    delete m_regions;
  }

  if (m_name != NULL) {
    delete[] m_name;
  }
}

const char* CacheImpl::getName() const {
  if (m_closed || m_destroyPending) {
    throw CacheClosedException("Cache::getName: cache closed");
  }
  return m_name;
}

bool CacheImpl::isClosed() const { return m_closed; }

void CacheImpl::setAttributes(const CacheAttributesPtr& attrs) {
  if (m_attributes == NULLPTR && attrs != NULLPTR) {
    m_attributes = attrs;
  }
}

void CacheImpl::getDistributedSystem(DistributedSystemPtr& dptr) const {
  if (m_closed || m_destroyPending) {
    throw CacheClosedException("Cache::getDistributedSystem: cache closed");
  }
  dptr = m_distributedSystem;
}

void CacheImpl::sendNotificationCloseMsgs() {
  HashMapOfPools pools = PoolManager::getAll();
  for (HashMapOfPools::Iterator iter = pools.begin(); iter != pools.end();
       ++iter) {
    ThinClientPoolHADM* pool =
        dynamic_cast<ThinClientPoolHADM*>(iter.second().ptr());
    if (pool != NULL) {
      pool->sendNotificationCloseMsgs();
    }
  }
}

void CacheImpl::close(bool keepalive) {
  TcrMessage::setKeepAlive(keepalive);
  // bug #247 fix for durable clients missing events when recycled
  sendNotificationCloseMsgs();
  {
    TryWriteGuard guardCacheDestroy(m_destroyCacheMutex, m_destroyPending);
    if (m_destroyPending) {
      return;
    }
    m_destroyPending = true;
  }

  if (m_closed || (!m_initialized)) return;

  // Close the distribution manager used for queries.
  if (m_remoteQueryServicePtr != NULLPTR) {
    m_remoteQueryServicePtr->close();
    m_remoteQueryServicePtr = NULLPTR;
  }

  // Close AdminRegion
  if (m_adminRegion != NULLPTR) {
    m_adminRegion->close();
    m_adminRegion = NULLPTR;
  }

  // The TCCM gets destroyed when CacheImpl is destroyed, but after that there
  // is still a window for the ping related registered task to get activated
  // because expiryTaskManager is closed in DS::disconnect. If this happens
  // then the handler will work on an already destroyed object which would
  // lead to a SEGV. So cancelling the task in TcrConnectionManager::close().
  if (m_tcrConnectionManager != NULL) {
    m_tcrConnectionManager->close();
  }

  MapOfRegionWithLock regions;
  getSubRegions(regions);

  for (MapOfRegionWithLock::iterator q = regions.begin(); q != regions.end();
       ++q) {
    // TODO: remove dynamic_cast here by having RegionInternal in the regions
    // map
    RegionInternal* rImpl = dynamic_cast<RegionInternal*>((*q).int_id_.ptr());
    if (rImpl != NULL) {
      rImpl->destroyRegionNoThrow(
          NULLPTR, false,
          CacheEventFlags::LOCAL | CacheEventFlags::CACHE_CLOSE);
    }
  }

  if (m_evictionControllerPtr != NULL) {
    m_evictionControllerPtr->stop();
    GF_SAFE_DELETE(m_evictionControllerPtr);
  }

  // Close CachePef Stats
  if (m_cacheStats) {
    m_cacheStats->close();
  }

  PoolManager::close(keepalive);

  LOGFINE("Closed pool manager with keepalive %s",
          keepalive ? "true" : "false");
  PdxTypeRegistry::cleanup();

  // Close CachePef Stats
  if (m_cacheStats) {
    GF_SAFE_DELETE(m_cacheStats);
  }

  m_regions->unbind_all();
  LOGDEBUG("CacheImpl::close( ): destroyed regions.");

  GF_SAFE_DELETE(m_tcrConnectionManager);
  m_cacheTXManager = NULLPTR;
  m_closed = true;

  LOGFINE("Cache closed.");
}

bool CacheImpl::isCacheDestroyPending() const { return m_destroyPending; }

void CacheImpl::setDefaultPool(PoolPtr pool) { m_defaultPool = pool; }

PoolPtr CacheImpl::getDefaultPool() { return m_defaultPool; }

void CacheImpl::validateRegionAttributes(
    const char* name, const RegionAttributesPtr& attrs) const {
  RegionKind kind = getRegionKind(attrs);
  std::string buffer = "Cache::createRegion: \"";
  buffer += name;
  buffer += "\" ";

  if (attrs->m_clientNotificationEnabled && kind == CPP_REGION) {
    buffer +=
        "Client notification can be enabled only for native client region";
    throw UnsupportedOperationException(buffer.c_str());
  }
}

// We'll pass a NULL loader function pointer and let the region.get method to
// do a load using a real C++ loader, instead of passing a member function
// pointer here
void CacheImpl::createRegion(const char* name,
                             const RegionAttributesPtr& aRegionAttributes,
                             RegionPtr& regionPtr) {
  {
    ACE_Guard<ACE_Thread_Mutex> _guard(m_initDoneLock);
    if (!m_initDone) {
      if (!(aRegionAttributes->getPoolName())) {
        m_tcrConnectionManager->init();
        m_remoteQueryServicePtr = new RemoteQueryService(this);
        SystemProperties* prop = DistributedSystem::getSystemProperties();
        if (prop && prop->statisticsEnabled()) {
          m_adminRegion = new AdminRegion(this);
        }
      }
      m_initDone = true;
    }
  }

  if (m_closed || m_destroyPending) {
    throw CacheClosedException("Cache::createRegion: cache closed");
  }
  if (aRegionAttributes == NULLPTR) {
    throw IllegalArgumentException(
        "Cache::createRegion: RegionAttributes is null");
  }
  std::string namestr(name);

  if (namestr.find('/') != std::string::npos) {
    throw IllegalArgumentException(
        "Malformed name string, contains region path seperator '/'");
  }

  validateRegionAttributes(name, aRegionAttributes);
  RegionInternal* rpImpl = NULL;
  {
    // For multi threading and the operations between bind and find seems to be
    // hard to be atomic since a regionImpl needs to be valid before it can be
    // bound
    MapOfRegionGuard guard1(m_regions->mutex());
    RegionPtr tmp;
    if (0 == m_regions->find(namestr, tmp)) {
      char buffer[256];
      ACE_OS::snprintf(
          buffer, 256,
          "Cache::createRegion: \"%s\" region exists in local cache",
          namestr.c_str());
      throw RegionExistsException(buffer);
    }

    CacheStatisticsPtr csptr(new CacheStatistics);
    try {
      rpImpl = createRegion_internal(namestr.c_str(), NULL, aRegionAttributes,
                                     csptr, false);
    } catch (const AuthenticationFailedException&) {
      throw;
    } catch (const AuthenticationRequiredException&) {
      throw;
    } catch (const Exception&) {
      //      LOGERROR( "Cache::createRegion: region creation failed, caught
      //      exception: %s", ex.getMessage() );
      //      ex.printStackTrace();
      throw;
    } catch (std::exception& ex) {
      char buffer[512];
      ACE_OS::snprintf(buffer, 512,
                       "Cache::createRegion: Failed to create Region \"%s\" "
                       "due to unknown exception: %s",
                       namestr.c_str(), ex.what());
      throw UnknownException(buffer);
    } catch (...) {
      char buffer[256];
      ACE_OS::snprintf(buffer, 256,
                       "Cache::createRegion: Failed to create Region \"%s\" "
                       "due to unknown exception.",
                       namestr.c_str());
      throw UnknownException(buffer);
    }
    if (rpImpl == NULL) {
      char buffer[256];
      ACE_OS::snprintf(buffer, 256,
                       "Cache::createRegion: Failed to create Region \"%s\"",
                       namestr.c_str());
      throw RegionCreationFailedException(buffer);
    }
    regionPtr = rpImpl;
    rpImpl->addDisMessToQueue();
    // Instantiate a PersistenceManager object if DiskPolicy is overflow
    if (aRegionAttributes->getDiskPolicy() == DiskPolicyType::OVERFLOWS) {
      PersistenceManagerPtr pmPtr = aRegionAttributes->getPersistenceManager();
      if (pmPtr == NULLPTR) {
        throw NullPointerException(
            "PersistenceManager could not be instantiated");
      }
      PropertiesPtr props = aRegionAttributes->getPersistenceProperties();
      pmPtr->init(regionPtr, props);
      rpImpl->setPersistenceManager(pmPtr);
    }

    rpImpl->acquireReadLock();
    m_regions->bind(regionPtr->getName(), regionPtr);

    // When region is created, added that region name in client meta data
    // service to fetch its
    // metadata for single hop.
    SystemProperties* props = DistributedSystem::getSystemProperties();
    if (!props->isGridClient()) {
      const char* poolName = aRegionAttributes->getPoolName();
      if (poolName != NULL) {
        PoolPtr pool = PoolManager::find(poolName);
        if (pool != NULLPTR && !pool->isDestroyed() &&
            pool->getPRSingleHopEnabled()) {
          ThinClientPoolDM* poolDM =
              dynamic_cast<ThinClientPoolDM*>(pool.ptr());
          if ((poolDM != NULL) &&
              (poolDM->getClientMetaDataService() != NULL)) {
            LOGFINE(
                "enqueued region %s for initial metadata refresh for "
                "singlehop ",
                name);
            poolDM->getClientMetaDataService()->enqueueForMetadataRefresh(
                regionPtr->getFullPath(), 0);
          }
        }
      }
    }
  }

  // schedule the root region expiry if regionExpiry enabled.
  rpImpl->setRegionExpiryTask();
  rpImpl->releaseReadLock();
  //   LOGFINE( "Returning from CacheImpl::createRegion call for Region %s",
  //   regionPtr->getFullPath() );
}

/**
 * Return the existing region (or subregion) with the specified
 * path that already exists or is already mapped into the cache.
 * Whether or not the path starts with a forward slash it is interpreted as a
 * full path starting at a root.
 *
 * @param path the path to the region
 * @param[out] rptr the region pointer that is returned
 * @return the Region or null if not found
 * @throws IllegalArgumentException if path is null, the empty string, or "/"
 */

void CacheImpl::getRegion(const char* path, RegionPtr& rptr) {
  TryReadGuard guardCacheDestroy(m_destroyCacheMutex, m_destroyPending);
  if (m_destroyPending) {
    rptr = NULLPTR;
    return;
  }

  MapOfRegionGuard guard(m_regions->mutex());
  std::string pathstr;
  if (path != NULL) {
    pathstr = path;
  }
  rptr = NULLPTR;
  std::string slash("/");
  if ((path == (void*)NULL) || (pathstr == slash) || (pathstr.length() < 1)) {
    LOGERROR("Cache::getRegion: path [%s] is not valid.", pathstr.c_str());
    throw IllegalArgumentException("Cache::getRegion: path is null or a /");
  }
  std::string fullname = pathstr;
  if (fullname.substr(0, 1) == slash) {
    fullname = pathstr.substr(1);
  }
  // find second separator
  uint32_t idx = static_cast<uint32_t>(fullname.find('/'));
  std::string stepname = fullname.substr(0, idx);
  RegionPtr region;
  if (0 == m_regions->find(stepname, region)) {
    if (stepname == fullname) {
      // done...
      rptr = region;
      return;
    }
    std::string remainder = fullname.substr(stepname.length() + 1);
    if (region != NULLPTR) {
      rptr = region->getSubregion(remainder.c_str());
    } else {
      rptr = NULLPTR;
      return;  // Return null if the parent region was not found.
    }
  }
}

RegionInternal* CacheImpl::createRegion_internal(
    const std::string& name, RegionInternal* rootRegion,
    const RegionAttributesPtr& attrs, const CacheStatisticsPtr& csptr,
    bool shared) {
  if (attrs == NULLPTR) {
    throw IllegalArgumentException(
        "createRegion: "
        "RegionAttributes is null");
  }

  RegionInternal* rptr = NULL;
  RegionKind regionKind = getRegionKind(attrs);
  const char* poolName = attrs->getPoolName();
  const char* regionEndpoints = attrs->getEndpoints();
  const char* cacheEndpoints =
      m_attributes == NULLPTR ? NULL : m_attributes->getEndpoints();

  /*if(m_defaultPool != NULLPTR && (poolName == NULL || strlen(poolName) == 0))
  {
    attrs->setPoolName(m_defaultPool->getName());
  }*/

  if (poolName != NULL) {
    PoolPtr pool = PoolManager::find(poolName);
    if (pool != NULLPTR && !pool->isDestroyed()) {
      bool isMultiUserSecureMode = pool->getMultiuserAuthentication();
      if (isMultiUserSecureMode && (attrs->getCachingEnabled())) {
        LOGERROR(
            "Pool [%s] is in multiuser authentication mode so region local "
            "caching is not supported.",
            poolName);
        throw IllegalStateException(
            "Pool is in multiuser authentication so region local caching is "
            "not supported.");
      }
    }
  }

  if (poolName && strlen(poolName) &&
      ((regionEndpoints && strlen(regionEndpoints)) ||
       (cacheEndpoints && strlen(cacheEndpoints)))) {
    LOGERROR(
        "Cache or region endpoints cannot be specified when pool name is "
        "specified for region %s",
        name.c_str());
    throw IllegalArgumentException(
        "Cache or region endpoints cannot be specified when pool name is "
        "specified");
  }

  DeleteObject<RegionInternal> delrptr(rptr);
  if (regionKind == THINCLIENT_REGION) {
    LOGINFO("Creating region %s with region endpoints %s", name.c_str(),
            attrs->getEndpoints());
    GF_NEW(rptr,
           ThinClientRegion(name, this, rootRegion, attrs, csptr, shared));
    ((ThinClientRegion*)rptr)->initTCR();
    delrptr.noDelete();
  } else if (regionKind == THINCLIENT_HA_REGION) {
    LOGINFO("Creating region %s with subscriptions enabled", name.c_str());
    GF_NEW(rptr,
           ThinClientHARegion(name, this, rootRegion, attrs, csptr, shared));
    ((ThinClientHARegion*)rptr)->initTCR();
    delrptr.noDelete();
  } else if (regionKind == THINCLIENT_POOL_REGION) {
    LOGINFO("Creating region %s attached to pool %s", name.c_str(),
            attrs->getPoolName());
    GF_NEW(rptr,
           ThinClientPoolRegion(name, this, rootRegion, attrs, csptr, shared));
    ((ThinClientPoolRegion*)rptr)->initTCR();
    delrptr.noDelete();
  } else {
    LOGINFO("Creating local region %s", name.c_str());
    GF_NEW(rptr, LocalRegion(name, this, rootRegion, attrs, csptr, shared));
    delrptr.noDelete();
  }
  return rptr;
}

void CacheImpl::rootRegions(VectorOfRegion& regions) {
  regions.clear();
  MapOfRegionGuard guard(m_regions->mutex());
  if (m_regions->current_size() == 0) return;
  regions.reserve(static_cast<int32_t>(m_regions->current_size()));
  for (MapOfRegionWithLock::iterator q = m_regions->begin();
       q != m_regions->end(); ++q) {
    if ((*q).int_id_->isDestroyed() == false) {
      regions.push_back((*q).int_id_);
    }
  }
}

EvictionController* CacheImpl::getEvictionController() {
  return m_evictionControllerPtr;
}

void CacheImpl::readyForEvents() {
  bool autoReadyForEvents =
      DistributedSystem::getSystemProperties()->autoReadyForEvents();
  bool isDurable = m_tcrConnectionManager->isDurable();

  if (!isDurable && autoReadyForEvents) {
    LOGERROR(
        "Only durable clients or clients with the "
        "auto-ready-for-events property set to false should call "
        "readyForEvents()");
    throw IllegalStateException(
        "Only durable clients or clients with the "
        "auto-ready-for-events property set to false should call "
        "readyForEvents()");
  }

  // Send the CLIENT_READY message to the server
  if (m_tcrConnectionManager->getNumEndPoints() > 0 && isDurable) {
    m_tcrConnectionManager->readyForEvents();
    return;
  }

  const HashMapOfPools& pools = PoolManager::getAll();
  if (pools.empty()) throw IllegalStateException("No pools found.");
  PoolPtr currPool = NULLPTR;
  for (HashMapOfPools::Iterator itr = pools.begin(); itr != pools.end();
       itr++) {
    currPool = itr.second();
    LOGDEBUG("Sending readyForEvents( ) with pool %s", currPool->getName());
    try {
      try {
        ThinClientPoolHADMPtr poolHADM =
            dynCast<ThinClientPoolHADMPtr>(currPool);
        poolHADM->readyForEvents();
      } catch (const ClassCastException&) {
        // Expect a ClassCastException for non-HA PoolDMs.
        continue;
      }
    } catch (Exception& ex) {
      LOGWARN("readyForEvents( ) failed for pool %s with exception: %s",
              currPool->getName(), ex.getMessage());
    }
  }
}

bool CacheImpl::getEndpointStatus(const std::string& endpoint) {
  const HashMapOfPools& pools = PoolManager::getAll();
  std::string fullName;

  /*
  fullName = endpoint.find(':') == std::string::npos ? endpoint :
  Utils::convertHostToCanonicalForm(endpoint.c_str() );
  */
  fullName = endpoint;

  if (pools.empty()) {
    return m_tcrConnectionManager->getEndpointStatus(fullName);
  }
  fullName = endpoint.find(':') == std::string::npos
                 ? endpoint
                 : Utils::convertHostToCanonicalForm(endpoint.c_str());
  ThinClientPoolDMPtr currPool =
      staticCast<ThinClientPoolDMPtr>(pools.begin().second());
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(currPool->m_endpointsLock);
  for (ACE_Map_Manager<std::string, TcrEndpoint*,
                       ACE_Recursive_Thread_Mutex>::iterator itr =
           currPool->m_endpoints.begin();
       itr != currPool->m_endpoints.end(); itr++) {
    TcrEndpoint* ep = (*itr).int_id_;
    if (ep->name().find(fullName) != std::string::npos) {
      return ep->getServerQueueStatusTEST();
    }
  }
  return false;
}

void CacheImpl::processMarker() {
  TryReadGuard guardCacheDestroy(m_destroyCacheMutex, m_destroyPending);
  if (m_destroyPending) {
    return;
  }

  MapOfRegionGuard guard(m_regions->mutex());

  for (MapOfRegionWithLock::iterator q = m_regions->begin();
       q != m_regions->end(); ++q) {
    if (!(*q).int_id_->isDestroyed()) {
      ThinClientHARegion* tcrHARegion =
          dynamic_cast<ThinClientHARegion*>((*q).int_id_.ptr());
      if (tcrHARegion != NULL) {
        TcrMessage* regionMsg = new TcrMessageClientMarker(true);
        tcrHARegion->receiveNotification(regionMsg);
        VectorOfRegion subregions;
        tcrHARegion->subregions(true, subregions);
        for (VectorOfRegion::Iterator iter = subregions.begin();
             iter != subregions.end(); ++iter) {
          if (!(*iter)->isDestroyed()) {
            ThinClientHARegion* subregion =
                dynamic_cast<ThinClientHARegion*>((*iter).ptr());
            if (subregion != NULL) {
              regionMsg = new TcrMessageClientMarker(true);
              subregion->receiveNotification(regionMsg);
            }
          }
        }
      }
    }
  }
}

// Version ordinal accessor for unit tests
void CacheImpl::setVersionOrdinalForTest(int8_t newVer) {
  Version::m_ordinal = newVer;
}

// Version ordinal accessor for unit tests
int8_t CacheImpl::getVersionOrdinalForTest() { return Version::m_ordinal; }

int CacheImpl::getPoolSize(const char* poolName) {
  PoolPtr pool = PoolManager::find(poolName);
  if (pool == NULLPTR) {
    return -1;
  } else {
    ThinClientPoolDM* dm = dynamic_cast<ThinClientPoolDM*>(pool.ptr());
    if (dm) {
      return dm->m_poolSize;
    } else {
      return -1;
    }
  }
}

RegionFactoryPtr CacheImpl::createRegionFactory(
    RegionShortcut preDefinedRegion) {
  RegionFactoryPtr rfPtr(new RegionFactory(preDefinedRegion));
  return rfPtr;
}

void CacheImpl::setRegionShortcut(AttributesFactoryPtr attrFact,
                                  RegionShortcut preDefinedRegionAttr) {
  switch (preDefinedRegionAttr) {
    case PROXY: {
      attrFact->setCachingEnabled(false);
    } break;
    case CACHING_PROXY: {
      attrFact->setCachingEnabled(true);
    } break;
    case CACHING_PROXY_ENTRY_LRU: {
      attrFact->setCachingEnabled(true);
      attrFact->setLruEntriesLimit(DEFAULT_LRU_MAXIMUM_ENTRIES);
    } break;
    case LOCAL: {
    } break;
    case LOCAL_ENTRY_LRU: {
      attrFact->setLruEntriesLimit(DEFAULT_LRU_MAXIMUM_ENTRIES);
    } break;
  }
}

std::map<std::string, RegionAttributesPtr> CacheImpl::getRegionShortcut() {
  std::map<std::string, RegionAttributesPtr> preDefined;

  {
    // PROXY
    RegionAttributesPtr regAttr_PROXY(new RegionAttributes());
    regAttr_PROXY->setCachingEnabled(false);
    preDefined["PROXY"] = regAttr_PROXY;
  }

  {
    // CACHING_PROXY
    RegionAttributesPtr regAttr_CACHING_PROXY(new RegionAttributes());
    regAttr_CACHING_PROXY->setCachingEnabled(true);
    preDefined["CACHING_PROXY"] = regAttr_CACHING_PROXY;
  }

  {
    // CACHING_PROXY_ENTRY_LRU
    RegionAttributesPtr regAttr_CACHING_PROXY_LRU(new RegionAttributes());
    regAttr_CACHING_PROXY_LRU->setCachingEnabled(true);
    regAttr_CACHING_PROXY_LRU->setLruEntriesLimit(DEFAULT_LRU_MAXIMUM_ENTRIES);
    preDefined["CACHING_PROXY_ENTRY_LRU"] = regAttr_CACHING_PROXY_LRU;
  }

  {
    // LOCAL
    RegionAttributesPtr regAttr_LOCAL(new RegionAttributes());
    preDefined["LOCAL"] = regAttr_LOCAL;
  }

  {
    // LOCAL_ENTRY_LRU
    RegionAttributesPtr regAttr_LOCAL_LRU(new RegionAttributes());
    regAttr_LOCAL_LRU->setLruEntriesLimit(DEFAULT_LRU_MAXIMUM_ENTRIES);
    preDefined["LOCAL_ENTRY_LRU"] = regAttr_LOCAL_LRU;
  }

  return preDefined;
}

CacheTransactionManagerPtr CacheImpl::getCacheTransactionManager() {
  return m_cacheTXManager;
}
MemberListForVersionStampPtr CacheImpl::getMemberListForVersionStamp() {
  return CacheImpl::s_versionStampMemIdList;
}
