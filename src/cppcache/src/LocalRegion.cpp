/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "LocalRegion.hpp"
#include <gfcpp/Log.hpp>
#include <gfcpp/SystemProperties.hpp>
#include "CacheImpl.hpp"
#include "CacheRegionHelper.hpp"
#include "CacheableToken.hpp"
#include "NanoTimer.hpp"
#include "Utils.hpp"

#include "EntryExpiryHandler.hpp"
#include "RegionExpiryHandler.hpp"
#include "ExpiryTaskManager.hpp"
#include "LRUEntriesMap.hpp"
#include "RegionGlobalLocks.hpp"
#include "TXState.hpp"
#include "VersionTag.hpp"
#include <vector>
#include <gfcpp/PoolManager.hpp>

using namespace gemfire;

LocalRegion::LocalRegion(const std::string& name, CacheImpl* cache,
                         RegionInternal* rPtr,
                         const RegionAttributesPtr& attributes,
                         const CacheStatisticsPtr& stats, bool shared)
    : RegionInternal(attributes),
      m_name(name),
      m_parentRegion(rPtr),
      m_cacheImpl(cache),
      m_destroyPending(false),
      m_listener(NULLPTR),
      m_writer(NULLPTR),
      m_loader(NULLPTR),
      m_released(false),
      m_entries(NULL),
      m_cacheStatistics(stats),
      m_transactionEnabled(false),
      m_isPRSingleHopEnabled(false),
      m_attachedPool(NULLPTR),
      m_persistenceManager(NULLPTR) {
  if (m_parentRegion != NULLPTR) {
    ((m_fullPath = m_parentRegion->getFullPath()) += "/") += m_name;
  } else {
    (m_fullPath = "/") += m_name;
  }
  // create entries map based on RegionAttributes...
  if (attributes->getCachingEnabled()) {
    m_entries = EntriesMapFactory::createMap(this, m_regionAttributes);
  }

  // Initialize callbacks
  CacheListenerPtr clptr;
  CacheWriterPtr cwptr;
  clptr = m_regionAttributes->getCacheListener();
  m_listener = clptr;
  cwptr = m_regionAttributes->getCacheWriter();
  m_writer = cwptr;
  CacheLoaderPtr cldptr;
  cldptr = m_regionAttributes->getCacheLoader();
  m_loader = cldptr;

  if (m_parentRegion != NULLPTR) {
    ((m_fullPath = m_parentRegion->getFullPath()) += "/") += m_name;
  } else {
    (m_fullPath = "/") += m_name;
  }

  m_regionStats = new RegionStats(m_fullPath.c_str());
  PoolPtr p = PoolManager::find(getAttributes()->getPoolName());
  // m_attachedPool = p;
  setPool(p);
}

const char* LocalRegion::getName() const { return m_name.c_str(); }

const char* LocalRegion::getFullPath() const { return m_fullPath.c_str(); }

RegionPtr LocalRegion::getParentRegion() const {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::getParentRegion);
  return m_parentRegion;
}

void LocalRegion::updateAccessAndModifiedTime(bool modified) {
  // locking not required since setters use atomic operations
  if (regionExpiryEnabled()) {
    time_t currTime = ACE_OS::gettimeofday().sec();
    LOGDEBUG("Setting last accessed time for region %s to %d", getFullPath(),
             currTime);
    m_cacheStatistics->setLastAccessedTime(static_cast<uint32_t>(currTime));
    if (modified) {
      LOGDEBUG("Setting last modified time for region %s to %d", getFullPath(),
               currTime);
      m_cacheStatistics->setLastModifiedTime(static_cast<uint32_t>(currTime));
    }
    // TODO:  should we really touch the parent region??
    RegionInternal* ri = dynamic_cast<RegionInternal*>(m_parentRegion.ptr());
    if (ri != NULL) {
      ri->updateAccessAndModifiedTime(modified);
    }
  }
}

CacheStatisticsPtr LocalRegion::getStatistics() const {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::getStatistics);
  bool m_statisticsEnabled = true;
  SystemProperties* props =
      m_cacheImpl->getCache()->getDistributedSystem()->getSystemProperties();
  if (props) {
    m_statisticsEnabled = props->statisticsEnabled();
  }
  if (!m_statisticsEnabled) {
    throw StatisticsDisabledException(
        "LocalRegion::getStatistics statistics disabled for this region");
  }

  return m_cacheStatistics;
}

void LocalRegion::invalidateRegion(const UserDataPtr& aCallbackArgument) {
  GfErrType err =
      invalidateRegionNoThrow(aCallbackArgument, CacheEventFlags::NORMAL);
  GfErrTypeToException("Region::invalidateRegion", err);
}

void LocalRegion::localInvalidateRegion(const UserDataPtr& aCallbackArgument) {
  GfErrType err =
      invalidateRegionNoThrow(aCallbackArgument, CacheEventFlags::LOCAL);
  GfErrTypeToException("Region::localInvalidateRegion", err);
}

void LocalRegion::destroyRegion(const UserDataPtr& aCallbackArgument) {
  GfErrType err =
      destroyRegionNoThrow(aCallbackArgument, true, CacheEventFlags::NORMAL);
  GfErrTypeToException("Region::destroyRegion", err);
}

void LocalRegion::localDestroyRegion(const UserDataPtr& aCallbackArgument) {
  GfErrType err =
      destroyRegionNoThrow(aCallbackArgument, true, CacheEventFlags::LOCAL);
  GfErrTypeToException("Region::localDestroyRegion", err);
}

void LocalRegion::tombstoneOperationNoThrow(
    const CacheableHashMapPtr& tombstoneVersions,
    const CacheableHashSetPtr& tombstoneKeys) {
  bool cachingEnabled = m_regionAttributes->getCachingEnabled();

  if (!cachingEnabled) return;

  if (tombstoneVersions.ptr() != NULL) {
    std::map<uint16_t, int64_t> gcVersions;
    for (HashMapT<CacheableKeyPtr, CacheablePtr>::Iterator itr =
             tombstoneVersions->begin();
         itr != tombstoneVersions->end(); ++itr) {
      try {
        DSMemberForVersionStampPtr member =
            dynCast<DSMemberForVersionStampPtr>(itr.first());
        uint16_t memberId =
            getCacheImpl()->getMemberListForVersionStamp()->add(member);
        int64_t version = (dynCast<CacheableInt64Ptr>(itr.second()))->value();
        gcVersions[memberId] = version;
      } catch (const ClassCastException&) {
        LOGERROR(
            "tombstone_operation contains incorrect gc versions in the "
            "message. Region %s",
            getFullPath());
        continue;
      }
    }
    m_entries->reapTombstones(gcVersions);
  } else {
    m_entries->reapTombstones(tombstoneKeys);
  }
}
RegionPtr LocalRegion::getSubregion(const char* path) {
  if (path == NULL) {
    throw IllegalArgumentException("LocalRegion::getSubregion: path is null");
  }

  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::getSubregion);
  std::string pathstr(path);
  std::string slash("/");
  if ((pathstr == slash) || (pathstr.length() < 1)) {
    LOGERROR("Get subregion path [%s] is not valid.", pathstr.c_str());
    throw IllegalArgumentException("Get subegion path is null or a /");
  }
  std::string fullname = pathstr;
  if (fullname.substr(0, 1) == slash) {
    fullname = pathstr.substr(1);
  }
  // find second separator
  size_t idx = fullname.find('/');
  std::string stepname = fullname.substr(0, idx);

  RegionPtr region, rptr;
  if (0 == m_subRegions.find(stepname, region)) {
    if (stepname == fullname) {
      // done...
      rptr = region;
    } else {
      std::string remainder = fullname.substr(stepname.length() + 1);
      rptr = region->getSubregion(remainder.c_str());
    }
  }
  return rptr;
}

RegionPtr LocalRegion::createSubregion(
    const char* subregionName, const RegionAttributesPtr& aRegionAttributes) {
  CHECK_DESTROY_PENDING(TryWriteGuard, LocalRegion::createSubregion);
  {
    std::string namestr = subregionName;
    if (namestr.find('/') != std::string::npos) {
      throw IllegalArgumentException(
          "Malformed name string, contains region path seperator '/'");
    }
  }

  MapOfRegionGuard guard1(m_subRegions.mutex());
  RegionPtr region_ptr;
  if (0 == m_subRegions.find(subregionName, region_ptr)) {
    throw RegionExistsException(
        "LocalRegion::createSubregion: named region exists in the region");
  }

  CacheStatisticsPtr csptr(new CacheStatistics);
  RegionInternal* rPtr = m_cacheImpl->createRegion_internal(
      subregionName, this, aRegionAttributes, csptr, false);
  region_ptr = rPtr;
  if (!rPtr) {
    throw OutOfMemoryException("createSubregion: failed to create region");
  }

  // Instantiate a PersistenceManager object if DiskPolicy is overflow
  if (aRegionAttributes->getDiskPolicy() == DiskPolicyType::OVERFLOWS) {
    PersistenceManagerPtr pmPtr = aRegionAttributes->getPersistenceManager();
    if (pmPtr == NULLPTR) {
      throw NullPointerException(
          "PersistenceManager could not be instantiated");
    }
    PropertiesPtr props = aRegionAttributes->getPersistenceProperties();
    pmPtr->init(RegionPtr(rPtr), props);
    rPtr->setPersistenceManager(pmPtr);
  }

  rPtr->acquireReadLock();
  m_subRegions.bind(rPtr->getName(), RegionPtr(rPtr));

  // schedule the sub region expiry if regionExpiry enabled.
  rPtr->setRegionExpiryTask();
  rPtr->releaseReadLock();
  return region_ptr;
}

void LocalRegion::subregions(const bool recursive, VectorOfRegion& sr) {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::subregions);
  sr.clear();
  if (m_subRegions.current_size() == 0) return;

  subregions_internal(recursive, sr);
}

RegionEntryPtr LocalRegion::getEntry(const CacheableKeyPtr& key) {
  if (getTXState() != NULL) {
    GfErrTypeThrowException("GetEntry is not supported in transaction",
                            GF_NOTSUP);
  }
  RegionEntryPtr rptr;
  CacheablePtr valuePtr;
  getEntry(key, valuePtr);
  if (valuePtr != NULLPTR) {
    rptr = createRegionEntry(key, valuePtr);
  }
  return rptr;
}

void LocalRegion::getEntry(const CacheableKeyPtr& key, CacheablePtr& valuePtr) {
  if (key == NULLPTR) {
    throw IllegalArgumentException("LocalRegion::getEntry: null key");
  }

  MapEntryImplPtr mePtr;
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::getEntry);
  if (m_regionAttributes->getCachingEnabled()) {
    m_entries->getEntry(key, mePtr, valuePtr);
  }
}

CacheablePtr LocalRegion::get(const CacheableKeyPtr& key,
                              const UserDataPtr& aCallbackArgument) {
  CacheablePtr rptr;
  int64 sampleStartNanos = Utils::startStatOpTime();
  GfErrType err = getNoThrow(key, rptr, aCallbackArgument);
  Utils::updateStatOpTime(m_regionStats->getStat(),
                          RegionStatType::getInstance()->getGetTimeId(),
                          sampleStartNanos);

  // rptr = handleReplay(err, rptr);

  GfErrTypeToException("Region::get", err);

  return rptr;
}

void LocalRegion::put(const CacheableKeyPtr& key, const CacheablePtr& value,
                      const UserDataPtr& aCallbackArgument) {
  CacheablePtr oldValue;
  int64 sampleStartNanos = Utils::startStatOpTime();
  VersionTagPtr versionTag;
  GfErrType err = putNoThrow(key, value, aCallbackArgument, oldValue, -1,
                             CacheEventFlags::NORMAL, versionTag);
  Utils::updateStatOpTime(m_regionStats->getStat(),
                          RegionStatType::getInstance()->getPutTimeId(),
                          sampleStartNanos);
  //  handleReplay(err, NULLPTR);
  GfErrTypeToException("Region::put", err);
}

void LocalRegion::localPut(const CacheableKeyPtr& key,
                           const CacheablePtr& value,
                           const UserDataPtr& aCallbackArgument) {
  CacheablePtr oldValue;
  VersionTagPtr versionTag;
  GfErrType err = putNoThrow(key, value, aCallbackArgument, oldValue, -1,
                             CacheEventFlags::LOCAL, versionTag);
  GfErrTypeToException("Region::localPut", err);
}

void LocalRegion::putAll(const HashMapOfCacheable& map, uint32_t timeout,
                         const UserDataPtr& aCallbackArgument) {
  if ((timeout * 1000) >= 0x7fffffff) {
    throw IllegalArgumentException(
        "Region::putAll: timeout parameter "
        "greater than maximum allowed (2^31/1000 i.e 2147483).");
  }
  int64 sampleStartNanos = Utils::startStatOpTime();
  GfErrType err = putAllNoThrow(map, timeout, aCallbackArgument);
  Utils::updateStatOpTime(m_regionStats->getStat(),
                          RegionStatType::getInstance()->getPutAllTimeId(),
                          sampleStartNanos);
  // handleReplay(err, NULLPTR);
  GfErrTypeToException("Region::putAll", err);
}

void LocalRegion::removeAll(const VectorOfCacheableKey& keys,
                            const UserDataPtr& aCallbackArgument) {
  if (keys.size() == 0) {
    throw IllegalArgumentException("Region::removeAll: zero keys provided");
  }
  int64 sampleStartNanos = Utils::startStatOpTime();
  GfErrType err = removeAllNoThrow(keys, aCallbackArgument);
  Utils::updateStatOpTime(m_regionStats->getStat(),
                          RegionStatType::getInstance()->getRemoveAllTimeId(),
                          sampleStartNanos);
  GfErrTypeToException("Region::removeAll", err);
}

void LocalRegion::create(const CacheableKeyPtr& key, const CacheablePtr& value,
                         const UserDataPtr& aCallbackArgument) {
  VersionTagPtr versionTag;
  GfErrType err = createNoThrow(key, value, aCallbackArgument, -1,
                                CacheEventFlags::NORMAL, versionTag);
  // handleReplay(err, NULLPTR);
  GfErrTypeToException("Region::create", err);
}

void LocalRegion::localCreate(const CacheableKeyPtr& key,
                              const CacheablePtr& value,
                              const UserDataPtr& aCallbackArgument) {
  VersionTagPtr versionTag;
  GfErrType err = createNoThrow(key, value, aCallbackArgument, -1,
                                CacheEventFlags::LOCAL, versionTag);
  GfErrTypeToException("Region::localCreate", err);
}

void LocalRegion::invalidate(const CacheableKeyPtr& key,
                             const UserDataPtr& aCallbackArgument) {
  VersionTagPtr versionTag;
  GfErrType err = invalidateNoThrow(key, aCallbackArgument, -1,
                                    CacheEventFlags::NORMAL, versionTag);
  //  handleReplay(err, NULLPTR);
  GfErrTypeToException("Region::invalidate", err);
}

void LocalRegion::localInvalidate(const CacheableKeyPtr& keyPtr,
                                  const UserDataPtr& aCallbackArgument) {
  VersionTagPtr versionTag;
  GfErrType err = invalidateNoThrow(keyPtr, aCallbackArgument, -1,
                                    CacheEventFlags::LOCAL, versionTag);
  GfErrTypeToException("Region::localInvalidate", err);
}

void LocalRegion::destroy(const CacheableKeyPtr& key,
                          const UserDataPtr& aCallbackArgument) {
  VersionTagPtr versionTag;

  GfErrType err = destroyNoThrow(key, aCallbackArgument, -1,
                                 CacheEventFlags::NORMAL, versionTag);
  // handleReplay(err, NULLPTR);
  GfErrTypeToException("Region::destroy", err);
}

void LocalRegion::localDestroy(const CacheableKeyPtr& key,
                               const UserDataPtr& aCallbackArgument) {
  VersionTagPtr versionTag;
  GfErrType err = destroyNoThrow(key, aCallbackArgument, -1,
                                 CacheEventFlags::LOCAL, versionTag);
  GfErrTypeToException("Region::localDestroy", err);
}

bool LocalRegion::remove(const CacheableKeyPtr& key, const CacheablePtr& value,
                         const UserDataPtr& aCallbackArgument) {
  VersionTagPtr versionTag;
  GfErrType err = removeNoThrow(key, value, aCallbackArgument, -1,
                                CacheEventFlags::NORMAL, versionTag);

  bool result = false;

  if (err == GF_NOERR) {
    result = true;
  } else if (err != GF_ENOENT && err != GF_CACHE_ENTRY_NOT_FOUND) {
    GfErrTypeToException("Region::remove", err);
  }

  return result;
}

bool LocalRegion::removeEx(const CacheableKeyPtr& key,
                           const UserDataPtr& aCallbackArgument) {
  VersionTagPtr versionTag;
  GfErrType err = removeNoThrowEx(key, aCallbackArgument, -1,
                                  CacheEventFlags::NORMAL, versionTag);
  bool result = false;

  if (err == GF_NOERR) {
    result = true;
  } else if (err != GF_ENOENT && err != GF_CACHE_ENTRY_NOT_FOUND) {
    GfErrTypeToException("Region::removeEx", err);
  }

  return result;
}

bool LocalRegion::localRemove(const CacheableKeyPtr& key,
                              const CacheablePtr& value,
                              const UserDataPtr& aCallbackArgument) {
  VersionTagPtr versionTag;
  GfErrType err = removeNoThrow(key, value, aCallbackArgument, -1,
                                CacheEventFlags::LOCAL, versionTag);

  bool result = false;

  if (err == GF_NOERR) {
    result = true;
  } else if (err != GF_ENOENT && err != GF_CACHE_ENTRY_NOT_FOUND) {
    GfErrTypeToException("Region::localRemove", err);
  }

  return result;
}

bool LocalRegion::localRemoveEx(const CacheableKeyPtr& key,
                                const UserDataPtr& aCallbackArgument) {
  VersionTagPtr versionTag;
  GfErrType err = removeNoThrowEx(key, aCallbackArgument, -1,
                                  CacheEventFlags::LOCAL, versionTag);

  bool result = false;

  if (err == GF_NOERR) {
    result = true;
  } else if (err != GF_ENOENT && err != GF_CACHE_ENTRY_NOT_FOUND) {
    GfErrTypeToException("Region::localRemoveEx", err);
  }

  return result;
}

void LocalRegion::keys(VectorOfCacheableKey& v) {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::keys);
  keys_internal(v);
}

void LocalRegion::serverKeys(VectorOfCacheableKey& v) {
  throw UnsupportedOperationException(
      "serverKeys is not supported for local regions.");
}

void LocalRegion::values(VectorOfCacheable& vc) {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::values);
  if (!m_regionAttributes->getCachingEnabled()) {
    return;
  }
  uint32_t size = m_entries->size();
  vc.clear();
  if (size == 0) return;
  m_entries->values(vc);
  // invalidToken should not be added by the MapSegments.
}

void LocalRegion::entries(VectorOfRegionEntry& me, bool recursive) {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::entries);
  me.clear();
  if (!m_regionAttributes->getCachingEnabled()) {
    return;
  }
  entries_internal(me, recursive);
}

void LocalRegion::getAll(const VectorOfCacheableKey& keys,
                         HashMapOfCacheablePtr values,
                         HashMapOfExceptionPtr exceptions, bool addToLocalCache,
                         const UserDataPtr& aCallbackArgument) {
  if (keys.size() == 0) {
    throw IllegalArgumentException("Region::getAll: zero keys provided");
  }
  // check for the combination which will result in no action
  if (values == NULLPTR &&
      !(addToLocalCache && m_regionAttributes->getCachingEnabled())) {
    throw IllegalArgumentException(
        "Region::getAll: either output \"values\""
        " parameter should be non-null, or \"addToLocalCache\" should be true "
        "and caching should be enabled for the region [%s]",
        getFullPath());
  }

  int64 sampleStartNanos = Utils::startStatOpTime();
  GfErrType err = getAllNoThrow(keys, values, exceptions, addToLocalCache,
                                aCallbackArgument);
  Utils::updateStatOpTime(m_regionStats->getStat(),
                          RegionStatType::getInstance()->getGetAllTimeId(),
                          sampleStartNanos);
  // handleReplay(err, NULLPTR);
  GfErrTypeToException("Region::getAll", err);
}
uint32 LocalRegion::size_remote() {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::size);
  if (m_regionAttributes->getCachingEnabled()) {
    return m_entries->size();
  }
  return 0;
}

uint32_t LocalRegion::size() {
  TXState* txState = getTXState();
  if (txState != NULL) {
    if (isLocalOp()) {
      return GF_NOTSUP;
    }
    return size_remote();
  }

  return LocalRegion::size_remote();
}

RegionServicePtr LocalRegion::getRegionService() const {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::getRegionService);
  return RegionServicePtr(m_cacheImpl->getCache());
}

CacheImpl* LocalRegion::getCacheImpl() {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::getCache);
  return m_cacheImpl;
}

bool LocalRegion::containsValueForKey_remote(
    const CacheableKeyPtr& keyPtr) const {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::containsValueForKey);
  if (!m_regionAttributes->getCachingEnabled()) {
    return false;
  }
  CacheablePtr valuePtr;
  MapEntryImplPtr mePtr;
  m_entries->getEntry(keyPtr, mePtr, valuePtr);
  if (mePtr == NULLPTR) {
    return false;
  }
  return (valuePtr != NULLPTR && !CacheableToken::isInvalid(valuePtr));
}

bool LocalRegion::containsValueForKey(const CacheableKeyPtr& keyPtr) const {
  if (keyPtr == NULLPTR) {
    throw IllegalArgumentException(
        "LocalRegion::containsValueForKey: "
        "key is null");
  }

  TXState* txState = getTXState();
  if (txState == NULL) {
    return LocalRegion::containsValueForKey_remote(keyPtr);
  }

  return containsValueForKey_remote(keyPtr);
}

bool LocalRegion::containsKeyOnServer(const CacheableKeyPtr& keyPtr) const {
  throw UnsupportedOperationException(
      "LocalRegion::containsKeyOnServer: is not supported.");
}
void LocalRegion::getInterestList(VectorOfCacheableKey& vlist) const {
  throw UnsupportedOperationException(
      "LocalRegion::getInterestList: is not supported.");
}
void LocalRegion::getInterestListRegex(VectorOfCacheableString& vregex) const {
  throw UnsupportedOperationException(
      "LocalRegion::getInterestListRegex: is not supported.");
}

bool LocalRegion::containsKey(const CacheableKeyPtr& keyPtr) const {
  if (keyPtr == NULLPTR) {
    throw IllegalArgumentException(
        "LocalRegion::containsKey: "
        "key is null");
  }
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::containsKey);
  return containsKey_internal(keyPtr);
}

void LocalRegion::setPersistenceManager(PersistenceManagerPtr& pmPtr) {
  m_persistenceManager = pmPtr;
  // set the memberVariable of LRUEntriesMap too.
  LRUEntriesMap* lruMap = dynamic_cast<LRUEntriesMap*>(m_entries);
  if (lruMap != NULL) {
    lruMap->setPersistenceManager(pmPtr);
  }
}

void LocalRegion::setRegionExpiryTask() {
  if (regionExpiryEnabled()) {
    RegionInternalPtr rptr(this);
    uint32_t duration = getRegionExpiryDuration();
    RegionExpiryHandler* handler =
        new RegionExpiryHandler(rptr, getRegionExpiryAction(), duration);
    long expiryTaskId =
        CacheImpl::expiryTaskManager->scheduleExpiryTask(handler, duration, 0);
    handler->setExpiryTaskId(expiryTaskId);
    LOGFINE(
        "expiry for region [%s], expiry task id = %d, duration = %d, "
        "action = %d",
        m_fullPath.c_str(), expiryTaskId, duration, getRegionExpiryAction());
  }
}

void LocalRegion::registerEntryExpiryTask(MapEntryImplPtr& entry) {
  // locking is not required here since only the thread that creates
  // the entry will register the expiry task for that entry
  ExpEntryProperties& expProps = entry->getExpProperties();
  expProps.initStartTime();
  RegionInternalPtr rptr(this);
  uint32_t duration = getEntryExpiryDuration();
  EntryExpiryHandler* handler =
      new EntryExpiryHandler(rptr, entry, getEntryExpirationAction(), duration);
  long id =
      CacheImpl::expiryTaskManager->scheduleExpiryTask(handler, duration, 0);
  if (Log::finestEnabled()) {
    CacheableKeyPtr key;
    entry->getKeyI(key);
    LOGFINEST(
        "entry expiry in region [%s], key [%s], task id = %d, "
        "duration = %d, action = %d",
        m_fullPath.c_str(), Utils::getCacheableKeyString(key)->asChar(), id,
        duration, getEntryExpirationAction());
  }
  expProps.setExpiryTaskId(id);
}

LocalRegion::~LocalRegion() {
  TryWriteGuard guard(m_rwLock, m_destroyPending);
  if (!m_destroyPending) {
    release(false);
  }
  m_listener = NULLPTR;
  m_writer = NULLPTR;
  m_loader = NULLPTR;

  GF_SAFE_DELETE(m_entries);
  GF_SAFE_DELETE(m_regionStats);
}

/**
 * Release the region resources if not released already.
 */
void LocalRegion::release(bool invokeCallbacks) {
  if (m_released) {
    return;
  }
  LOGFINE("LocalRegion::release entered for region %s", m_fullPath.c_str());
  m_released = true;

  if (m_regionStats != NULL) {
    m_regionStats->close();
  }
  if (invokeCallbacks) {
    try {
      if (m_loader != NULLPTR) {
        m_loader->close(RegionPtr(this));
      }
      if (m_writer != NULLPTR) {
        m_writer->close(RegionPtr(this));
      }
      // TODO:  shouldn't listener also be here instead of
      // during CacheImpl.close()
    } catch (...) {
      LOGWARN(
          "Region close caught unknown exception in loader/writer "
          "close; continuing");
    }
  }

  if (m_persistenceManager != NULLPTR) {
    m_persistenceManager->close();
    m_persistenceManager = NULLPTR;
  }
  if (m_entries != NULL && m_regionAttributes->getCachingEnabled()) {
    m_entries->close();
  }
  LOGFINE("LocalRegion::release done for region %s", m_fullPath.c_str());
}

/** Returns whether the specified key currently exists in this region.
* This method is equivalent to <code>getEntry(key) != null</code>.
*
* @param keyPtr the key to check for an existing entry, type is CacheableString
*&
* @return true if there is an entry in this region for the specified key
*@throw RegionDestroyedException,  if region is destroyed.
*@throw IllegalArgumentException, if the key is 'null'.
*@throw NotConnectedException, if not connected to gemfire system.
*/
bool LocalRegion::containsKey_internal(const CacheableKeyPtr& keyPtr) const {
  if (keyPtr == NULLPTR) {
    throw IllegalArgumentException("Region::containsKey: key is null");
  }
  if (!m_regionAttributes->getCachingEnabled()) {
    return false;
  }
  return m_entries->containsKey(keyPtr);
}

void LocalRegion::subregions_internal(const bool recursive,
                                      VectorOfRegion& sr) {
  MapOfRegionGuard guard(m_subRegions.mutex());

  if (m_subRegions.current_size() == 0) return;

  VectorOfRegion subRegions;

  for (MapOfRegionWithLock::iterator p = m_subRegions.begin();
       p != m_subRegions.end(); ++p) {
    sr.push_back((*p).int_id_);
    // seperate list so children can be descended.
    if (recursive) {
      subRegions.push_back((*p).int_id_);
    }
  }

  if (recursive == true) {
    // decend...
    for (int32_t i = 0; i < subRegions.size(); i++) {
      dynamic_cast<LocalRegion*>(subRegions.at(i).ptr())
          ->subregions_internal(true, sr);
    }
  }
}

GfErrType LocalRegion::getNoThrow(const CacheableKeyPtr& keyPtr,
                                  CacheablePtr& value,
                                  const UserDataPtr& aCallbackArgument) {
  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);
  GfErrType err = GF_NOERR;
  if (keyPtr == NULLPTR) {
    return GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION;
  }
  TXState* txState = getTXState();
  if (txState != NULL) {
    if (isLocalOp()) {
      return GF_NOTSUP;
    }
    VersionTagPtr versionTag;
    err = getNoThrow_remote(keyPtr, value, aCallbackArgument, versionTag);
    if (err == GF_NOERR) {
      txState->setDirty();
    }
    if (CacheableToken::isInvalid(value) ||
        CacheableToken::isTombstone(value)) {
      value = NULLPTR;
    }
    return err;
  }

  m_regionStats->incGets();
  m_cacheImpl->m_cacheStats->incGets();

  // TODO:  CacheableToken::isInvalid should be completely hidden
  // inside MapSegment; this should be done both for the value obtained
  // from local cache as well as oldValue in every instance
  MapEntryImplPtr me;
  int updateCount = -1;
  bool isLoaderInvoked = false;
  bool isLocal = false;
  bool cachingEnabled = m_regionAttributes->getCachingEnabled();
  CacheablePtr localValue = NULLPTR;
  if (cachingEnabled) {
    isLocal = m_entries->get(keyPtr, value, me);
    if (isLocal && (value != NULLPTR && !CacheableToken::isInvalid(value))) {
      m_regionStats->incHits();
      m_cacheImpl->m_cacheStats->incHits();
      updateAccessAndModifiedTimeForEntry(me, false);
      updateAccessAndModifiedTime(false);
      return err;  // found it in local cache...
    }
    localValue = value;
    value = NULLPTR;
    // start tracking the entry
    if (!m_regionAttributes->getConcurrencyChecksEnabled()) {
      updateCount =
          m_entries->addTrackerForEntry(keyPtr, value, true, false, false);
      LOGDEBUG(
          "Region::get: added tracking with update counter [%d] for key "
          "[%s] with value [%s]",
          updateCount, Utils::getCacheableKeyString(keyPtr)->asChar(),
          Utils::getCacheableString(value)->asChar());
    }
  }

  // remove tracking for the entry before exiting the function
  struct RemoveTracking {
   private:
    const CacheableKeyPtr& m_key;
    const int& m_updateCount;
    LocalRegion& m_region;

   public:
    RemoveTracking(const CacheableKeyPtr& key, const int& updateCount,
                   LocalRegion& region)
        : m_key(key), m_updateCount(updateCount), m_region(region) {}
    ~RemoveTracking() {
      if (m_updateCount >= 0 &&
          !m_region.getAttributes()->getConcurrencyChecksEnabled()) {
        m_region.m_entries->removeTrackerForEntry(m_key);
      }
    }
  } _removeTracking(keyPtr, updateCount, *this);

  // The control will come here only when caching is disabled or/and
  // the entry was not found. In this case atleast update the region
  // access times.
  updateAccessAndModifiedTime(false);
  m_regionStats->incMisses();
  m_cacheImpl->m_cacheStats->incMisses();
  VersionTagPtr versionTag;
  // Get from some remote source (e.g. external java server) if required.
  err = getNoThrow_remote(keyPtr, value, aCallbackArgument, versionTag);

  // Its a cache missor it is invalid token then Check if we have a local
  // loader.
  if ((value == NULLPTR || CacheableToken::isInvalid(value) ||
       CacheableToken::isTombstone(value)) &&
      m_loader != NULLPTR) {
    try {
      isLoaderInvoked = true;
      /*Update the statistics*/
      int64 sampleStartNanos = Utils::startStatOpTime();
      value = m_loader->load(RegionPtr(this), keyPtr, aCallbackArgument);
      Utils::updateStatOpTime(
          m_regionStats->getStat(),
          RegionStatType::getInstance()->getLoaderCallTimeId(),
          sampleStartNanos);
      m_regionStats->incLoaderCallsCompleted();
    } catch (const Exception& ex) {
      LOGERROR("Error in CacheLoader::load: %s: %s", ex.getName(),
               ex.getMessage());
      err = GF_CACHE_LOADER_EXCEPTION;
    } catch (...) {
      LOGERROR("Error in CacheLoader::load, unknown");
      err = GF_CACHE_LOADER_EXCEPTION;
    }
    if (err != GF_NOERR) {
      return err;
    }
  }

  CacheablePtr oldValue;
  // Found it somehow, so store it.
  if (value != NULLPTR /*&& value != CacheableToken::invalid( )*/ &&
      cachingEnabled &&
      !(CacheableToken::isTombstone(value) &&
        (localValue == NULLPTR || CacheableToken::isInvalid(localValue)))) {
    //  try to create the entry and if that returns an existing value
    // (e.g. from another thread or notification) then return that
    LOGDEBUG(
        "Region::get: creating entry with tracking update counter [%d] for key "
        "[%s]",
        updateCount, Utils::getCacheableKeyString(keyPtr)->asChar());
    if ((err = putLocal("Region::get", false, keyPtr, value, oldValue,
                        cachingEnabled, updateCount, 0, versionTag)) !=
        GF_NOERR) {
      if (err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION) {
        LOGDEBUG(
            "Region::get: putLocal for key [%s] failed because the cache already contains \
          an entry with higher version.",
            Utils::getCacheableKeyString(keyPtr)->asChar());
        if (CacheableToken::isInvalid(value) ||
            CacheableToken::isTombstone(value)) {
          value = NULLPTR;
        }
        // don't do anything and  exit
        return GF_NOERR;
      }

      LOGDEBUG("Region::get: putLocal for key [%s] failed with error %d",
               Utils::getCacheableKeyString(keyPtr)->asChar(), err);
      err = GF_NOERR;
      if (oldValue != NULLPTR && !CacheableToken::isInvalid(oldValue)) {
        LOGDEBUG("Region::get: returning updated value [%s] for key [%s]",
                 Utils::getCacheableString(oldValue)->asChar(),
                 Utils::getCacheableKeyString(keyPtr)->asChar());
        value = oldValue;
      }
    }
    // signal no explicit removal of tracking to the RemoveTracking object
    updateCount = -1;
  }

  if (CacheableToken::isInvalid(value) || CacheableToken::isTombstone(value)) {
    value = NULLPTR;
  }

  // invokeCacheListenerForEntryEvent method has the check that if oldValue
  // is a CacheableToken then it sets it to NULL; also determines if it
  // should be AFTER_UPDATE or AFTER_CREATE depending on oldValue, so don't
  // check here.
  if (isLoaderInvoked == false && err == GF_NOERR && value != NULLPTR) {
    err = invokeCacheListenerForEntryEvent(
        keyPtr, oldValue, value, aCallbackArgument, CacheEventFlags::NORMAL,
        AFTER_UPDATE, isLocal);
  }

  return err;
}

GfErrType LocalRegion::getAllNoThrow(const VectorOfCacheableKey& keys,
                                     const HashMapOfCacheablePtr& values,
                                     const HashMapOfExceptionPtr& exceptions,
                                     bool addToLocalCache,
                                     const UserDataPtr& aCallbackArgument) {
  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);
  GfErrType err = GF_NOERR;
  CacheablePtr value;

  TXState* txState = getTXState();
  if (txState != NULL) {
    if (isLocalOp()) {
      return GF_NOTSUP;
    }
    //		if(!txState->isReplay())
    //		{
    //			VectorOfCacheablePtr args(new VectorOfCacheable());
    //			args->push_back(VectorOfCacheableKeyPtr(new
    // VectorOfCacheableKey(keys)));
    //			args->push_back(values);
    //			args->push_back(exceptions);
    //			args->push_back(CacheableBoolean::create(addToLocalCache));
    //			txState->recordTXOperation(GF_GET_ALL, getFullPath(),
    // NULLPTR,
    // args);
    //		}
    err = getAllNoThrow_remote(&keys, values, exceptions, NULLPTR, false,
                               aCallbackArgument);
    if (err == GF_NOERR) {
      txState->setDirty();
    }

    return err;
  }
  // keys not in cache with their tracking numbers to be gotten using
  // a remote call
  VectorOfCacheableKey serverKeys;
  bool cachingEnabled = m_regionAttributes->getCachingEnabled();
  bool regionAccessed = false;

  for (int32_t index = 0; index < keys.size(); ++index) {
    const CacheableKeyPtr& key = keys[index];
    MapEntryImplPtr me;
    value = NULLPTR;
    m_regionStats->incGets();
    m_cacheImpl->m_cacheStats->incGets();
    if (values != NULLPTR && cachingEnabled) {
      if (m_entries->get(key, value, me) && value != NULLPTR &&
          !CacheableToken::isInvalid(value)) {
        m_regionStats->incHits();
        m_cacheImpl->m_cacheStats->incHits();
        updateAccessAndModifiedTimeForEntry(me, false);
        regionAccessed = true;
        values->insert(key, value);
      } else {
        value = NULLPTR;
      }
    }
    if (value == NULLPTR) {
      // Add to missed keys list.
      serverKeys.push_back(key);

      m_regionStats->incMisses();
      m_cacheImpl->m_cacheStats->incMisses();
    }
    // TODO: No support for loaders in getAll for now.
  }
  if (regionAccessed) {
    updateAccessAndModifiedTime(false);
  }
  if (serverKeys.size() > 0) {
    err = getAllNoThrow_remote(&serverKeys, values, exceptions, NULLPTR,
                               addToLocalCache, aCallbackArgument);
  }
  m_regionStats->incGetAll();
  return err;
}

namespace gemfire {
// encapsulates actions that need to be taken for a put() operation
class PutActions {
 public:
  static const EntryEventType s_beforeEventType = BEFORE_UPDATE;
  static const EntryEventType s_afterEventType = AFTER_UPDATE;
  static const bool s_addIfAbsent = true;
  static const bool s_failIfPresent = false;
  TXState* m_txState;

  inline explicit PutActions(LocalRegion& region) : m_region(region) {
    m_txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();
  }

  inline static const char* name() { return "Region::put"; }

  inline static GfErrType checkArgs(const CacheableKeyPtr& key,
                                    const CacheablePtr& value,
                                    DataInput* delta = NULL) {
    if (key == NULLPTR || (value == NULLPTR && delta == NULL)) {
      return GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION;
    }
    return GF_NOERR;
  }

  inline void getCallbackOldValue(bool cachingEnabled,
                                  const CacheableKeyPtr& key,
                                  MapEntryImplPtr& entry,
                                  CacheablePtr& oldValue) const {
    if (cachingEnabled) {
      m_region.m_entries->getEntry(key, entry, oldValue);
    }
  }

  inline static void logCacheWriterFailure(const CacheableKeyPtr& key,
                                           const CacheablePtr& oldValue) {
    bool isUpdate = (oldValue != NULLPTR);
    LOGFINER("Cache writer vetoed %s for key %s",
             (isUpdate ? "update" : "create"),
             Utils::getCacheableKeyString(key)->asChar());
  }

  inline GfErrType remoteUpdate(const CacheableKeyPtr& key,
                                const CacheablePtr& value,
                                const UserDataPtr& aCallbackArgument,
                                VersionTagPtr& versionTag) {
    //    	if(m_txState != NULL && !m_txState->isReplay())
    //    	{
    //    		VectorOfCacheablePtr args(new VectorOfCacheable());
    //    		args->push_back(value);
    //    		args->push_back(aCallbackArgument);
    //    		m_txState->recordTXOperation(GF_PUT,
    //    m_region.getFullPath(), key, args);
    //    	}
    // propagate the put to remote server, if any
    return m_region.putNoThrow_remote(key, value, aCallbackArgument,
                                      versionTag);
  }

  inline GfErrType localUpdate(const CacheableKeyPtr& key,
                               const CacheablePtr& value,
                               CacheablePtr& oldValue, bool cachingEnabled,
                               const CacheEventFlags eventFlags,
                               int updateCount, VersionTagPtr versionTag,
                               DataInput* delta = NULL,
                               EventIdPtr eventId = NULLPTR,
                               bool afterRemote = false) {
    return m_region.putLocal(name(), false, key, value, oldValue,
                             cachingEnabled, updateCount, 0, versionTag, delta,
                             eventId);
  }

 private:
  LocalRegion& m_region;
};

// encapsulates actions that need to be taken for a put() operation. This
// implementation allows
// null values in Put during transaction. See defect #743
class PutActionsTx : public PutActions {
 public:
  inline explicit PutActionsTx(LocalRegion& region) : PutActions(region) {}
  inline static GfErrType checkArgs(const CacheableKeyPtr& key,
                                    const CacheablePtr& value,
                                    DataInput* delta = NULL) {
    if (key == NULLPTR) {
      return GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION;
    }
    return GF_NOERR;
  }
};

// encapsulates actions that need to be taken for a create() operation
class CreateActions {
 public:
  static const EntryEventType s_beforeEventType = BEFORE_CREATE;
  static const EntryEventType s_afterEventType = AFTER_CREATE;
  static const bool s_addIfAbsent = true;
  static const bool s_failIfPresent = true;
  TXState* m_txState;

  inline explicit CreateActions(LocalRegion& region) : m_region(region) {
    m_txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();
  }

  inline static const char* name() { return "Region::create"; }

  inline static GfErrType checkArgs(const CacheableKeyPtr& key,
                                    const CacheablePtr& value,
                                    DataInput* delta) {
    if (key == NULLPTR) {
      return GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION;
    }
    return GF_NOERR;
  }

  inline void getCallbackOldValue(bool cachingEnabled,
                                  const CacheableKeyPtr& key,
                                  MapEntryImplPtr& entry,
                                  CacheablePtr& oldValue) const {}

  inline static void logCacheWriterFailure(const CacheableKeyPtr& key,
                                           const CacheablePtr& oldValue) {
    LOGFINER("Cache writer vetoed create for key %s",
             Utils::getCacheableKeyString(key)->asChar());
  }

  inline GfErrType remoteUpdate(const CacheableKeyPtr& key,
                                const CacheablePtr& value,
                                const UserDataPtr& aCallbackArgument,
                                VersionTagPtr& versionTag) {
    // propagate the create to remote server, if any
    //  	  if(m_txState != NULL && !m_txState->isReplay())
    //  	  {
    //  		  VectorOfCacheablePtr args(new VectorOfCacheable());
    //  		  args->push_back(value);
    //  		  args->push_back(aCallbackArgument);
    //  		  m_txState->recordTXOperation(GF_CREATE,
    //  m_region.getFullPath(), key, args);
    //  	  }
    return m_region.createNoThrow_remote(key, value, aCallbackArgument,
                                         versionTag);
  }

  inline GfErrType localUpdate(const CacheableKeyPtr& key,
                               const CacheablePtr& value,
                               CacheablePtr& oldValue, bool cachingEnabled,
                               const CacheEventFlags eventFlags,
                               int updateCount, VersionTagPtr versionTag,
                               DataInput* delta = NULL,
                               EventIdPtr eventId = NULLPTR,
                               bool afterRemote = false) {
    return m_region.putLocal(name(), true, key, value, oldValue, cachingEnabled,
                             updateCount, 0, versionTag);
  }

 private:
  LocalRegion& m_region;
};

// encapsulates actions that need to be taken for a destroy() operation
class DestroyActions {
 public:
  static const EntryEventType s_beforeEventType = BEFORE_DESTROY;
  static const EntryEventType s_afterEventType = AFTER_DESTROY;
  static const bool s_addIfAbsent = true;
  static const bool s_failIfPresent = false;
  TXState* m_txState;

  inline explicit DestroyActions(LocalRegion& region) : m_region(region) {
    m_txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();
  }

  inline static const char* name() { return "Region::destroy"; }

  inline static GfErrType checkArgs(const CacheableKeyPtr& key,
                                    const CacheablePtr& value,
                                    DataInput* delta) {
    if (key == NULLPTR) {
      return GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION;
    }
    return GF_NOERR;
  }

  inline void getCallbackOldValue(bool cachingEnabled,
                                  const CacheableKeyPtr& key,
                                  MapEntryImplPtr& entry,
                                  CacheablePtr& oldValue) const {
    if (cachingEnabled) {
      m_region.m_entries->getEntry(key, entry, oldValue);
    }
  }

  inline static void logCacheWriterFailure(const CacheableKeyPtr& key,
                                           const CacheablePtr& oldValue) {
    LOGFINER("Cache writer vetoed destroy for key %s",
             Utils::getCacheableKeyString(key)->asChar());
  }

  inline GfErrType remoteUpdate(const CacheableKeyPtr& key,
                                const CacheablePtr& value,
                                const UserDataPtr& aCallbackArgument,
                                VersionTagPtr& versionTag) {
    // propagate the destroy to remote server, if any
    //    	if(m_txState != NULL && !m_txState->isReplay())
    //    	{
    //    		VectorOfCacheablePtr args(new VectorOfCacheable());
    //    		args->push_back(aCallbackArgument);
    //    		m_txState->recordTXOperation(GF_DESTROY,
    //    m_region.getFullPath(), key, args);
    //    	}

    return m_region.destroyNoThrow_remote(key, aCallbackArgument, versionTag);
  }

  inline GfErrType localUpdate(const CacheableKeyPtr& key,
                               const CacheablePtr& value,
                               CacheablePtr& oldValue, bool cachingEnabled,
                               const CacheEventFlags eventFlags,
                               int updateCount, VersionTagPtr versionTag,
                               DataInput* delta = NULL,
                               EventIdPtr eventId = NULLPTR,
                               bool afterRemote = false) {
    if (cachingEnabled) {
      MapEntryImplPtr entry;
      //  for notification invoke the listener even if the key does
      // not exist locally
      GfErrType err;
      LOGDEBUG("Region::destroy: region [%s] destroying key [%s]",
               m_region.getFullPath(),
               Utils::getCacheableKeyString(key)->asChar());
      if ((err = m_region.m_entries->remove(key, oldValue, entry, updateCount,
                                            versionTag, afterRemote)) !=
          GF_NOERR) {
        if (eventFlags.isNotification()) {
          LOGDEBUG(
              "Region::destroy: region [%s] destroy key [%s] for "
              "notification having value [%s] failed with %d",
              m_region.getFullPath(),
              Utils::getCacheableKeyString(key)->asChar(),
              Utils::getCacheableString(oldValue)->asChar(), err);
          err = GF_NOERR;
        }
        return err;
      }
      if (oldValue != NULLPTR) {
        LOGDEBUG(
            "Region::destroy: region [%s] destroyed key [%s] having "
            "value [%s]",
            m_region.getFullPath(), Utils::getCacheableKeyString(key)->asChar(),
            Utils::getCacheableString(oldValue)->asChar());
        // any cleanup required for the entry (e.g. removing from LRU list)
        if (entry != NULLPTR) {
          entry->cleanup(eventFlags);
        }
        // entry/region expiration
        if (!eventFlags.isEvictOrExpire()) {
          m_region.updateAccessAndModifiedTime(true);
        }
        // update the stats
        m_region.m_regionStats->setEntries(m_region.m_entries->size());
        m_region.m_cacheImpl->m_cacheStats->incEntries(-1);
      }
    }
    // update the stats
    m_region.m_regionStats->incDestroys();
    m_region.m_cacheImpl->m_cacheStats->incDestroys();
    return GF_NOERR;
  }

 private:
  LocalRegion& m_region;
};

// encapsulates actions that need to be taken for a remove() operation
class RemoveActions {
 public:
  static const EntryEventType s_beforeEventType = BEFORE_DESTROY;
  static const EntryEventType s_afterEventType = AFTER_DESTROY;
  static const bool s_addIfAbsent = true;
  static const bool s_failIfPresent = false;
  TXState* m_txState;
  bool allowNULLValue;

  inline explicit RemoveActions(LocalRegion& region)
      : m_region(region), m_ServerResponse(GF_ENOENT) {
    m_txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();
    allowNULLValue = false;
  }

  inline static const char* name() { return "Region::remove"; }

  inline static GfErrType checkArgs(const CacheableKeyPtr& key,
                                    const CacheablePtr& value,
                                    DataInput* delta) {
    if (key == NULLPTR) {
      return GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION;
    }
    return GF_NOERR;
  }

  inline void getCallbackOldValue(bool cachingEnabled,
                                  const CacheableKeyPtr& key,
                                  MapEntryImplPtr& entry,
                                  CacheablePtr& oldValue) const {
    if (cachingEnabled) {
      m_region.m_entries->getEntry(key, entry, oldValue);
    }
  }

  inline static void logCacheWriterFailure(const CacheableKeyPtr& key,
                                           const CacheablePtr& oldValue) {
    LOGFINER("Cache writer vetoed remove for key %s",
             Utils::getCacheableKeyString(key)->asChar());
  }

  inline GfErrType remoteUpdate(const CacheableKeyPtr& key,
                                const CacheablePtr& value,
                                const UserDataPtr& aCallbackArgument,
                                VersionTagPtr& versionTag) {
    // propagate the remove to remote server, if any
    CacheablePtr valuePtr;
    GfErrType err = GF_NOERR;
    if (!allowNULLValue && m_region.getAttributes()->getCachingEnabled()) {
      m_region.getEntry(key, valuePtr);
      DataOutput out1;
      DataOutput out2;

      if (valuePtr != NULLPTR && value != NULLPTR) {
        if (valuePtr->classId() != value->classId() ||
            valuePtr->typeId() != value->typeId()) {
          err = GF_ENOENT;
          return err;
        }
        valuePtr->toData(out1);
        value->toData(out2);
        if (out1.getBufferLength() != out2.getBufferLength()) {
          err = GF_ENOENT;
          return err;
        }
        if (memcmp(out1.getBuffer(), out2.getBuffer(),
                   out1.getBufferLength()) != 0) {
          err = GF_ENOENT;
          return err;
        }
      } else if ((valuePtr == NULLPTR || CacheableToken::isInvalid(valuePtr))) {
        //        	if(m_txState != NULL && !m_txState->isReplay())
        //        	{
        //        		VectorOfCacheablePtr args(new
        //        VectorOfCacheable());
        //        		args->push_back(value);
        //        		args->push_back(aCallbackArgument);
        //        		m_txState->recordTXOperation(GF_REMOVE,
        //        m_region.getFullPath(), key, args);
        //        	}

        m_ServerResponse = m_region.removeNoThrow_remote(
            key, value, aCallbackArgument, versionTag);

        return m_ServerResponse;
      } else if (valuePtr != NULLPTR && value == NULLPTR) {
        err = GF_ENOENT;
        return err;
      }
    }
    //  	if(m_txState != NULL && !m_txState->isReplay())
    //  	{
    //  		VectorOfCacheablePtr args(new VectorOfCacheable());
    //  		args->push_back(value);
    //  		args->push_back(aCallbackArgument);
    //  		m_txState->recordTXOperation(GF_REMOVE,
    //  m_region.getFullPath(), key, args);
    //  	}
    if (allowNULLValue) {
      m_ServerResponse =
          m_region.removeNoThrowEX_remote(key, aCallbackArgument, versionTag);
    } else {
      m_ServerResponse = m_region.removeNoThrow_remote(
          key, value, aCallbackArgument, versionTag);
    }
    LOGDEBUG("serverResponse::%d", m_ServerResponse);
    return m_ServerResponse;
  }

  inline GfErrType localUpdate(const CacheableKeyPtr& key,
                               const CacheablePtr& value,
                               CacheablePtr& oldValue, bool cachingEnabled,
                               const CacheEventFlags eventFlags,
                               int updateCount, VersionTagPtr versionTag,
                               DataInput* delta = NULL,
                               EventIdPtr eventId = NULLPTR,
                               bool afterRemote = false) {
    CacheablePtr valuePtr;
    GfErrType err = GF_NOERR;
    if (!allowNULLValue && cachingEnabled) {
      m_region.getEntry(key, valuePtr);
      DataOutput out1;
      DataOutput out2;
      if (valuePtr != NULLPTR && value != NULLPTR) {
        if (valuePtr->classId() != value->classId() ||
            valuePtr->typeId() != value->typeId()) {
          err = GF_ENOENT;
          return err;
        }
        valuePtr->toData(out1);
        value->toData(out2);
        if (out1.getBufferLength() != out2.getBufferLength()) {
          err = GF_ENOENT;
          return err;
        }
        if (memcmp(out1.getBuffer(), out2.getBuffer(),
                   out1.getBufferLength()) != 0) {
          err = GF_ENOENT;
          return err;
        }
      } else if (value == NULLPTR && (!CacheableToken::isInvalid(valuePtr) ||
                                      valuePtr == NULLPTR)) {
        err = (m_ServerResponse == 0 && valuePtr == NULLPTR) ? GF_NOERR
                                                             : GF_ENOENT;
        if (updateCount >= 0 &&
            !m_region.getAttributes()
                 ->getConcurrencyChecksEnabled()) {  // This means server has
                                                     // deleted an entry & same
                                                     // entry has been destroyed
                                                     // locally
          // So call removeTrackerForEntry to remove key that was added in the
          // map during addTrackerForEntry call.
          m_region.m_entries->removeTrackerForEntry(key);
        }
        return err;
      } else if (valuePtr == NULLPTR && value != NULLPTR &&
                 m_ServerResponse != 0) {
        err = GF_ENOENT;
        return err;
      }
    }

    if (cachingEnabled) {
      MapEntryImplPtr entry;
      //  for notification invoke the listener even if the key does
      // not exist locally
      GfErrType err;
      LOGDEBUG("Region::remove: region [%s] removing key [%s]",
               m_region.getFullPath(),
               Utils::getCacheableKeyString(key)->asChar());
      if ((err = m_region.m_entries->remove(key, oldValue, entry, updateCount,
                                            versionTag, afterRemote)) !=
          GF_NOERR) {
        if (eventFlags.isNotification()) {
          LOGDEBUG(
              "Region::remove: region [%s] remove key [%s] for "
              "notification having value [%s] failed with %d",
              m_region.getFullPath(),
              Utils::getCacheableKeyString(key)->asChar(),
              Utils::getCacheableString(oldValue)->asChar(), err);
          err = GF_NOERR;
        }
        return err;
      }
      if (oldValue != NULLPTR) {
        LOGDEBUG(
            "Region::remove: region [%s] removed key [%s] having "
            "value [%s]",
            m_region.getFullPath(), Utils::getCacheableKeyString(key)->asChar(),
            Utils::getCacheableString(oldValue)->asChar());
        // any cleanup required for the entry (e.g. removing from LRU list)
        if (entry != NULLPTR) {
          entry->cleanup(eventFlags);
        }
        // entry/region expiration
        if (!eventFlags.isEvictOrExpire()) {
          m_region.updateAccessAndModifiedTime(true);
        }
        // update the stats
        m_region.m_regionStats->setEntries(m_region.m_entries->size());
        m_region.m_cacheImpl->m_cacheStats->incEntries(-1);
      }
    }
    // update the stats
    m_region.m_regionStats->incDestroys();
    m_region.m_cacheImpl->m_cacheStats->incDestroys();
    return GF_NOERR;
  }

 private:
  LocalRegion& m_region;
  GfErrType m_ServerResponse;
};

class RemoveActionsEx : public RemoveActions {
 public:
  inline explicit RemoveActionsEx(LocalRegion& region) : RemoveActions(region) {
    allowNULLValue = true;
  }
};

// encapsulates actions that need to be taken for a invalidate() operation
class InvalidateActions {
 public:
  static const EntryEventType s_beforeEventType = BEFORE_INVALIDATE;
  static const EntryEventType s_afterEventType = AFTER_INVALIDATE;
  static const bool s_addIfAbsent = true;
  static const bool s_failIfPresent = false;
  TXState* m_txState;

  inline explicit InvalidateActions(LocalRegion& region) : m_region(region) {
    m_txState = TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();
  }

  inline static const char* name() { return "Region::invalidate"; }

  inline static GfErrType checkArgs(const CacheableKeyPtr& key,
                                    const CacheablePtr& value,
                                    DataInput* delta = NULL) {
    if (key == NULLPTR) {
      return GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION;
    }
    return GF_NOERR;
  }

  inline void getCallbackOldValue(bool cachingEnabled,
                                  const CacheableKeyPtr& key,
                                  MapEntryImplPtr& entry,
                                  CacheablePtr& oldValue) const {
    if (cachingEnabled) {
      m_region.m_entries->getEntry(key, entry, oldValue);
    }
  }

  inline static void logCacheWriterFailure(const CacheableKeyPtr& key,
                                           const CacheablePtr& oldValue) {
    bool isUpdate = (oldValue != NULLPTR);
    LOGFINER("Cache writer vetoed %s for key %s",
             (isUpdate ? "update" : "invalidate"),
             Utils::getCacheableKeyString(key)->asChar());
  }

  inline GfErrType remoteUpdate(const CacheableKeyPtr& key,
                                const CacheablePtr& value,
                                const UserDataPtr& aCallbackArgument,
                                VersionTagPtr& versionTag) {
    //    	if(m_txState != NULL && !m_txState->isReplay())
    //    	{
    //    		VectorOfCacheablePtr args(new VectorOfCacheable());
    //    		args->push_back(aCallbackArgument);
    //    		m_txState->recordTXOperation(GF_INVALIDATE,
    //    m_region.getFullPath(), key, args);
    //    	}
    // propagate the invalidate to remote server, if any
    return m_region.invalidateNoThrow_remote(key, aCallbackArgument,
                                             versionTag);
  }

  inline GfErrType localUpdate(const CacheableKeyPtr& key,
                               const CacheablePtr& value,
                               CacheablePtr& oldValue, bool cachingEnabled,
                               const CacheEventFlags eventFlags,
                               int updateCount, VersionTagPtr versionTag,
                               DataInput* delta = NULL,
                               EventIdPtr eventId = NULLPTR,
                               bool afterRemote = false) {
    return m_region.invalidateLocal(name(), key, value, eventFlags, versionTag);
  }

 private:
  LocalRegion& m_region;
};
}  // namespace gemfire

template <typename TAction>
GfErrType LocalRegion::updateNoThrow(const CacheableKeyPtr& key,
                                     const CacheablePtr& value,
                                     const UserDataPtr& aCallbackArgument,
                                     CacheablePtr& oldValue, int updateCount,
                                     const CacheEventFlags eventFlags,
                                     VersionTagPtr versionTag, DataInput* delta,
                                     EventIdPtr eventId) {
  GfErrType err = GF_NOERR;
  if ((err = TAction::checkArgs(key, value, delta)) != GF_NOERR) {
    return err;
  }

  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);

  TAction action(*this);
  TXState* txState = action.m_txState;
  if (txState != NULL) {
    if (isLocalOp(&eventFlags)) {
      return GF_NOTSUP;
    }
    /* adongre - Coverity II
     * CID 29194 (6): Parse warning (PW.PARAMETER_HIDDEN)
     */
    // VersionTagPtr versionTag;
    err = action.remoteUpdate(key, value, aCallbackArgument, versionTag);
    if (err == GF_NOERR) {
      txState->setDirty();
    }

    return err;
  }

  bool cachingEnabled = m_regionAttributes->getCachingEnabled();
  MapEntryImplPtr entry;

  //  do not invoke the writer in case of notification/eviction
  // or expiration
  if (m_writer != NULLPTR && eventFlags.invokeCacheWriter()) {
    action.getCallbackOldValue(cachingEnabled, key, entry, oldValue);
    // invokeCacheWriterForEntryEvent method has the check that if oldValue
    // is a CacheableToken then it sets it to NULL; also determines if it
    // should be BEFORE_UPDATE or BEFORE_CREATE depending on oldValue
    if (!invokeCacheWriterForEntryEvent(key, oldValue, value, aCallbackArgument,
                                        eventFlags,
                                        TAction::s_beforeEventType)) {
      TAction::logCacheWriterFailure(key, oldValue);
      return GF_CACHEWRITER_ERROR;
    }
  }
  bool remoteOpDone = false;
  //  try the remote update; but if this fails (e.g. due to security
  // exception) do not do the local update
  // uses the technique of adding a tracking to the entry before proceeding
  // for put; if the update counter changes when the remote update completes
  // then it means that the local entry was overwritten in the meantime
  // by a notification or another thread, so we do not do the local update
  if (!eventFlags.isLocal() && !eventFlags.isNotification()) {
    if (cachingEnabled && updateCount < 0 &&
        !m_regionAttributes->getConcurrencyChecksEnabled()) {
      // add a tracking for the entry
      if ((updateCount = m_entries->addTrackerForEntry(
               key, oldValue, TAction::s_addIfAbsent, TAction::s_failIfPresent,
               true)) < 0) {
        if (oldValue != NULLPTR) {
          // fail for "create" when entry exists
          return GF_CACHE_ENTRY_EXISTS;
        }
      }
    }
    // propagate the update to remote server, if any
    err = action.remoteUpdate(key, value, aCallbackArgument, versionTag);
    if (err != GF_NOERR) {
      if (updateCount >= 0 &&
          !m_regionAttributes->getConcurrencyChecksEnabled()) {
        m_entries->removeTrackerForEntry(key);
      }
      return err;
    }
    remoteOpDone = true;
  }
  if (!eventFlags.isNotification() || getProcessedMarker()) {
    if ((err = action.localUpdate(key, value, oldValue, cachingEnabled,
                                  eventFlags, updateCount, versionTag, delta,
                                  eventId, remoteOpDone)) ==
        GF_CACHE_ENTRY_UPDATED) {
      LOGFINEST(
          "%s: did not change local value for key [%s] since it has "
          "been updated by another thread while operation was in progress",
          TAction::name(), Utils::getCacheableKeyString(key)->asChar());
      err = GF_NOERR;
    } else if (err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION) {
      LOGDEBUG(
          "Region::localUpdate: updateNoThrow<%s> for key [%s] failed because the cache already contains \
        an entry with higher version. The cache listener will not be invoked.",
          TAction::name(), Utils::getCacheableKeyString(key)->asChar());
      // Cache listener won't be called in this case
      return GF_NOERR;
    } else if (err == GF_INVALID_DELTA) {
      LOGDEBUG(
          "Region::localUpdate: updateNoThrow<%s> for key [%s] failed because "
          "of invalid delta.",
          TAction::name(), Utils::getCacheableKeyString(key)->asChar());
      m_cacheImpl->m_cacheStats->incFailureOnDeltaReceived();
      // Get full object from server.
      CacheablePtr& newValue1 = const_cast<CacheablePtr&>(value);
      VersionTagPtr versionTag1;
      err = getNoThrow_FullObject(eventId, newValue1, versionTag1);
      if (err == GF_NOERR && newValue1 != NULLPTR) {
        err = m_entries->put(key, newValue1, entry, oldValue, updateCount, 0,
                             versionTag1 != NULLPTR ? versionTag1 : versionTag);
        if (err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION) {
          LOGDEBUG(
              "Region::localUpdate: updateNoThrow<%s> for key [%s] failed because the cache already contains \
            an entry with higher version. The cache listener will not be invoked.",
              TAction::name(), Utils::getCacheableKeyString(key)->asChar());
          // Cache listener won't be called in this case
          return GF_NOERR;
        } else if (err != GF_NOERR) {
          return err;
        }
      }
    } else if (err != GF_NOERR) {
      return err;
    }
  } else {  // if (getProcessedMarker())
    action.getCallbackOldValue(cachingEnabled, key, entry, oldValue);
    if (updateCount >= 0 &&
        !m_regionAttributes->getConcurrencyChecksEnabled()) {
      m_entries->removeTrackerForEntry(key);
    }
  }
  // invokeCacheListenerForEntryEvent method has the check that if oldValue
  // is a CacheableToken then it sets it to NULL; also determines if it
  // should be AFTER_UPDATE or AFTER_CREATE depending on oldValue
  err =
      invokeCacheListenerForEntryEvent(key, oldValue, value, aCallbackArgument,
                                       eventFlags, TAction::s_afterEventType);
  return err;
}

template <typename TAction>
GfErrType LocalRegion::updateNoThrowTX(const CacheableKeyPtr& key,
                                       const CacheablePtr& value,
                                       const UserDataPtr& aCallbackArgument,
                                       CacheablePtr& oldValue, int updateCount,
                                       const CacheEventFlags eventFlags,
                                       VersionTagPtr versionTag,
                                       DataInput* delta, EventIdPtr eventId) {
  GfErrType err = GF_NOERR;
  if ((err = TAction::checkArgs(key, value, delta)) != GF_NOERR) {
    return err;
  }

  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);
  TAction action(*this);

  bool cachingEnabled = m_regionAttributes->getCachingEnabled();
  MapEntryImplPtr entry;

  if (!eventFlags.isNotification() || getProcessedMarker()) {
    if ((err = action.localUpdate(key, value, oldValue, cachingEnabled,
                                  eventFlags, updateCount, versionTag, delta,
                                  eventId)) == GF_CACHE_ENTRY_UPDATED) {
      LOGFINEST(
          "%s: did not change local value for key [%s] since it has "
          "been updated by another thread while operation was in progress",
          TAction::name(), Utils::getCacheableKeyString(key)->asChar());
      err = GF_NOERR;
    } else if (err == GF_CACHE_ENTRY_NOT_FOUND) {
      // Entry not found. Possibly because the entry was added and removed in
      // the
      // same transaction. Ignoring this error #739
      LOGFINE(
          "%s: No entry found. Possibly because the entry was added and "
          "removed in the same transaction. "
          "Ignoring this error. ",
          TAction::name(), Utils::getCacheableKeyString(key)->asChar());
      err = GF_NOERR;
    } else if (err != GF_NOERR) {
      return err;
    }
  } else {  // if (getProcessedMarker())
    action.getCallbackOldValue(cachingEnabled, key, entry, oldValue);
    if (updateCount >= 0 &&
        !m_regionAttributes->getConcurrencyChecksEnabled()) {
      m_entries->removeTrackerForEntry(key);
    }
  }
  // invokeCacheListenerForEntryEvent method has the check that if oldValue
  // is a CacheableToken then it sets it to NULL; also determines if it
  // should be AFTER_UPDATE or AFTER_CREATE depending on oldValue
  err =
      invokeCacheListenerForEntryEvent(key, oldValue, value, aCallbackArgument,
                                       eventFlags, TAction::s_afterEventType);
  return err;
}

GfErrType LocalRegion::putNoThrow(const CacheableKeyPtr& key,
                                  const CacheablePtr& value,
                                  const UserDataPtr& aCallbackArgument,
                                  CacheablePtr& oldValue, int updateCount,
                                  const CacheEventFlags eventFlags,
                                  VersionTagPtr versionTag, DataInput* delta,
                                  EventIdPtr eventId) {
  return updateNoThrow<PutActions>(key, value, aCallbackArgument, oldValue,
                                   updateCount, eventFlags, versionTag, delta,
                                   eventId);
}

GfErrType LocalRegion::putNoThrowTX(const CacheableKeyPtr& key,
                                    const CacheablePtr& value,
                                    const UserDataPtr& aCallbackArgument,
                                    CacheablePtr& oldValue, int updateCount,
                                    const CacheEventFlags eventFlags,
                                    VersionTagPtr versionTag, DataInput* delta,
                                    EventIdPtr eventId) {
  return updateNoThrowTX<PutActionsTx>(key, value, aCallbackArgument, oldValue,
                                       updateCount, eventFlags, versionTag,
                                       delta, eventId);
}

GfErrType LocalRegion::createNoThrow(const CacheableKeyPtr& key,
                                     const CacheablePtr& value,
                                     const UserDataPtr& aCallbackArgument,
                                     int updateCount,
                                     const CacheEventFlags eventFlags,
                                     VersionTagPtr versionTag) {
  CacheablePtr oldValue;
  return updateNoThrow<CreateActions>(key, value, aCallbackArgument, oldValue,
                                      updateCount, eventFlags, versionTag);
}

GfErrType LocalRegion::destroyNoThrow(const CacheableKeyPtr& key,
                                      const UserDataPtr& aCallbackArgument,
                                      int updateCount,
                                      const CacheEventFlags eventFlags,
                                      VersionTagPtr versionTag) {
  CacheablePtr oldValue;
  return updateNoThrow<DestroyActions>(key, NULLPTR, aCallbackArgument,
                                       oldValue, updateCount, eventFlags,
                                       versionTag);
}

GfErrType LocalRegion::destroyNoThrowTX(const CacheableKeyPtr& key,
                                        const UserDataPtr& aCallbackArgument,
                                        int updateCount,
                                        const CacheEventFlags eventFlags,
                                        VersionTagPtr versionTag) {
  CacheablePtr oldValue;
  return updateNoThrowTX<DestroyActions>(key, NULLPTR, aCallbackArgument,
                                         oldValue, updateCount, eventFlags,
                                         versionTag);
}

GfErrType LocalRegion::removeNoThrow(const CacheableKeyPtr& key,
                                     const CacheablePtr& value,
                                     const UserDataPtr& aCallbackArgument,
                                     int updateCount,
                                     const CacheEventFlags eventFlags,
                                     VersionTagPtr versionTag) {
  CacheablePtr oldValue;
  return updateNoThrow<RemoveActions>(key, value, aCallbackArgument, oldValue,
                                      updateCount, eventFlags, versionTag);
}

GfErrType LocalRegion::removeNoThrowEx(const CacheableKeyPtr& key,
                                       const UserDataPtr& aCallbackArgument,
                                       int updateCount,
                                       const CacheEventFlags eventFlags,
                                       VersionTagPtr versionTag) {
  CacheablePtr oldValue;
  return updateNoThrow<RemoveActionsEx>(key, NULLPTR, aCallbackArgument,
                                        oldValue, updateCount, eventFlags,
                                        versionTag);
}

GfErrType LocalRegion::invalidateNoThrow(const CacheableKeyPtr& key,
                                         const UserDataPtr& aCallbackArgument,
                                         int updateCount,
                                         const CacheEventFlags eventFlags,
                                         VersionTagPtr versionTag) {
  CacheablePtr oldValue;
  return updateNoThrow<InvalidateActions>(key, NULLPTR, aCallbackArgument,
                                          oldValue, updateCount, eventFlags,
                                          versionTag);
}

GfErrType LocalRegion::invalidateNoThrowTX(const CacheableKeyPtr& key,
                                           const UserDataPtr& aCallbackArgument,
                                           int updateCount,
                                           const CacheEventFlags eventFlags,
                                           VersionTagPtr versionTag) {
  CacheablePtr oldValue;
  return updateNoThrowTX<InvalidateActions>(key, NULLPTR, aCallbackArgument,
                                            oldValue, updateCount, eventFlags,
                                            versionTag);
}

GfErrType LocalRegion::putAllNoThrow(const HashMapOfCacheable& map,
                                     uint32_t timeout,
                                     const UserDataPtr& aCallbackArgument) {
  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);
  GfErrType err = GF_NOERR;
  // VersionTagPtr versionTag;
  VersionedCacheableObjectPartListPtr
      versionedObjPartListPtr;  //= new VersionedCacheableObjectPartList();
  TXState* txState = getTXState();
  if (txState != NULL) {
    if (isLocalOp()) {
      return GF_NOTSUP;
    }
    // if(!txState->isReplay())
    //{
    //	VectorOfCacheablePtr args(new VectorOfCacheable());
    //	args->push_back(HashMapOfCacheablePtr(new HashMapOfCacheable(map)));
    //	args->push_back(CacheableInt32::create(timeout));
    //	txState->recordTXOperation(GF_PUT_ALL, getFullPath(), NULLPTR, args);
    //}
    err = putAllNoThrow_remote(map, /*versionTag*/ versionedObjPartListPtr,
                               timeout, aCallbackArgument);
    if (err == GF_NOERR) {
      txState->setDirty();
    }

    return err;
  }

  bool cachingEnabled = m_regionAttributes->getCachingEnabled();
  MapOfOldValue oldValueMap;

  // remove tracking for the entries befor exiting the function
  struct RemoveTracking {
   private:
    const MapOfOldValue& m_oldValueMap;
    LocalRegion& m_region;

   public:
    RemoveTracking(const MapOfOldValue& oldValueMap, LocalRegion& region)
        : m_oldValueMap(oldValueMap), m_region(region) {}
    ~RemoveTracking() {
      if (!m_region.getAttributes()->getConcurrencyChecksEnabled()) {
        // need to remove the tracking added to the entries at the end
        for (MapOfOldValue::const_iterator iter = m_oldValueMap.begin();
             iter != m_oldValueMap.end(); ++iter) {
          if (iter->second.second >= 0) {
            m_region.m_entries->removeTrackerForEntry(iter->first);
          }
        }
      }
    }
  } _removeTracking(oldValueMap, *this);

  if (cachingEnabled || m_writer != NULLPTR) {
    CacheablePtr oldValue;
    for (HashMapOfCacheable::Iterator iter = map.begin(); iter != map.end();
         ++iter) {
      const CacheableKeyPtr& key = iter.first();
      if (cachingEnabled &&
          !m_regionAttributes->getConcurrencyChecksEnabled()) {
        int updateCount =
            m_entries->addTrackerForEntry(key, oldValue, true, false, true);
        oldValueMap.insert(
            std::make_pair(key, std::make_pair(oldValue, updateCount)));
      }
      if (m_writer != NULLPTR) {
        // invokeCacheWriterForEntryEvent method has the check that if oldValue
        // is a CacheableToken then it sets it to NULL; also determines if it
        // should be BEFORE_UPDATE or BEFORE_CREATE depending on oldValue
        if (!invokeCacheWriterForEntryEvent(
                key, oldValue, iter.second(), aCallbackArgument,
                CacheEventFlags::LOCAL, BEFORE_UPDATE)) {
          PutActions::logCacheWriterFailure(key, oldValue);
          return GF_CACHEWRITER_ERROR;
        }
      }
    }
  }
  // try remote putAll, if any
  if ((err = putAllNoThrow_remote(map, versionedObjPartListPtr, timeout,
                                  aCallbackArgument)) != GF_NOERR) {
    return err;
  }
  // next the local puts
  GfErrType localErr;
  VersionTagPtr versionTag;

  if (cachingEnabled) {
    if (m_isPRSingleHopEnabled) { /*New PRSingleHop Case:: PR Singlehop
                                     condition*/
      for (int keyIndex = 0;
           keyIndex < versionedObjPartListPtr->getSucceededKeys()->size();
           keyIndex++) {
        const CacheablePtr valPtr =
            versionedObjPartListPtr->getSucceededKeys()->at(keyIndex);
        HashMapOfCacheable::Iterator mapIter = map.find(valPtr);
        CacheableKeyPtr key = NULLPTR;
        CacheablePtr value = NULLPTR;

        if (mapIter != map.end()) {
          key = mapIter.first();
          value = mapIter.second();
        } else {
          // ThrowERROR
          LOGERROR(
              "ERROR :: LocalRegion::putAllNoThrow() Key must be found in the "
              "usermap");
        }

        if (versionedObjPartListPtr != NULLPTR &&
            versionedObjPartListPtr.ptr() != NULL) {
          LOGDEBUG("versionedObjPartListPtr->getVersionedTagptr().size() = %d ",
                   versionedObjPartListPtr->getVersionedTagptr().size());
          if (versionedObjPartListPtr->getVersionedTagptr().size() > 0) {
            versionTag =
                versionedObjPartListPtr->getVersionedTagptr()[keyIndex];
          }
        }
        std::pair<CacheablePtr, int>& p = oldValueMap[key];
        if ((localErr = LocalRegion::putNoThrow(
                 key, value, aCallbackArgument, p.first, p.second,
                 CacheEventFlags::LOCAL | CacheEventFlags::NOCACHEWRITER,
                 versionTag)) == GF_CACHE_ENTRY_UPDATED) {
          LOGFINEST(
              "Region::putAll: did not change local value for key [%s] "
              "since it has been updated by another thread while operation was "
              "in progress",
              Utils::getCacheableKeyString(key)->asChar());
        } else if (localErr == GF_CACHE_LISTENER_EXCEPTION) {
          LOGFINER("Region::putAll: invoke listener error [%d] for key [%s]",
                   localErr, Utils::getCacheableKeyString(key)->asChar());
          err = localErr;
        } else if (localErr != GF_NOERR) {
          return localErr;
        }
      }      // End of for loop
    } else { /*Non SingleHop case :: PUTALL has taken multiple hops*/
      LOGDEBUG(
          "NILKANTH LocalRegion::putAllNoThrow m_isPRSingleHopEnabled = %d "
          "expected false",
          m_isPRSingleHopEnabled);
      int index = 0;
      for (HashMapOfCacheable::Iterator iter = map.begin(); iter != map.end();
           ++iter) {
        const CacheableKeyPtr& key = iter.first();
        const CacheablePtr& value = iter.second();
        std::pair<CacheablePtr, int>& p = oldValueMap[key];

        if (versionedObjPartListPtr != NULLPTR &&
            versionedObjPartListPtr.ptr() != NULL) {
          LOGDEBUG("versionedObjPartListPtr->getVersionedTagptr().size() = %d ",
                   versionedObjPartListPtr->getVersionedTagptr().size());
          if (versionedObjPartListPtr->getVersionedTagptr().size() > 0) {
            versionTag = versionedObjPartListPtr->getVersionedTagptr()[index++];
          }
        }
        if ((localErr = LocalRegion::putNoThrow(
                 key, value, aCallbackArgument, p.first, p.second,
                 CacheEventFlags::LOCAL | CacheEventFlags::NOCACHEWRITER,
                 versionTag)) == GF_CACHE_ENTRY_UPDATED) {
          LOGFINEST(
              "Region::putAll: did not change local value for key [%s] "
              "since it has been updated by another thread while operation was "
              "in progress",
              Utils::getCacheableKeyString(key)->asChar());
        } else if (localErr == GF_CACHE_LISTENER_EXCEPTION) {
          LOGFINER("Region::putAll: invoke listener error [%d] for key [%s]",
                   localErr, Utils::getCacheableKeyString(key)->asChar());
          err = localErr;
        } else if (localErr != GF_NOERR) {
          return localErr;
        }
      }
    }
  }

  m_regionStats->incPutAll();
  return err;
}

GfErrType LocalRegion::removeAllNoThrow(const VectorOfCacheableKey& keys,
                                        const UserDataPtr& aCallbackArgument) {
  // 1. check destroy pending
  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);
  GfErrType err = GF_NOERR;
  VersionedCacheableObjectPartListPtr versionedObjPartListPtr;

  // 2.check transaction state and do remote op
  TXState* txState = getTXState();
  if (txState != NULL) {
    if (isLocalOp()) return GF_NOTSUP;
    err = removeAllNoThrow_remote(keys, versionedObjPartListPtr,
                                  aCallbackArgument);
    if (err == GF_NOERR) txState->setDirty();
    return err;
  }

  // 3.add tracking
  bool cachingEnabled = m_regionAttributes->getCachingEnabled();

  // 4. do remote removeAll
  err =
      removeAllNoThrow_remote(keys, versionedObjPartListPtr, aCallbackArgument);
  if (err != GF_NOERR) {
    return err;
  }

  // 5. update local cache
  GfErrType localErr;
  VersionTagPtr versionTag;
  if (cachingEnabled) {
    VectorOfCacheableKey* keysPtr;
    if (m_isPRSingleHopEnabled) {
      keysPtr = versionedObjPartListPtr->getSucceededKeys().ptr();
    } else {
      keysPtr = const_cast<VectorOfCacheableKey*>(&keys);
    }

    for (int keyIndex = 0; keyIndex < keysPtr->size(); keyIndex++) {
      CacheableKeyPtr key = keysPtr->at(keyIndex);
      if (versionedObjPartListPtr != NULLPTR &&
          versionedObjPartListPtr.ptr() != NULL) {
        LOGDEBUG("versionedObjPartListPtr->getVersionedTagptr().size() = %d ",
                 versionedObjPartListPtr->getVersionedTagptr().size());
        if (versionedObjPartListPtr->getVersionedTagptr().size() > 0) {
          versionTag = versionedObjPartListPtr->getVersionedTagptr()[keyIndex];
        }
        if (versionTag == NULLPTR) {
          LOGDEBUG(
              "RemoveAll hits EntryNotFoundException at server side for key "
              "[%s], not to destroy it from local cache.",
              Utils::getCacheableKeyString(key)->asChar());
          continue;
        }
      }

      if ((localErr = LocalRegion::destroyNoThrow(
               key, aCallbackArgument, -1,
               CacheEventFlags::LOCAL | CacheEventFlags::NOCACHEWRITER,
               versionTag)) == GF_CACHE_ENTRY_UPDATED) {
        LOGFINEST(
            "Region::removeAll: did not remove local value for key [%s] "
            "since it has been updated by another thread while operation was "
            "in progress",
            Utils::getCacheableKeyString(key)->asChar());
      } else if (localErr == GF_CACHE_LISTENER_EXCEPTION) {
        LOGFINER("Region::removeAll: invoke listener error [%d] for key [%s]",
                 localErr, Utils::getCacheableKeyString(key)->asChar());
        err = localErr;
      } else if (localErr == GF_CACHE_ENTRY_NOT_FOUND) {
        LOGFINER("Region::removeAll: error [%d] for key [%s]", localErr,
                 Utils::getCacheableKeyString(key)->asChar());
      } else if (localErr != GF_NOERR) {
        return localErr;
      }
    }  // End of for loop
  }

  // 6.update stats
  m_regionStats->incRemoveAll();
  return err;
}

void LocalRegion::clear(const UserDataPtr& aCallbackArgument) {
  /*update the stats */
  int64 sampleStartNanos = Utils::startStatOpTime();
  localClear(aCallbackArgument);
  Utils::updateStatOpTime(m_regionStats->getStat(),
                          RegionStatType::getInstance()->getClearsId(),
                          sampleStartNanos);
}
void LocalRegion::localClear(const UserDataPtr& aCallbackArgument) {
  GfErrType err = localClearNoThrow(aCallbackArgument, CacheEventFlags::LOCAL);
  if (err != GF_NOERR) GfErrTypeToException("LocalRegion::localClear", err);
}
GfErrType LocalRegion::localClearNoThrow(const UserDataPtr& aCallbackArgument,
                                         const CacheEventFlags eventFlags) {
  bool cachingEnabled = m_regionAttributes->getCachingEnabled();
  /*Update the stats for clear*/
  m_regionStats->incClears();
  GfErrType err = GF_NOERR;
  TryReadGuard guard(m_rwLock, m_destroyPending);
  if (m_released || m_destroyPending) return err;
  if (!invokeCacheWriterForRegionEvent(aCallbackArgument, eventFlags,
                                       BEFORE_REGION_CLEAR)) {
    LOGFINE("Cache writer prevented region clear");
    return GF_CACHEWRITER_ERROR;
  }
  if (cachingEnabled == true) m_entries->clear();
  if (!eventFlags.isNormal()) {
    err = invokeCacheListenerForRegionEvent(aCallbackArgument, eventFlags,
                                            AFTER_REGION_CLEAR);
  }
  return err;
}

GfErrType LocalRegion::invalidateLocal(const char* name,
                                       const CacheableKeyPtr& keyPtr,
                                       const CacheablePtr& value,
                                       const CacheEventFlags eventFlags,
                                       VersionTagPtr versionTag) {
  if (keyPtr == NULLPTR) {
    return GF_CACHE_ILLEGAL_ARGUMENT_EXCEPTION;
  }
  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);

  GfErrType err = GF_NOERR;

  bool cachingEnabled = m_regionAttributes->getCachingEnabled();
  CacheablePtr oldValue;
  MapEntryImplPtr me;

  if (!eventFlags.isNotification() || getProcessedMarker()) {
    if (cachingEnabled) {
      LOGDEBUG("%s: region [%s] invalidating key [%s], value [%s]", name,
               getFullPath(), Utils::getCacheableKeyString(keyPtr)->asChar(),
               Utils::getCacheableString(value)->asChar());
      /* adongre - Coverity II
       * CID 29193: Parse warning (PW.PARAMETER_HIDDEN)
       */
      // VersionTagPtr versionTag;
      if ((err = m_entries->invalidate(keyPtr, me, oldValue, versionTag)) !=
          GF_NOERR) {
        if (eventFlags.isNotification()) {
          LOGDEBUG(
              "Region::invalidate: region [%s] invalidate key [%s] "
              "failed with error %d",
              getFullPath(), Utils::getCacheableKeyString(keyPtr)->asChar(),
              err);
        }
        if (err == GF_CACHE_CONCURRENT_MODIFICATION_EXCEPTION) {
          LOGDEBUG(
              "Region::invalidateLocal: invalidate for key [%s] failed because the cache already contains \
            an entry with higher version. The cache listener will not be invoked.",
              Utils::getCacheableKeyString(keyPtr)->asChar());
          // Cache listener won't be called in this case
          return GF_NOERR;
        }
        //  for notification invoke the listener even if the key does
        // not exist locally
        if (!eventFlags.isNotification() || err != GF_CACHE_ENTRY_NOT_FOUND) {
          return err;
        } else {
          err = GF_NOERR;
        }
      } else {
        LOGDEBUG("Region::invalidate: region [%s] invalidated key [%s]",
                 getFullPath(), Utils::getCacheableKeyString(keyPtr)->asChar());
      }
      // entry/region expiration
      if (!eventFlags.isEvictOrExpire()) {
        updateAccessAndModifiedTime(true);
      }
    }
  } else {  // if (getProcessedMarker())
    if (cachingEnabled) {
      m_entries->getEntry(keyPtr, me, oldValue);
    }
  }
  return err;
}

GfErrType LocalRegion::invalidateRegionNoThrow(
    const UserDataPtr& aCallbackArgument, const CacheEventFlags eventFlags) {
  CHECK_DESTROY_PENDING_NOTHROW(TryReadGuard);
  GfErrType err = GF_NOERR;

  if (m_regionAttributes->getCachingEnabled()) {
    VectorOfCacheableKey v;
    keys_internal(v);
    uint32_t size = v.size();
    MapEntryImplPtr me;
    for (uint32_t i = 0; i < size; i++) {
      {
        CacheablePtr oldValue;
        // invalidate all the entries with a NULL versionTag
        VersionTagPtr versionTag;
        m_entries->invalidate(v.at(i), me, oldValue, versionTag);
        if (!eventFlags.isEvictOrExpire()) {
          updateAccessAndModifiedTimeForEntry(me, true);
        }
      }
    }
    if (!eventFlags.isEvictOrExpire()) {
      updateAccessAndModifiedTime(true);
    }
  }

  // try remote region invalidate, if any
  if (!eventFlags.isLocal()) {
    err = invalidateRegionNoThrow_remote(aCallbackArgument);
    if (err != GF_NOERR) return err;
  }

  if (m_subRegions.current_size() > 0) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> subguard(m_subRegions.mutex());
    for (MapOfRegionWithLock::iterator p = m_subRegions.begin();
         p != m_subRegions.end(); ++p) {
      RegionInternal* subRegion =
          dynamic_cast<RegionInternal*>((*p).int_id_.ptr());
      if (subRegion != NULL) {
        err = subRegion->invalidateRegionNoThrow(aCallbackArgument, eventFlags);
        if (err != GF_NOERR) {
          return err;
        }
      }
    }
  }
  err = invokeCacheListenerForRegionEvent(aCallbackArgument, eventFlags,
                                          AFTER_REGION_INVALIDATE);

  return err;
}

GfErrType LocalRegion::destroyRegionNoThrow(
    const UserDataPtr& aCallbackArgument, bool removeFromParent,
    const CacheEventFlags eventFlags) {
  // Get global locks to synchronize with failover thread.
  // TODO:  This should go into RegionGlobalLocks
  // The distMngrsLock is required before RegionGlobalLocks since failover
  // thread acquires distMngrsLock and then tries to acquire endpoints lock
  // which is already taken by RegionGlobalLocks here.
  DistManagersLockGuard _guard(m_cacheImpl->tcrConnectionManager());
  RegionGlobalLocks acquireLocks(this);

  // Fix for BUG:849, i.e Remove subscription on region before destroying the
  // region
  if (eventFlags == CacheEventFlags::LOCAL) {
    if (unregisterKeysBeforeDestroyRegion() != GF_NOERR) {
      LOGDEBUG(
          "DEBUG :: LocalRegion::destroyRegionNoThrow UnregisteredKeys Failed");
    }
  }

  TryWriteGuard guard(m_rwLock, m_destroyPending);
  if (m_destroyPending) {
    if (eventFlags.isCacheClose()) {
      return GF_NOERR;
    } else {
      return GF_CACHE_REGION_DESTROYED_EXCEPTION;
    }
  }

  m_destroyPending = true;
  LOGDEBUG("LocalRegion::destroyRegionNoThrow( ): set flag destroy-pending.");

  GfErrType err = GF_NOERR;

  //  do not invoke the writer for expiry or notification
  if (!eventFlags.isNotification() && !eventFlags.isEvictOrExpire()) {
    if (!invokeCacheWriterForRegionEvent(aCallbackArgument, eventFlags,
                                         BEFORE_REGION_DESTROY)) {
      //  do not let CacheWriter veto when this is Cache::close()
      if (!eventFlags.isCacheClose()) {
        LOGFINE("Cache writer prevented region destroy");
        m_destroyPending = false;
        return GF_CACHEWRITER_ERROR;
      }
    }
    //  for the expiry case try the local destroy first and remote
    // destroy only if local destroy succeeds
    if (!eventFlags.isLocal()) {
      err = destroyRegionNoThrow_remote(aCallbackArgument);
      if (err != GF_NOERR) {
        m_destroyPending = false;
        return err;
      }
    }
  }

  LOGFINE("Region %s is being destroyed", m_fullPath.c_str());
  {
    MapOfRegionGuard guard(m_subRegions.mutex());
    for (MapOfRegionWithLock::iterator p = m_subRegions.begin();
         p != m_subRegions.end(); ++p) {
      // TODO: remove unnecessary dynamic_cast by having m_subRegions hold
      // RegionInternal and invoke the destroy method in that
      RegionInternal* subRegion =
          dynamic_cast<RegionInternal*>((*p).int_id_.ptr());
      if (subRegion != NULL) {
        //  for subregions never remove from parent since that will cause
        // the region to be destroyed and SEGV; unbind_all takes care of that
        // Also don't send remote destroy message for sub-regions
        err = subRegion->destroyRegionNoThrow(
            aCallbackArgument, false, eventFlags | CacheEventFlags::LOCAL);
        //  for Cache::close() keep going as far as possible
        if (err != GF_NOERR && !eventFlags.isCacheClose()) {
          m_destroyPending = false;
          return err;
        }
      }
    }
  }
  m_subRegions.unbind_all();

  //  for the expiry case try the local destroy first and remote
  // destroy only if local destroy succeeds
  if (eventFlags.isEvictOrExpire() && !eventFlags.isLocal()) {
    err = destroyRegionNoThrow_remote(aCallbackArgument);
    if (err != GF_NOERR) {
      m_destroyPending = false;
      return err;
    }
  }
  //  if we are not removing from parent then this is a proper
  // region close so invoke listener->close() also
  err = invokeCacheListenerForRegionEvent(aCallbackArgument, eventFlags,
                                          AFTER_REGION_DESTROY);

  release(true);
  if (m_regionAttributes->getCachingEnabled()) {
    GF_SAFE_DELETE(m_entries);
  }
  GF_D_ASSERT(m_destroyPending);

  if (removeFromParent) {
    if (m_parentRegion == NULLPTR) {
      m_cacheImpl->removeRegion(m_name.c_str());
    } else {
      LocalRegion* parent = dynamic_cast<LocalRegion*>(m_parentRegion.ptr());
      if (parent != NULL) {
        parent->removeRegion(m_name);
        if (!eventFlags.isEvictOrExpire()) {
          parent->updateAccessAndModifiedTime(true);
        }
      }
    }
  }
  return err;
}

GfErrType LocalRegion::putLocal(const char* name, bool isCreate,
                                const CacheableKeyPtr& key,
                                const CacheablePtr& value,
                                CacheablePtr& oldValue, bool cachingEnabled,
                                int updateCount, int destroyTracker,
                                VersionTagPtr versionTag, DataInput* delta,
                                EventIdPtr eventId) {
  GfErrType err = GF_NOERR;
  bool isUpdate = !isCreate;
  if (cachingEnabled) {
    MapEntryImplPtr entry;
    LOGDEBUG("%s: region [%s] putting key [%s], value [%s]", name,
             getFullPath(), Utils::getCacheableKeyString(key)->asChar(),
             Utils::getCacheableString(value)->asChar());
    if (isCreate) {
      err = m_entries->create(key, value, entry, oldValue, updateCount,
                              destroyTracker, versionTag);
    } else {
      err = m_entries->put(key, value, entry, oldValue, updateCount,
                           destroyTracker, versionTag, isUpdate, delta);
      if (err == GF_INVALID_DELTA) {
        m_cacheImpl->m_cacheStats->incFailureOnDeltaReceived();
        // PXR: Get full object from server.
        CacheablePtr& newValue1 = const_cast<CacheablePtr&>(value);
        VersionTagPtr versionTag1;
        err = getNoThrow_FullObject(eventId, newValue1, versionTag1);
        if (err == GF_NOERR && newValue1 != NULLPTR) {
          err = m_entries->put(
              key, newValue1, entry, oldValue, updateCount, destroyTracker,
              versionTag1 != NULLPTR ? versionTag1 : versionTag, isUpdate);
        }
      }
      if (delta != NULL &&
          err == GF_NOERR) {  // Means that delta is on and there is no failure.
        m_cacheImpl->m_cacheStats->incDeltaReceived();
      }
    }
    if (err != GF_NOERR) {
      return err;
    }
    LOGDEBUG("%s: region [%s] %s key [%s], value [%s]", name, getFullPath(),
             isUpdate ? "updated" : "created",
             Utils::getCacheableKeyString(key)->asChar(),
             Utils::getCacheableString(value)->asChar());
    // entry/region expiration
    if (entryExpiryEnabled()) {
      if (isUpdate && entry->getExpProperties().getExpiryTaskId() != -1) {
        updateAccessAndModifiedTimeForEntry(entry, true);
      } else {
        registerEntryExpiryTask(entry);
      }
    }
    updateAccessAndModifiedTime(true);
  }
  // update the stats
  if (isUpdate) {
    m_regionStats->incPuts();
    m_cacheImpl->m_cacheStats->incPuts();
  } else {
    if (cachingEnabled) {
      m_regionStats->setEntries(m_entries->size());
      m_cacheImpl->m_cacheStats->incEntries(1);
    }
    m_regionStats->incCreates();
    m_cacheImpl->m_cacheStats->incCreates();
  }
  return err;
}

void LocalRegion::keys_internal(VectorOfCacheableKey& v) {
  if (!m_regionAttributes->getCachingEnabled()) {
    return;
  }
  uint32_t size = m_entries->size();
  v.clear();
  if (size == 0) {
    return;
  }
  m_entries->keys(v);
}

void LocalRegion::entries_internal(VectorOfRegionEntry& me,
                                   const bool recursive) {
  m_entries->entries(me);

  if (recursive == true) {
    MapOfRegionGuard guard(m_subRegions.mutex());
    for (MapOfRegionWithLock::iterator p = m_subRegions.begin();
         p != m_subRegions.end(); ++p) {
      dynamic_cast<LocalRegion*>((*p).int_id_.ptr())
          ->entries_internal(me, true);
    }
  }
}

int LocalRegion::removeRegion(const std::string& name) {
  if (m_subRegions.current_size() == 0) {
    return 0;
  }
  return m_subRegions.unbind(name);
}

bool LocalRegion::invokeCacheWriterForEntryEvent(
    const CacheableKeyPtr& key, CacheablePtr& oldValue,
    const CacheablePtr& newValue, const UserDataPtr& aCallbackArgument,
    CacheEventFlags eventFlags, EntryEventType type) {
  // Check if we have a local cache writer. If so, invoke and return.
  bool bCacheWriterReturn = true;
  if (m_writer != NULLPTR) {
    if (oldValue != NULLPTR && CacheableToken::isInvalid(oldValue)) {
      oldValue = NULLPTR;
    }
    EntryEvent event(RegionPtr(this), key, oldValue, newValue,
                     aCallbackArgument, eventFlags.isNotification());
    const char* eventStr = "unknown";
    try {
      bool updateStats = true;
      /*Update the CacheWriter Stats*/
      int64 sampleStartNanos = Utils::startStatOpTime();
      switch (type) {
        case BEFORE_UPDATE: {
          if (oldValue != NULLPTR) {
            eventStr = "beforeUpdate";
            bCacheWriterReturn = m_writer->beforeUpdate(event);
            break;
          }
          // if oldValue is NULL then fall to BEFORE_CREATE case
        }
        case BEFORE_CREATE: {
          eventStr = "beforeCreate";
          bCacheWriterReturn = m_writer->beforeCreate(event);
          break;
        }
        case BEFORE_DESTROY: {
          eventStr = "beforeDestroy";
          bCacheWriterReturn = m_writer->beforeDestroy(event);
          break;
        }
        default: {
          updateStats = false;
          break;
        }
      }

      if (updateStats) {
        Utils::updateStatOpTime(
            m_regionStats->getStat(),
            RegionStatType::getInstance()->getWriterCallTimeId(),
            sampleStartNanos);
        m_regionStats->incWriterCallsCompleted();
      }

    } catch (const Exception& ex) {
      LOGERROR("Exception in CacheWriter::%s: %s: %s", eventStr, ex.getName(),
               ex.getMessage());
      bCacheWriterReturn = false;
    } catch (...) {
      LOGERROR("Unknown exception in CacheWriter::%s", eventStr);
      bCacheWriterReturn = false;
    }
  }
  return bCacheWriterReturn;
}

bool LocalRegion::invokeCacheWriterForRegionEvent(
    const UserDataPtr& aCallbackArgument, CacheEventFlags eventFlags,
    RegionEventType type) {
  // Check if we have a local cache writer. If so, invoke and return.
  bool bCacheWriterReturn = true;
  if (m_writer != NULLPTR) {
    RegionEvent event(RegionPtr(this), aCallbackArgument,
                      eventFlags.isNotification());
    const char* eventStr = "unknown";
    try {
      bool updateStats = true;
      /*Update the CacheWriter Stats*/
      int64 sampleStartNanos = Utils::startStatOpTime();
      switch (type) {
        case BEFORE_REGION_DESTROY: {
          eventStr = "beforeRegionDestroy";
          bCacheWriterReturn = m_writer->beforeRegionDestroy(event);
          break;
        }
        case BEFORE_REGION_CLEAR: {
          eventStr = "beforeRegionClear";
          bCacheWriterReturn = m_writer->beforeRegionClear(event);
          break;
        }
        default: {
          updateStats = false;
          break;
        }
      }
      if (updateStats) {
        Utils::updateStatOpTime(
            m_regionStats->getStat(),
            RegionStatType::getInstance()->getWriterCallTimeId(),
            sampleStartNanos);
        m_regionStats->incWriterCallsCompleted();
      }
    } catch (const Exception& ex) {
      LOGERROR("Exception in CacheWriter::%s: %s", eventStr, ex.getName(),
               ex.getMessage());
      bCacheWriterReturn = false;
    } catch (...) {
      LOGERROR("Unknown exception in CacheWriter::%s", eventStr);
      bCacheWriterReturn = false;
    }
  }
  return bCacheWriterReturn;
}

GfErrType LocalRegion::invokeCacheListenerForEntryEvent(
    const CacheableKeyPtr& key, CacheablePtr& oldValue,
    const CacheablePtr& newValue, const UserDataPtr& aCallbackArgument,
    CacheEventFlags eventFlags, EntryEventType type, bool isLocal) {
  GfErrType err = GF_NOERR;

  // Check if we have a local cache listener. If so, invoke and return.
  if (m_listener != NULLPTR) {
    if (oldValue != NULLPTR && CacheableToken::isInvalid(oldValue)) {
      oldValue = NULLPTR;
    }
    EntryEvent event(RegionPtr(this), key, oldValue, newValue,
                     aCallbackArgument, eventFlags.isNotification());
    const char* eventStr = "unknown";
    try {
      bool updateStats = true;
      /*Update the CacheWriter Stats*/
      int64 sampleStartNanos = Utils::startStatOpTime();
      switch (type) {
        case AFTER_UPDATE: {
          //  when CREATE is received from server for notification
          // then force an afterUpdate even if key is not present in cache.
          if (oldValue != NULLPTR || eventFlags.isNotificationUpdate() ||
              isLocal) {
            eventStr = "afterUpdate";
            m_listener->afterUpdate(event);
            break;
          }
          // if oldValue is NULL then fall to AFTER_CREATE case
        }
        case AFTER_CREATE: {
          eventStr = "afterCreate";
          m_listener->afterCreate(event);
          break;
        }
        case AFTER_DESTROY: {
          eventStr = "afterDestroy";
          m_listener->afterDestroy(event);
          break;
        }
        case AFTER_INVALIDATE: {
          eventStr = "afterInvalidate";
          m_listener->afterInvalidate(event);
          break;
        }
        default: {
          updateStats = false;
          break;
        }
      }
      if (updateStats) {
        m_cacheImpl->m_cacheStats->incListenerCalls();
        Utils::updateStatOpTime(
            m_regionStats->getStat(),
            RegionStatType::getInstance()->getListenerCallTimeId(),
            sampleStartNanos);
        m_regionStats->incListenerCallsCompleted();
      }
    } catch (const Exception& ex) {
      LOGERROR("Exception in CacheListener for key[%s]::%s: %s: %s",
               Utils::getCacheableKeyString(key)->asChar(), eventStr,
               ex.getName(), ex.getMessage());
      err = GF_CACHE_LISTENER_EXCEPTION;
    } catch (...) {
      LOGERROR("Unknown exception in CacheListener for key[%s]::%s",
               Utils::getCacheableKeyString(key)->asChar(), eventStr);
      err = GF_CACHE_LISTENER_EXCEPTION;
    }
  }
  return err;
}

GfErrType LocalRegion::invokeCacheListenerForRegionEvent(
    const UserDataPtr& aCallbackArgument, CacheEventFlags eventFlags,
    RegionEventType type) {
  GfErrType err = GF_NOERR;

  // Check if we have a local cache listener. If so, invoke and return.
  if (m_listener != NULLPTR) {
    RegionEvent event(RegionPtr(this), aCallbackArgument,
                      eventFlags.isNotification());
    const char* eventStr = "unknown";
    try {
      bool updateStats = true;
      /*Update the CacheWriter Stats*/
      int64 sampleStartNanos = Utils::startStatOpTime();
      switch (type) {
        case AFTER_REGION_DESTROY: {
          eventStr = "afterRegionDestroy";
          m_listener->afterRegionDestroy(event);
          m_cacheImpl->m_cacheStats->incListenerCalls();
          if (eventFlags.isCacheClose()) {
            eventStr = "close";
            m_listener->close(RegionPtr(this));
            m_cacheImpl->m_cacheStats->incListenerCalls();
          }
          break;
        }
        case AFTER_REGION_INVALIDATE: {
          eventStr = "afterRegionInvalidate";
          m_listener->afterRegionInvalidate(event);
          m_cacheImpl->m_cacheStats->incListenerCalls();
          break;
        }
        case AFTER_REGION_CLEAR: {
          eventStr = "afterRegionClear";
          m_listener->afterRegionClear(event);
          break;
        }
        default: {
          updateStats = false;
          break;
        }
      }
      if (updateStats) {
        Utils::updateStatOpTime(
            m_regionStats->getStat(),
            RegionStatType::getInstance()->getListenerCallTimeId(),
            sampleStartNanos);
        m_regionStats->incListenerCallsCompleted();
      }
    } catch (const Exception& ex) {
      LOGERROR("Exception in CacheListener::%s: %s: %s", eventStr, ex.getName(),
               ex.getMessage());
      err = GF_CACHE_LISTENER_EXCEPTION;
    } catch (...) {
      LOGERROR("Unknown exception in CacheListener::%s", eventStr);
      err = GF_CACHE_LISTENER_EXCEPTION;
    }
  }
  return err;
}

// TODO:  pass current time instead of evaluating it twice, here
// and in region
void LocalRegion::updateAccessAndModifiedTimeForEntry(MapEntryImplPtr& ptr,
                                                      bool modified) {
  // locking is not required since setters use atomic operations
  if (ptr != NULLPTR && entryExpiryEnabled()) {
    ExpEntryProperties& expProps = ptr->getExpProperties();
    uint32_t currTime = static_cast<uint32_t>(ACE_OS::gettimeofday().sec());
    CacheableStringPtr keyStr;
    if (Log::debugEnabled()) {
      CacheableKeyPtr key;
      ptr->getKeyI(key);
      keyStr = Utils::getCacheableKeyString(key);
    }
    LOGDEBUG("Setting last accessed time for key [%s] in region %s to %d",
             keyStr->asChar(), getFullPath(), currTime);
    expProps.updateLastAccessTime(currTime);
    if (modified) {
      LOGDEBUG("Setting last modified time for key [%s] in region %s to %d",
               keyStr->asChar(), getFullPath(), currTime);
      expProps.updateLastModifiedTime(currTime);
    }
  }
}

uint32_t LocalRegion::adjustLruEntriesLimit(uint32_t limit) {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::adjustLruEntriesLimit);

  RegionAttributesPtr attrs = m_regionAttributes;
  if (!attrs->getCachingEnabled()) return 0;
  bool hadlru = (attrs->getLruEntriesLimit() != 0);
  bool needslru = (limit != 0);
  if (hadlru != needslru) {
    throw IllegalStateException(
        "Cannot disable or enable LRU, can only adjust limit.");
  }
  uint32_t oldValue = attrs->getLruEntriesLimit();
  setLruEntriesLimit(limit);
  if (needslru) {
    // checked in AttributesMutator already to assert that LRU was enabled..
    LRUEntriesMap* lrumap = static_cast<LRUEntriesMap*>(m_entries);

    lrumap->adjustLimit(limit);
  }
  return oldValue;
}

ExpirationAction::Action LocalRegion::adjustRegionExpiryAction(
    ExpirationAction::Action action) {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::adjustRegionExpiryAction);

  RegionAttributesPtr attrs = m_regionAttributes;
  bool hadExpiry = (getRegionExpiryDuration() != 0);
  if (!hadExpiry) {
    throw IllegalStateException(
        "Cannot change region ExpirationAction for region created without "
        "region expiry.");
  }
  ExpirationAction::Action oldValue = getRegionExpiryAction();

  setRegionTimeToLiveExpirationAction(action);
  setRegionIdleTimeoutExpirationAction(action);
  // m_regionExpirationAction = action;

  return oldValue;
}

ExpirationAction::Action LocalRegion::adjustEntryExpiryAction(
    ExpirationAction::Action action) {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::adjustEntryExpiryAction);

  RegionAttributesPtr attrs = m_regionAttributes;
  bool hadExpiry = (getEntryExpiryDuration() != 0);
  if (!hadExpiry) {
    throw IllegalStateException(
        "Cannot change entry ExpirationAction for region created without entry "
        "expiry.");
  }
  ExpirationAction::Action oldValue = getEntryExpirationAction();

  setEntryTimeToLiveExpirationAction(action);
  setEntryIdleTimeoutExpirationAction(action);

  return oldValue;
}

int32_t LocalRegion::adjustRegionExpiryDuration(int32_t duration) {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::adjustRegionExpiryDuration);

  RegionAttributesPtr attrs = m_regionAttributes;
  bool hadExpiry = (getEntryExpiryDuration() != 0);
  if (!hadExpiry) {
    throw IllegalStateException(
        "Cannot change region  expiration duration for region created without "
        "region expiry.");
  }
  int32_t oldValue = getRegionExpiryDuration();

  setRegionTimeToLive(duration);
  setRegionIdleTimeout(duration);

  return oldValue;
}

int32_t LocalRegion::adjustEntryExpiryDuration(int32_t duration) {
  CHECK_DESTROY_PENDING(TryReadGuard, LocalRegion::adjustEntryExpiryDuration);

  RegionAttributesPtr attrs = m_regionAttributes;
  bool hadExpiry = (getEntryExpiryDuration() != 0);
  if (!hadExpiry) {
    throw IllegalStateException(
        "Cannot change entry expiration duration for region created without "
        "entry expiry.");
  }
  int32_t oldValue = getEntryExpiryDuration();
  setEntryTimeToLive(duration);
  setEntryIdleTimeout(duration);

  return oldValue;
}

/** they used to public methods in hpp file */
bool LocalRegion::isStatisticsEnabled() {
  bool status = true;
  if (m_cacheImpl == NULL) {
    return false;
  }
  if (m_cacheImpl->getCache() != NULL) {
    SystemProperties* props =
        m_cacheImpl->getCache()->getDistributedSystem()->getSystemProperties();
    if (props) {
      status = props->statisticsEnabled();
    }
  }
  return status;
}

bool LocalRegion::useModifiedTimeForRegionExpiry() {
  uint32_t region_ttl = m_regionAttributes->getRegionTimeToLive();
  if (region_ttl > 0) {
    return true;
  } else {
    return false;
  }
}

bool LocalRegion::useModifiedTimeForEntryExpiry() {
  uint32_t entry_ttl = m_regionAttributes->getEntryTimeToLive();
  if (entry_ttl > 0) {
    return true;
  } else {
    return false;
  }
}

bool LocalRegion::isEntryIdletimeEnabled() {
  if (m_regionAttributes->getCachingEnabled() &&
      0 != m_regionAttributes->getEntryIdleTimeout()) {
    return true;
  } else {
    return false;
  }
}

ExpirationAction::Action LocalRegion::getEntryExpirationAction() const {
  uint32_t entry_ttl = m_regionAttributes->getEntryTimeToLive();
  if (entry_ttl > 0) {
    return m_regionAttributes->getEntryTimeToLiveAction();
  } else {
    return m_regionAttributes->getEntryIdleTimeoutAction();
  }
}

ExpirationAction::Action LocalRegion::getRegionExpiryAction() const {
  uint32_t region_ttl = m_regionAttributes->getRegionTimeToLive();
  if (region_ttl > 0) {
    return m_regionAttributes->getRegionTimeToLiveAction();
  } else {
    return m_regionAttributes->getRegionIdleTimeoutAction();
  }
}

uint32_t LocalRegion::getRegionExpiryDuration() const {
  uint32_t region_ttl = m_regionAttributes->getRegionTimeToLive();
  uint32_t region_idle = m_regionAttributes->getRegionIdleTimeout();
  if (region_ttl > 0) {
    return region_ttl;
  } else {
    return region_idle;
  }
}

uint32_t LocalRegion::getEntryExpiryDuration() const {
  uint32_t entry_ttl = m_regionAttributes->getEntryTimeToLive();
  uint32_t entry_idle = m_regionAttributes->getEntryIdleTimeout();

  if (entry_ttl > 0) {
    return entry_ttl;
  } else {
    return entry_idle;
  }
}

/** methods to be overridden by derived classes*/
GfErrType LocalRegion::unregisterKeysBeforeDestroyRegion() { return GF_NOERR; }

GfErrType LocalRegion::getNoThrow_remote(const CacheableKeyPtr& keyPtr,
                                         CacheablePtr& valPtr,
                                         const UserDataPtr& aCallbackArgument,
                                         VersionTagPtr& versionTag) {
  return GF_NOERR;
}

GfErrType LocalRegion::putNoThrow_remote(const CacheableKeyPtr& keyPtr,
                                         const CacheablePtr& cvalue,
                                         const UserDataPtr& aCallbackArgument,
                                         VersionTagPtr& versionTag,
                                         bool checkDelta) {
  return GF_NOERR;
}

GfErrType LocalRegion::putAllNoThrow_remote(
    const HashMapOfCacheable& map,
    VersionedCacheableObjectPartListPtr& putAllResponse, uint32_t timeout,
    const UserDataPtr& aCallbackArgument) {
  return GF_NOERR;
}

GfErrType LocalRegion::removeAllNoThrow_remote(
    const VectorOfCacheableKey& keys,
    VersionedCacheableObjectPartListPtr& versionedObjPartList,
    const UserDataPtr& aCallbackArgument) {
  return GF_NOERR;
}

GfErrType LocalRegion::createNoThrow_remote(
    const CacheableKeyPtr& keyPtr, const CacheablePtr& cvalue,
    const UserDataPtr& aCallbackArgument, VersionTagPtr& versionTag) {
  return GF_NOERR;
}

GfErrType LocalRegion::destroyNoThrow_remote(
    const CacheableKeyPtr& keyPtr, const UserDataPtr& aCallbackArgument,
    VersionTagPtr& versionTag) {
  return GF_NOERR;
}

GfErrType LocalRegion::removeNoThrow_remote(
    const CacheableKeyPtr& keyPtr, const CacheablePtr& cvalue,
    const UserDataPtr& aCallbackArgument, VersionTagPtr& versionTag) {
  return GF_NOERR;
}

GfErrType LocalRegion::removeNoThrowEX_remote(
    const CacheableKeyPtr& keyPtr, const UserDataPtr& aCallbackArgument,
    VersionTagPtr& versionTag) {
  return GF_NOERR;
}

GfErrType LocalRegion::invalidateNoThrow_remote(
    const CacheableKeyPtr& keyPtr, const UserDataPtr& aCallbackArgument,
    VersionTagPtr& versionTag) {
  return GF_NOERR;
}

GfErrType LocalRegion::getAllNoThrow_remote(
    const VectorOfCacheableKey* keys, const HashMapOfCacheablePtr& values,
    const HashMapOfExceptionPtr& exceptions,
    const VectorOfCacheableKeyPtr& resultKeys, bool addToLocalCache,
    const UserDataPtr& aCallbackArgument) {
  return GF_NOERR;
}

GfErrType LocalRegion::invalidateRegionNoThrow_remote(
    const UserDataPtr& aCallbackArgument) {
  return GF_NOERR;
}

GfErrType LocalRegion::destroyRegionNoThrow_remote(
    const UserDataPtr& aCallbackArgument) {
  return GF_NOERR;
}

void LocalRegion::adjustCacheListener(const CacheListenerPtr& aListener) {
  WriteGuard guard(m_rwLock);
  setCacheListener(aListener);
  m_listener = aListener;
}

void LocalRegion::adjustCacheListener(const char* lib, const char* func) {
  WriteGuard guard(m_rwLock);
  setCacheListener(lib, func);
  m_listener = m_regionAttributes->getCacheListener();
}

void LocalRegion::adjustCacheLoader(const CacheLoaderPtr& aLoader) {
  WriteGuard guard(m_rwLock);
  setCacheLoader(aLoader);
  m_loader = aLoader;
}

void LocalRegion::adjustCacheLoader(const char* lib, const char* func) {
  WriteGuard guard(m_rwLock);
  setCacheLoader(lib, func);
  m_loader = m_regionAttributes->getCacheLoader();
}

void LocalRegion::adjustCacheWriter(const CacheWriterPtr& aWriter) {
  WriteGuard guard(m_rwLock);
  setCacheWriter(aWriter);
  m_writer = aWriter;
}

void LocalRegion::adjustCacheWriter(const char* lib, const char* func) {
  WriteGuard guard(m_rwLock);
  setCacheWriter(lib, func);
  m_writer = m_regionAttributes->getCacheWriter();
}

void LocalRegion::evict(int32_t percentage) {
  TryReadGuard guard(m_rwLock, m_destroyPending);
  if (m_released || m_destroyPending) return;
  if (m_entries != NULL) {
    int32_t size = m_entries->size();
    int32_t entriesToEvict = (int32_t)(percentage * size) / 100;
    // only invoked from EvictionController so static_cast is always safe
    LRUEntriesMap* lruMap = static_cast<LRUEntriesMap*>(m_entries);
    LOGINFO("Evicting %d entries. Current entry count is %d", entriesToEvict,
            size);
    lruMap->processLRU(entriesToEvict);
  }
}
void LocalRegion::invokeAfterAllEndPointDisconnected() {
  if (m_listener != NULLPTR) {
    int64 sampleStartNanos = Utils::startStatOpTime();
    try {
      m_listener->afterRegionDisconnected(RegionPtr(this));
    } catch (const Exception& ex) {
      LOGERROR("Exception in CacheListener::afterRegionDisconnected: %s: %s",
               ex.getName(), ex.getMessage());
    } catch (...) {
      LOGERROR("Unknown exception in CacheListener::afterRegionDisconnected");
    }
    Utils::updateStatOpTime(
        m_regionStats->getStat(),
        RegionStatType::getInstance()->getListenerCallTimeId(),
        sampleStartNanos);
    m_regionStats->incListenerCallsCompleted();
  }
}

GfErrType LocalRegion::getNoThrow_FullObject(EventIdPtr eventId,
                                             CacheablePtr& fullObject,
                                             VersionTagPtr& versionTag) {
  return GF_NOERR;
}

CacheablePtr LocalRegion::handleReplay(GfErrType& err,
                                       CacheablePtr value) const {
  if (err == GF_TRANSACTION_DATA_REBALANCED_EXCEPTION ||
      err == GF_TRANSACTION_DATA_NODE_HAS_DEPARTED_EXCEPTION) {
    bool isRollBack = (err == GF_TRANSACTION_DATA_REBALANCED_EXCEPTION);
    TXState* txState = getTXState();
    if (txState == NULL) {
      GfErrTypeThrowException("TXState is NULL",
                              GF_CACHE_ILLEGAL_STATE_EXCEPTION);
    }

    CacheablePtr ret = txState->replay(isRollBack);
    err = GF_NOERR;
    return ret;
  }

  return value;
}

TombstoneListPtr LocalRegion::getTombstoneList() { return m_tombstoneList; }
