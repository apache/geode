#ifndef __GEMFIRE_LOCALREGION_H__
#define __GEMFIRE_LOCALREGION_H__

/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*
* The specification of function behaviors is found in the corresponding .cpp
*file.
*
*========================================================================
*/

/**
* @file
*/

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/CacheStatistics.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/CacheableKey.hpp>
#include <gfcpp/Cacheable.hpp>
#include <gfcpp/UserData.hpp>
#include <gfcpp/Cache.hpp>
#include <gfcpp/EntryEvent.hpp>
#include <gfcpp/RegionEvent.hpp>
#include "EventType.hpp"
#include <gfcpp/PersistenceManager.hpp>
#include <gfcpp/RegionEntry.hpp>
#include <gfcpp/CacheListener.hpp>
#include <gfcpp/CacheWriter.hpp>
#include <gfcpp/CacheLoader.hpp>
#include <gfcpp/AttributesMutator.hpp>
#include <gfcpp/AttributesFactory.hpp>

#include "RegionInternal.hpp"
#include "RegionStats.hpp"
#include "EntriesMapFactory.hpp"
#include "SpinLock.hpp"
#include "SerializationRegistry.hpp"
#include "MapWithLock.hpp"
#include "CacheableToken.hpp"
#include "ExpMapEntry.hpp"
#include "TombstoneList.hpp"

#include <ace/ACE.h>
#include <ace/Hash_Map_Manager_T.h>
#include <ace/Recursive_Thread_Mutex.h>

#include <string>
#include <unordered_map>
#include "TSSTXStateWrapper.hpp"

namespace gemfire {

#ifndef CHECK_DESTROY_PENDING
#define CHECK_DESTROY_PENDING(lock, function)                      \
  lock checkGuard(m_rwLock, m_destroyPending);                     \
  if (m_destroyPending) {                                          \
    std::string err_msg = ": region " + m_fullPath + " destroyed"; \
    throw RegionDestroyedException(#function, err_msg.c_str());    \
  }
#endif

#ifndef CHECK_DESTROY_PENDING_NOTHROW
#define CHECK_DESTROY_PENDING_NOTHROW(lock)     \
  lock checkGuard(m_rwLock, m_destroyPending);  \
  if (m_destroyPending) {                       \
    return GF_CACHE_REGION_DESTROYED_EXCEPTION; \
  }
#endif

class PutActions;
class PutActionsTx;
class CreateActions;
class DestroyActions;
class RemoveActions;
class InvalidateActions;

typedef std::unordered_map<CacheableKeyPtr, std::pair<CacheablePtr, int> >
    MapOfOldValue;

/**
* @class LocalRegion LocalRegion.hpp
*
* This class manages subregions and cached data. Each region
* can contain multiple subregions and entries for data.
* Regions provide a hierachical name space
* within the cache. Also, a region can be used to group cached
* objects for management purposes.
*
* The Region interface basically contains two set of APIs: Region management
* APIs; and (potentially) distributed operations on entries. Non-distributed
* operations on entries  are provided by <code>RegionEntry</code>.
*
* Each <code>Cache</code>  defines regions called the root regions.
* User applications can use the root regions to create subregions
* for isolated name space and object grouping.
*
* A region's name can be any String except that it should not contain
* the region name separator, a forward slash (/).
*
* <code>Regions</code>  can be referenced by a relative path name from any
* region
* higher in the hierarchy in {@link Region::getSubregion}. You can get the
* relative
* path from the root region with {@link Region::getFullPath}. The name separator
* is used to concatenate all the region names together from the root, starting
* with the root's subregions.
*/
class CPPCACHE_EXPORT LocalRegion : public RegionInternal {
  /**
   * @brief Public Methods for Region
   */
 public:
  /**
  * @brief constructor/destructor
  */
  LocalRegion(const std::string& name, CacheImpl* cache, RegionInternal* rPtr,
              const RegionAttributesPtr& attributes,
              const CacheStatisticsPtr& stats, bool shared = false);
  virtual ~LocalRegion();

  const char* getName() const;
  const char* getFullPath() const;
  RegionPtr getParentRegion() const;
  RegionAttributesPtr getAttributes() const { return m_regionAttributes; }
  AttributesMutatorPtr getAttributesMutator() const {
    return AttributesMutatorPtr(new AttributesMutator(RegionPtr(this)));
  }
  void updateAccessAndModifiedTime(bool modified);
  CacheStatisticsPtr getStatistics() const;
  virtual void clear(const UserDataPtr& aCallbackArgument = NULLPTR);
  virtual void localClear(const UserDataPtr& aCallbackArgument = NULLPTR);
  GfErrType localClearNoThrow(
      const UserDataPtr& aCallbackArgument = NULLPTR,
      const CacheEventFlags eventFlags = CacheEventFlags::NORMAL);
  void invalidateRegion(const UserDataPtr& aCallbackArgument = NULLPTR);
  void localInvalidateRegion(const UserDataPtr& aCallbackArgument = NULLPTR);
  void destroyRegion(const UserDataPtr& aCallbackArgument = NULLPTR);
  void localDestroyRegion(const UserDataPtr& aCallbackArgument = NULLPTR);
  RegionPtr getSubregion(const char* path);
  RegionPtr createSubregion(const char* subregionName,
                            const RegionAttributesPtr& aRegionAttributes);
  void subregions(const bool recursive, VectorOfRegion& sr);
  RegionEntryPtr getEntry(const CacheableKeyPtr& key);
  void getEntry(const CacheableKeyPtr& key, CacheablePtr& valuePtr);
  CacheablePtr get(const CacheableKeyPtr& key,
                   const UserDataPtr& aCallbackArgument);
  void put(const CacheableKeyPtr& key, const CacheablePtr& value,
           const UserDataPtr& aCallbackArgument = NULLPTR);
  void localPut(const CacheableKeyPtr& key, const CacheablePtr& value,
                const UserDataPtr& aCallbackArgument = NULLPTR);
  void create(const CacheableKeyPtr& key, const CacheablePtr& value,
              const UserDataPtr& aCallbackArgument = NULLPTR);
  void localCreate(const CacheableKeyPtr& key, const CacheablePtr& value,
                   const UserDataPtr& aCallbackArgument = NULLPTR);
  void invalidate(const CacheableKeyPtr& key,
                  const UserDataPtr& aCallbackArgument = NULLPTR);
  void localInvalidate(const CacheableKeyPtr& key,
                       const UserDataPtr& aCallbackArgument = NULLPTR);
  void destroy(const CacheableKeyPtr& key,
               const UserDataPtr& aCallbackArgument = NULLPTR);
  void localDestroy(const CacheableKeyPtr& key,
                    const UserDataPtr& aCallbackArgument = NULLPTR);
  bool remove(const CacheableKeyPtr& key, const CacheablePtr& value,
              const UserDataPtr& aCallbackArgument = NULLPTR);
  bool removeEx(const CacheableKeyPtr& key,
                const UserDataPtr& aCallbackArgument = NULLPTR);
  bool localRemove(const CacheableKeyPtr& key, const CacheablePtr& value,
                   const UserDataPtr& aCallbackArgument = NULLPTR);
  bool localRemoveEx(const CacheableKeyPtr& key,
                     const UserDataPtr& aCallbackArgument = NULLPTR);
  void keys(VectorOfCacheableKey& v);
  void serverKeys(VectorOfCacheableKey& v);
  void values(VectorOfCacheable& vc);
  void entries(VectorOfRegionEntry& me, bool recursive);
  void getAll(const VectorOfCacheableKey& keys, HashMapOfCacheablePtr values,
              HashMapOfExceptionPtr exceptions, bool addToLocalCache,
              const UserDataPtr& aCallbackArgument = NULLPTR);
  void putAll(const HashMapOfCacheable& map,
              uint32_t timeout = DEFAULT_RESPONSE_TIMEOUT,
              const UserDataPtr& aCallbackArgument = NULLPTR);
  void removeAll(const VectorOfCacheableKey& keys,
                 const UserDataPtr& aCallbackArgument = NULLPTR);
  uint32_t size();
  virtual uint32_t size_remote();
  RegionServicePtr getRegionService() const;
  virtual bool containsValueForKey_remote(const CacheableKeyPtr& keyPtr) const;
  bool containsValueForKey(const CacheableKeyPtr& keyPtr) const;
  bool containsKey(const CacheableKeyPtr& keyPtr) const;
  virtual bool containsKeyOnServer(const CacheableKeyPtr& keyPtr) const;
  virtual void getInterestList(VectorOfCacheableKey& vlist) const;
  virtual void getInterestListRegex(VectorOfCacheableString& vregex) const;

  /** @brief Public Methods from RegionInternal
   *  There are all virtual methods
   */
  PersistenceManagerPtr getPersistenceManager() { return m_persistenceManager; }
  void setPersistenceManager(PersistenceManagerPtr& pmPtr);

  virtual GfErrType getNoThrow(const CacheableKeyPtr& key, CacheablePtr& value,
                               const UserDataPtr& aCallbackArgument);
  virtual GfErrType getAllNoThrow(
      const VectorOfCacheableKey& keys, const HashMapOfCacheablePtr& values,
      const HashMapOfExceptionPtr& exceptions, bool addToLocalCache,
      const UserDataPtr& aCallbackArgument = NULLPTR);
  virtual GfErrType putNoThrow(const CacheableKeyPtr& key,
                               const CacheablePtr& value,
                               const UserDataPtr& aCallbackArgument,
                               CacheablePtr& oldValue, int updateCount,
                               const CacheEventFlags eventFlags,
                               VersionTagPtr versionTag,
                               DataInput* delta = NULL,
                               EventIdPtr eventId = NULLPTR);
  virtual GfErrType putNoThrowTX(const CacheableKeyPtr& key,
                                 const CacheablePtr& value,
                                 const UserDataPtr& aCallbackArgument,
                                 CacheablePtr& oldValue, int updateCount,
                                 const CacheEventFlags eventFlags,
                                 VersionTagPtr versionTag,
                                 DataInput* delta = NULL,
                                 EventIdPtr eventId = NULLPTR);
  virtual GfErrType createNoThrow(const CacheableKeyPtr& key,
                                  const CacheablePtr& value,
                                  const UserDataPtr& aCallbackArgument,
                                  int updateCount,
                                  const CacheEventFlags eventFlags,
                                  VersionTagPtr versionTag);
  virtual GfErrType destroyNoThrow(const CacheableKeyPtr& key,
                                   const UserDataPtr& aCallbackArgument,
                                   int updateCount,
                                   const CacheEventFlags eventFlags,
                                   VersionTagPtr versionTag);
  virtual GfErrType destroyNoThrowTX(const CacheableKeyPtr& key,
                                     const UserDataPtr& aCallbackArgument,
                                     int updateCount,
                                     const CacheEventFlags eventFlags,
                                     VersionTagPtr versionTag);
  virtual GfErrType removeNoThrow(const CacheableKeyPtr& key,
                                  const CacheablePtr& value,
                                  const UserDataPtr& aCallbackArgument,
                                  int updateCount,
                                  const CacheEventFlags eventFlags,
                                  VersionTagPtr versionTag);
  virtual GfErrType removeNoThrowEx(const CacheableKeyPtr& key,
                                    const UserDataPtr& aCallbackArgument,
                                    int updateCount,
                                    const CacheEventFlags eventFlags,
                                    VersionTagPtr versionTag);
  virtual GfErrType putAllNoThrow(
      const HashMapOfCacheable& map,
      uint32_t timeout = DEFAULT_RESPONSE_TIMEOUT,
      const UserDataPtr& aCallbackArgument = NULLPTR);
  virtual GfErrType removeAllNoThrow(
      const VectorOfCacheableKey& keys,
      const UserDataPtr& aCallbackArgument = NULLPTR);
  virtual GfErrType invalidateNoThrow(const CacheableKeyPtr& keyPtr,
                                      const UserDataPtr& aCallbackArgument,
                                      int updateCount,
                                      const CacheEventFlags eventFlags,
                                      VersionTagPtr versionTag);
  virtual GfErrType invalidateNoThrowTX(const CacheableKeyPtr& keyPtr,
                                        const UserDataPtr& aCallbackArgument,
                                        int updateCount,
                                        const CacheEventFlags eventFlags,
                                        VersionTagPtr versionTag);
  GfErrType invalidateRegionNoThrow(const UserDataPtr& aCallbackArgument,
                                    const CacheEventFlags eventFlags);
  GfErrType destroyRegionNoThrow(const UserDataPtr& aCallbackArgument,
                                 bool removeFromParent,
                                 const CacheEventFlags eventFlags);
  void tombstoneOperationNoThrow(const CacheableHashMapPtr& tombstoneVersions,
                                 const CacheableHashSetPtr& tombstoneKeys);

  //  moved putLocal to public since this is used by a few other
  // classes like CacheableObjectPartList now
  /** put an entry in local cache without invoking any callbacks */
  GfErrType putLocal(const char* name, bool isCreate,
                     const CacheableKeyPtr& keyPtr,
                     const CacheablePtr& valuePtr, CacheablePtr& oldValue,
                     bool cachingEnabled, int updateCount, int destroyTracker,
                     VersionTagPtr versionTag, DataInput* delta = NULL,
                     EventIdPtr eventId = NULLPTR);
  GfErrType invalidateLocal(const char* name, const CacheableKeyPtr& keyPtr,
                            const CacheablePtr& value,
                            const CacheEventFlags eventFlags,
                            VersionTagPtr versionTag);

  void setRegionExpiryTask();
  void acquireReadLock() { m_rwLock.acquire_read(); }
  void releaseReadLock() { m_rwLock.release(); }

  // behaviors for attributes mutator
  uint32_t adjustLruEntriesLimit(uint32_t limit);
  ExpirationAction::Action adjustRegionExpiryAction(
      ExpirationAction::Action action);
  ExpirationAction::Action adjustEntryExpiryAction(
      ExpirationAction::Action action);
  int32_t adjustRegionExpiryDuration(int32_t duration);
  int32_t adjustEntryExpiryDuration(int32_t duration);

  // other public methods
  RegionStats* getRegionStats() { return m_regionStats; }
  inline bool cacheEnabled() { return m_regionAttributes->getCachingEnabled(); }
  inline bool cachelessWithListener() {
    return !m_regionAttributes->getCachingEnabled() && (m_listener != NULLPTR);
  }
  virtual bool isDestroyed() const { return m_destroyPending; }
  /* above public methods are inherited from RegionInternal */

  virtual void adjustCacheListener(const CacheListenerPtr& aListener);
  virtual void adjustCacheListener(const char* libpath,
                                   const char* factoryFuncName);
  virtual void adjustCacheLoader(const CacheLoaderPtr& aLoader);
  virtual void adjustCacheLoader(const char* libpath,
                                 const char* factoryFuncName);
  virtual void adjustCacheWriter(const CacheWriterPtr& aWriter);
  virtual void adjustCacheWriter(const char* libpath,
                                 const char* factoryFuncName);
  virtual CacheImpl* getCacheImpl();
  virtual void evict(int32_t percentage);

  virtual void acquireGlobals(bool isFailover){};
  virtual void releaseGlobals(bool isFailover){};

  virtual bool getProcessedMarker() { return true; }
  EntriesMap* getEntryMap() { return m_entries; }
  virtual TombstoneListPtr getTombstoneList();

 protected:
  /* virtual protected methods */
  virtual void release(bool invokeCallbacks = true);
  virtual GfErrType getNoThrow_remote(const CacheableKeyPtr& keyPtr,
                                      CacheablePtr& valPtr,
                                      const UserDataPtr& aCallbackArgument,
                                      VersionTagPtr& versionTag);
  virtual GfErrType putNoThrow_remote(const CacheableKeyPtr& keyPtr,
                                      const CacheablePtr& cvalue,
                                      const UserDataPtr& aCallbackArgument,
                                      VersionTagPtr& versionTag,
                                      bool checkDelta = true);
  virtual GfErrType putAllNoThrow_remote(
      const HashMapOfCacheable& map,
      VersionedCacheableObjectPartListPtr& versionedObjPartList,
      uint32_t timeout, const UserDataPtr& aCallbackArgument);
  virtual GfErrType removeAllNoThrow_remote(
      const VectorOfCacheableKey& keys,
      VersionedCacheableObjectPartListPtr& versionedObjPartList,
      const UserDataPtr& aCallbackArgument);
  virtual GfErrType createNoThrow_remote(const CacheableKeyPtr& keyPtr,
                                         const CacheablePtr& cvalue,
                                         const UserDataPtr& aCallbackArgument,
                                         VersionTagPtr& versionTag);
  virtual GfErrType destroyNoThrow_remote(const CacheableKeyPtr& keyPtr,
                                          const UserDataPtr& aCallbackArgument,
                                          VersionTagPtr& versionTag);
  virtual GfErrType removeNoThrow_remote(const CacheableKeyPtr& keyPtr,
                                         const CacheablePtr& cvalue,
                                         const UserDataPtr& aCallbackArgument,
                                         VersionTagPtr& versionTag);
  virtual GfErrType removeNoThrowEX_remote(const CacheableKeyPtr& keyPtr,
                                           const UserDataPtr& aCallbackArgument,
                                           VersionTagPtr& versionTag);
  virtual GfErrType invalidateNoThrow_remote(
      const CacheableKeyPtr& keyPtr, const UserDataPtr& aCallbackArgument,
      VersionTagPtr& versionTag);
  virtual GfErrType getAllNoThrow_remote(
      const VectorOfCacheableKey* keys, const HashMapOfCacheablePtr& values,
      const HashMapOfExceptionPtr& exceptions,
      const VectorOfCacheableKeyPtr& resultKeys, bool addToLocalCache,
      const UserDataPtr& aCallbackArgument);
  virtual GfErrType invalidateRegionNoThrow_remote(
      const UserDataPtr& aCallbackArgument);
  virtual GfErrType destroyRegionNoThrow_remote(
      const UserDataPtr& aCallbackArgument);
  virtual GfErrType unregisterKeysBeforeDestroyRegion();
  virtual const PoolPtr& getPool() { return m_attachedPool; }

  void setPool(const PoolPtr& p) { m_attachedPool = p; }

  TXState* getTXState() const {
    return TSSTXStateWrapper::s_gemfireTSSTXState->getTXState();
  }

  CacheablePtr handleReplay(GfErrType& err, CacheablePtr value) const;

  bool isLocalOp(const CacheEventFlags* eventFlags = NULL) {
    return typeid(*this) == typeid(LocalRegion) ||
           (eventFlags && eventFlags->isLocal());
  }

  // template method for put and create
  template <typename TAction>
  GfErrType updateNoThrow(const CacheableKeyPtr& key, const CacheablePtr& value,
                          const UserDataPtr& aCallbackArgument,
                          CacheablePtr& oldValue, int updateCount,
                          const CacheEventFlags eventFlags,
                          VersionTagPtr versionTag, DataInput* delta = NULL,
                          EventIdPtr eventId = NULLPTR);

  template <typename TAction>
  GfErrType updateNoThrowTX(const CacheableKeyPtr& key,
                            const CacheablePtr& value,
                            const UserDataPtr& aCallbackArgument,
                            CacheablePtr& oldValue, int updateCount,
                            const CacheEventFlags eventFlags,
                            VersionTagPtr versionTag, DataInput* delta = NULL,
                            EventIdPtr eventId = NULLPTR);

  /* protected attributes */
  std::string m_name;
  RegionPtr m_parentRegion;
  MapOfRegionWithLock m_subRegions;
  std::string m_fullPath;
  CacheImpl* m_cacheImpl;
  volatile bool m_destroyPending;
  CacheListenerPtr m_listener;
  CacheWriterPtr m_writer;
  CacheLoaderPtr m_loader;
  volatile bool m_released;
  EntriesMap* m_entries;  // map containing cache entries...
  RegionStats* m_regionStats;
  CacheStatisticsPtr m_cacheStatistics;
  bool m_transactionEnabled;
  TombstoneListPtr m_tombstoneList;
  bool m_isPRSingleHopEnabled;
  PoolPtr m_attachedPool;

  mutable ACE_RW_Thread_Mutex m_rwLock;
  void keys_internal(VectorOfCacheableKey& v);
  bool containsKey_internal(const CacheableKeyPtr& keyPtr) const;
  int removeRegion(const std::string& name);

  bool invokeCacheWriterForEntryEvent(const CacheableKeyPtr& key,
                                      CacheablePtr& oldValue,
                                      const CacheablePtr& newValue,
                                      const UserDataPtr& aCallbackArgument,
                                      CacheEventFlags eventFlags,
                                      EntryEventType type);
  bool invokeCacheWriterForRegionEvent(const UserDataPtr& aCallbackArgument,
                                       CacheEventFlags eventFlags,
                                       RegionEventType type);
  GfErrType invokeCacheListenerForEntryEvent(
      const CacheableKeyPtr& key, CacheablePtr& oldValue,
      const CacheablePtr& newValue, const UserDataPtr& aCallbackArgument,
      CacheEventFlags eventFlags, EntryEventType type, bool isLocal = false);
  GfErrType invokeCacheListenerForRegionEvent(
      const UserDataPtr& aCallbackArgument, CacheEventFlags eventFlags,
      RegionEventType type);
  // functions related to expirations.
  void updateAccessAndModifiedTimeForEntry(MapEntryImplPtr& ptr, bool modified);
  void registerEntryExpiryTask(MapEntryImplPtr& entry);
  void subregions_internal(const bool recursive, VectorOfRegion& sr);
  void entries_internal(VectorOfRegionEntry& me, const bool recursive);

  PersistenceManagerPtr m_persistenceManager;

  bool isStatisticsEnabled();
  bool useModifiedTimeForRegionExpiry();
  bool useModifiedTimeForEntryExpiry();
  bool isEntryIdletimeEnabled();
  ExpirationAction::Action getEntryExpirationAction() const;
  ExpirationAction::Action getRegionExpiryAction() const;
  uint32_t getRegionExpiryDuration() const;
  uint32_t getEntryExpiryDuration() const;
  void invokeAfterAllEndPointDisconnected();
  // Disallow copy constructor and assignment operator.
  LocalRegion(const LocalRegion&);
  LocalRegion& operator=(const LocalRegion&);

  virtual GfErrType getNoThrow_FullObject(EventIdPtr eventId,
                                          CacheablePtr& fullObject,
                                          VersionTagPtr& versionTag);

  // these classes encapsulate actions specific to update operations
  // used by the template <code>updateNoThrow</code> class
  friend class PutActions;
  friend class PutActionsTx;
  friend class CreateActions;
  friend class DestroyActions;
  friend class RemoveActions;
  friend class InvalidateActions;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_LOCALREGION_H__
