#ifndef __GEMFIRE_REGIONINTERNAL_H__
#define __GEMFIRE_REGIONINTERNAL_H__

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

#include <gfcpp/Region.hpp>
#include "RegionStats.hpp"
#include <string>
#include <map>
#include "EventId.hpp"

namespace gemfire {

/**
 * @class CacheEventFlags RegionInternal.hpp
 *
 * This class encapsulates the flags (e.g. notification, expiration, local)
 * for cache events for various NoThrow methods.
 *
 *
 */
class CacheEventFlags {
 private:
  uint8_t m_flags;
  static const uint8_t GF_NORMAL = 0x01;
  static const uint8_t GF_LOCAL = 0x02;
  static const uint8_t GF_NOTIFICATION = 0x04;
  static const uint8_t GF_NOTIFICATION_UPDATE = 0x08;
  static const uint8_t GF_EVICTION = 0x10;
  static const uint8_t GF_EXPIRATION = 0x20;
  static const uint8_t GF_CACHE_CLOSE = 0x40;
  static const uint8_t GF_NOCACHEWRITER = 0x80;

  // private constructor
  inline CacheEventFlags(const uint8_t flags) : m_flags(flags) {}

  // disable constructors and assignment
  CacheEventFlags();
  CacheEventFlags& operator=(const CacheEventFlags&);

 public:
  static const CacheEventFlags NORMAL;
  static const CacheEventFlags LOCAL;
  static const CacheEventFlags NOTIFICATION;
  static const CacheEventFlags NOTIFICATION_UPDATE;
  static const CacheEventFlags EVICTION;
  static const CacheEventFlags EXPIRATION;
  static const CacheEventFlags CACHE_CLOSE;
  static const CacheEventFlags NOCACHEWRITER;

  inline CacheEventFlags(const CacheEventFlags& flags)
      : m_flags(flags.m_flags) {}

  inline CacheEventFlags operator|(const CacheEventFlags& flags) const {
    return CacheEventFlags(m_flags | flags.m_flags);
  }

  inline uint32_t operator&(const CacheEventFlags& flags) const {
    return (m_flags & flags.m_flags);
  }

  inline bool operator==(const CacheEventFlags& flags) const {
    return (m_flags == flags.m_flags);
  }

  inline bool isNormal() const { return (m_flags & GF_NORMAL); }

  inline bool isLocal() const { return (m_flags & GF_LOCAL); }

  inline bool isNotification() const { return (m_flags & GF_NOTIFICATION); }

  inline bool isNotificationUpdate() const {
    return (m_flags & GF_NOTIFICATION_UPDATE);
  }

  inline bool isEviction() const { return (m_flags & GF_EVICTION); }

  inline bool isExpiration() const { return (m_flags & GF_EXPIRATION); }

  inline bool isCacheClose() const { return (m_flags & GF_CACHE_CLOSE); }

  inline bool isNoCacheWriter() const { return (m_flags & GF_NOCACHEWRITER); }

  inline bool isEvictOrExpire() const {
    return (m_flags & (GF_EVICTION | GF_EXPIRATION));
  }

  // special optimized method for CacheWriter invocation condition
  inline bool invokeCacheWriter() const {
    return ((m_flags & (GF_NOTIFICATION | GF_EVICTION | GF_EXPIRATION |
                        GF_NOCACHEWRITER)) == 0x0);
  }
};

class TombstoneList;
typedef SharedPtr<TombstoneList> TombstoneListPtr;
class VersionTag;
typedef SharedPtr<VersionTag> VersionTagPtr;
_GF_PTR_DEF_(MapEntryImpl, MapEntryImplPtr);
/**
* @class RegionInternal RegionInternal.hpp
*
* This class specifies internal common interface for all regions.
*/
class RegionInternal : public Region {
 public:
  /**
  * @brief destructor
  */
  virtual ~RegionInternal();
  /** @brief Default implementation of Public Methods from Region
   */
  virtual void registerKeys(const VectorOfCacheableKey& keys,
                            bool isDurable = false,
                            bool getInitialValues = false,
                            bool receiveValues = true);
  virtual void unregisterKeys(const VectorOfCacheableKey& keys);
  virtual void registerAllKeys(bool isDurable = false,
                               VectorOfCacheableKeyPtr resultKeys = NULLPTR,
                               bool getInitialValues = false,
                               bool receiveValues = true);
  virtual void unregisterAllKeys();

  virtual void registerRegex(const char* regex, bool isDurable = false,
                             VectorOfCacheableKeyPtr resultKeys = NULLPTR,
                             bool getInitialValues = false,
                             bool receiveValues = true);
  virtual void unregisterRegex(const char* regex);

  virtual SelectResultsPtr query(
      const char* predicate, uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
  virtual bool existsValue(const char* predicate,
                           uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);
  virtual SerializablePtr selectValue(
      const char* predicate, uint32_t timeout = DEFAULT_QUERY_RESPONSE_TIMEOUT);

  /** @brief Public Methods
  */
  virtual PersistenceManagerPtr getPersistenceManager() = 0;
  virtual void setPersistenceManager(PersistenceManagerPtr& pmPtr) = 0;

  virtual GfErrType getNoThrow(const CacheableKeyPtr& key, CacheablePtr& value,
                               const UserDataPtr& aCallbackArgument) = 0;
  virtual GfErrType getAllNoThrow(const VectorOfCacheableKey& keys,
                                  const HashMapOfCacheablePtr& values,
                                  const HashMapOfExceptionPtr& exceptions,
                                  bool addToLocalCache,
                                  const UserDataPtr& aCallbackArgument) = 0;
  virtual GfErrType putNoThrow(const CacheableKeyPtr& key,
                               const CacheablePtr& value,
                               const UserDataPtr& aCallbackArgument,
                               CacheablePtr& oldValue, int updateCount,
                               const CacheEventFlags eventFlags,
                               VersionTagPtr versionTag,
                               DataInput* delta = NULL,
                               EventIdPtr eventId = NULLPTR) = 0;
  virtual GfErrType createNoThrow(const CacheableKeyPtr& key,
                                  const CacheablePtr& value,
                                  const UserDataPtr& aCallbackArgument,
                                  int updateCount,
                                  const CacheEventFlags eventFlags,
                                  VersionTagPtr versionTag) = 0;
  virtual GfErrType destroyNoThrow(const CacheableKeyPtr& key,
                                   const UserDataPtr& aCallbackArgument,
                                   int updateCount,
                                   const CacheEventFlags eventFlags,
                                   VersionTagPtr versionTag) = 0;
  virtual GfErrType removeNoThrow(const CacheableKeyPtr& key,
                                  const CacheablePtr& value,
                                  const UserDataPtr& aCallbackArgument,
                                  int updateCount,
                                  const CacheEventFlags eventFlags,
                                  VersionTagPtr versionTag) = 0;
  virtual GfErrType invalidateNoThrow(const CacheableKeyPtr& keyPtr,
                                      const UserDataPtr& aCallbackArgument,
                                      int updateCount,
                                      const CacheEventFlags eventFlags,
                                      VersionTagPtr versionTag) = 0;
  virtual GfErrType invalidateRegionNoThrow(
      const UserDataPtr& aCallbackArgument,
      const CacheEventFlags eventFlags) = 0;
  virtual GfErrType destroyRegionNoThrow(const UserDataPtr& aCallbackArgument,
                                         bool removeFromParent,
                                         const CacheEventFlags eventFlags) = 0;

  virtual void setRegionExpiryTask() = 0;
  virtual void acquireReadLock() = 0;
  virtual void releaseReadLock() = 0;
  // behaviors for attributes mutator
  virtual uint32_t adjustLruEntriesLimit(uint32_t limit) = 0;
  virtual ExpirationAction::Action adjustRegionExpiryAction(
      ExpirationAction::Action action) = 0;
  virtual ExpirationAction::Action adjustEntryExpiryAction(
      ExpirationAction::Action action) = 0;
  virtual int32_t adjustRegionExpiryDuration(int32_t duration) = 0;
  virtual int32_t adjustEntryExpiryDuration(int32_t duration) = 0;
  virtual void adjustCacheListener(const CacheListenerPtr& aListener) = 0;
  virtual void adjustCacheListener(const char* libpath,
                                   const char* factoryFuncName) = 0;
  virtual void adjustCacheLoader(const CacheLoaderPtr& aLoader) = 0;
  virtual void adjustCacheLoader(const char* libpath,
                                 const char* factoryFuncName) = 0;
  virtual void adjustCacheWriter(const CacheWriterPtr& aWriter) = 0;
  virtual void adjustCacheWriter(const char* libpath,
                                 const char* factoryFuncName) = 0;

  virtual RegionStats* getRegionStats() = 0;
  virtual bool cacheEnabled() = 0;
  virtual bool isDestroyed() const = 0;
  virtual void evict(int32_t percentage) = 0;
  virtual CacheImpl* getCacheImpl() = 0;
  virtual TombstoneListPtr getTombstoneList();

  // KN: added now.
  virtual void updateAccessAndModifiedTime(bool modified) = 0;
  virtual void updateAccessAndModifiedTimeForEntry(MapEntryImplPtr& ptr,
                                                   bool modified) = 0;
  RegionEntryPtr createRegionEntry(const CacheableKeyPtr& key,
                                   const CacheablePtr& value);
  virtual void addDisMessToQueue(){};

  virtual void txDestroy(const CacheableKeyPtr& key,
                         const UserDataPtr& callBack, VersionTagPtr versionTag);
  virtual void txInvalidate(const CacheableKeyPtr& key,
                            const UserDataPtr& callBack,
                            VersionTagPtr versionTag);
  virtual void txPut(const CacheableKeyPtr& key, const CacheablePtr& value,
                     const UserDataPtr& callBack, VersionTagPtr versionTag);
  inline bool isConcurrencyCheckEnabled() const {
    return m_regionAttributes->getConcurrencyChecksEnabled();
  }
  virtual const PoolPtr& getPool() = 0;

 protected:
  /**
  * @brief constructor
  */
  RegionInternal(const RegionAttributesPtr& attributes);

  void setLruEntriesLimit(uint32_t limit);
  void setRegionTimeToLiveExpirationAction(ExpirationAction::Action action);
  void setRegionIdleTimeoutExpirationAction(ExpirationAction::Action action);
  void setEntryTimeToLiveExpirationAction(ExpirationAction::Action action);
  void setEntryIdleTimeoutExpirationAction(ExpirationAction::Action action);
  void setRegionTimeToLive(int32_t duration);
  void setRegionIdleTimeout(int32_t duration);
  void setEntryTimeToLive(int32_t duration);
  void setEntryIdleTimeout(int32_t duration);
  void setCacheListener(const CacheListenerPtr& aListener);
  void setCacheListener(const char* libpath, const char* factoryFuncName);
  void setPartitionResolver(const PartitionResolverPtr& aListener);
  void setPartitionResolver(const char* libpath, const char* factoryFuncName);
  void setCacheLoader(const CacheLoaderPtr& aLoader);
  void setCacheLoader(const char* libpath, const char* factoryFuncName);
  void setCacheWriter(const CacheWriterPtr& aWriter);
  void setCacheWriter(const char* libpath, const char* factoryFuncName);
  void setEndpoints(const char* endpoints);
  void setClientNotificationEnabled(bool clientNotificationEnabled);

  RegionAttributesPtr m_regionAttributes;

  inline bool entryExpiryEnabled() const {
    return m_regionAttributes->getEntryExpiryEnabled();
  }

  inline bool regionExpiryEnabled() const {
    return m_regionAttributes->getRegionExpiryEnabled();
  }

 private:
  // Disallow copy constructor and assignment operator.
  RegionInternal(const RegionInternal&);
  RegionInternal& operator=(const RegionInternal&);
};

typedef SharedPtr<RegionInternal> RegionInternalPtr;

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_REGIONINTERNAL_H__
