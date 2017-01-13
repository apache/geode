/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "RegionInternal.hpp"
#include <gfcpp/RegionEntry.hpp>
#include "TombstoneList.hpp"

namespace gemfire {

// Static initializers for CacheEventFlags
const CacheEventFlags CacheEventFlags::NORMAL(CacheEventFlags::GF_NORMAL);
const CacheEventFlags CacheEventFlags::LOCAL(CacheEventFlags::GF_LOCAL);
const CacheEventFlags CacheEventFlags::NOTIFICATION(
    CacheEventFlags::GF_NOTIFICATION);
const CacheEventFlags CacheEventFlags::NOTIFICATION_UPDATE(
    CacheEventFlags::GF_NOTIFICATION_UPDATE);
const CacheEventFlags CacheEventFlags::EVICTION(CacheEventFlags::GF_EVICTION);
const CacheEventFlags CacheEventFlags::EXPIRATION(
    CacheEventFlags::GF_EXPIRATION);
const CacheEventFlags CacheEventFlags::CACHE_CLOSE(
    CacheEventFlags::GF_CACHE_CLOSE);
const CacheEventFlags CacheEventFlags::NOCACHEWRITER(
    CacheEventFlags::GF_NOCACHEWRITER);

RegionInternal::RegionInternal(const RegionAttributesPtr& attributes)
    : m_regionAttributes(attributes) {}

RegionInternal::~RegionInternal() {}

void RegionInternal::registerKeys(const VectorOfCacheableKey& keys,
                                  bool isDurable, bool getInitialValues,
                                  bool receiveValues) {
  throw UnsupportedOperationException(
      "registerKeys only supported by "
      "Thin Client Region.");
}

void RegionInternal::unregisterKeys(const VectorOfCacheableKey& keys) {
  throw UnsupportedOperationException(
      "unregisterKeys only supported by "
      "Thin Client Region.");
}

void RegionInternal::registerAllKeys(bool isDurable,
                                     VectorOfCacheableKeyPtr resultKeys,
                                     bool getInitialValues,
                                     bool receiveValues) {
  throw UnsupportedOperationException(
      "registerAllKeys only supported by Thin Client Region.");
}

void RegionInternal::unregisterAllKeys() {
  throw UnsupportedOperationException(
      "unregisterAllKeys only supported by Thin Client Region.");
}

void RegionInternal::registerRegex(const char* regex, bool isDurable,
                                   VectorOfCacheableKeyPtr resultKeys,
                                   bool getInitialValues, bool receiveValues) {
  throw UnsupportedOperationException(
      "registerRegex only supported by Thin Client Region.");
}

void RegionInternal::unregisterRegex(const char* regex) {
  throw UnsupportedOperationException(
      "unregisterRegex only supported by Thin Client Region.");
}

SelectResultsPtr RegionInternal::query(const char* predicate,
                                       uint32_t timeout) {
  throw UnsupportedOperationException(
      "query only supported by Thin Client Region.");
}

bool RegionInternal::existsValue(const char* predicate, uint32_t timeout) {
  throw UnsupportedOperationException(
      "existsValue only supported by Thin Client Region.");
}

SerializablePtr RegionInternal::selectValue(const char* predicate,
                                            uint32_t timeout) {
  throw UnsupportedOperationException(
      "selectValue only supported by Thin Client Region.");
}

TombstoneListPtr RegionInternal::getTombstoneList() {
  throw UnsupportedOperationException(
      "getTombstoneList only supported by LocalRegion.");
}

RegionEntryPtr RegionInternal::createRegionEntry(const CacheableKeyPtr& key,
                                                 const CacheablePtr& value) {
  return RegionEntryPtr(new RegionEntry(RegionPtr(this), key, value));
}

void RegionInternal::setLruEntriesLimit(uint32_t limit) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_lruEntriesLimit = limit;
  }
}

void RegionInternal::setRegionTimeToLiveExpirationAction(
    ExpirationAction::Action action) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_regionTimeToLiveExpirationAction = action;
  }
}

void RegionInternal::setRegionIdleTimeoutExpirationAction(
    ExpirationAction::Action action) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_regionIdleTimeoutExpirationAction = action;
  }
}

void RegionInternal::setEntryTimeToLiveExpirationAction(
    ExpirationAction::Action action) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_entryTimeToLiveExpirationAction = action;
  }
}

void RegionInternal::setEntryIdleTimeoutExpirationAction(
    ExpirationAction::Action action) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_entryIdleTimeoutExpirationAction = action;
  }
}

void RegionInternal::setRegionTimeToLive(int32_t duration) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_regionTimeToLive = duration;
  }
}

void RegionInternal::setRegionIdleTimeout(int32_t duration) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_regionIdleTimeout = duration;
  }
}

void RegionInternal::setEntryTimeToLive(int32_t duration) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_entryTimeToLive = duration;
  }
}

void RegionInternal::setEntryIdleTimeout(int32_t duration) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_entryIdleTimeout = duration;
  }
}

void RegionInternal::setCacheListener(const CacheListenerPtr& aListener) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_cacheListener = aListener;
  }
}

void RegionInternal::setCacheListener(const char* libpath,
                                      const char* factoryFuncName) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->setCacheListener(libpath, factoryFuncName);
  }
}

void RegionInternal::setPartitionResolver(
    const PartitionResolverPtr& aResolver) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_partitionResolver = aResolver;
  }
}

void RegionInternal::setPartitionResolver(const char* libpath,
                                          const char* factoryFuncName) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->setPartitionResolver(libpath, factoryFuncName);
  }
}

void RegionInternal::setCacheLoader(const CacheLoaderPtr& aLoader) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_cacheLoader = aLoader;
  }
}

void RegionInternal::setCacheLoader(const char* libpath,
                                    const char* factoryFuncName) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->setCacheLoader(libpath, factoryFuncName);
  }
}

void RegionInternal::setCacheWriter(const CacheWriterPtr& aWriter) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_cacheWriter = aWriter;
  }
}

void RegionInternal::setCacheWriter(const char* libpath,
                                    const char* factoryFuncName) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->setCacheWriter(libpath, factoryFuncName);
  }
}

void RegionInternal::setEndpoints(const char* endpoints) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->setEndpoints(endpoints);
  }
}

void RegionInternal::setClientNotificationEnabled(
    bool clientNotificationEnabled) {
  if (m_regionAttributes != NULLPTR) {
    m_regionAttributes->m_clientNotificationEnabled = clientNotificationEnabled;
  }
}

void RegionInternal::txDestroy(const CacheableKeyPtr& key,
                               const UserDataPtr& callBack,
                               VersionTagPtr versionTag) {
  throw UnsupportedOperationException(
      "txDestroy only supported by Thin Client Region.");
}

void RegionInternal::txInvalidate(const CacheableKeyPtr& key,
                                  const UserDataPtr& callBack,
                                  VersionTagPtr versionTag) {
  throw UnsupportedOperationException(
      "txInvalidate only supported by Thin Client Region.");
}

void RegionInternal::txPut(const CacheableKeyPtr& key,
                           const CacheablePtr& value,
                           const UserDataPtr& callBack,
                           VersionTagPtr versionTag) {
  throw UnsupportedOperationException(
      "txPut only supported by Thin Client Region.");
}
}  // namespace gemfire
