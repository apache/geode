/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

namespace gemfire {
class Region;
};

#include <gfcpp/Cache.hpp>
#include <gfcpp/ExpirationAttributes.hpp>
#include <Utils.hpp>
#include <gfcpp/DistributedSystem.hpp>
#include <stdlib.h>
#include <string.h>
#include <gfcpp/Pool.hpp>
#include <gfcpp/PoolManager.hpp>
using namespace gemfire;

AttributesFactory::AttributesFactory() : m_regionAttributes() {}

AttributesFactory::AttributesFactory(
    const RegionAttributesPtr& regionAttributes)
    : m_regionAttributes(*regionAttributes) {}

AttributesFactory::~AttributesFactory() {}

void AttributesFactory::setCacheLoader(const CacheLoaderPtr& cacheLoader) {
  m_regionAttributes.m_cacheLoader = cacheLoader;
}

void AttributesFactory::setCacheWriter(const CacheWriterPtr& cacheWriter) {
  m_regionAttributes.m_cacheWriter = cacheWriter;
}
void AttributesFactory::setCacheListener(const CacheListenerPtr& aListener) {
  m_regionAttributes.m_cacheListener = aListener;
}
void AttributesFactory::setPartitionResolver(
    const PartitionResolverPtr& aResolver) {
  m_regionAttributes.m_partitionResolver = aResolver;
}

void AttributesFactory::setCacheLoader(const char* lib, const char* func) {
  m_regionAttributes.setCacheLoader(lib, func);
}

void AttributesFactory::setCacheWriter(const char* lib, const char* func) {
  m_regionAttributes.setCacheWriter(lib, func);
}

void AttributesFactory::setCacheListener(const char* lib, const char* func) {
  m_regionAttributes.setCacheListener(lib, func);
}

void AttributesFactory::setPartitionResolver(const char* lib,
                                             const char* func) {
  m_regionAttributes.setPartitionResolver(lib, func);
}

void AttributesFactory::setEntryIdleTimeout(ExpirationAction::Action action,
                                            int idleTimeout) {
  m_regionAttributes.m_entryIdleTimeout = idleTimeout;
  m_regionAttributes.m_entryIdleTimeoutExpirationAction = action;
}

void AttributesFactory::setEntryTimeToLive(ExpirationAction::Action action,
                                           int timeToLive) {
  m_regionAttributes.m_entryTimeToLive = timeToLive;
  m_regionAttributes.m_entryTimeToLiveExpirationAction = action;
}

void AttributesFactory::setRegionIdleTimeout(ExpirationAction::Action action,
                                             int idleTimeout) {
  m_regionAttributes.m_regionIdleTimeout = idleTimeout;
  m_regionAttributes.m_regionIdleTimeoutExpirationAction = action;
}
void AttributesFactory::setRegionTimeToLive(ExpirationAction::Action action,
                                            int timeToLive) {
  m_regionAttributes.m_regionTimeToLive = timeToLive;
  m_regionAttributes.m_regionTimeToLiveExpirationAction = action;
}

void AttributesFactory::setInitialCapacity(int initialCapacity) {
  m_regionAttributes.m_initialCapacity = initialCapacity;
}

void AttributesFactory::setLoadFactor(float loadFactor) {
  m_regionAttributes.m_loadFactor = loadFactor;
}

void AttributesFactory::setConcurrencyLevel(uint8_t concurrencyLevel) {
  m_regionAttributes.m_concurrencyLevel = concurrencyLevel;
}

/*
void AttributesFactory::setStatisticsEnabled( bool statisticsEnabled)
{
   m_regionAttributes.m_statisticsEnabled = statisticsEnabled;
}
*/

RegionAttributesPtr AttributesFactory::createRegionAttributes() {
  RegionAttributesPtr res;
  /*
  if( m_regionAttributes.m_poolName != NULL )
  {
          PoolPtr pool= PoolManager::find( m_regionAttributes.m_poolName );
    if (pool == NULLPTR) {
      throw IllegalStateException("Pool not found while creating region
  attributes");
    }
          setClientNotificationEnabled(pool->getSubscriptionEnabled());
          if( pool->getSubscriptionRedundancy() >0 )
  setClientNotificationEnabled(true);
  }
  */
  validateAttributes(m_regionAttributes);
  res = new RegionAttributes(m_regionAttributes);
  return res;
}

void AttributesFactory::validateAttributes(RegionAttributes& attrs) {
  if (!attrs.m_caching) {
    if (attrs.m_entryTimeToLive != 0) {
      throw IllegalStateException(
          "Entry TimeToLive use is incompatible with disabled caching");
    }

    if (attrs.m_entryIdleTimeout != 0) {
      throw IllegalStateException(
          "Entry IdleTimeout use is incompatible with disabled caching");
    }

    if (attrs.m_lruEntriesLimit != 0) {
      throw IllegalStateException(
          "Non-zero LRU entries limit is incompatible with disabled caching");
    }
    if (attrs.m_diskPolicy != DiskPolicyType::NONE) {
      if (attrs.m_lruEntriesLimit == 0) {
        throw IllegalStateException(
            "When DiskPolicy is OVERFLOWS, LRU entries limit must be non-zero "
            "with disabled caching");
      }
    }
  }

  if (attrs.m_diskPolicy != DiskPolicyType::NONE) {
    if (attrs.m_persistenceManager == NULLPTR &&
        (attrs.m_persistenceLibrary == NULL ||
         attrs.m_persistenceFactory == NULL)) {
      throw IllegalStateException(
          "Persistence Manager must be set if DiskPolicy is OVERFLOWS");
    }
  }
  if (attrs.m_diskPolicy != DiskPolicyType::NONE) {
    if (attrs.m_lruEntriesLimit == 0) {
      throw IllegalStateException(
          "LRU entries limit cannot be zero if DiskPolicy is OVERFLOWS");
    }
  }
}

void AttributesFactory::setLruEntriesLimit(const uint32_t entriesLimit) {
  m_regionAttributes.m_lruEntriesLimit = entriesLimit;
}

void AttributesFactory::setDiskPolicy(
    const DiskPolicyType::PolicyType diskPolicy) {
  if (diskPolicy == DiskPolicyType::PERSIST) {
    throw IllegalStateException("Persistence feature is not supported");
  }
  m_regionAttributes.m_diskPolicy = diskPolicy;
}

void AttributesFactory::setCachingEnabled(bool cachingEnabled) {
  m_regionAttributes.m_caching = cachingEnabled;
}

void AttributesFactory::setPersistenceManager(
    const PersistenceManagerPtr& persistenceManager,
    const PropertiesPtr& props) {
  m_regionAttributes.m_persistenceManager = persistenceManager;
  m_regionAttributes.m_persistenceProperties = props;
}

void AttributesFactory::setPersistenceManager(const char* lib, const char* func,
                                              const PropertiesPtr& config) {
  m_regionAttributes.setPersistenceManager(lib, func, config);
}

void AttributesFactory::setPoolName(const char* name) {
  m_regionAttributes.setPoolName(name);
}

void AttributesFactory::setCloningEnabled(bool isClonable) {
  m_regionAttributes.setCloningEnabled(isClonable);
}
void AttributesFactory::setConcurrencyChecksEnabled(bool enable) {
  m_regionAttributes.setConcurrencyChecksEnabled(enable);
}
