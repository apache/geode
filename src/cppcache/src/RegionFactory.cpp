/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
*/

#include <gfcpp/CacheFactory.hpp>
#include <gfcpp/RegionFactory.hpp>
#include <CppCacheLibrary.hpp>
#include <gfcpp/Cache.hpp>
#include <CacheImpl.hpp>
#include <gfcpp/SystemProperties.hpp>
#include <gfcpp/PoolManager.hpp>
#include <CacheConfig.hpp>
#include <CacheRegionHelper.hpp>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Guard_T.h>
#include <map>
#include <string>

extern ACE_Recursive_Thread_Mutex* g_disconnectLock;

namespace gemfire {

RegionFactory::RegionFactory(RegionShortcut preDefinedRegion) {
  m_preDefinedRegion = preDefinedRegion;
  AttributesFactoryPtr afPtr(new AttributesFactory());
  m_attributeFactory = afPtr;
  setRegionShortcut();
}

RegionFactory::~RegionFactory() {}

RegionPtr RegionFactory::create(const char* name) {
  RegionPtr retRegionPtr = NULLPTR;
  RegionAttributesPtr regAttr = m_attributeFactory->createRegionAttributes();

  // assuming pool name is not DEFAULT_POOL_NAME
  if (regAttr->getPoolName() != NULL && strlen(regAttr->getPoolName()) > 0) {
    // poolname is set
    CachePtr cache = CacheFactory::getAnyInstance();
    CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cache.ptr());
    cacheImpl->createRegion(name, regAttr, retRegionPtr);
  } else {
    // need to look default Pool
    ACE_Guard<ACE_Recursive_Thread_Mutex> connectGuard(*g_disconnectLock);
    // if local region no need to create default pool
    if (m_preDefinedRegion != LOCAL) {
      PoolPtr pool = CacheFactory::createOrGetDefaultPool();
      if (pool == NULLPTR) {
        throw IllegalStateException("Pool is not defined create region.");
      }
      m_attributeFactory->setPoolName(pool->getName());
    }

    regAttr = m_attributeFactory->createRegionAttributes();
    CachePtr cache = CacheFactory::getAnyInstance();
    CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cache.ptr());
    cacheImpl->createRegion(name, regAttr, retRegionPtr);
  }

  return retRegionPtr;
}

void RegionFactory::setRegionShortcut() {
  CacheImpl::setRegionShortcut(m_attributeFactory, m_preDefinedRegion);
}

RegionFactoryPtr RegionFactory::setCacheLoader(
    const CacheLoaderPtr& cacheLoader) {
  m_attributeFactory->setCacheLoader(cacheLoader);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setCacheWriter(
    const CacheWriterPtr& cacheWriter) {
  m_attributeFactory->setCacheWriter(cacheWriter);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}
RegionFactoryPtr RegionFactory::setCacheListener(
    const CacheListenerPtr& aListener) {
  m_attributeFactory->setCacheListener(aListener);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}
RegionFactoryPtr RegionFactory::setPartitionResolver(
    const PartitionResolverPtr& aResolver) {
  m_attributeFactory->setPartitionResolver(aResolver);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setCacheLoader(const char* lib,
                                               const char* func) {
  m_attributeFactory->setCacheLoader(lib, func);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setCacheWriter(const char* lib,
                                               const char* func) {
  m_attributeFactory->setCacheWriter(lib, func);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setCacheListener(const char* lib,
                                                 const char* func) {
  m_attributeFactory->setCacheListener(lib, func);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setPartitionResolver(const char* lib,
                                                     const char* func) {
  m_attributeFactory->setPartitionResolver(lib, func);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setEntryIdleTimeout(
    ExpirationAction::Action action, int idleTimeout) {
  m_attributeFactory->setEntryIdleTimeout(action, idleTimeout);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setEntryTimeToLive(
    ExpirationAction::Action action, int timeToLive) {
  m_attributeFactory->setEntryTimeToLive(action, timeToLive);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setRegionIdleTimeout(
    ExpirationAction::Action action, int idleTimeout) {
  m_attributeFactory->setRegionIdleTimeout(action, idleTimeout);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}
RegionFactoryPtr RegionFactory::setRegionTimeToLive(
    ExpirationAction::Action action, int timeToLive) {
  m_attributeFactory->setRegionTimeToLive(action, timeToLive);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setInitialCapacity(int initialCapacity) {
  char excpStr[256] = {0};
  if (initialCapacity < 0) {
    ACE_OS::snprintf(excpStr, 256, "initialCapacity must be >= 0 ");
    throw IllegalArgumentException(excpStr);
  }
  m_attributeFactory->setInitialCapacity(initialCapacity);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setLoadFactor(float loadFactor) {
  m_attributeFactory->setLoadFactor(loadFactor);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setConcurrencyLevel(uint8_t concurrencyLevel) {
  m_attributeFactory->setConcurrencyLevel(concurrencyLevel);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}
RegionFactoryPtr RegionFactory::setConcurrencyChecksEnabled(bool enable) {
  m_attributeFactory->setConcurrencyChecksEnabled(enable);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}
RegionFactoryPtr RegionFactory::setLruEntriesLimit(
    const uint32_t entriesLimit) {
  m_attributeFactory->setLruEntriesLimit(entriesLimit);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setDiskPolicy(
    const DiskPolicyType::PolicyType diskPolicy) {
  m_attributeFactory->setDiskPolicy(diskPolicy);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setCachingEnabled(bool cachingEnabled) {
  m_attributeFactory->setCachingEnabled(cachingEnabled);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setPersistenceManager(
    const PersistenceManagerPtr& persistenceManager,
    const PropertiesPtr& config) {
  m_attributeFactory->setPersistenceManager(persistenceManager, config);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setPersistenceManager(
    const char* lib, const char* func, const PropertiesPtr& config) {
  m_attributeFactory->setPersistenceManager(lib, func, config);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setPoolName(const char* name) {
  m_attributeFactory->setPoolName(name);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}

RegionFactoryPtr RegionFactory::setCloningEnabled(bool isClonable) {
  m_attributeFactory->setCloningEnabled(isClonable);
  RegionFactoryPtr rfPtr(this);
  return rfPtr;
}
}  // namespace gemfire
