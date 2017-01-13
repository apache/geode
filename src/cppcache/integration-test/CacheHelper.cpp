/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/GemfireCppCache.hpp>
#include <stdlib.h>
#include <gfcpp/SystemProperties.hpp>
#include <ace/OS.h>
#include <ace/INET_Addr.h>
#include <ace/SOCK_Acceptor.h>
#include <fstream>

#include "TimeBomb.hpp"
#include <list>
#include "DistributedSystemImpl.hpp"
#include "Utils.hpp"
#include <gfcpp/PoolManager.hpp>
#include <CacheRegionHelper.hpp>

#include "CacheHelper.hpp"
#define __DUNIT_NO_MAIN__
#include "fw_dunit.hpp"

#include <chrono>
#include <thread>

#ifndef ROOT_NAME
#define ROOT_NAME "Root"
#endif

#ifndef ROOT_SCOPE
#define ROOT_SCOPE LOCAL
#endif

#if defined(WIN32)
#define GFSH "gfsh.bat"
#define COPY_COMMAND "copy /y"
#define DELETE_COMMAND "del /f"
#define PATH_SEP "\\"
#else
#define GFSH "gfsh"
#define COPY_COMMAND "cp -f"
#define DELETE_COMMAND "rm -f"
#define PATH_SEP "/"
#endif

using namespace gemfire;

extern ClientCleanup gClientCleanup;

#define SEED 0
#define RANDOM_NUMBER_OFFSET 14000
#define RANDOM_NUMBER_DIVIDER 15000

CachePtr CacheHelper::getCache() { return cachePtr; }

CacheHelper& CacheHelper::getHelper() {
  if (singleton == NULL) {
    singleton = new CacheHelper();
  }
  return *singleton;
}

CacheHelper::CacheHelper(const char* member_id, const PropertiesPtr& configPtr,
                         const bool noRootRegion) {
  PropertiesPtr pp = configPtr;
  if (pp == NULLPTR) {
    pp = Properties::create();
  }

  cachePtr = CacheFactory::createCacheFactory(pp)->create();

  m_doDisconnect = false;

  if (noRootRegion) return;

  try {
    RegionFactoryPtr regionFactoryPtr =
        cachePtr->createRegionFactory(CACHING_PROXY);
    rootRegionPtr = regionFactoryPtr->create(ROOT_NAME);
  } catch (const RegionExistsException&) {
    rootRegionPtr = cachePtr->getRegion(ROOT_NAME);
  }

  showRegionAttributes(*rootRegionPtr->getAttributes());
}

/** rootRegionPtr will still be null... */
CacheHelper::CacheHelper(const char* member_id, const char* cachexml,
                         const PropertiesPtr& configPtr) {
  PropertiesPtr pp = configPtr;
  if (pp == NULLPTR) {
    pp = Properties::create();
  }
  if (cachexml != NULL) {
    std::string tmpXmlFile(cachexml);
    std::string newFile;
    CacheHelper::createDuplicateXMLFile(newFile, tmpXmlFile);
    pp->insert("cache-xml-file", newFile.c_str());
  }
  cachePtr = CacheFactory::createCacheFactory(pp)->create();

  m_doDisconnect = false;
}

CacheHelper::CacheHelper(const PropertiesPtr& configPtr,
                         const bool noRootRegion) {
  PropertiesPtr pp = configPtr;
  if (pp == NULLPTR) {
    pp = Properties::create();
  }

  cachePtr = CacheFactory::createCacheFactory(pp)->create();

  m_doDisconnect = false;

  if (noRootRegion) return;

  try {
    RegionFactoryPtr regionFactoryPtr =
        cachePtr->createRegionFactory(CACHING_PROXY);
    rootRegionPtr = regionFactoryPtr->create(ROOT_NAME);
  } catch (const RegionExistsException&) {
    rootRegionPtr = cachePtr->getRegion(ROOT_NAME);
  }

  showRegionAttributes(*rootRegionPtr->getAttributes());
}

CacheHelper::CacheHelper(const bool isThinclient,
                         const PropertiesPtr& configPtr,
                         const bool noRootRegion) {
  PropertiesPtr pp = configPtr;
  if (pp == NULLPTR) {
    pp = Properties::create();
  }
  try {
    LOG(" in cachehelper before createCacheFactory");
    cachePtr = CacheFactory::createCacheFactory(pp)->create();
    m_doDisconnect = false;
  } catch (const Exception& excp) {
    LOG("GemFire exception while creating cache, logged in following line");
    LOG(excp.getMessage());
  } catch (...) {
    LOG("Throwing exception while creating cache....");
  }
}

CacheHelper::CacheHelper(const bool isThinclient, bool pdxIgnoreUnreadFields,
                         bool pdxReadSerialized, const PropertiesPtr& configPtr,
                         const bool noRootRegion) {
  PropertiesPtr pp = configPtr;
  if (pp == NULLPTR) {
    pp = Properties::create();
  }
  try {
    CacheFactoryPtr cfPtr = CacheFactory::createCacheFactory(pp);
    LOGINFO("pdxReadSerialized = %d ", pdxReadSerialized);
    LOGINFO("pdxIgnoreUnreadFields = %d ", pdxIgnoreUnreadFields);
    cfPtr->setPdxReadSerialized(pdxReadSerialized);
    cfPtr->setPdxIgnoreUnreadFields(pdxIgnoreUnreadFields);
    cachePtr = cfPtr->create();
    m_doDisconnect = false;
  } catch (const Exception& excp) {
    LOG("GemFire exception while creating cache, logged in following line");
    LOG(excp.getMessage());
  } catch (...) {
    LOG("Throwing exception while creating cache....");
  }
}

CacheHelper::CacheHelper(const bool isthinClient, const char* poolName,
                         const char* locators, const char* serverGroup,
                         const PropertiesPtr& configPtr, int redundancy,
                         bool clientNotification, int subscriptionAckInterval,
                         int connections, int loadConditioningInterval,
                         bool isMultiuserMode, bool prSingleHop,
                         bool threadLocal) {
  PropertiesPtr pp = configPtr;
  if (pp == NULLPTR) {
    pp = Properties::create();
  }

  try {
    CacheFactoryPtr cacheFac = CacheFactory::createCacheFactory(pp);
    cacheFac->setPRSingleHopEnabled(prSingleHop);
    cacheFac->setThreadLocalConnections(threadLocal);
    printf(" Setting pr-single-hop to prSingleHop = %d ", prSingleHop);
    printf("Setting threadLocal to %d ", threadLocal);
    if (locators) {
      addServerLocatorEPs(locators, cacheFac);
      if (serverGroup) {
        cacheFac->setServerGroup(serverGroup);
      }
    }
    cacheFac->setSubscriptionRedundancy(redundancy);
    cacheFac->setSubscriptionEnabled(clientNotification);
    cacheFac->setMultiuserAuthentication(isMultiuserMode);
    if (loadConditioningInterval > 0) {
      cacheFac->setLoadConditioningInterval(loadConditioningInterval);
    }
    printf("Setting connections to %d ", connections);
    if (connections >= 0) {
      cacheFac->setMinConnections(connections);
      cacheFac->setMaxConnections(connections);
    }
    if (subscriptionAckInterval != -1) {
      cacheFac->setSubscriptionAckInterval(subscriptionAckInterval);
    }

    cachePtr = cacheFac->create();
  } catch (const Exception& excp) {
    LOG("GemFire exception while creating cache, logged in following line");
    LOG(excp.getMessage());
  } catch (...) {
    LOG("Throwing exception while creating cache....");
  }
}

CacheHelper::CacheHelper(const int redundancyLevel,
                         const PropertiesPtr& configPtr) {
  PropertiesPtr pp = configPtr;
  if (pp == NULLPTR) {
    pp = Properties::create();
  }

  CacheFactoryPtr cacheFac = CacheFactory::createCacheFactory(pp);
  cachePtr = cacheFac->create();
  m_doDisconnect = false;
}

CacheHelper::~CacheHelper() {
  // CacheHelper::cleanupTmpConfigFiles();
  disconnect();
}

void CacheHelper::closePool(const char* poolName, bool keepAlive) {
  PoolPtr pool = PoolManager::find(poolName);
  pool->destroy(keepAlive);
}

void CacheHelper::disconnect(bool keepalive) {
  if (cachePtr == NULLPTR) {
    return;
  }

  LOG("Beginning cleanup after CacheHelper.");

  DistributedSystemPtr systemPtr;
  if (m_doDisconnect) {
    systemPtr = cachePtr->getDistributedSystem();
  }

  // rootRegionPtr->localDestroyRegion();
  rootRegionPtr = NULLPTR;
  LOG("Destroyed root region.");
  try {
    LOG("Closing cache.");
    if (cachePtr != NULLPTR) {
      cachePtr->close(keepalive);
    }
    LOG("Closing cache complete.");
  } catch (Exception& ex) {
    LOG("Exception thrown while closing cache: ");
    LOG(ex.getMessage());
  } catch (...) {
    LOG("exception throw while closing cache");
  }

  cachePtr = NULLPTR;
  LOG("Closed cache.");
  try {
    if (m_doDisconnect) {
      LOG("Disconnecting...");
      systemPtr->disconnect();
      LOG("Finished disconnect.");
    }
  } catch (...) {
    LOG("Throwing exception while disconnecting....");
  }
  singleton = NULL;
  LOG("Finished cleanup after CacheHelper.");
}

void CacheHelper::createPlainRegion(const char* regionName,
                                    RegionPtr& regionPtr) {
  createPlainRegion(regionName, regionPtr, 10);
}

void CacheHelper::createPlainRegion(const char* regionName,
                                    RegionPtr& regionPtr, uint32_t size) {
  RegionAttributesPtr regAttrs;
  AttributesFactory attrFactory;
  // set lru attributes...
  attrFactory.setLruEntriesLimit(0);     // no limit.
  attrFactory.setInitialCapacity(size);  // no limit.
  // then...
  regAttrs = attrFactory.createRegionAttributes();
  showRegionAttributes(*regAttrs);
  // This is using subregions (deprecated) so not placing the new cache API here
  regionPtr = rootRegionPtr->createSubregion(regionName, regAttrs);
  ASSERT(regionPtr != NULLPTR, "failed to create region.");
}

void CacheHelper::createLRURegion(const char* regionName,
                                  RegionPtr& regionPtr) {
  createLRURegion(regionName, regionPtr, 10);
}
void CacheHelper::createLRURegion(const char* regionName, RegionPtr& regionPtr,
                                  uint32_t size) {
  RegionAttributesPtr regAttrs;
  AttributesFactory attrFactory;
  // set lru attributes...
  attrFactory.setLruEntriesLimit(size);
  attrFactory.setInitialCapacity(size);
  // then...
  regAttrs = attrFactory.createRegionAttributes();
  showRegionAttributes(*regAttrs);
  // This is using subregions (deprecated) so not placing the new cache API here
  regionPtr = rootRegionPtr->createSubregion(regionName, regAttrs);
  ASSERT(regionPtr != NULLPTR, "failed to create region.");
}

void CacheHelper::createDistRegion(const char* regionName,
                                   RegionPtr& regionPtr) {
  createDistRegion(regionName, regionPtr, 10);
}

void CacheHelper::createDistRegion(const char* regionName, RegionPtr& regionPtr,
                                   uint32_t size) {
  RegionAttributesPtr regAttrs;
  AttributesFactory attrFactory;
  // set lru attributes...
  attrFactory.setLruEntriesLimit(0);     // no limit.
  attrFactory.setInitialCapacity(size);  // no limit.
  // then...
  regAttrs = attrFactory.createRegionAttributes();
  showRegionAttributes(*regAttrs);
  // This is using subregions (deprecated) so not placing the new cache API here
  regionPtr = rootRegionPtr->createSubregion(regionName, regAttrs);
  ASSERT(regionPtr != NULLPTR, "failed to create region.");
}

RegionPtr CacheHelper::getRegion(const char* name) {
  return cachePtr->getRegion(name);
}

RegionPtr CacheHelper::createRegion(const char* name, bool ack, bool caching,
                                    const CacheListenerPtr& listener,
                                    bool clientNotificationEnabled,
                                    bool scopeLocal,
                                    bool concurrencyCheckEnabled,
                                    int32_t tombstonetimeout) {
  AttributesFactory af;
  af.setCachingEnabled(caching);
  if (listener != NULLPTR) {
    af.setCacheListener(listener);
  }
  if (concurrencyCheckEnabled) {
    af.setConcurrencyChecksEnabled(concurrencyCheckEnabled);
  }

  RegionAttributesPtr rattrsPtr = af.createRegionAttributes();

  CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cachePtr.ptr());
  RegionPtr regionPtr;
  cacheImpl->createRegion(name, rattrsPtr, regionPtr);
  return regionPtr;
}

RegionPtr CacheHelper::createRegion(const char* name, bool ack, bool caching,
                                    int ettl, int eit, int rttl, int rit,
                                    int lel, ExpirationAction::Action action,
                                    const char* endpoints,
                                    bool clientNotificationEnabled) {
  AttributesFactory af;
  af.setCachingEnabled(caching);
  af.setLruEntriesLimit(lel);
  af.setEntryIdleTimeout(action, eit);
  af.setEntryTimeToLive(action, ettl);
  af.setRegionIdleTimeout(action, rit);
  af.setRegionTimeToLive(action, rttl);

  RegionAttributesPtr rattrsPtr = af.createRegionAttributes();

  CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cachePtr.ptr());
  RegionPtr regionPtr;
  cacheImpl->createRegion(name, rattrsPtr, regionPtr);
  return regionPtr;
}

PoolPtr CacheHelper::createPool(const char* poolName, const char* locators,
                                const char* serverGroup, int redundancy,
                                bool clientNotification,
                                int subscriptionAckInterval, int connections,
                                int loadConditioningInterval,
                                bool isMultiuserMode) {
  // printf(" in createPool isMultiuserMode = %d \n", isMultiuserMode);
  PoolFactoryPtr poolFacPtr = PoolManager::createFactory();

  addServerLocatorEPs(locators, poolFacPtr);
  if (serverGroup) {
    poolFacPtr->setServerGroup(serverGroup);
  }

  poolFacPtr->setSubscriptionRedundancy(redundancy);
  poolFacPtr->setSubscriptionEnabled(clientNotification);
  poolFacPtr->setMultiuserAuthentication(isMultiuserMode);
  // poolFacPtr->setStatisticInterval(1000);
  if (loadConditioningInterval > 0) {
    poolFacPtr->setLoadConditioningInterval(loadConditioningInterval);
  }

  if (connections >= 0) {
    poolFacPtr->setMinConnections(connections);
    poolFacPtr->setMaxConnections(connections);
  }
  if (subscriptionAckInterval != -1) {
    poolFacPtr->setSubscriptionAckInterval(subscriptionAckInterval);
  }

  return poolFacPtr->create(poolName);
}

// this will create pool even endpoints and locatorhost has been not defined
PoolPtr CacheHelper::createPool2(const char* poolName, const char* locators,
                                 const char* serverGroup, const char* servers,
                                 int redundancy, bool clientNotification,
                                 int subscriptionAckInterval, int connections) {
  PoolFactoryPtr poolFacPtr = PoolManager::createFactory();

  if (servers != 0)  // with explicit server list
  {
    addServerLocatorEPs(servers, poolFacPtr, false);
    // do region creation with end
  } else if (locators != 0)  // with locator
  {
    addServerLocatorEPs(locators, poolFacPtr);
    if (serverGroup) {
      poolFacPtr->setServerGroup(serverGroup);
    }
  }

  poolFacPtr->setSubscriptionRedundancy(redundancy);
  poolFacPtr->setSubscriptionEnabled(clientNotification);
  if (connections >= 0) {
    poolFacPtr->setMinConnections(connections);
    poolFacPtr->setMaxConnections(connections);
  }
  if (subscriptionAckInterval != -1) {
    poolFacPtr->setSubscriptionAckInterval(subscriptionAckInterval);
  }

  return poolFacPtr->create(poolName);
}

void CacheHelper::logPoolAttributes(PoolPtr& pool) {
  LOG("logPoolAttributes() entered");
  LOGINFO("CPPTEST: Pool attribtes for pool %s are as follows:",
          pool->getName());
  LOGINFO("getFreeConnectionTimeout: %d", pool->getFreeConnectionTimeout());
  LOGINFO("getLoadConditioningInterval: %d",
          pool->getLoadConditioningInterval());
  LOGINFO("getSocketBufferSize: %d", pool->getSocketBufferSize());
  LOGINFO("getReadTimeout: %d", pool->getReadTimeout());
  LOGINFO("getMinConnections: %d", pool->getMinConnections());
  LOGINFO("getMaxConnections: %d", pool->getMaxConnections());
  LOGINFO("getIdleTimeout: %d", pool->getIdleTimeout());
  LOGINFO("getPingInterval: %d", pool->getPingInterval());
  LOGINFO("getStatisticInterval: %d", pool->getStatisticInterval());
  LOGINFO("getRetryAttempts: %d", pool->getRetryAttempts());
  LOGINFO("getSubscriptionEnabled: %s",
          pool->getSubscriptionEnabled() ? "true" : "false");
  LOGINFO("getSubscriptionRedundancy: %d", pool->getSubscriptionRedundancy());
  LOGINFO("getSubscriptionMessageTrackingTimeout: %d",
          pool->getSubscriptionMessageTrackingTimeout());
  LOGINFO("getSubscriptionAckInterval: %d", pool->getSubscriptionAckInterval());
  LOGINFO("getServerGroup: %s", pool->getServerGroup());
  LOGINFO("getThreadLocalConnections: %s",
          pool->getThreadLocalConnections() ? "true" : "false");
  LOGINFO("getPRSingleHopEnabled: %s",
          pool->getPRSingleHopEnabled() ? "true" : "false");
}

void CacheHelper::createPoolWithLocators(const char* name, const char* locators,
                                         bool clientNotificationEnabled,
                                         int subscriptionRedundancy,
                                         int subscriptionAckInterval,
                                         int connections, bool isMultiuserMode,
                                         const char* serverGroup) {
  LOG("createPool() entered.");
  printf(" in createPoolWithLocators isMultiuserMode = %d\n", isMultiuserMode);
  PoolPtr poolPtr =
      createPool(name, locators, serverGroup, subscriptionRedundancy,
                 clientNotificationEnabled, subscriptionAckInterval,
                 connections, -1, isMultiuserMode);
  ASSERT(poolPtr != NULLPTR, "Failed to create pool.");
  logPoolAttributes(poolPtr);
  LOG("Pool created.");
}

RegionPtr CacheHelper::createRegionAndAttachPool(
    const char* name, bool ack, const char* poolName, bool caching, int ettl,
    int eit, int rttl, int rit, int lel, ExpirationAction::Action action) {
  RegionShortcut preDefRA = PROXY;
  if (caching) {
    preDefRA = CACHING_PROXY;
  }
  if (lel > 0) {
    preDefRA = CACHING_PROXY_ENTRY_LRU;
  }
  RegionFactoryPtr regionFactoryPtr = cachePtr->createRegionFactory(preDefRA);
  regionFactoryPtr->setLruEntriesLimit(lel);
  regionFactoryPtr->setEntryIdleTimeout(action, eit);
  regionFactoryPtr->setEntryTimeToLive(action, ettl);
  regionFactoryPtr->setRegionIdleTimeout(action, rit);
  regionFactoryPtr->setRegionTimeToLive(action, rttl);
  if (poolName != NULL) {
    regionFactoryPtr->setPoolName(poolName);
  }
  return regionFactoryPtr->create(name);
}

RegionPtr CacheHelper::createRegionAndAttachPool2(
    const char* name, bool ack, const char* poolName,
    const PartitionResolverPtr& aResolver, bool caching, int ettl, int eit,
    int rttl, int rit, int lel, ExpirationAction::Action action) {
  RegionShortcut preDefRA = PROXY;
  if (caching) {
    preDefRA = CACHING_PROXY;
  }
  if (lel > 0) {
    preDefRA = CACHING_PROXY_ENTRY_LRU;
  }
  RegionFactoryPtr regionFactoryPtr = cachePtr->createRegionFactory(preDefRA);
  regionFactoryPtr->setLruEntriesLimit(lel);
  regionFactoryPtr->setEntryIdleTimeout(action, eit);
  regionFactoryPtr->setEntryTimeToLive(action, ettl);
  regionFactoryPtr->setRegionIdleTimeout(action, rit);
  regionFactoryPtr->setRegionTimeToLive(action, rttl);
  regionFactoryPtr->setPoolName(poolName);
  regionFactoryPtr->setPartitionResolver(aResolver);
  return regionFactoryPtr->create(name);
}

void CacheHelper::addServerLocatorEPs(const char* epList, PoolFactoryPtr pfPtr,
                                      bool poolLocators) {
  std::unordered_set<std::string> endpointNames;
  Utils::parseEndpointNamesString(epList, endpointNames);
  for (std::unordered_set<std::string>::iterator iter = endpointNames.begin();
       iter != endpointNames.end(); ++iter) {
    size_t position = (*iter).find_first_of(":");
    if (position != std::string::npos) {
      std::string hostname = (*iter).substr(0, position);
      int portnumber = atoi(((*iter).substr(position + 1)).c_str());
      if (poolLocators) {
        LOG((*iter));

        pfPtr->addLocator(hostname.c_str(), portnumber);
      } else {
        pfPtr->addServer(hostname.c_str(), portnumber);
      }
    }
  }
}

void CacheHelper::addServerLocatorEPs(const char* epList,
                                      CacheFactoryPtr cacheFac,
                                      bool poolLocators) {
  std::unordered_set<std::string> endpointNames;
  Utils::parseEndpointNamesString(epList, endpointNames);
  for (std::unordered_set<std::string>::iterator iter = endpointNames.begin();
       iter != endpointNames.end(); ++iter) {
    size_t position = (*iter).find_first_of(":");
    if (position != std::string::npos) {
      std::string hostname = (*iter).substr(0, position);
      int portnumber = atoi(((*iter).substr(position + 1)).c_str());
      if (poolLocators) {
        cacheFac->addLocator(hostname.c_str(), portnumber);
      } else {
        printf("ankur Server: %d", portnumber);
        cacheFac->addServer(hostname.c_str(), portnumber);
      }
    }
  }
}

RegionPtr CacheHelper::createPooledRegion(
    const char* name, bool ack, const char* locators, const char* poolName,
    bool caching, bool clientNotificationEnabled, int ettl, int eit, int rttl,
    int rit, int lel, const CacheListenerPtr& cacheListener,
    ExpirationAction::Action action) {
  PoolFactoryPtr poolFacPtr = PoolManager::createFactory();
  poolFacPtr->setSubscriptionEnabled(clientNotificationEnabled);

  if (locators) {
    LOG("adding pool locators");
    addServerLocatorEPs(locators, poolFacPtr);
  }

  if ((PoolManager::find(poolName)) ==
      NULLPTR) {  // Pool does not exist with the same name.
    PoolPtr pptr = poolFacPtr->create(poolName);
  }

  RegionShortcut preDefRA = PROXY;
  if (caching) {
    preDefRA = CACHING_PROXY;
  }
  if (lel > 0) {
    preDefRA = CACHING_PROXY_ENTRY_LRU;
  }
  RegionFactoryPtr regionFactoryPtr = cachePtr->createRegionFactory(preDefRA);
  regionFactoryPtr->setLruEntriesLimit(lel);
  regionFactoryPtr->setEntryIdleTimeout(action, eit);
  regionFactoryPtr->setEntryTimeToLive(action, ettl);
  regionFactoryPtr->setRegionIdleTimeout(action, rit);
  regionFactoryPtr->setRegionTimeToLive(action, rttl);
  regionFactoryPtr->setPoolName(poolName);
  if (cacheListener != NULLPTR) {
    regionFactoryPtr->setCacheListener(cacheListener);
  }
  return regionFactoryPtr->create(name);
}

RegionPtr CacheHelper::createPooledRegionConcurrencyCheckDisabled(
    const char* name, bool ack, const char* locators, const char* poolName,
    bool caching, bool clientNotificationEnabled, bool concurrencyCheckEnabled,
    int ettl, int eit, int rttl, int rit, int lel,
    const CacheListenerPtr& cacheListener, ExpirationAction::Action action) {
  PoolFactoryPtr poolFacPtr = PoolManager::createFactory();
  poolFacPtr->setSubscriptionEnabled(clientNotificationEnabled);

  LOG("adding pool locators");
  addServerLocatorEPs(locators, poolFacPtr);

  if ((PoolManager::find(poolName)) ==
      NULLPTR) {  // Pool does not exist with the same name.
    PoolPtr pptr = poolFacPtr->create(poolName);
  }

  RegionShortcut preDefRA = PROXY;
  if (caching) {
    preDefRA = CACHING_PROXY;
  }
  if (lel > 0) {
    preDefRA = CACHING_PROXY_ENTRY_LRU;
  }
  RegionFactoryPtr regionFactoryPtr = cachePtr->createRegionFactory(preDefRA);
  regionFactoryPtr->setLruEntriesLimit(lel);
  regionFactoryPtr->setEntryIdleTimeout(action, eit);
  regionFactoryPtr->setEntryTimeToLive(action, ettl);
  regionFactoryPtr->setRegionIdleTimeout(action, rit);
  regionFactoryPtr->setRegionTimeToLive(action, rttl);
  regionFactoryPtr->setConcurrencyChecksEnabled(concurrencyCheckEnabled);
  regionFactoryPtr->setPoolName(poolName);
  if (cacheListener != NULLPTR) {
    regionFactoryPtr->setCacheListener(cacheListener);
  }
  return regionFactoryPtr->create(name);
}

RegionPtr CacheHelper::createRegionDiscOverFlow(
    const char* name, bool caching, bool clientNotificationEnabled, int ettl,
    int eit, int rttl, int rit, int lel, ExpirationAction::Action action) {
  AttributesFactory af;
  af.setCachingEnabled(caching);
  af.setLruEntriesLimit(lel);
  af.setEntryIdleTimeout(action, eit);
  af.setEntryTimeToLive(action, ettl);
  af.setRegionIdleTimeout(action, rit);
  af.setRegionTimeToLive(action, rttl);
  af.setCloningEnabled(true);
  if (lel > 0) {
    af.setDiskPolicy(DiskPolicyType::OVERFLOWS);
    PropertiesPtr sqLiteProps = Properties::create();
    sqLiteProps->insert("PageSize", "65536");
    sqLiteProps->insert("MaxPageCount", "1073741823");
    std::string sqlite_dir =
        "SqLiteRegionData" +
        std::to_string(static_cast<long long int>(ACE_OS::getpid()));
    sqLiteProps->insert("PersistenceDirectory", sqlite_dir.c_str());
    af.setPersistenceManager("SqLiteImpl", "createSqLiteInstance", sqLiteProps);
  }

  RegionAttributesPtr rattrsPtr = af.createRegionAttributes();
  CacheImpl* cacheImpl = CacheRegionHelper::getCacheImpl(cachePtr.ptr());
  RegionPtr regionPtr;
  cacheImpl->createRegion(name, rattrsPtr, regionPtr);
  return regionPtr;
}

RegionPtr CacheHelper::createPooledRegionDiscOverFlow(
    const char* name, bool ack, const char* locators, const char* poolName,
    bool caching, bool clientNotificationEnabled, int ettl, int eit, int rttl,
    int rit, int lel, const CacheListenerPtr& cacheListener,
    ExpirationAction::Action action) {
  PoolFactoryPtr poolFacPtr = PoolManager::createFactory();
  poolFacPtr->setSubscriptionEnabled(clientNotificationEnabled);

  if (locators)  // with locator
  {
    LOG("adding pool locators");
    addServerLocatorEPs(locators, poolFacPtr);
  }
  if ((PoolManager::find(poolName)) ==
      NULLPTR) {  // Pool does not exist with the same name.
    PoolPtr pptr = poolFacPtr->create(poolName);
  }

  if (!caching) {
    LOG("createPooledRegionDiscOverFlow: setting caching=false does not make "
        "sense");
    FAIL(
        "createPooledRegionDiscOverFlow: setting caching=false does not make "
        "sense");
  }
  RegionShortcut preDefRA = CACHING_PROXY;
  if (lel > 0) {
    preDefRA = CACHING_PROXY_ENTRY_LRU;
  }
  RegionFactoryPtr regionFactoryPtr = cachePtr->createRegionFactory(preDefRA);
  regionFactoryPtr->setLruEntriesLimit(lel);
  regionFactoryPtr->setEntryIdleTimeout(action, eit);
  regionFactoryPtr->setEntryTimeToLive(action, ettl);
  regionFactoryPtr->setRegionIdleTimeout(action, rit);
  regionFactoryPtr->setRegionTimeToLive(action, rttl);
  regionFactoryPtr->setPoolName(poolName);
  regionFactoryPtr->setCloningEnabled(true);
  if (lel > 0) {
    regionFactoryPtr->setDiskPolicy(DiskPolicyType::OVERFLOWS);
    PropertiesPtr sqLiteProps = Properties::create();
    sqLiteProps->insert("PageSize", "65536");
    sqLiteProps->insert("MaxPageCount", "1073741823");
    std::string sqlite_dir =
        "SqLiteRegionData" +
        std::to_string(static_cast<long long int>(ACE_OS::getpid()));
    sqLiteProps->insert("PersistenceDirectory", sqlite_dir.c_str());
    regionFactoryPtr->setPersistenceManager(
        "SqLiteImpl", "createSqLiteInstance", sqLiteProps);
  }
  if (cacheListener != NULLPTR) {
    regionFactoryPtr->setCacheListener(cacheListener);
  }
  return regionFactoryPtr->create(name);
}

RegionPtr CacheHelper::createPooledRegionSticky(
    const char* name, bool ack, const char* locators, const char* poolName,
    bool caching, bool clientNotificationEnabled, int ettl, int eit, int rttl,
    int rit, int lel, const CacheListenerPtr& cacheListener,
    ExpirationAction::Action action) {
  PoolFactoryPtr poolFacPtr = PoolManager::createFactory();
  poolFacPtr->setSubscriptionEnabled(clientNotificationEnabled);
  poolFacPtr->setThreadLocalConnections(true);
  poolFacPtr->setPRSingleHopEnabled(false);

  LOG("adding pool locators");
  addServerLocatorEPs(locators, poolFacPtr);

  if ((PoolManager::find(poolName)) ==
      NULLPTR) {  // Pool does not exist with the same name.
    PoolPtr pptr = poolFacPtr->create(poolName);
    LOG("createPooledRegionSticky logPoolAttributes");
    logPoolAttributes(pptr);
  }

  RegionShortcut preDefRA = PROXY;
  if (caching) {
    preDefRA = CACHING_PROXY;
  }
  if (lel > 0) {
    preDefRA = CACHING_PROXY_ENTRY_LRU;
  }
  RegionFactoryPtr regionFactoryPtr = cachePtr->createRegionFactory(preDefRA);
  regionFactoryPtr->setLruEntriesLimit(lel);
  regionFactoryPtr->setEntryIdleTimeout(action, eit);
  regionFactoryPtr->setEntryTimeToLive(action, ettl);
  regionFactoryPtr->setRegionIdleTimeout(action, rit);
  regionFactoryPtr->setRegionTimeToLive(action, rttl);
  regionFactoryPtr->setPoolName(poolName);
  if (cacheListener != NULLPTR) {
    regionFactoryPtr->setCacheListener(cacheListener);
  }
  return regionFactoryPtr->create(name);
}

RegionPtr CacheHelper::createPooledRegionStickySingleHop(
    const char* name, bool ack, const char* locators, const char* poolName,
    bool caching, bool clientNotificationEnabled, int ettl, int eit, int rttl,
    int rit, int lel, const CacheListenerPtr& cacheListener,
    ExpirationAction::Action action) {
  LOG("createPooledRegionStickySingleHop");
  PoolFactoryPtr poolFacPtr = PoolManager::createFactory();
  poolFacPtr->setSubscriptionEnabled(clientNotificationEnabled);
  poolFacPtr->setThreadLocalConnections(true);
  poolFacPtr->setPRSingleHopEnabled(true);
  LOG("adding pool locators");
  addServerLocatorEPs(locators, poolFacPtr);

  if ((PoolManager::find(poolName)) ==
      NULLPTR) {  // Pool does not exist with the same name.
    PoolPtr pptr = poolFacPtr->create(poolName);
    LOG("createPooledRegionStickySingleHop logPoolAttributes");
    logPoolAttributes(pptr);
  }

  RegionShortcut preDefRA = PROXY;
  if (caching) {
    preDefRA = CACHING_PROXY;
  }
  if (lel > 0) {
    preDefRA = CACHING_PROXY_ENTRY_LRU;
  }
  RegionFactoryPtr regionFactoryPtr = cachePtr->createRegionFactory(preDefRA);
  regionFactoryPtr->setLruEntriesLimit(lel);
  regionFactoryPtr->setEntryIdleTimeout(action, eit);
  regionFactoryPtr->setEntryTimeToLive(action, ettl);
  regionFactoryPtr->setRegionIdleTimeout(action, rit);
  regionFactoryPtr->setRegionTimeToLive(action, rttl);
  regionFactoryPtr->setPoolName(poolName);
  if (cacheListener != NULLPTR) {
    regionFactoryPtr->setCacheListener(cacheListener);
  }
  return regionFactoryPtr->create(name);
}

RegionPtr CacheHelper::createSubregion(RegionPtr& parent, const char* name,
                                       bool ack, bool caching,
                                       const CacheListenerPtr& listener) {
  AttributesFactory af;
  af.setCachingEnabled(caching);
  if (listener != NULLPTR) {
    af.setCacheListener(listener);
  }
  RegionAttributesPtr rattrsPtr = af.createRegionAttributes();

  return parent->createSubregion(name, rattrsPtr);
}

CacheableStringPtr CacheHelper::createCacheable(const char* value) {
  return CacheableString::create(value);
}

void CacheHelper::showKeys(VectorOfCacheableKey& vecKeys) {
  fprintf(stdout, "vecKeys.size() = %d\n", vecKeys.size());
  for (uint32_t i = 0; i < static_cast<uint32_t>(vecKeys.size()); i++) {
    char msg[1024];
    size_t wrote = vecKeys.at(i)->logString(msg, 1023);
    msg[wrote] = '\0';  // just in case...
    fprintf(stdout, "key[%d] - %s\n", i, msg);
  }
  fflush(stdout);
}

void CacheHelper::showRegionAttributes(RegionAttributes& attributes) {
  printf("caching=%s\n", attributes.getCachingEnabled() ? "true" : "false");
  printf("Entry Time To Live = %d\n", attributes.getEntryTimeToLive());
  printf("Entry Idle Timeout = %d\n", attributes.getEntryIdleTimeout());
  printf("Region Time To Live = %d\n", attributes.getRegionTimeToLive());
  printf("Region Idle Timeout = %d\n", attributes.getRegionIdleTimeout());
  printf("Initial Capacity = %d\n", attributes.getInitialCapacity());
  printf("Load Factor = %f\n", attributes.getLoadFactor());
  printf("End Points = %s\n",
         (attributes.getEndpoints() != NULL ? attributes.getEndpoints()
                                            : "(null)"));
}

QueryServicePtr CacheHelper::getQueryService() {
  return cachePtr->getQueryService();
}

const char* CacheHelper::getTcrEndpoints(bool& isLocalServer,
                                         int numberOfServers) {
  static char* gfjavaenv = ACE_OS::getenv("GFJAVA");
  std::string gfendpoints;
  static bool gflocalserver = false;
  char tmp[100];

  if (gfendpoints.empty()) {
    ASSERT(gfjavaenv != NULL,
           "Environment variable GFJAVA for java build directory is not set.");
    if ((ACE_OS::strchr(gfjavaenv, '\\') != NULL) ||
        (ACE_OS::strchr(gfjavaenv, '/') != NULL)) {
      gflocalserver = true;
      /* Support for multiple servers Max = 10*/
      switch (numberOfServers) {
        case 1:
          // gfendpoints = "localhost:24680";
          {
            gfendpoints = "localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort1);
            gfendpoints += tmp;
          }
          break;
        case 2:
          // gfendpoints = "localhost:24680,localhost:24681";
          {
            gfendpoints = "localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort1);
            gfendpoints += tmp;
            gfendpoints += ",localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort2);
            gfendpoints += tmp;
          }
          break;
        case 3:
          // gfendpoints = "localhost:24680,localhost:24681,localhost:24682";
          {
            gfendpoints = "localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort1);
            gfendpoints += tmp;
            gfendpoints += ",localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort2);
            gfendpoints += tmp;
            gfendpoints += ",localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort3);
            gfendpoints += tmp;
          }
          break;
        default:
          // ASSERT( ( numberOfServers <= 10 )," More than 10 servers not
          // supported");
          // TODO: need to support more servers, need to generate random ports
          // here
          ASSERT((numberOfServers <= 4), " More than 4 servers not supported");
          gfendpoints = "localhost:";
          sprintf(tmp, "%d", CacheHelper::staticHostPort1);
          gfendpoints += tmp;
          gfendpoints += ",localhost:";
          sprintf(tmp, "%d", CacheHelper::staticHostPort2);
          gfendpoints += tmp;
          gfendpoints += ",localhost:";
          sprintf(tmp, "%d", CacheHelper::staticHostPort3);
          gfendpoints += tmp;
          gfendpoints += ",localhost:";
          sprintf(tmp, "%d", CacheHelper::staticHostPort4);
          gfendpoints += tmp;
          /*gfendpoints = "localhost:24680";
          char temp[8];
          for(int i =1; i <= numberOfServers - 1; i++) {
           gfendpoints += ",localhost:2468";
           gfendpoints += ACE_OS::itoa(i,temp,10);
          }*/
          break;
      }
    } else {
      gfendpoints = gfjavaenv;
    }
  }
  isLocalServer = gflocalserver;
  printf("getHostPort :: %s \n", gfendpoints.c_str());
  return (new std::string(gfendpoints.c_str()))->c_str();
}

const char* CacheHelper::getstaticLocatorHostPort1() {
  return getLocatorHostPort(staticLocatorHostPort1);
}

const char* CacheHelper::getstaticLocatorHostPort2() {
  return getLocatorHostPort(staticLocatorHostPort2);
}

const char* CacheHelper::getLocatorHostPort(int locPort) {
  char tmp[128];
  std::string gfendpoints;
  gfendpoints = "localhost:";
  sprintf(tmp, "%d", locPort);
  gfendpoints += tmp;
  return (new std::string(gfendpoints.c_str()))->c_str();
  ;
}

const char* CacheHelper::getTcrEndpoints2(bool& isLocalServer,
                                          int numberOfServers) {
  static char* gfjavaenv = ACE_OS::getenv("GFJAVA");
  std::string gfendpoints;
  static bool gflocalserver = false;
  char tmp[128];

  if (gfendpoints.empty()) {
    if ((ACE_OS::strchr(gfjavaenv, '\\') != NULL) ||
        (ACE_OS::strchr(gfjavaenv, '/') != NULL)) {
      gflocalserver = true;
      /* Support for multiple servers Max = 10*/
      switch (numberOfServers) {
        case 1:
          // gfendpoints = "localhost:24680";
          {
            gfendpoints = "localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort1);
            gfendpoints += tmp;
          }
          break;
        case 2:
          // gfendpoints = "localhost:24680,localhost:24681";
          {
            gfendpoints = "localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort1);
            gfendpoints += tmp;
            gfendpoints += ",localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort2);
            gfendpoints += tmp;
          }
          break;
        case 3:
          // gfendpoints = "localhost:24680,localhost:24681,localhost:24682";
          {
            gfendpoints = "localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort1);
            gfendpoints += tmp;
            gfendpoints += ",localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort2);
            gfendpoints += tmp;
            gfendpoints += ",localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort3);
            gfendpoints += tmp;
          }
          break;
        case 4:
          // gfendpoints =
          // "localhost:24680,localhost:24681,localhost:24682,localhost:24683";
          {
            gfendpoints = "localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort1);
            gfendpoints += tmp;
            gfendpoints += ",localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort2);
            gfendpoints += tmp;
            gfendpoints += ",localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort3);
            gfendpoints += tmp;
            gfendpoints += ",localhost:";
            sprintf(tmp, "%d", CacheHelper::staticHostPort4);
            gfendpoints += tmp;
          }
          break;
        default:
          ASSERT((numberOfServers <= 10),
                 " More than 10 servers not supported");
          gfendpoints = "localhost:24680";
          char temp[8];
          for (int i = 1; i <= numberOfServers - 1; i++) {
            gfendpoints += ",localhost:2468";
            gfendpoints += ACE_OS::itoa(i, temp, 10);
          }
          break;
      }
    } else {
      gfendpoints = gfjavaenv;
    }
  }
  ASSERT(gfjavaenv != NULL,
         "Environment variable GFJAVA for java build directory is not set.");
  isLocalServer = gflocalserver;
  return (new std::string(gfendpoints.c_str()))->c_str();
}

const char* CacheHelper::getLocatorHostPort(bool& isLocator,
                                            bool& isLocalServer,
                                            int numberOfLocators) {
  static char* gfjavaenv = ACE_OS::getenv("GFJAVA");
  static std::string gflchostport;
  static bool gflocator = false;
  static bool gflocalserver = false;
  char tmp[100];

  if (gflchostport.empty()) {
    if ((ACE_OS::strchr(gfjavaenv, '\\') != NULL) ||
        (ACE_OS::strchr(gfjavaenv, '/') != NULL)) {
      gflocator = true;
      gflocalserver = true;
      switch (numberOfLocators) {
        case 1:
          // gflchostport = "localhost:34756";
          {
            gflchostport = "localhost:";
            sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort1);
            gflchostport += tmp;
          }
          break;
        case 2:
          // gflchostport = "localhost:34756,localhost:34757";
          {
            gflchostport = "localhost:";
            sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort1);
            gflchostport += tmp;
            sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort2);
            gflchostport += ",localhost:";
            gflchostport += tmp;
          }
          break;
        default:
          // gflchostport = "localhost:34756,localhost:34757,localhost:34758";
          {
            gflchostport = "localhost:";
            sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort1);
            gflchostport += tmp;
            sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort2);
            gflchostport += ",localhost:";
            gflchostport += tmp;
            sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort3);
            gflchostport += ",localhost:";
            gflchostport += tmp;
          }
          break;
      }
    } else {
      gflchostport = "";
    }
  }
  ASSERT(gfjavaenv != NULL,
         "Environment variable GFJAVA for java build directory is not set.");
  isLocator = gflocator;
  isLocalServer = gflocalserver;
  printf("getLocatorHostPort  :: %s  \n", gflchostport.c_str());
  return gflchostport.c_str();
}

void CacheHelper::cleanupServerInstances() {
  CacheHelper::cleanupTmpConfigFiles();
  if (staticServerInstanceList.size() > 0) {
    while (staticServerInstanceList.size() > 0) {
      int instance = staticServerInstanceList.front();

      staticServerInstanceList.remove(instance);  // for safety
      closeServer(instance);
    }
  }
}
void CacheHelper::initServer(int instance, const char* xml,
                             const char* locHostport, const char* authParam,
                             bool ssl, bool enableDelta, bool multiDS,
                             bool testServerGC) {
  if (!isServerCleanupCallbackRegistered &&
      gClientCleanup.registerCallback(&CacheHelper::cleanupServerInstances)) {
    isServerCleanupCallbackRegistered = true;
    printf("TimeBomb registered server cleanupcallback \n");
  }
  printf("Inside initServer added\n");
  if (authParam != NULL) {
    printf("Inside initServer with authParam = %s\n", authParam);
  } else {
    printf("Inside initServer with authParam as NULL\n");
    authParam = "";
  }
  static const char* gfjavaenv = ACE_OS::getenv("GFJAVA");
  static const char* gfLogLevel = ACE_OS::getenv("GFE_LOGLEVEL");
  static const char* gfSecLogLevel = ACE_OS::getenv("GFE_SECLOGLEVEL");
  static const char* path = ACE_OS::getenv("TESTSRC");
  static const char* mcastPort = ACE_OS::getenv("MCAST_PORT");
  static const char* mcastAddr = ACE_OS::getenv("MCAST_ADDR");
  static char* classpath = ACE_OS::getenv("GF_CLASSPATH");

  char cmd[2048];
  char tmp[128];
  char currWDPath[2048];
  int portNum;
  std::string currDir = ACE_OS::getcwd(currWDPath, 2048);

  ASSERT(gfjavaenv != NULL,
         "Environment variable GFJAVA for java build directory is not set.");
  ASSERT(path != NULL,
         "Environment variable TESTSRC for test source directory is not set.");
  ASSERT(mcastPort != NULL,
         "Environment variable MCAST_PORT for multicast port is not set.");
  ASSERT(mcastAddr != NULL,
         "Environment variable MCAST_ADDR for multicast address is not set.");
  ASSERT(!currDir.empty(),
         "Current working directory could not be determined.");
  if (gfLogLevel == NULL || gfLogLevel[0] == '\0') {
    gfLogLevel = "config";
  }
  if (gfSecLogLevel == NULL || gfSecLogLevel[0] == '\0') {
    gfSecLogLevel = "config";
  }

  if ((ACE_OS::strchr(gfjavaenv, '\\') == NULL) &&
      (ACE_OS::strchr(gfjavaenv, '/') == NULL)) {
    return;
  }

  std::string xmlFile = "";
  std::string sname = "GFECS";
  currDir += PATH_SEP;

  switch (instance) {
    case 0:
      // note: this need to take for multiple tests run
      xmlFile += "cacheserver.xml";
      sname += "0";
      if (multiDS) {
        mcastPort = "5431";
        mcastAddr = "224.10.11.";
      }
      break;
    case 1:
      xmlFile += "cacheserver.xml";
      sprintf(tmp, "%d", CacheHelper::staticHostPort1);
      sname += tmp;  // sname += "1";
      portNum = CacheHelper::staticHostPort1;

      if (multiDS) {
        mcastPort = "5431";
        mcastAddr = "224.10.11.";
      }
      break;
    case 2:
      xmlFile += "cacheserver2.xml";
      sprintf(tmp, "%d", CacheHelper::staticHostPort2);
      sname += tmp;  // sname += "3";
      portNum = CacheHelper::staticHostPort2;
      // sname += "2";
      if (multiDS) {
        mcastPort = "5431";
        mcastAddr = "224.10.11.";
      }
      break;
    case 3:
      xmlFile += "cacheserver3.xml";
      sprintf(tmp, "%d", CacheHelper::staticHostPort3);
      sname += tmp;  // sname += "3";
      portNum = CacheHelper::staticHostPort3;
      // sname += "3";
      if (multiDS) {
        mcastPort = "5433";
        mcastAddr = "224.10.11.";
      }
      break;
    case 4:
      xmlFile += "cacheserver4.xml";
      // sname += "4";
      sprintf(tmp, "%d", CacheHelper::staticHostPort4);
      sname += tmp;  // sname += "3";
      portNum = CacheHelper::staticHostPort4;
      if (multiDS) {
        mcastPort = "5433";
        mcastAddr = "224.10.11.";
      }
      break;
    default: /* Support for any number of servers Max 10*/
      ASSERT((instance <= 10), " More than 10 servers not supported");
      ASSERT(xml != NULL,
             "xml == NULL : For server instance > 3 xml file is must");
      char temp[8];
      portNum = CacheHelper::staticHostPort4;
      sname += ACE_OS::itoa(CacheHelper::staticHostPort4, temp, 10);
      break;
  }

  currDir += sname;

  if (xml != NULL) {
    /*
    xmlFile = path;
    xmlFile += PATH_SEP;
    xmlFile += xml;
    */
    xmlFile = xml;
  }

  std::string xmlFile_new;
  printf(" xml file name = %s \n", xmlFile.c_str());
  CacheHelper::createDuplicateXMLFile(xmlFile_new, xmlFile);
  // sprintf( tmp, "%d.xml", portNum );
  // xmlFile += tmp;
  xmlFile = xmlFile_new;

  printf("  creating dir = %s \n", sname.c_str());
  ACE_OS::mkdir(sname.c_str());

  //    sprintf( cmd, "/bin/cp %s/../test.gemfire.properties
  //    %s/",currDir.c_str(),
  // currDir.c_str()  );
  // LOG( cmd );
  // ACE_OS::system( cmd );

  sprintf(cmd, "%s/bin/%s stop server --dir=%s 2>&1", gfjavaenv, GFSH,
          currDir.c_str());

  LOG(cmd);
  ACE_OS::system(cmd);
  std::string deltaProperty = "";
  if (!enableDelta) {
    deltaProperty = "delta-propagation=false";
  }
  long defaultTombstone_timeout = 600000;
  long defaultTombstone_gc_threshold = 100000;
  long userTombstone_timeout = 1000;
  long userTombstone_gc_threshold = 10;
  if (testServerGC) {
    ACE_OS::mkdir("backupDirectory1");
    ACE_OS::mkdir("backupDirectory2");
    ACE_OS::mkdir("backupDirectory3");
    ACE_OS::mkdir("backupDirectory4");
  }

  if (locHostport != NULL) {  // check number of locator host port.
    std::string gemfireProperties = generateGemfireProperties(currDir, ssl);

    sprintf(
        cmd,
        "%s/bin/%s start server --classpath=%s --name=%s "
        "--cache-xml-file=%s --dir=%s --server-port=%d --log-level=%s "
        "--properties-file=%s %s %s "
        "--J=-Dgemfire.tombstone-timeout=%ld "
        "--J=-Dgemfire.tombstone-gc-hreshold=%ld "
        "--J=-Dgemfire.security-log-level=%s --J=-Xmx1024m --J=-Xms128m 2>&1",
        gfjavaenv, GFSH, classpath, sname.c_str(), xmlFile.c_str(),
        currDir.c_str(), portNum, gfLogLevel, gemfireProperties.c_str(),
        authParam, deltaProperty.c_str(),
        testServerGC ? userTombstone_timeout : defaultTombstone_timeout,
        testServerGC ? userTombstone_gc_threshold
                     : defaultTombstone_gc_threshold,
        gfSecLogLevel);
  } else {
    sprintf(
        cmd,
        "%s/bin/%s start server --classpath=%s --name=%s "
        "--cache-xml-file=%s --dir=%s --server-port=%d --log-level=%s %s %s "
        "--J=-Dgemfire.tombstone-timeout=%ld "
        "--J=-Dgemfire.tombstone-gc-hreshold=%ld "
        "--J=-Dgemfire.security-log-level=%s --J=-Xmx1024m --J=-Xms128m 2>&1",
        gfjavaenv, GFSH, classpath, sname.c_str(), xmlFile.c_str(),
        currDir.c_str(), portNum, gfLogLevel, authParam, deltaProperty.c_str(),
        testServerGC ? userTombstone_timeout : defaultTombstone_timeout,
        testServerGC ? userTombstone_gc_threshold
                     : defaultTombstone_gc_threshold,
        gfSecLogLevel);
  }

  LOG(cmd);
  int e = ACE_OS::system(cmd);
  ASSERT(0 == e, "cmd failed");

  staticServerInstanceList.push_back(instance);
  printf("added server instance %d\n", instance);
}

void CacheHelper::createDuplicateXMLFile(std::string& originalFile,
                                         int hostport1, int hostport2,
                                         int locport1, int locport2) {
  char cmd[1024];
  char currWDPath[2048];
  std::string currDir = ACE_OS::getcwd(currWDPath, 2048);
  currDir += PATH_SEP;
  std::string testSrc = ACE_OS::getenv("TESTSRC");
  testSrc += PATH_SEP;
  // file name will have hostport1.xml(i.e. CacheHelper::staticHostPort1) as
  // suffix
  sprintf(cmd,
          "sed -e s/HOST_PORT1/%d/g -e s/HOST_PORT2/%d/g -e s/HOST_PORT3/%d/g "
          "-e s/HOST_PORT4/%d/g -e s/LOC_PORT1/%d/g -e s/LOC_PORT2/%d/g <%s%s> "
          "%s%s%d.xml",
          hostport1, hostport2, CacheHelper::staticHostPort3,
          CacheHelper::staticHostPort4, locport1, locport2, testSrc.c_str(),
          originalFile.c_str(), currDir.c_str(), originalFile.c_str(),
          hostport1);

  LOG(cmd);
  int e = ACE_OS::system(cmd);
  ASSERT(0 == e, "cmd failed");

  // this file need to delete
  sprintf(cmd, "%s%s%d.xml", currDir.c_str(), originalFile.c_str(), hostport1);
  std::string s(cmd);
  CacheHelper::staticConfigFileList.push_back(s);

  printf("createDuplicateXMLFile added file %s %ld", cmd,
         CacheHelper::staticConfigFileList.size());
}

void CacheHelper::createDuplicateXMLFile(std::string& duplicateFile,
                                         std::string& originalFile) {
  CacheHelper::createDuplicateXMLFile(
      originalFile, CacheHelper::staticHostPort1, CacheHelper::staticHostPort2,
      CacheHelper::staticLocatorHostPort1, CacheHelper::staticLocatorHostPort2);

  char tmp[32];

  sprintf(tmp, "%d.xml", CacheHelper::staticHostPort1);

  char currWDPath[2048];
  duplicateFile = ACE_OS::getcwd(currWDPath, 2048);
  duplicateFile += PATH_SEP;
  duplicateFile += originalFile + tmp;
}

void CacheHelper::closeServer(int instance) {
  static char* gfjavaenv = ACE_OS::getenv("GFJAVA");

  char cmd[2048];
  char tmp[128];
  char currWDPath[2048];

  std::string currDir = ACE_OS::getcwd(currWDPath, 2048);

  ASSERT(gfjavaenv != NULL,
         "Environment variable GFJAVA for java build directory is not set.");
  ASSERT(!currDir.empty(),
         "Current working directory could not be determined.");

  if ((ACE_OS::strchr(gfjavaenv, '\\') == NULL) &&
      (ACE_OS::strchr(gfjavaenv, '/') == NULL)) {
    return;
  }

  currDir += "/GFECS";
  switch (instance) {
    case 0:
      currDir += "0";
      break;
    case 1:
      sprintf(tmp, "%d", CacheHelper::staticHostPort1);
      currDir += tmp;  // currDir += "1";
      break;
    case 2:
      sprintf(tmp, "%d", CacheHelper::staticHostPort2);
      currDir += tmp;  // currDir += "2";
      break;
    case 3:
      sprintf(tmp, "%d", CacheHelper::staticHostPort3);
      currDir += tmp;  // currDir += "3";
      break;
    default: /* Support for any number of servers Max 10*/
      // ASSERT( ( instance <= 10 )," More than 10 servers not supported");
      // TODO: need to support more then three servers
      ASSERT((instance <= 4), " More than 4 servers not supported");
      char temp[8];
      currDir += ACE_OS::itoa(CacheHelper::staticHostPort4, temp, 10);
      break;
  }

  sprintf(cmd, "%s/bin/%s stop server --dir=%s 2>&1", gfjavaenv, GFSH,
          currDir.c_str());

  LOG(cmd);
  ACE_OS::system(cmd);

  terminate_process_file(currDir + "/vf.gf.server.pid",
                         std::chrono::seconds(10));

  staticServerInstanceList.remove(instance);
}
// closing locator
void CacheHelper::closeLocator(int instance, bool ssl) {
  static char* gfjavaenv = ACE_OS::getenv("GFJAVA");

  char cmd[2048];
  char currWDPath[2048];
  int portnum = 0;
  std::string currDir = ACE_OS::getcwd(currWDPath, 2048);
  std::string keystore = std::string(ACE_OS::getenv("TESTSRC")) + "/keystore";

  ASSERT(gfjavaenv != NULL,
         "Environment variable GFJAVA for java build directory is not set.");
  ASSERT(!currDir.empty(),
         "Current working directory could not be determined.");
  if ((ACE_OS::strchr(gfjavaenv, '\\') == NULL) &&
      (ACE_OS::strchr(gfjavaenv, '/') == NULL)) {
    return;
  }

  currDir += PATH_SEP;
  currDir += "GFELOC";
  char tmp[100];

  switch (instance) {
    case 1:
      // portnum = 34756;
      portnum = CacheHelper::staticLocatorHostPort1;
      sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort1);
      currDir += tmp;  // currDir += "1";
      break;
    case 2:
      // portnum = 34757;
      portnum = CacheHelper::staticLocatorHostPort2;
      sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort2);
      currDir += tmp;
      // currDir += "2";
      break;
    case 3:
      // portnum = 34758;
      portnum = CacheHelper::staticLocatorHostPort3;
      sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort3);
      currDir += tmp;
      // currDir += "3";
      break;
    default: /* Support for any number of Locator Max 10*/
      // TODO://
      ASSERT((instance <= 3), " More than 3 servers not supported");
      char temp[8];
      currDir += ACE_OS::itoa(instance, temp, 10);
      break;
  }

  sprintf(cmd, "%s/bin/%s stop locator --dir=%s", gfjavaenv, GFSH,
          currDir.c_str());
  LOG(cmd);
  ACE_OS::system(cmd);

  terminate_process_file(currDir + "/vf.gf.locator.pid",
                         std::chrono::seconds(10));

  sprintf(cmd, "%s .%stest.gemfire.properties", DELETE_COMMAND, PATH_SEP);
  LOG(cmd);
  ACE_OS::system(cmd);

  staticLocatorInstanceList.remove(instance);
}

template <class Rep, class Period>
void CacheHelper::terminate_process_file(
    const std::string& pidFileName,
    const std::chrono::duration<Rep, Period>& duration) {
  auto timeout = std::chrono::system_clock::now() + duration;

  std::string pid;
  read_single_line(pidFileName, pid);

  if (!pid.empty()) {
    LOG("CacheHelper::terminate_process_file: process running. pidFileName=" +
        pidFileName + ", pid=" + pid);

    // Wait for process to terminate or timeout
    auto start = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < timeout) {
      if (!file_exists(pidFileName)) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - start);
        LOG("CacheHelper::terminate_process_file: process exited. "
            "pidFileName=" +
            pidFileName + ", pid=" + pid + ", elapsed=" +
            std::to_string(elapsed.count()) + "ms");
        return;
      }
      std::this_thread::yield();
    }
    LOG("CacheHelper::terminate_process_file: timeout. pidFileName=" +
        pidFileName + ", pid=" + pid);

    // Didn't exit on its own, kill it.
    LOG("ACE::terminate_process: pid=" + pid);
    ACE::terminate_process(std::stoi(pid));
  }
}

bool CacheHelper::file_exists(const std::string& fileName) {
  std::ifstream file(fileName);
  return file.is_open();
}

void CacheHelper::read_single_line(const std::string& fileName,
                                   std::string& str) {
  std::ifstream f(fileName);
  std::getline(f, str);
}

void CacheHelper::cleanupTmpConfigFiles() {
  std::list<std::string>::const_iterator its;

  char cmd[1024];
  for (its = CacheHelper::staticConfigFileList.begin();
       its != CacheHelper::staticConfigFileList.end(); ++its) {
    try {
      sprintf(cmd, "rm %s", its->c_str());
      LOG(cmd);
      ACE_OS::system(cmd);
    } catch (...) {
    }
  }
}

void CacheHelper::cleanupLocatorInstances() {
  CacheHelper::cleanupTmpConfigFiles();
  if (staticLocatorInstanceList.size() > 0) {
    while (staticLocatorInstanceList.size() > 0) {
      int instance = staticLocatorInstanceList.front();

      staticLocatorInstanceList.remove(instance);  // for safety
      closeLocator(instance);                      // this will also remove
    }
  }
}

// starting locator
void CacheHelper::initLocator(int instance, bool ssl, bool multiDS, int dsId,
                              int remoteLocator) {
  if (!isLocatorCleanupCallbackRegistered &&
      gClientCleanup.registerCallback(&CacheHelper::cleanupLocatorInstances)) {
    isLocatorCleanupCallbackRegistered = true;
  }
  static char* gfjavaenv = ACE_OS::getenv("GFJAVA");

  char cmd[2048];
  char currWDPath[2048];
  std::string currDir = ACE_OS::getcwd(currWDPath, 2048);
  //    std::string keystore = std::string(ACE_OS::getenv("TESTSRC")) +
  //    "/keystore";

  ASSERT(gfjavaenv != NULL,
         "Environment variable GFJAVA for java build directory is not set.");
  ASSERT(!currDir.empty(),
         "Current working directory could not be determined.");

  if ((ACE_OS::strchr(gfjavaenv, '\\') == NULL) &&
      (ACE_OS::strchr(gfjavaenv, '/') == NULL)) {
    return;
  }
  std::string locDirname = "GFELOC";
  int portnum = 0;
  currDir += PATH_SEP;
  char tmp[100];
  switch (instance) {
    case 1:
      // portnum = 34756;
      portnum = CacheHelper::staticLocatorHostPort1;
      sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort1);
      locDirname += tmp;
      // locDirname += "1";
      break;
    case 2:
      // portnum = 34757;
      portnum = CacheHelper::staticLocatorHostPort2;
      sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort2);
      locDirname += tmp;
      // locDirname += "2";
      break;
    default:
      // portnum = 34758;
      portnum = CacheHelper::staticLocatorHostPort3;
      sprintf(tmp, "%d", CacheHelper::staticLocatorHostPort3);
      locDirname += tmp;
      // locDirname += "3";
      break;
  }

  currDir += locDirname;

  ACE_OS::mkdir(locDirname.c_str());

  std::string gemfireFile =
      generateGemfireProperties(currDir, ssl, dsId, remoteLocator);

  sprintf(cmd, "%s/bin/%s stop locator --dir=%s --properties-file=%s ",
          gfjavaenv, GFSH, currDir.c_str(), gemfireFile.c_str());

  LOG(cmd);
  ACE_OS::system(cmd);

  sprintf(cmd,
          "%s/bin/%s start locator --name=%s --port=%d --dir=%s "
          "--properties-file=%s ",
          gfjavaenv, GFSH, locDirname.c_str(), portnum, currDir.c_str(),
          gemfireFile.c_str());

  LOG(cmd);
  ACE_OS::system(cmd);
  staticLocatorInstanceList.push_back(instance);
}

void CacheHelper::clearSecProp() {
  PropertiesPtr tmpSecProp =
      DistributedSystem::getSystemProperties()->getSecurityProperties();
  tmpSecProp->remove("security-username");
  tmpSecProp->remove("security-password");
}
void CacheHelper::setJavaConnectionPoolSize(long size) {
  DistributedSystem::getSystemProperties()->setjavaConnectionPoolSize(size);
}

bool CacheHelper::setSeed() {
  char* testName = ACE_OS::getenv("TESTNAME");

  int seed = hashcode(testName);

  printf("seed for process %d\n", seed);
  // The integration tests rely on the pseudo-random
  // number generator being seeded with a very particular
  // value specific to the test by way of the test name.
  // Whilst this approach is pessimal, it can not be
  // remedied as the test depend upon it.
  ACE_OS::srand(seed);
  return true;
}

int CacheHelper::hashcode(char* str) {
  if (str == NULL) {
    return 0;
  }
  int localHash = 0;

  int prime = 31;
  char* data = str;
  for (int i = 0; i < 50 && (data[i] != '\0'); i++) {
    localHash = prime * localHash + data[i];
  }
  if (localHash > 0) return localHash;
  return -1 * localHash;
}

int CacheHelper::getRandomNumber() {
  // char * testName = ACE_OS::getenv( "TESTNAME" );

  // int seed = hashcode(testName);
  return (ACE_OS::rand() % RANDOM_NUMBER_DIVIDER) + RANDOM_NUMBER_OFFSET;
}

int CacheHelper::getRandomAvailablePort() {
  while (true) {
    int port = CacheHelper::getRandomNumber();
    ACE_INET_Addr addr(port, "localhost");
    ACE_SOCK_Acceptor acceptor;
    int result = acceptor.open(addr, 0, AF_INET);
    if (result == -1) {
      continue;
    } else {
      result = acceptor.close();
      if (result == -1) {
        continue;
      } else {
        return port;
      }
    }
  }
}

PoolPtr CacheHelper::getPoolPtr(const char* poolName) {
  return PoolManager::find(poolName);
}

std::string CacheHelper::unitTestOutputFile() {
  char currWDPath[512];
  char* wdPath ATTR_UNUSED = ACE_OS::getcwd(currWDPath, 512);

  char* testName = ACE_OS::getenv("TESTNAME");
  strcat(currWDPath, "/");
  strcat(currWDPath, testName);
  strcat(currWDPath, ".log");

  std::string str(currWDPath);
  return str;
}

int CacheHelper::getNumLocatorListUpdates(const char* s) {
  std::string searchStr(s);
  std::string testFile = CacheHelper::unitTestOutputFile();
  FILE* fp = fopen(testFile.c_str(), "r");
  ASSERT(NULL != fp, "Failed to open log file.");

  char buf[512];
  int numMatched = 0;
  while (fgets(buf, sizeof(buf), fp)) {
    std::string line(buf);
    if (line.find(searchStr) != std::string::npos) numMatched++;
  }
  return numMatched;
}

std::string CacheHelper::generateGemfireProperties(const std::string& path,
                                                   const bool ssl,
                                                   const int dsId,
                                                   const int remoteLocator) {
  char cmd[2048];
  std::string keystore = std::string(ACE_OS::getenv("TESTSRC")) + "/keystore";

  std::string gemfireFile = path;
  gemfireFile += "/test.gemfire.properties";
  sprintf(cmd, "%s %s%stest.gemfire.properties", DELETE_COMMAND, path.c_str(),
          PATH_SEP);
  LOG(cmd);
  ACE_OS::system(cmd);
  FILE* urandom = /*ACE_OS::*/
      fopen(gemfireFile.c_str(), "w");
  char gemStr[258];
  sprintf(gemStr, "locators=localhost[%d],localhost[%d],localhost[%d]\n",
          CacheHelper::staticLocatorHostPort1,
          CacheHelper::staticLocatorHostPort2,
          CacheHelper::staticLocatorHostPort3);
  std::string msg = gemStr;

  msg += "log-level=config\n";
  msg += "mcast-port=0\n";
  msg += "enable-network-partition-detection=false\n";

  if (ssl) {
    msg += "jmx-manager-ssl-enabled=false\n";
    msg += "cluster-ssl-enabled=true\n";
    msg += "cluster-ssl-require-authentication=true\n";
    msg += "cluster-ssl-ciphers=SSL_RSA_WITH_NULL_MD5\n";
    msg += "cluster-ssl-keystore-type=jks\n";
    msg += "cluster-ssl-keystore=" + keystore + "/server_keystore.jks\n";
    msg += "cluster-ssl-keystore-password=gemstone\n";
    msg += "cluster-ssl-truststore=" + keystore + "/server_truststore.jks\n";
    msg += "cluster-ssl-truststore-password=gemstone\n";
    msg += "security-username=xxxx\n";
    msg += "security-userPassword=yyyy \n";
  }
  if (remoteLocator != 0) {
    sprintf(gemStr, "distributed-system-id=%d\n remote-locators=localhost[%d]",
            dsId, remoteLocator);
  } else {
    sprintf(gemStr, "distributed-system-id=%d\n ", dsId);
  }
  msg += gemStr;

  /*ACE_OS::*/
  fwrite(msg.c_str(), msg.size(), 1, urandom);
  /*ACE_OS::*/
  fflush(urandom);
  /*ACE_OS::*/
  fclose(urandom);
  LOG(gemfireFile.c_str());
  return gemfireFile;
}
