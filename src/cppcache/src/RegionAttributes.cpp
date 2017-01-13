/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/Cache.hpp>
#include <Utils.hpp>
#include <gfcpp/DataOutput.hpp>
#include <string.h>
#include <stdlib.h>
#include <gfcpp/GemfireTypeIds.hpp>
#include <CacheXmlParser.hpp>
#include <ace/DLL.h>
#include <ace/OS.h>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/Properties.hpp>

using namespace gemfire;
RegionAttributes::RegionAttributes()
    : Serializable(),
      m_regionTimeToLiveExpirationAction(ExpirationAction::INVALIDATE),
      m_regionIdleTimeoutExpirationAction(ExpirationAction::INVALIDATE),
      m_entryTimeToLiveExpirationAction(ExpirationAction::INVALIDATE),
      m_entryIdleTimeoutExpirationAction(ExpirationAction::INVALIDATE),
      m_lruEvictionAction(ExpirationAction::LOCAL_DESTROY),
      m_cacheWriter(NULLPTR),
      m_cacheLoader(NULLPTR),
      m_cacheListener(NULLPTR),
      m_partitionResolver(NULLPTR),
      m_lruEntriesLimit(0),
      m_caching(true),
      m_maxValueDistLimit(100 * 1024),
      m_entryIdleTimeout(0),
      m_entryTimeToLive(0),
      m_regionIdleTimeout(0),
      m_regionTimeToLive(0),
      m_initialCapacity(10000),
      m_loadFactor(0.75),
      m_concurrencyLevel(16),
      m_cacheLoaderLibrary(NULL),
      m_cacheWriterLibrary(NULL),
      m_cacheListenerLibrary(NULL),
      m_partitionResolverLibrary(NULL),
      m_cacheLoaderFactory(NULL),
      m_cacheWriterFactory(NULL),
      m_cacheListenerFactory(NULL),
      m_partitionResolverFactory(NULL),
      m_diskPolicy(DiskPolicyType::NONE),
      m_endpoints(NULL),
      m_clientNotificationEnabled(false),
      m_persistenceLibrary(NULL),
      m_persistenceFactory(NULL),
      m_persistenceProperties(NULLPTR),
      m_persistenceManager(NULLPTR),
      m_poolName(NULL),
      m_isClonable(false),
      m_isConcurrencyChecksEnabled(true) {}

RegionAttributes::RegionAttributes(const RegionAttributes& rhs)
    : m_regionTimeToLiveExpirationAction(
          rhs.m_regionTimeToLiveExpirationAction),
      m_regionIdleTimeoutExpirationAction(
          rhs.m_regionIdleTimeoutExpirationAction),
      m_entryTimeToLiveExpirationAction(rhs.m_entryTimeToLiveExpirationAction),
      m_entryIdleTimeoutExpirationAction(
          rhs.m_entryIdleTimeoutExpirationAction),
      m_lruEvictionAction(rhs.m_lruEvictionAction),
      m_cacheWriter(rhs.m_cacheWriter),
      m_cacheLoader(rhs.m_cacheLoader),
      m_cacheListener(rhs.m_cacheListener),
      m_partitionResolver(rhs.m_partitionResolver),
      m_lruEntriesLimit(rhs.m_lruEntriesLimit),
      m_caching(rhs.m_caching),
      m_maxValueDistLimit(rhs.m_maxValueDistLimit),
      m_entryIdleTimeout(rhs.m_entryIdleTimeout),
      m_entryTimeToLive(rhs.m_entryTimeToLive),
      m_regionIdleTimeout(rhs.m_regionIdleTimeout),
      m_regionTimeToLive(rhs.m_regionTimeToLive),
      m_initialCapacity(rhs.m_initialCapacity),
      m_loadFactor(rhs.m_loadFactor),
      m_concurrencyLevel(rhs.m_concurrencyLevel),
      m_diskPolicy(rhs.m_diskPolicy),
      m_clientNotificationEnabled(rhs.m_clientNotificationEnabled),
      m_persistenceProperties(rhs.m_persistenceProperties),
      m_persistenceManager(rhs.m_persistenceManager),
      m_isClonable(rhs.m_isClonable),
      m_isConcurrencyChecksEnabled(rhs.m_isConcurrencyChecksEnabled) {
  if (rhs.m_cacheLoaderLibrary != NULL) {
    size_t len = strlen(rhs.m_cacheLoaderLibrary) + 1;
    m_cacheLoaderLibrary = new char[len];
    ACE_OS::strncpy(m_cacheLoaderLibrary, rhs.m_cacheLoaderLibrary, len);
  } else {
    m_cacheLoaderLibrary = NULL;
  }
  if (rhs.m_cacheWriterLibrary != NULL) {
    size_t len = strlen(rhs.m_cacheWriterLibrary) + 1;
    m_cacheWriterLibrary = new char[len];
    ACE_OS::strncpy(m_cacheWriterLibrary, rhs.m_cacheWriterLibrary, len);
  } else {
    m_cacheWriterLibrary = NULL;
  }
  if (rhs.m_cacheListenerLibrary != NULL) {
    size_t len = strlen(rhs.m_cacheListenerLibrary) + 1;
    m_cacheListenerLibrary = new char[len];
    ACE_OS::strncpy(m_cacheListenerLibrary, rhs.m_cacheListenerLibrary, len);
  } else {
    m_cacheListenerLibrary = NULL;
  }
  if (rhs.m_partitionResolverLibrary != NULL) {
    size_t len = strlen(rhs.m_partitionResolverLibrary) + 1;
    m_partitionResolverLibrary = new char[len];
    ACE_OS::strncpy(m_partitionResolverLibrary, rhs.m_partitionResolverLibrary,
                    len);
  } else {
    m_partitionResolverLibrary = NULL;
  }
  if (rhs.m_cacheLoaderFactory != NULL) {
    size_t len = strlen(rhs.m_cacheLoaderFactory) + 1;
    m_cacheLoaderFactory = new char[len];
    ACE_OS::strncpy(m_cacheLoaderFactory, rhs.m_cacheLoaderFactory, len);
  } else {
    m_cacheLoaderFactory = NULL;
  }
  if (rhs.m_cacheWriterFactory != NULL) {
    size_t len = strlen(rhs.m_cacheWriterFactory) + 1;
    m_cacheWriterFactory = new char[len];
    ACE_OS::strncpy(m_cacheWriterFactory, rhs.m_cacheWriterFactory, len);
  } else {
    m_cacheWriterFactory = NULL;
  }
  if (rhs.m_cacheListenerFactory != NULL) {
    size_t len = strlen(rhs.m_cacheListenerFactory) + 1;
    m_cacheListenerFactory = new char[len];
    ACE_OS::strncpy(m_cacheListenerFactory, rhs.m_cacheListenerFactory, len);
  } else {
    m_cacheListenerFactory = NULL;
  }
  if (rhs.m_partitionResolverFactory != NULL) {
    size_t len = strlen(rhs.m_partitionResolverFactory) + 1;
    m_partitionResolverFactory = new char[len];
    ACE_OS::strncpy(m_partitionResolverFactory, rhs.m_partitionResolverFactory,
                    len);
  } else {
    m_partitionResolverFactory = NULL;
  }
  if (rhs.m_endpoints != NULL) {
    size_t len = strlen(rhs.m_endpoints) + 1;
    m_endpoints = new char[len];
    ACE_OS::strncpy(m_endpoints, rhs.m_endpoints, len);
  } else {
    m_endpoints = NULL;
  }
  if (rhs.m_poolName != NULL) {
    size_t len = strlen(rhs.m_poolName) + 1;
    m_poolName = new char[len];
    ACE_OS::strncpy(m_poolName, rhs.m_poolName, len);
  } else {
    m_poolName = NULL;
  }
  if (rhs.m_persistenceLibrary != NULL) {
    size_t len = strlen(rhs.m_persistenceLibrary) + 1;
    m_persistenceLibrary = new char[len];
    ACE_OS::strncpy(m_persistenceLibrary, rhs.m_persistenceLibrary, len);
  } else {
    m_persistenceLibrary = NULL;
  }
  if (rhs.m_persistenceFactory != NULL) {
    size_t len = strlen(rhs.m_persistenceFactory) + 1;
    m_persistenceFactory = new char[len];
    ACE_OS::strncpy(m_persistenceFactory, rhs.m_persistenceFactory, len);
  } else {
    m_persistenceFactory = NULL;
  }
}

#define RA_DELSTRING(x) \
  if (x != NULL) {      \
    delete[] x;         \
  }                     \
  x = NULL

RegionAttributes::~RegionAttributes() {
  RA_DELSTRING(m_cacheLoaderLibrary);
  RA_DELSTRING(m_cacheWriterLibrary);
  RA_DELSTRING(m_cacheListenerLibrary);
  RA_DELSTRING(m_partitionResolverLibrary);
  RA_DELSTRING(m_cacheLoaderFactory);
  RA_DELSTRING(m_cacheWriterFactory);
  RA_DELSTRING(m_cacheListenerFactory);
  RA_DELSTRING(m_partitionResolverFactory);
  RA_DELSTRING(m_endpoints);
  RA_DELSTRING(m_persistenceLibrary);
  RA_DELSTRING(m_persistenceFactory);
  RA_DELSTRING(m_poolName);
}

namespace gemfire_impl {

/**
 * lib should be in the form required by ACE_DLL, typically just like specifying
 * a
 * lib in java System.loadLibrary( "x" ); Where x is a component of the name
 * lib<x>.so on unix, or <x>.dll on windows.
 */
void* getFactoryFunc(const char* lib, const char* funcName) {
  ACE_DLL dll;
  if (dll.open(lib, ACE_DEFAULT_SHLIB_MODE, 0) == -1) {
    // error...
    char msg[1000];
    ACE_OS::snprintf(msg, 1000, "cannot open library: %s", lib);
    throw IllegalArgumentException(msg);
  }
  void* func = dll.symbol(funcName);
  if (func == NULL) {
    char msg[1000];
    ACE_OS::snprintf(msg, 1000, "cannot find factory function %s in library %s",
                     funcName, lib);
    throw IllegalArgumentException(msg);
  }
  return func;
}
}  // namespace gemfire_impl

CacheLoaderPtr RegionAttributes::getCacheLoader() {
  if ((m_cacheLoader == NULLPTR) && (m_cacheLoaderLibrary != NULL)) {
    if (CacheXmlParser::managedCacheLoaderFn != NULL &&
        strchr(m_cacheLoaderFactory, '.') != NULL) {
      // this is a managed library
      m_cacheLoader = (*CacheXmlParser::managedCacheLoaderFn)(
          m_cacheLoaderLibrary, m_cacheLoaderFactory);
    } else {
      CacheLoader* (*funcptr)();
      funcptr =
          reinterpret_cast<CacheLoader* (*)()>(gemfire_impl::getFactoryFunc(
              m_cacheLoaderLibrary, m_cacheLoaderFactory));
      m_cacheLoader = funcptr();
    }
  }
  return m_cacheLoader;
}

CacheWriterPtr RegionAttributes::getCacheWriter() {
  if ((m_cacheWriter == NULLPTR) && (m_cacheWriterLibrary != NULL)) {
    if (CacheXmlParser::managedCacheWriterFn != NULL &&
        strchr(m_cacheWriterFactory, '.') != NULL) {
      // this is a managed library
      m_cacheWriter = (*CacheXmlParser::managedCacheWriterFn)(
          m_cacheWriterLibrary, m_cacheWriterFactory);
    } else {
      CacheWriter* (*funcptr)();
      funcptr =
          reinterpret_cast<CacheWriter* (*)()>(gemfire_impl::getFactoryFunc(
              m_cacheWriterLibrary, m_cacheWriterFactory));
      m_cacheWriter = funcptr();
    }
  }
  return m_cacheWriter;
}

CacheListenerPtr RegionAttributes::getCacheListener() {
  if ((m_cacheListener == NULLPTR) && (m_cacheListenerLibrary != NULL)) {
    if (CacheXmlParser::managedCacheListenerFn != NULL &&
        strchr(m_cacheListenerFactory, '.') != NULL) {
      // LOGDEBUG( "RegionAttributes::getCacheListener: Trying to create
      // instance from managed library." );
      // this is a managed library
      m_cacheListener = (*CacheXmlParser::managedCacheListenerFn)(
          m_cacheListenerLibrary, m_cacheListenerFactory);
    } else {
      CacheListener* (*funcptr)();
      funcptr =
          reinterpret_cast<CacheListener* (*)()>(gemfire_impl::getFactoryFunc(
              m_cacheListenerLibrary, m_cacheListenerFactory));
      m_cacheListener = funcptr();
    }
  }
  return m_cacheListener;
}

PartitionResolverPtr RegionAttributes::getPartitionResolver() {
  if ((m_partitionResolver == NULLPTR) &&
      (m_partitionResolverLibrary != NULL)) {
    if (CacheXmlParser::managedPartitionResolverFn != NULL &&
        strchr(m_partitionResolverFactory, '.') != NULL) {
      // LOGDEBUG( "RegionAttributes::getCacheListener: Trying to create
      // instance from managed library." );
      // this is a managed library
      m_partitionResolver = (*CacheXmlParser::managedPartitionResolverFn)(
          m_partitionResolverLibrary, m_partitionResolverFactory);
    } else {
      PartitionResolver* (*funcptr)();
      funcptr = reinterpret_cast<PartitionResolver* (*)()>(
          gemfire_impl::getFactoryFunc(m_partitionResolverLibrary,
                                       m_partitionResolverFactory));
      m_partitionResolver = funcptr();
    }
  }
  return m_partitionResolver;
}

PersistenceManagerPtr RegionAttributes::getPersistenceManager() {
  if ((m_persistenceManager == NULLPTR) && (m_persistenceLibrary != NULL)) {
    if (CacheXmlParser::managedPartitionResolverFn != NULL &&
        strchr(m_persistenceFactory, '.') != NULL) {
      LOGDEBUG(
          "RegionAttributes::getPersistenceManager: Trying to create instance "
          "from managed library.");
      // this is a managed library
      m_persistenceManager = (*CacheXmlParser::managedPersistenceManagerFn)(
          m_persistenceLibrary, m_persistenceFactory);
    } else {
      PersistenceManager* (*funcptr)();
      funcptr = reinterpret_cast<PersistenceManager* (*)()>(
          gemfire_impl::getFactoryFunc(m_persistenceLibrary,
                                       m_persistenceFactory));
      m_persistenceManager = funcptr();
    }
  }
  return m_persistenceManager;
}
const char* RegionAttributes::getCacheLoaderFactory() {
  return m_cacheLoaderFactory;
}

const char* RegionAttributes::getCacheWriterFactory() {
  return m_cacheWriterFactory;
}

const char* RegionAttributes::getCacheListenerFactory() {
  return m_cacheListenerFactory;
}

const char* RegionAttributes::getPartitionResolverFactory() {
  return m_partitionResolverFactory;
}

const char* RegionAttributes::getPersistenceFactory() {
  return m_persistenceFactory;
}
const char* RegionAttributes::getCacheLoaderLibrary() {
  return m_cacheLoaderLibrary;
}

const char* RegionAttributes::getCacheWriterLibrary() {
  return m_cacheWriterLibrary;
}

const char* RegionAttributes::getCacheListenerLibrary() {
  return m_cacheListenerLibrary;
}

const char* RegionAttributes::getPartitionResolverLibrary() {
  return m_partitionResolverLibrary;
}

const char* RegionAttributes::getEndpoints() { return m_endpoints; }
bool RegionAttributes::getClientNotificationEnabled() const {
  return m_clientNotificationEnabled;
}
const char* RegionAttributes::getPersistenceLibrary() {
  return m_persistenceLibrary;
}

PropertiesPtr RegionAttributes::getPersistenceProperties() {
  return m_persistenceProperties;
}

int RegionAttributes::getRegionTimeToLive() { return m_regionTimeToLive; }

ExpirationAction::Action RegionAttributes::getRegionTimeToLiveAction() {
  return m_regionTimeToLiveExpirationAction;
}

int RegionAttributes::getRegionIdleTimeout() { return m_regionIdleTimeout; }

ExpirationAction::Action RegionAttributes::getRegionIdleTimeoutAction() {
  return m_regionIdleTimeoutExpirationAction;
}

int RegionAttributes::getEntryTimeToLive() { return m_entryTimeToLive; }

ExpirationAction::Action RegionAttributes::getEntryTimeToLiveAction() {
  return m_entryTimeToLiveExpirationAction;
}

int RegionAttributes::getEntryIdleTimeout() { return m_entryIdleTimeout; }

ExpirationAction::Action RegionAttributes::getEntryIdleTimeoutAction() {
  return m_entryIdleTimeoutExpirationAction;
}

int RegionAttributes::getInitialCapacity() const { return m_initialCapacity; }

float RegionAttributes::getLoadFactor() const { return m_loadFactor; }

uint8_t RegionAttributes::getConcurrencyLevel() const {
  return m_concurrencyLevel;
}

const ExpirationAction::Action RegionAttributes::getLruEvictionAction() const {
  return m_lruEvictionAction;
}

uint32_t RegionAttributes::getLruEntriesLimit() const {
  return m_lruEntriesLimit;
}

DiskPolicyType::PolicyType RegionAttributes::getDiskPolicy() const {
  return m_diskPolicy;
}
const char* RegionAttributes::getPoolName() const { return m_poolName; }
Serializable* RegionAttributes::createDeserializable() {
  return new RegionAttributes();
}

int32_t RegionAttributes::classId() const { return 0; }

int8_t RegionAttributes::typeId() const {
  return GemfireTypeIds::RegionAttributes;
}

namespace gemfire_impl {
void writeBool(DataOutput& out, bool field) {
  out.write(static_cast<int8_t>(field ? 1 : 0));
}

void readBool(DataInput& in, bool* field) {
  int8_t v = 0;
  in.read(&v);
  *field = v ? true : false;
}

void writeCharStar(DataOutput& out, const char* field) {
  if (field == NULL) {
    out.writeBytes(reinterpret_cast<const int8_t*>(""),
                   static_cast<uint32_t>(0));
  } else {
    out.writeBytes((int8_t*)field, static_cast<uint32_t>(strlen(field)) + 1);
  }
}

/** this one allocates the memory and modifies field to point to it. */
void readCharStar(DataInput& in, char** field) {
  GF_D_ASSERT(*field == NULL);
  int32_t memlen = 0;
  in.readArrayLen(&memlen);
  if (memlen != 0) {
    *field = new char[memlen];
    in.readBytesOnly(reinterpret_cast<int8_t*>(*field), memlen);
  }
}
}  // namespace gemfire_impl

void RegionAttributes::toData(DataOutput& out) const {
  out.writeInt(static_cast<int32_t>(m_regionTimeToLive));
  out.writeInt(static_cast<int32_t>(m_regionTimeToLiveExpirationAction));
  out.writeInt(static_cast<int32_t>(m_regionIdleTimeout));
  out.writeInt(static_cast<int32_t>(m_regionIdleTimeoutExpirationAction));
  out.writeInt(static_cast<int32_t>(m_entryTimeToLive));
  out.writeInt(static_cast<int32_t>(m_entryTimeToLiveExpirationAction));
  out.writeInt(static_cast<int32_t>(m_entryIdleTimeout));
  out.writeInt(static_cast<int32_t>(m_entryIdleTimeoutExpirationAction));
  out.writeInt(static_cast<int32_t>(m_initialCapacity));
  out.writeFloat(m_loadFactor);
  out.writeInt(static_cast<int32_t>(m_maxValueDistLimit));
  out.writeInt(static_cast<int32_t>(m_concurrencyLevel));
  out.writeInt(static_cast<int32_t>(m_lruEntriesLimit));
  out.writeInt(static_cast<int32_t>(m_lruEvictionAction));

  gemfire_impl::writeBool(out, m_caching);
  gemfire_impl::writeBool(out, m_clientNotificationEnabled);

  gemfire_impl::writeCharStar(out, m_cacheLoaderLibrary);
  gemfire_impl::writeCharStar(out, m_cacheLoaderFactory);
  gemfire_impl::writeCharStar(out, m_cacheWriterLibrary);
  gemfire_impl::writeCharStar(out, m_cacheWriterFactory);
  gemfire_impl::writeCharStar(out, m_cacheListenerLibrary);
  gemfire_impl::writeCharStar(out, m_cacheListenerFactory);
  gemfire_impl::writeCharStar(out, m_partitionResolverLibrary);
  gemfire_impl::writeCharStar(out, m_partitionResolverFactory);
  out.writeInt(static_cast<int32_t>(m_diskPolicy));
  gemfire_impl::writeCharStar(out, m_endpoints);
  gemfire_impl::writeCharStar(out, m_persistenceLibrary);
  gemfire_impl::writeCharStar(out, m_persistenceFactory);
  out.writeObject(m_persistenceProperties);
  gemfire_impl::writeCharStar(out, m_poolName);
  gemfire_impl::writeBool(out, m_isConcurrencyChecksEnabled);
}

Serializable* RegionAttributes::fromData(DataInput& in) {
  in.readInt(reinterpret_cast<int32_t*>(&m_regionTimeToLive));
  in.readInt(reinterpret_cast<int32_t*>(&m_regionTimeToLiveExpirationAction));
  in.readInt(reinterpret_cast<int32_t*>(&m_regionIdleTimeout));
  in.readInt(reinterpret_cast<int32_t*>(&m_regionIdleTimeoutExpirationAction));
  in.readInt(reinterpret_cast<int32_t*>(&m_entryTimeToLive));
  in.readInt(reinterpret_cast<int32_t*>(&m_entryTimeToLiveExpirationAction));
  in.readInt(reinterpret_cast<int32_t*>(&m_entryIdleTimeout));
  in.readInt(reinterpret_cast<int32_t*>(&m_entryIdleTimeoutExpirationAction));
  in.readInt(reinterpret_cast<int32_t*>(&m_initialCapacity));
  in.readFloat(&m_loadFactor);
  in.readInt(reinterpret_cast<int32_t*>(&m_maxValueDistLimit));
  in.readInt(reinterpret_cast<int32_t*>(&m_concurrencyLevel));
  in.readInt(reinterpret_cast<int32_t*>(&m_lruEntriesLimit));
  in.readInt(reinterpret_cast<int32_t*>(&m_lruEvictionAction));

  gemfire_impl::readBool(in, &m_caching);
  gemfire_impl::readBool(in, &m_clientNotificationEnabled);

  gemfire_impl::readCharStar(in, &m_cacheLoaderLibrary);
  gemfire_impl::readCharStar(in, &m_cacheLoaderFactory);
  gemfire_impl::readCharStar(in, &m_cacheWriterLibrary);
  gemfire_impl::readCharStar(in, &m_cacheWriterFactory);
  gemfire_impl::readCharStar(in, &m_cacheListenerLibrary);
  gemfire_impl::readCharStar(in, &m_cacheListenerFactory);
  gemfire_impl::readCharStar(in, &m_partitionResolverLibrary);
  gemfire_impl::readCharStar(in, &m_partitionResolverFactory);
  in.readInt(reinterpret_cast<int32_t*>(&m_diskPolicy));
  gemfire_impl::readCharStar(in, &m_endpoints);
  gemfire_impl::readCharStar(in, &m_persistenceLibrary);
  gemfire_impl::readCharStar(in, &m_persistenceFactory);
  in.readObject(m_persistenceProperties, true);
  gemfire_impl::readCharStar(in, &m_poolName);
  gemfire_impl::readBool(in, &m_isConcurrencyChecksEnabled);

  return this;
}

/** Return true if all the attributes are equal to those of other. */
bool RegionAttributes::operator==(const RegionAttributes& other) const {
  if (m_regionTimeToLive != other.m_regionTimeToLive) return false;
  if (m_regionTimeToLiveExpirationAction !=
      other.m_regionTimeToLiveExpirationAction) {
    return false;
  }
  if (m_regionIdleTimeout != other.m_regionIdleTimeout) return false;
  if (m_regionIdleTimeoutExpirationAction !=
      other.m_regionIdleTimeoutExpirationAction) {
    return false;
  }
  if (m_entryTimeToLive != other.m_entryTimeToLive) return false;
  if (m_entryTimeToLiveExpirationAction !=
      other.m_entryTimeToLiveExpirationAction) {
    return false;
  }
  if (m_entryIdleTimeout != other.m_entryIdleTimeout) return false;
  if (m_entryIdleTimeoutExpirationAction !=
      other.m_entryIdleTimeoutExpirationAction) {
    return false;
  }
  if (m_initialCapacity != other.m_initialCapacity) return false;
  if (m_loadFactor != other.m_loadFactor) return false;
  if (m_maxValueDistLimit != other.m_maxValueDistLimit) return false;
  if (m_concurrencyLevel != other.m_concurrencyLevel) return false;
  if (m_lruEntriesLimit != other.m_lruEntriesLimit) return false;
  if (m_lruEvictionAction != other.m_lruEvictionAction) return false;
  if (m_caching != other.m_caching) return false;
  if (m_clientNotificationEnabled != other.m_clientNotificationEnabled) {
    return false;
  }

  if (0 != compareStringAttribute(m_cacheLoaderLibrary,
                                  other.m_cacheLoaderLibrary)) {
    return false;
  }
  if (0 != compareStringAttribute(m_cacheLoaderFactory,
                                  other.m_cacheLoaderFactory)) {
    return false;
  }
  if (0 != compareStringAttribute(m_cacheWriterLibrary,
                                  other.m_cacheWriterLibrary)) {
    return false;
  }
  if (0 != compareStringAttribute(m_cacheWriterFactory,
                                  other.m_cacheWriterFactory)) {
    return false;
  }
  if (0 != compareStringAttribute(m_cacheListenerLibrary,
                                  other.m_cacheListenerLibrary)) {
    return false;
  }
  if (0 != compareStringAttribute(m_cacheListenerFactory,
                                  other.m_cacheListenerFactory)) {
    return false;
  }
  if (0 != compareStringAttribute(m_partitionResolverLibrary,
                                  other.m_partitionResolverLibrary)) {
    return false;
  }
  if (0 != compareStringAttribute(m_partitionResolverFactory,
                                  other.m_partitionResolverFactory)) {
    return false;
  }
  if (m_diskPolicy != other.m_diskPolicy) return false;
  if (0 != compareStringAttribute(m_endpoints, other.m_endpoints)) return false;
  if (0 != compareStringAttribute(m_persistenceLibrary,
                                  other.m_persistenceLibrary)) {
    return false;
  }
  if (0 != compareStringAttribute(m_persistenceFactory,
                                  other.m_persistenceFactory)) {
    return false;
  }
  if (m_isConcurrencyChecksEnabled != other.m_isConcurrencyChecksEnabled) {
    return false;
  }

  return true;
}

int32_t RegionAttributes::compareStringAttribute(char* attributeA,
                                                 char* attributeB) {
  if (attributeA == NULL && attributeB == NULL) {
    return 0;
  } else if (attributeA == NULL && attributeB != NULL) {
    return -1;
  } else if (attributeA != NULL && attributeB == NULL) {
    return -1;
  }
  return (strcmp(attributeA, attributeB));
}

/** Return true if any of the attributes are not equal to those of other. */
bool RegionAttributes::operator!=(const RegionAttributes& other) const {
  return !(*this == other);
}

/* Throws IllegalStateException when attributes targetted for use on a server do
 * not meet requirements. */
void RegionAttributes::validateSerializableAttributes() {
  if (m_cacheLoader != NULLPTR) {
    throw IllegalStateException(
        "CacheLoader must be set with setCacheLoader(library, factory) in "
        "members of type SERVER");
  }
  if (m_cacheWriter != NULLPTR) {
    throw IllegalStateException(
        "CacheWriter must be set with setCacheWriter(library, factory) in "
        "members of type SERVER");
  }
  if (m_cacheListener != NULLPTR) {
    throw IllegalStateException(
        "CacheListener must be set with setCacheListener(library, factory) in "
        "members of type SERVER");
  }
  if (m_partitionResolver != NULLPTR) {
    throw IllegalStateException(
        "PartitionResolver must be set with setPartitionResolver(library, "
        "factory) in members of type SERVER");
  }
  if (m_persistenceManager != NULLPTR) {
    throw IllegalStateException(
        "persistenceManager must be set with setPersistenceManager(library, "
        "factory,config) in members of type SERVER");
  }
}

void RegionAttributes::setCacheListener(const char* lib, const char* func) {
  GF_R_ASSERT(lib != NULL);
  GF_R_ASSERT(func != NULL);
  copyStringAttribute(m_cacheListenerLibrary, lib);
  copyStringAttribute(m_cacheListenerFactory, func);
}

void RegionAttributes::setPartitionResolver(const char* lib, const char* func) {
  GF_R_ASSERT(lib != NULL);
  GF_R_ASSERT(func != NULL);
  copyStringAttribute(m_partitionResolverLibrary, lib);
  copyStringAttribute(m_partitionResolverFactory, func);
}

void RegionAttributes::setCacheLoader(const char* lib, const char* func) {
  GF_R_ASSERT(lib != NULL);
  GF_R_ASSERT(func != NULL);
  copyStringAttribute(m_cacheLoaderLibrary, lib);
  copyStringAttribute(m_cacheLoaderFactory, func);
}

void RegionAttributes::setCacheWriter(const char* lib, const char* func) {
  GF_R_ASSERT(lib != NULL);
  GF_R_ASSERT(func != NULL);
  copyStringAttribute(m_cacheWriterLibrary, lib);
  copyStringAttribute(m_cacheWriterFactory, func);
}

void RegionAttributes::setPersistenceManager(const char* lib, const char* func,
                                             const PropertiesPtr& config) {
  GF_R_ASSERT(lib != NULL);
  GF_R_ASSERT(func != NULL);
  copyStringAttribute(m_persistenceLibrary, lib);
  copyStringAttribute(m_persistenceFactory, func);
  m_persistenceProperties = config;
}

void RegionAttributes::setEndpoints(const char* endpoints) {
  copyStringAttribute(m_endpoints, endpoints);
}

void RegionAttributes::setPoolName(const char* poolName) {
  copyStringAttribute(m_poolName, poolName);
}

void RegionAttributes::setCachingEnabled(bool enable) { m_caching = enable; }

void RegionAttributes::setLruEntriesLimit(int limit) {
  m_lruEntriesLimit = limit;
}
void RegionAttributes::setDiskPolicy(DiskPolicyType::PolicyType diskPolicy) {
  m_diskPolicy = diskPolicy;
}

void RegionAttributes::copyStringAttribute(char*& lhs, const char* rhs) {
  if (lhs != NULL) {
    delete[] lhs;
  }
  lhs = Utils::copyString(rhs);
}

void RegionAttributes::setCloningEnabled(bool isClonable) {
  m_isClonable = isClonable;
}

void RegionAttributes::setConcurrencyChecksEnabled(bool enable) {
  m_isConcurrencyChecksEnabled = enable;
}
