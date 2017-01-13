#ifndef __GEMFIRE_REGIONATTRIBUTES_H__
#define __GEMFIRE_REGIONATTRIBUTES_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "CacheLoader.hpp"
#include "ExpirationAttributes.hpp"
#include "CacheWriter.hpp"
#include "CacheListener.hpp"
#include "PartitionResolver.hpp"
#include "Properties.hpp"
#include "Serializable.hpp"
#include "DiskPolicyType.hpp"
#include "PersistenceManager.hpp"

namespace gemfire {
class CacheImpl;

/**
 * @class RegionAttributes RegionAttributes.hpp
 * Defines attributes for configuring a region.
 *
 * These are
 * <code>CacheListener</code>, <code>CacheLoader</code>,
 * <code>CacheWriter</code>,
 * scope expiration attributes
 * for the region itself; expiration attributes for the region entries;
 * and whether statistics are enabled for the region and its entries.
 *
 * To create an instance of this interface, use {@link
 * AttributesFactory::createRegionAttributes}.
 *
 * For compatibility rules and default values, see {@link AttributesFactory}.
 *
 * <p>Note that the <code>RegionAttributes</code> are not distributed with the
 * region.
 *
 * @see AttributesFactory
 * @see AttributesMutator
 * @see Region::getAttributes
 */
class AttributesFactory;
class AttributesMutator;
class Cache;
class Region;

class CPPCACHE_EXPORT RegionAttributes : public Serializable {
  /**
   * @brief public static methods
   */
 public:
  /** Gets the cache loader for the region.
   * @return  a pointer that points to the region's ,
   * <code>CacheLoader</code> , NULLPTR if there is no CacheLoader for this
   * region.
   */
  CacheLoaderPtr getCacheLoader();

  /** Gets the cache writer for the region.
   * @return  a pointer that points to the region's ,
   * <code>CacheWriter</code> , NULLPTR if there is no CacheWriter for this
   * region
   */
  CacheWriterPtr getCacheWriter();

  /** Gets the cache listener for the region.
   * @return  a pointer that points to the region's ,
   * <code>CacheListener</code> , NULLPTR if there is no CacheListener defined
   * for this region.
   */
  CacheListenerPtr getCacheListener();

  /** Gets the partition resolver for the partition region.
  * @return  a pointer that points to the region's ,
  * <code>PartitionResolver</code> , NULLPTR if there is no PartitionResolver
  * defined
  * for this region.
  */
  PartitionResolverPtr getPartitionResolver();

  /** Gets the <code>timeToLive</code> expiration attributes for the region as a
   * whole.
   * @return the timeToLive expiration attributes for this region
   */
  int getRegionTimeToLive();
  ExpirationAction::Action getRegionTimeToLiveAction();

  /** Gets the idleTimeout expiration attributes for the region as a whole.
   *
   * @return the IdleTimeout expiration attributes for this region
   */
  int getRegionIdleTimeout();
  ExpirationAction::Action getRegionIdleTimeoutAction();

  /** Gets the <code>timeToLive</code> expiration attributes for entries in this
   * region.
   * @return the timeToLive expiration attributes for entries in this region
   */
  int getEntryTimeToLive();
  ExpirationAction::Action getEntryTimeToLiveAction();

  /** Gets the <code>idleTimeout</code> expiration attributes for entries in
   * this region.
   * @return the idleTimeout expiration attributes for entries in this region
   */
  int getEntryIdleTimeout();
  ExpirationAction::Action getEntryIdleTimeoutAction();

  /**
   * If true, this region will store data in the current process.
   * @return true or false, indicating cachingEnabled state.
   */
  inline bool getCachingEnabled() const { return m_caching; }

  // MAP ATTRIBUTES

  /** Returns the initial capacity of the entry's local cache.
   * @return the initial capacity of the entry's local cache
   */
  int getInitialCapacity() const;

  /** Returns the load factor of the entry's local cache.
   * @return the load factor of the entry's local cache
   */
  float getLoadFactor() const;

  /** Returns the concurrencyLevel of the entry's local cache.
   * @return the concurrencyLevel
   * @see AttributesFactory
   */
  uint8_t getConcurrencyLevel() const;

  /**
   * Returns the maximum number of entries this cache will hold before
   * using LRU eviction. A return value of zero, 0, indicates no limit.
   */
  uint32_t getLruEntriesLimit() const;

  /** Returns the disk policy type of the region.
   *
   * @return the <code>DiskPolicyType::PolicyType</code>, default is
   * DiskPolicyType::NONE.
   */
  DiskPolicyType::PolicyType getDiskPolicy() const;

  /**
   * Returns the ExpirationAction used for LRU Eviction, default is
   * LOCAL_DESTROY.
   */
  const ExpirationAction::Action getLruEvictionAction() const;

  /**
   * Returns the name of the pool attached to the region.
   */
  const char* getPoolName() const;

  /*destructor
   *
   */
  virtual ~RegionAttributes();

  /** Serialize out to stream */
  virtual void toData(DataOutput& out) const;

  /** Initialize members from serialized data. */
  virtual Serializable* fromData(DataInput& in);

  /** Return an empty instance for deserialization. */
  static Serializable* createDeserializable();

  /** Return class id for serialization. */
  virtual int32_t classId() const;

  /** Return type id for serialization. */
  virtual int8_t typeId() const;

  // return zero deliberately
  virtual uint32_t objectSize() const { return 0; }

  /**
   * This method returns the path of the library from which
   * the factory function will be invoked on a cache server.
   */
  const char* getCacheLoaderLibrary();

  /**
   * This method returns the symbol name of the factory function from which
   * the loader will be created on a cache server.
   */
  const char* getCacheLoaderFactory();

  /**
   * This method returns the path of the library from which
   * the factory function will be invoked on a cache server.
   */
  const char* getCacheListenerLibrary();

  /**
   * This method returns the symbol name of the factory function from which
   * the loader will be created on a cache server.
   */
  const char* getCacheListenerFactory();

  /**
   * This method returns the path of the library from which
   * the factory function will be invoked on a cache server.
   */
  const char* getCacheWriterLibrary();

  /**
   * This method returns the symbol name of the factory function from which
   * the loader will be created on a cache server.
   */
  const char* getCacheWriterFactory();

  /**
  * This method returns the path of the library from which
  * the factory function will be invoked on a cache server.
  */
  const char* getPartitionResolverLibrary();

  /**
   * This method returns the symbol name of the factory function from which
   * the loader will be created on a cache server.
   */
  const char* getPartitionResolverFactory();

  /** Return true if all the attributes are equal to those of other. */
  bool operator==(const RegionAttributes& other) const;

  /** Return true if any of the attributes are not equal to those of other. */
  bool operator!=(const RegionAttributes& other) const;

  /** throws IllegalStateException if the attributes are not suited for
   * serialization
   * such as those that have a cache callback (listener, loader, or writer) set
   * directly instead of through the string value setters.
   */
  void validateSerializableAttributes();

  /**
   * This method returns the list of servername:portno separated by comma
   */
  const char* getEndpoints();

  /**
   * This method returns the setting of client notification
   */
  bool getClientNotificationEnabled() const;

  /**
   * This method returns the path of the library from which
   * the factory function will be invoked on a cache server.
   */
  const char* getPersistenceLibrary();

  /**
   * This method returns the symbol name of the factory function from which
   * the persistence will be created on a cache server.
   */
  const char* getPersistenceFactory();

  /**
   * This method returns the properties pointer which is set for persistence.
   */
  PropertiesPtr getPersistenceProperties();

  /** Gets the persistence for the region.
   * @return  a pointer that points to the region's ,
   * <code>PersistenceManager</code> , NULLPTR if there is no PersistenceManager
   * for this
   * region.
   */
  PersistenceManagerPtr getPersistenceManager();

  /** TODO
    * Returns the name of the {@link Pool} that this region
    * will use to communicate with servers, if any.
    * @return the name of the client-server {@link Pool}
    */
  const char* getPoolName() { return m_poolName; }
  bool getCloningEnabled() { return m_isClonable; }

  /**
  * Returns true if concurrent update checks are turned on for this region.
  * <p>
  * @return true if concurrent update checks are turned on
  */
  bool getConcurrencyChecksEnabled() { return m_isConcurrencyChecksEnabled; }

 private:
  // Helper function that safely compares two attribute string
  // taking into consideration the fact the one or the other
  // might be NULL
  static int32_t compareStringAttribute(char* attributeA, char* attributeB);

  // Helper function that safely copies one string attribute to
  // another.
  static void copyStringAttribute(char*& lhs, const char* rhs);

  void setCacheListener(const char* libpath, const char* factoryFuncName);
  void setCacheLoader(const char* libpath, const char* factoryFuncName);
  void setCacheWriter(const char* libpath, const char* factoryFuncName);
  void setPartitionResolver(const char* libpath, const char* factoryFuncName);
  void setPersistenceManager(const char* lib, const char* func,
                             const PropertiesPtr& config);
  void setEndpoints(const char* endpoints);
  void setPoolName(const char* poolName);
  void setCloningEnabled(bool isClonable);
  void setCachingEnabled(bool enable);
  void setLruEntriesLimit(int limit);
  void setDiskPolicy(DiskPolicyType::PolicyType diskPolicy);
  void setConcurrencyChecksEnabled(bool enable);
  inline bool getEntryExpiryEnabled() const {
    return (m_entryTimeToLive != 0 || m_entryIdleTimeout != 0);
  }

  inline bool getRegionExpiryEnabled() const {
    return (m_regionTimeToLive != 0 || m_regionIdleTimeout != 0);
  }

  // will be created by the factory
  RegionAttributes(const RegionAttributes& rhs);
  RegionAttributes();

  ExpirationAction::Action m_regionTimeToLiveExpirationAction;
  ExpirationAction::Action m_regionIdleTimeoutExpirationAction;
  ExpirationAction::Action m_entryTimeToLiveExpirationAction;
  ExpirationAction::Action m_entryIdleTimeoutExpirationAction;
  ExpirationAction::Action m_lruEvictionAction;
  CacheWriterPtr m_cacheWriter;
  CacheLoaderPtr m_cacheLoader;
  CacheListenerPtr m_cacheListener;
  PartitionResolverPtr m_partitionResolver;
  uint32_t m_lruEntriesLimit;
  bool m_caching;
  uint32_t m_maxValueDistLimit;
  uint32_t m_entryIdleTimeout;
  uint32_t m_entryTimeToLive;
  uint32_t m_regionIdleTimeout;
  uint32_t m_regionTimeToLive;
  uint32_t m_initialCapacity;
  float m_loadFactor;
  uint8_t m_concurrencyLevel;
  char* m_cacheLoaderLibrary;
  char* m_cacheWriterLibrary;
  char* m_cacheListenerLibrary;
  char* m_partitionResolverLibrary;
  char* m_cacheLoaderFactory;
  char* m_cacheWriterFactory;
  char* m_cacheListenerFactory;
  char* m_partitionResolverFactory;
  DiskPolicyType::PolicyType m_diskPolicy;
  char* m_endpoints;
  bool m_clientNotificationEnabled;
  char* m_persistenceLibrary;
  char* m_persistenceFactory;
  PropertiesPtr m_persistenceProperties;
  PersistenceManagerPtr m_persistenceManager;
  char* m_poolName;
  bool m_isClonable;
  bool m_isConcurrencyChecksEnabled;
  friend class AttributesFactory;
  friend class AttributesMutator;
  friend class Cache;
  friend class CacheImpl;
  friend class Region;
  friend class RegionInternal;
  friend class RegionXmlCreation;

 private:
  const RegionAttributes& operator=(const RegionAttributes&);
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_REGIONATTRIBUTES_H__
