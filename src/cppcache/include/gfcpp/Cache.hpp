#ifndef __GEMFIRE_CACHE_H__
#define __GEMFIRE_CACHE_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "GemFireCache.hpp"
#include "gf_types.hpp"
#include "Region.hpp"
#include "DistributedSystem.hpp"
#include "QueryService.hpp"
#include "PoolFactory.hpp"
#include "RegionShortcut.hpp"
#include "RegionFactory.hpp"
#include "InternalCacheTransactionManager2PC.hpp"

/**
 * @file
 */

namespace gemfire {

class CacheFactory;
class CacheRegionHelper;
class Pool;

/**
 * @class Cache Cache.hpp
 *
 * Cache are obtained from create method on the {@link CacheFactory#create}
 * class
 * <p>
 * When a cache will no longer be used, it should be {@link #close closed}.
 * Once it {@link Cache::isClosed is closed} any attempt to use it
 * will cause a <code>CacheClosedException</code> to be thrown.
 *
 * <p>A cache can have multiple root regions, each with a different name.
 *
 */
class CPPCACHE_EXPORT Cache : public GemFireCache {
  /**
   * @brief public methods
   */
 public:
  /**
   * Returns the {@link RegionFactory} to create the region.
   * Before creating the Region, one can set region attributes using this
   * instance.
   *
   * @param regionShortcut
   *        To create the region specific type, @see RegionShortcut
   */
  virtual RegionFactoryPtr createRegionFactory(RegionShortcut regionShortcut);

  /**
   * Initializes the cache from an xml file
   *
   * @param cacheXml
   *        Valid cache.xml file
   */
  virtual void initializeDeclarativeCache(const char* cacheXml);

  /** Returns the name of this cache.
   * @return the string name of this cache
   */
  virtual const char* getName() const;

  /**
   * Indicates if this cache has been closed.
   * After a new cache object is created, this method returns false;
   * After the close is called on this cache object, this method
   * returns true.
   *
   * @return true, if this cache is closed; false, otherwise
   */
  virtual bool isClosed() const;

  /**
   * Returns the distributed system that this cache was
   * {@link CacheFactory::createCacheFactory created} with.
   */
  virtual DistributedSystemPtr getDistributedSystem() const;

  /**
   * Terminates this object cache and releases all the local resources.
   * After this cache is closed, any further
   * method call on this cache or any region object will throw
   * <code>CacheClosedException</code>, unless otherwise noted.
   * If Cache instance created from Pool(pool is in multiuser mode), then it
   * reset user related security data.
   * @throws CacheClosedException,  if the cache is already closed.
   */
  virtual void close();

  /**
   * Terminates this object cache and releases all the local resources.
   * After this cache is closed, any further
   * method call on this cache or any region object will throw
   * <code>CacheClosedException</code>, unless otherwise noted.
   * If Cache instance created from Pool(pool is in multiuser mode), then it
   * reset user related security data.
   * @param keepalive whether to keep a durable client's queue alive
   * @throws CacheClosedException,  if the cache is already closed.
   */
  virtual void close(bool keepalive);

  /** Look up a region with the full path from root.
   *
   * If Pool attached with Region is in multiusersecure mode then don't use
   * return instance of region as no credential are attached with this instance.
   * Get region from RegionService instance of Cache.@see
   * Cache#createAuthenticatedView(PropertiesPtr).
   *
   * @param path the region's name, such as <code>AuthRegion</code>.
   * @returns region, or NULLPTR if no such region exists.
   */
  virtual RegionPtr getRegion(const char* path);

  /**
   * Returns a set of root regions in the cache. This set is a snapshot and
   * is not backed by the Cache. The vector passed in is cleared and the
   * regions are added to it.
   *
   * @param regions the returned set of
   * regions
   */
  virtual void rootRegions(VectorOfRegion& regions);

  /**
  * Gets the QueryService from which a new Query can be obtained.
  * @returns A smart pointer to the QueryService.
  */
  virtual QueryServicePtr getQueryService();

  /**
  * Gets the QueryService from which a new Query can be obtained.
  * @param poolName
  *        Pass poolname if pool is created from cache.xml or {@link
  * PoolManager}
  * @returns A smart pointer to the QueryService.
  */
  virtual QueryServicePtr getQueryService(const char* poolName);

  /**
   * Send the "client ready" message to the server from a durable client.
   */
  virtual void readyForEvents();

  /**
   * Creates an authenticated cache using the given user security properties.
   * Multiple instances with different user properties can be created with a
   * single client cache.
   *
   * Application must use this instance to do operations, when
   * multiuser-authentication is set to true.
   *
   * @see RegionService
   * @see PoolFactory#setMultiuserAuthentication(boolean)
   * @return the {@link RegionService} instance associated with a user and given
   *         properties.
   * @throws UnsupportedOperationException
   *           when invoked with multiuser-authentication as false.
   *
   * @param userSecurityProperties
   *        the security properties of a user.
   *
   * @param poolName
   *        the pool that the users should be authenticated against. Set if
   * there are more than one Pool in Cache.
   */

  virtual RegionServicePtr createAuthenticatedView(
      PropertiesPtr userSecurityProperties, const char* poolName = NULL);

  /**
  * Get the CacheTransactionManager instance for this Cache.
  * @return The CacheTransactionManager instance.
  * @throws CacheClosedException if the cache is closed.
  */
  virtual CacheTransactionManagerPtr getCacheTransactionManager();

  /**
    * Returns whether Cache saves unread fields for Pdx types.
    */
  virtual bool getPdxIgnoreUnreadFields();

  /**
  * Returns whether { @link PdxInstance} is preferred for PDX types instead of
  * C++ object.
  */
  virtual bool getPdxReadSerialized();

  /**
   * Returns a factory that can create a {@link PdxInstance}.
   * @param className the fully qualified class name that the PdxInstance will
   * become
   * when it is fully deserialized.
   * @throws IllegalStateException if the className is NULL or invalid.
   * @return the factory
   */
  virtual PdxInstanceFactoryPtr createPdxInstanceFactory(const char* className);

  /**
    * @brief destructor
    */
  virtual ~Cache();

 private:
  /**
   * @brief constructors
   */
  Cache(const char* name, DistributedSystemPtr sys, bool ignorePdxUnreadFields,
        bool readPdxSerialized);
  Cache(const char* name, DistributedSystemPtr sys, const char* id_data,
        bool ignorePdxUnreadFields, bool readPdxSerialized);
  CacheImpl* m_cacheImpl;

 protected:
  Cache() { m_cacheImpl = NULL; }

  static bool isPoolInMultiuserMode(RegionPtr regionPtr);

  friend class CacheFactory;
  friend class CacheRegionHelper;
  friend class Pool;
  friend class FunctionService;
  friend class CacheXmlCreation;
  friend class RegionXmlCreation;
};

};      // namespace gemfire
#endif  // ifndef __GEMFIRE_CACHE_H__
