/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_RegionService_H__
#define __GEMFIRE_RegionService_H__

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "VectorT.hpp"

/**
 * @file
 */

namespace gemfire {

class Region;
class QueryService;

/**
 * A RegionService provides access to existing {@link Region regions} that exist
 * in a {@link GemFireCache GemFire cache}.
 * Regions can be obtained using {@link #getRegion}
 * and queried using {@link #getQueryService}.
 * The service should be {@link #close closed} to free up resources
 * once it is no longer needed.
 * Once it {@link #isClosed is closed} any attempt to use it or any {@link
 * Region regions}
 * obtained from it will cause a {@link CacheClosedException} to be thrown.
 * <p>
 * Instances of the interface are created using one of the following methods:
 * <ul>
 * <li> {@link CacheFactory#create()} creates a client instance of {@link
 * Cache}.
 * <li> {@link Cache#createAuthenticatedView(Properties)} creates a client
 * multiuser authenticated cache view.
 * </ul>
 * <p>
 *
 */

class CPPCACHE_EXPORT RegionService : public SharedBase {
  /**
   * @brief public methods
   */
 public:
  /**
   * Indicates if this cache has been closed.
   * After a new cache object is created, this method returns false;
   * After the close is called on this cache object, this method
   * returns true.
   *
   * @return true, if this cache is closed; false, otherwise
   */
  virtual bool isClosed() const = 0;

  /**
   * Terminates this object cache and releases all the local resources.
   * After this cache is closed, any further
   * method call on this cache or any region object will throw
   * <code>CacheClosedException</code>, unless otherwise noted.
   * If RegionService is created from {@link Cache#createAuthenticatedView" },
   * then it clears user related security data.
   * @param keepalive whether to keep a durable CQ kept alive for this user.
   * @throws CacheClosedException,  if the cache is already closed.
   */
  virtual void close() = 0;

  /** Look up a region with the name.
   *
   * @param name the region's name, such as <code>root</code>.
   * @returns region, or NULLPTR if no such region exists.
   */
  virtual RegionPtr getRegion(const char* name) = 0;

  /**
  * Gets the QueryService from which a new Query can be obtained.
  * @returns A smart pointer to the QueryService.
  */
  virtual QueryServicePtr getQueryService() = 0;

  /**
   * Returns a set of root regions in the cache. This set is a snapshot and
   * is not backed by the Cache. The vector passed in is cleared and the
   * regions are added to it.
   *
   * @param regions the returned set of
   * regions
   */
  virtual void rootRegions(VectorOfRegion& regions) = 0;

  /**
  * Returns a factory that can create a {@link PdxInstance}.
  * @param className the fully qualified class name that the PdxInstance will
  * become
  * when it is fully deserialized.
  * @return the factory
  */
  virtual PdxInstanceFactoryPtr createPdxInstanceFactory(
      const char* className) = 0;
};

};      // namespace gemfire
#endif  // ifndef __GEMFIRE_RegionService_H__
