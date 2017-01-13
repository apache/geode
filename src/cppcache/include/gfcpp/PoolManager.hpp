#ifndef __GEMFIRE_POOL_MANAGER_HPP__
#define __GEMFIRE_POOL_MANAGER_HPP__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "Pool.hpp"
#include "PoolFactory.hpp"
#include "Region.hpp"

namespace gemfire {

typedef HashMapT<CacheableStringPtr, PoolPtr> HashMapOfPools;

/**
 * Manages creation and access to {@link Pool connection pools} for clients.
 * <p>
 * To create a pool get a factory by calling {@link #createFactory}.
 * <p>
 * To find an existing pool by name call {@link #find}.
 * <p>
 * To get rid of all created pools call {@link #close}.
 *
 *
 */
class CPPCACHE_EXPORT PoolManager {
 public:
  /**
   * Creates a new {@link PoolFactory pool factory},
   * which is used to configure and create new {@link Pool}s.
   * @return the new pool factory
   */
  static PoolFactoryPtr createFactory();

  /**
   * Returns a map containing all the pools in this manager.
   * The keys are pool names
   * and the values are {@link Pool} instances.
   * <p> The map contains the pools that this manager knows of at the time of
   * this call.
   * The map is free to be changed without affecting this manager.
   * @return a Map that is a snapshot of all the pools currently known to this
   * manager.
   */
  static const HashMapOfPools& getAll();

  /**
   * Find by name an existing connection pool returning
   * the existing pool or <code>NULLPTR</code> if it does not exist.
   * @param name is the name of the connection pool
   * @return the existing connection pool or <code>NULLPTR</code> if it does not
   * exist.
   */
  static PoolPtr find(const char* name);

  /**
   * Find the pool used by the given region.
   * @param region is the region that is using the pool.
   * @return the pool used by that region or <code> NULLPTR </code> if the
   * region does
   * not have a pool.
   */
  static PoolPtr find(RegionPtr region);

  /**
   * Unconditionally destroys all created pools that are in this manager.
   * @param keepAlive defines whether the server should keep the durable
   * client's subscriptions alive for the <code>durable-client-timeout</code>.
   * @see DistributedSystem#connect for a description of
   * <code>durable-client-timeout</code>.
   */
  static void close(bool keepAlive = false);

 private:
  PoolManager();
};

};      // Namespace gemfire
#endif  //__GEMFIRE_POOL_MANAGER_HPP__
