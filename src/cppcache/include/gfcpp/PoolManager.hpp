#pragma once

#ifndef GEODE_GFCPP_POOLMANAGER_H_
#define GEODE_GFCPP_POOLMANAGER_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "Pool.hpp"
#include "PoolFactory.hpp"
#include "Region.hpp"

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_POOLMANAGER_H_
