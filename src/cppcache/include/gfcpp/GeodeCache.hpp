#pragma once

#ifndef GEODE_GFCPP_GEMFIRECACHE_H_
#define GEODE_GFCPP_GEMFIRECACHE_H_

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
#include "RegionService.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

/**
 * GeodeCache represents the singleton cache that must be created
 * in order to connect to Geode server.
 * Users must create a {@link Cache}.
 * Instances of this interface are created using one of the following methods:
 * <ul>
 * <li> {@link ClientCacheFactory#create()} creates a client instance of {@link
 * Cache}.
 * </ul>
 *
 */

class CPPCACHE_EXPORT GeodeCache : public RegionService {
  /**
   * @brief public methods
   */
 public:
  /** Returns the name of this cache.
   * @return the string name of this cache
   */
  virtual const char* getName() const = 0;

  /**
   * Initializes the cache from an xml file
   *
   * @param cacheXml
   *        Valid cache.xml file
   */
  virtual void initializeDeclarativeCache(const char* cacheXml) = 0;

  /**
  * Returns the distributed system that this cache was
  * {@link CacheFactory::createCacheFactory created} with.
  */
  virtual DistributedSystemPtr getDistributedSystem() const = 0;

  /**
   * Returns whether Cache saves unread fields for Pdx types.
   */
  virtual bool getPdxIgnoreUnreadFields() = 0;

  /**
  * Returns whether { @link PdxInstance} is preferred for PDX types instead of
  * C++ object.
  */
  virtual bool getPdxReadSerialized() = 0;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_GEMFIRECACHE_H_
