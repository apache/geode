#pragma once

#ifndef GEODE_POOLXMLCREATION_H_
#define GEODE_POOLXMLCREATION_H_

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

#include <string>
#include <vector>
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/Pool.hpp>
#include "PoolXmlCreation.hpp"
#include <gfcpp/PoolFactory.hpp>

namespace apache {
namespace geode {
namespace client {
class Cache;

/**
 * Represents a {@link Pool} that is created declaratively.
 *
 * @since 3.0
 */
class CPPCACHE_EXPORT PoolXmlCreation {
 private:
  /** An <code>AttributesFactory</code> for creating default
    * <code>PoolAttribute</code>s */
  PoolFactoryPtr poolFactory;

  /** The name of this pool */
  std::string poolName;

  /*
  std::vector<std::string> locatorhosts;
  std::vector<std::string> locatorports;
  std::vector<std::string> serverhosts;
  std::vector<std::string> serverports;
  */

 public:
  ~PoolXmlCreation();
  /**
   * Creates a new <code>PoolXmlCreation</code> with the given pool name.
   */
  PoolXmlCreation(const char* name, PoolFactoryPtr factory);

  /** Add a locator */
  // void addLocator(const char * host, const char * port);

  /** Add a server */
  // void addServer(const char * host, const char * port);

  /**
   * Creates a {@link Pool} using the
   * description provided by this <code>PoolXmlCreation</code>.
   *
   * @throws OutOfMemoryException if the memory allocation failed
   * @throws NotConnectedException if the cache is not connected
   * @throws InvalidArgumentException if the attributePtr is NULL.
   * or if PoolAttributes is null or if poolName is null or
   * the empty string
   * @throws PoolExistsException
   * @throws CacheClosedException if the cache is closed
   *         at the time of region creation
   * @throws UnknownException otherwise
   *
   */
  PoolPtr create();
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_POOLXMLCREATION_H_
