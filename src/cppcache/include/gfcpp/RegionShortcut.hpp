#pragma once

#ifndef GEODE_GFCPP_REGIONSHORTCUT_H_
#define GEODE_GFCPP_REGIONSHORTCUT_H_

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

/**
 * Each enum represents a predefined {@link RegionAttributes} in a {@link
 * Cache}.
 * These enum values can be used to create regions using a {@link RegionFactory}
 * obtained by calling {@link Cache#createRegionFactory(RegionShortcut)}.
 * <p>Another way to use predefined region attributes is in cache.xml by setting
 * the refid attribute on a region element or region-attributes element to the
 * string of each value.
 */
namespace apache {
namespace geode {
namespace client {
enum RegionShortcut {

  /**
   * A PROXY region has no local state and forwards all operations to a server.
   */
  PROXY,

  /**
   * A CACHING_PROXY region has local state but can also send operations to a
   * server.
   * If the local state is not found then the operation is sent to the server
   * and the local state is updated to contain the server result.
   */
  CACHING_PROXY,

  /**
   * A CACHING_PROXY_LRU region has local state but can also send operations to
   * a server.
   * If the local state is not found then the operation is sent to the server
   * and the local state is updated to contain the server result.
   * It will also destroy entries once it detects that the number of enteries
   * crossing default limit of #100000.
   */
  CACHING_PROXY_ENTRY_LRU,

  /**
   * A LOCAL region only has local state and never sends operations to a server.
   */
  LOCAL,

  /**
   * A LOCAL_LRU region only has local state and never sends operations to a
   * server.
   * It will also destroy entries once it detects that the number of enteries
   * crossing default limit of #100000.
   */
  LOCAL_ENTRY_LRU
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_REGIONSHORTCUT_H_
