#pragma once

#ifndef GEODE_GFCPP_CACHEATTRIBUTES_H_
#define GEODE_GFCPP_CACHEATTRIBUTES_H_

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

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

namespace apache {
namespace geode {
namespace client {

/**
 * @class CacheAttributes CacheAttributes.hpp
 * Defines attributes for configuring a cache.
 * Currently the following attributes are defined:
 * redundancyLevel: Redundancy for HA client queues.
 * endpoints: Cache level endpoints list.
 *
 * To create an instance of this interface, use {@link
 * CacheAttributesFactory::createCacheAttributes}.
 *
 * For compatibility rules and default values, see {@link
 * CacheAttributesFactory}.
 *
 * <p>Note that the <code>CacheAttributes</code> are not distributed with the
 * region.
 *
 * @see CacheAttributesFactory
 */
class CacheAttributesFactory;

_GF_PTR_DEF_(CacheAttributes, CacheAttributesPtr);

class CPPCACHE_EXPORT CacheAttributes : public SharedBase {
  /**
   * @brief public static methods
   */
 public:
  /**
   * Gets redundancy level for regions in the cache.
   */
  int getRedundancyLevel();

  /**
   * Gets cache level endpoints list.
   */
  char* getEndpoints();

  ~CacheAttributes();

  bool operator==(const CacheAttributes& other) const;

  bool operator!=(const CacheAttributes& other) const;

 private:
  /** Sets redundancy level.
   *
   */
  void setRedundancyLevel(int redundancyLevel);

  /** Sets cache level endpoints list.
   *
   */
  void setEndpoints(char* endpoints);
  // will be created by the factory

  CacheAttributes(const CacheAttributes& rhs);
  CacheAttributes();

  int32_t compareStringAttribute(char* attributeA, char* attributeB) const;
  void copyStringAttribute(char*& lhs, const char* rhs);

  int m_redundancyLevel;
  char* m_endpoints;
  bool m_cacheMode;

  friend class CacheAttributesFactory;
  friend class CacheImpl;

  const CacheAttributes& operator=(const CacheAttributes&);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CACHEATTRIBUTES_H_
