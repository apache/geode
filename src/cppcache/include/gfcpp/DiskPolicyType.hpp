#pragma once

#ifndef GEODE_GFCPP_DISKPOLICYTYPE_H_
#define GEODE_GFCPP_DISKPOLICYTYPE_H_

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

namespace apache {
namespace geode {
namespace client {
/**
 * @class DiskPolicyType DiskPolicyType.hpp
 * Enumerated type for disk policy.
 * @see RegionAttributes::getDiskPolicy
 * @see AttributesFactory::setDiskPolicy
 */
class CPPCACHE_EXPORT DiskPolicyType {
  // public static methods
 public:
  /**
   * Values for setting PolicyType.
   */
  typedef enum { NONE = 0, OVERFLOWS, PERSIST } PolicyType;

  /** Returns the Name of the Lru action represented by specified ordinal. */
  static const char* fromOrdinal(const uint8_t ordinal);

  /** Returns the type of the Lru action represented by name. */
  static PolicyType fromName(const char* name);

  /** Returns whether this is one of the overflow to disk type.
   * @return true if this is any action other than NONE
   */
  inline static bool isOverflow(const PolicyType type) {
    return (type == DiskPolicyType::OVERFLOWS);
  }

  /** Return whether this is <code>NONE</code>. */
  inline static bool isNone(const PolicyType type) {
    return (type == DiskPolicyType::NONE);
  }
  /** Return whether this is <code>persist</code>. */
  inline static bool isPersist(const PolicyType type) {
    return (type == DiskPolicyType::PERSIST);
  }

 private:
  /** No instance allowed. */
  DiskPolicyType(){};
  static const char* names[];
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_DISKPOLICYTYPE_H_
