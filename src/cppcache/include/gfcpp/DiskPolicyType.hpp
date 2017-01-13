#ifndef __GEMFIRE_DISKPOLICYTYPE_H__
#define __GEMFIRE_DISKPOLICYTYPE_H__
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

namespace gemfire {
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
}  // namespace gemfire
#endif  // ifndef __GEMFIRE_DISKPOLICYTYPE_H__
