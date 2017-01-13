#ifndef __GEMFIRE_CACHEATTRIBUTES_H__
#define __GEMFIRE_CACHEATTRIBUTES_H__
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
#include "gf_types.hpp"

namespace gemfire {

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

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_CACHEATTRIBUTES_H__
