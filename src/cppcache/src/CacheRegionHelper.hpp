#ifndef __GEMFIRE_IMPL_CACHEHELPER_H__
#define __GEMFIRE_IMPL_CACHEHELPER_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *
 * The specification of function behaviors is found in the corresponding .cpp
 *file.
 *
 *========================================================================
 */

/**
 * @file
 */
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Region.hpp>
#include <gfcpp/Cache.hpp>
#include "CacheImpl.hpp"
#include <gfcpp/DistributedSystem.hpp>

namespace gemfire {

class CacheRegionHelper {
  /**
   * CacheHelper
   *
   */
 public:
  inline static CacheImpl* getCacheImpl(const Cache* cache) {
    return cache->m_cacheImpl;
  }

  inline static DistributedSystemImpl* getDistributedSystemImpl() {
    return DistributedSystem::m_impl;
  }
};
}  // namespace gemfire
#endif  // ifndef __GEMFIRE_IMPL_CACHEHELPER_H__
