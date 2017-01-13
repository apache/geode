/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __GEMFIRE_IMPL_ENTRIESMAPFACTORY_H__
#define __GEMFIRE_IMPL_ENTRIESMAPFACTORY_H__

#include <gfcpp/gfcpp_globals.hpp>
#include "EntriesMap.hpp"
#include <gfcpp/RegionAttributes.hpp>

namespace gemfire {

class CPPCACHE_EXPORT EntriesMapFactory {
 public:
  /** @brief used internally by Region implementation to create the appropriate
   * type of entries map.
   */
  static EntriesMap* createMap(RegionInternal*,
                               const RegionAttributesPtr& attrs);

 private:
  /** @brief not to be instantiated. */
  EntriesMapFactory() {}

};  // class

};  // namespace

#endif  // __GEMFIRE_IMPL_ENTRIESMAPFACTORY_H__
