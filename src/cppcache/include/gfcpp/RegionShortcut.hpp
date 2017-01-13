#ifndef __GEMFIRE_REGIONSHORTCUT_H__
#define __GEMFIRE_REGIONSHORTCUT_H__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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
namespace gemfire {
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

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_REGIONSHORTCUT_H__
