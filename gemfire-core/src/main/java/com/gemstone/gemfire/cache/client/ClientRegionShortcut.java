/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.cache.*;

/**
 * Each enum represents a predefined {@link RegionAttributes} in a {@link ClientCache}.
 * These enum values can be used to create regions using a {@link ClientRegionFactory}
 * obtained by calling {@link ClientCache#createClientRegionFactory(ClientRegionShortcut)}.
 * <p>Another way to use predefined region attributes is in cache.xml by setting
 * the refid attribute on a region element or region-attributes element to the
 * string of each value.
 * @since 6.5
 * @author darrel
 */
public enum ClientRegionShortcut {
  /**
   * A PROXY region has no local state and forwards all operations to a server.
   * The actual RegionAttributes for a PROXY set the {@link DataPolicy} to {@link DataPolicy#EMPTY}.
   */
  PROXY,

  /**
   * A CACHING_PROXY region has local state but can also send operations to a server.
   * If the local state is not found then the operation is sent to the server
   * and the local state is updated to contain the server result.
   * The actual RegionAttributes for a CACHING_PROXY set the {@link DataPolicy} to {@link DataPolicy#NORMAL}.
   */
  CACHING_PROXY,
    
  /**
   * A CACHING_PROXY_HEAP_LRU region has local state but can also send operations to a server.
   * If the local state is not found then the operation is sent to the server
   * and the local state is updated to contain the server result.
   * It will also destroy entries once it detects that the java vm is running low
   * of memory.
   * The actual RegionAttributes for a CACHING_PROXY_HEAP_LRU set the {@link DataPolicy} to {@link DataPolicy#NORMAL}.
   * and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
     * with {@link EvictionAction#LOCAL_DESTROY}.
     */
  CACHING_PROXY_HEAP_LRU,
  /**
   * A CACHING_PROXY_OVERFLOW region has local state but can also send operations to a server.
   * If the local state is not found then the operation is sent to the server
   * and the local state is updated to contain the server result.
   * It will also move the values of entries to disk once it detects that the
   * java vm is running low of memory.
   * The actual RegionAttributes for a CACHING_PROXY_OVERFLOW set the {@link DataPolicy} to {@link DataPolicy#NORMAL}.
   * and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
     * with {@link EvictionAction#OVERFLOW_TO_DISK}.
     */
  CACHING_PROXY_OVERFLOW,

  /**
   * A LOCAL region only has local state and never sends operations to a server.
   * The actual RegionAttributes for a LOCAL region set the {@link DataPolicy} to {@link DataPolicy#NORMAL}.
   */
    LOCAL,
  /**
   * A LOCAL_PERSISTENT region only has local state and never sends operations to a server
   * but it does write its state to disk and can recover that state when the region
   * is created.
   * The actual RegionAttributes for a LOCAL_PERSISTENT region set the {@link DataPolicy} to {@link DataPolicy#PERSISTENT_REPLICATE}.
   */
    LOCAL_PERSISTENT,
    /**
     * A LOCAL_HEAP_LRU region only has local state and never sends operations to a server.
     * It will also destroy entries once it detects that the java vm is running low
     * of memory.
     * The actual RegionAttributes for a LOCAL_HEAP_LRU region set the {@link DataPolicy} to {@link DataPolicy#NORMAL} and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
     * with {@link EvictionAction#LOCAL_DESTROY}.
     */
    LOCAL_HEAP_LRU,
    /**
     * A LOCAL_OVERFLOW region only has local state and never sends operations to a server.
     * It will also move the values of entries to disk once it detects that the
     * java vm is running low of memory.
     * The actual RegionAttributes for a LOCAL_OVERFLOW region set the {@link DataPolicy} to {@link DataPolicy#NORMAL} and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
     * with {@link EvictionAction#OVERFLOW_TO_DISK}.
     */
    LOCAL_OVERFLOW,
    /**
     * A LOCAL_PERSISTENT_OVERFLOW region only has local state and never sends operations to a server
     * but it does write its state to disk and can recover that state when the region
     * is created.
     * It will also remove the values of entries from memory once it detects that the
     * java vm is running low of memory.
     * The actual RegionAttributes for a LOCAL_PERSISTENT_OVERFLOW region set the {@link DataPolicy} to {@link DataPolicy#PERSISTENT_REPLICATE} and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
     * with {@link EvictionAction#OVERFLOW_TO_DISK}.
     */
    LOCAL_PERSISTENT_OVERFLOW
}