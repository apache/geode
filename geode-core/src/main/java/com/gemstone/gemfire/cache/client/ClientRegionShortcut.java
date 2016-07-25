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

package com.gemstone.gemfire.cache.client;

import com.gemstone.gemfire.cache.*;

/**
 * Each enum represents a predefined {@link RegionAttributes} in a {@link ClientCache}.
 * These enum values can be used to create regions using a {@link ClientRegionFactory}
 * obtained by calling {@link ClientCache#createClientRegionFactory(ClientRegionShortcut)}.
 * <p>Another way to use predefined region attributes is in cache.xml by setting
 * the refid attribute on a region element or region-attributes element to the
 * string of each value.
 * @since GemFire 6.5
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
