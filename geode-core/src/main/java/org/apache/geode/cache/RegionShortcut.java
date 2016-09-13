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

package com.gemstone.gemfire.cache;

/**
 * Each enum represents a predefined {@link RegionAttributes} in a {@link Cache}.
 * These enum values can be used to create regions using a {@link RegionFactory}
 * obtained by calling {@link Cache#createRegionFactory(RegionShortcut)}.
 * <p>Another way to use predefined region attributes is in cache.xml by setting
 * the refid attribute on a region element or region-attributes element to the
 * string of each value.
 * @since GemFire 6.5
 */
public enum RegionShortcut {
  /**
   * A PARTITION has local state that is partitioned across each peer member
   * that created the region.
   * The actual RegionAttributes for a PARTITION region set the {@link DataPolicy} to {@link DataPolicy#PARTITION}.
   */
  PARTITION,
  /**
   * A PARTITION_REDUNDANT has local state that is partitioned across each peer member
   * that created the region.
   * In addition an extra copy of the data is kept in memory.
   * The actual RegionAttributes for a PARTITION_REDUNDANT region set the {@link DataPolicy} to {@link DataPolicy#PARTITION} and the redundant-copies to 1.
   */
  PARTITION_REDUNDANT,
  /**
   * A PARTITION_PERSISTENT has local state that is partitioned across each peer member
   * that created the region.
   * In addition its state is written to disk and recovered from disk when the region
   * is created.
   * The actual RegionAttributes for a PARTITION_PERSISTENT region set the {@link DataPolicy} to {@link DataPolicy#PERSISTENT_PARTITION}.
   */
  PARTITION_PERSISTENT,
  /**
   * A PARTITION_REDUNDANT_PERSISTENT has local state that is partitioned across each peer member
   * that created the region.
   * In addition its state is written to disk and recovered from disk when the region
   * is created.
   * In addition an extra copy of the data is kept in memory.
   * The actual RegionAttributes for a PARTITION_REDUNDANT_PERSISTENT region set the {@link DataPolicy} to {@link DataPolicy#PERSISTENT_PARTITION} and the redundant-copies to 1.
   */
  PARTITION_REDUNDANT_PERSISTENT,
  /**
   * A PARTITION_OVERFLOW has local state that is partitioned across each peer member
   * that created the region.
   * It will also move the values of entries to disk once it detects that the
   * java vm is running low of memory.
   * The actual RegionAttributes for a PARTITION_OVERFLOW region set the {@link DataPolicy} to {@link DataPolicy#PARTITION}.
   * and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#OVERFLOW_TO_DISK}.
   */
  PARTITION_OVERFLOW,
  /**
   * A PARTITION_REDUNDANT_OVERFLOW has local state that is partitioned across each peer member
   * that created the region.
   * In addition an extra copy of the data is kept in memory.
   * It will also move the values of entries to disk once it detects that the
   * java vm is running low of memory.
   * The actual RegionAttributes for a PARTITION_REDUNDANT_OVERFLOW region set the {@link DataPolicy} to {@link DataPolicy#PARTITION}, the redundant-copies to 1,
   * and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#OVERFLOW_TO_DISK}.
   */
  PARTITION_REDUNDANT_OVERFLOW,
  /**
   * A PARTITION_PERSISTENT_OVERFLOW has local state that is partitioned across each peer member
   * that created the region.
   * In addition its state is written to disk and recovered from disk when the region
   * is created.
   * It will also remove the values of entries from memory once it detects that the
   * java vm is running low of memory.
   * The actual RegionAttributes for a PARTITION_PERSISTENT_OVERFLOW region set the {@link DataPolicy} to {@link DataPolicy#PERSISTENT_PARTITION}
   * and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#OVERFLOW_TO_DISK}.
   */
  PARTITION_PERSISTENT_OVERFLOW,
  /**
   * A PARTITION_REDUNDANT_PERSISTENT_OVERFLOW has local state that is partitioned across each peer member
   * that created the region.
   * In addition its state is written to disk and recovered from disk when the region
   * is created.
   * In addition an extra copy of the data is kept in memory.
   * It will also remove the values of entries from memory once it detects that the
   * java vm is running low of memory.
   * The actual RegionAttributes for a PARTITION_REDUNDANT_PERSISTENT_OVERFLOW region set the {@link DataPolicy} to {@link DataPolicy#PERSISTENT_PARTITION}, the redundant-copies to 1,
   * and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#OVERFLOW_TO_DISK}.
   */
  PARTITION_REDUNDANT_PERSISTENT_OVERFLOW,
  /**
   * A PARTITION_HEAP_LRU has local state that is partitioned across each peer member
   * that created the region.
   * It will also destroy entries once it detects that the java vm is running low
   * of memory.
   * The actual RegionAttributes for a PARTITION_HEAP_LRU region set the {@link DataPolicy} to {@link DataPolicy#PARTITION}
   * and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#LOCAL_DESTROY}.
   */
  PARTITION_HEAP_LRU,
  /**
   * A PARTITION_REDUNDANT_HEAP_LRU has local state that is partitioned across each peer member
   * that created the region.
   * In addition an extra copy of the data is kept in memory.
   * It will also destroy entries once it detects that the java vm is running low
   * of memory.
   * The actual RegionAttributes for a PARTITION_REDUNDANT_HEAP_LRU region set the {@link DataPolicy} to {@link DataPolicy#PARTITION}, the redundant-copies to 1,
   * and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#LOCAL_DESTROY}.
   */
  PARTITION_REDUNDANT_HEAP_LRU,

  /**
   * A REPLICATE has local state that is kept in sync with all other replicate
   * regions that exist in its peers.
   * The actual RegionAttributes for a REPLICATE region set the {@link DataPolicy} to {@link DataPolicy#REPLICATE} and {@link Scope} to {@link Scope#DISTRIBUTED_ACK}.
   */
  REPLICATE,
  /**
   * A REPLICATE_PERSISTENT has local state that is kept in sync with all other replicate
   * regions that exist in its peers.
   * In addition its state is written to disk and recovered from disk when the region
   * is created.
   * The actual RegionAttributes for a REPLICATE_PERSISTENT region set the {@link DataPolicy} to {@link DataPolicy#PERSISTENT_REPLICATE} and {@link Scope} to {@link Scope#DISTRIBUTED_ACK}.
   */
  REPLICATE_PERSISTENT,

  /**
   * A REPLICATE_OVERFLOW has local state that is kept in sync with all other replicate
   * regions that exist in its peers.
   * The actual RegionAttributes for a REPLICATE_OVERFLOW region set the {@link DataPolicy}
   * to {@link DataPolicy#REPLICATE}, the {@link Scope} to {@link Scope#DISTRIBUTED_ACK}
   * and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#OVERFLOW_TO_DISK}.
   */
  REPLICATE_OVERFLOW,
  /**
   * A REPLICATE_PERSISTENT_OVERFLOW has local state that is kept in sync with all other replicate
   * regions that exist in its peers.
   * In addition its state is written to disk and recovered from disk when the region
   * is created.
   * It will also remove the values of entries from memory once it detects that the
   * java vm is running low of memory.
   * The actual RegionAttributes for a REPLICATE_PERSISTENT_OVERFLOW region set the {@link DataPolicy} 
   * to {@link DataPolicy#PERSISTENT_REPLICATE}, the {@link Scope} to {@link Scope#DISTRIBUTED_ACK},
   * and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#OVERFLOW_TO_DISK}.
   */
  REPLICATE_PERSISTENT_OVERFLOW,
  /**
   * A REPLICATE_HEAP_LRU has local state that is kept in sync with all other replicate
   * regions that exist in its peers.
   * It will also destroy entries once it detects that the java vm is running low
   * of memory.
   * The actual RegionAttributes for a REPLICATE_HEAP_LRU region set the {@link DataPolicy} to {@link DataPolicy#PRELOADED}, the {@link Scope} to {@link Scope#DISTRIBUTED_ACK},
   * {@link SubscriptionAttributes} to {@link InterestPolicy#ALL},
   * and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#LOCAL_DESTROY}.   
   */
  REPLICATE_HEAP_LRU,

  /**
   * A LOCAL region only has local state and never sends operations to others.
   * The actual RegionAttributes for a LOCAL region  set the {@link DataPolicy} to {@link DataPolicy#NORMAL}.
   */
  LOCAL,
  /**
   * A LOCAL_PERSISTENT region only has local state and never sends operations to others
   * but it does write its state to disk and can recover that state when the region
   * is created.
   * The actual RegionAttributes for a LOCAL_PERSISTENT region set the {@link DataPolicy} to {@link DataPolicy#PERSISTENT_REPLICATE}.
   */
  LOCAL_PERSISTENT,

  /**
   * A LOCAL_HEAP_LRU region only has local state and never sends operations to others.
   * It will also destroy entries once it detects that the java vm is running low
   * of memory.
   * The actual RegionAttributes for a LOCAL_HEAP_LRU region set the {@link DataPolicy} to {@link DataPolicy#NORMAL} and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#LOCAL_DESTROY}.
   */
  LOCAL_HEAP_LRU,
  /**
   * A LOCAL_OVERFLOW region only has local state and never sends operations to others.
   * It will also move the values of entries to disk once it detects that the
   * java vm is running low of memory.
   * The actual RegionAttributes for a LOCAL_OVERFLOW region set the {@link DataPolicy} to {@link DataPolicy#NORMAL} and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#OVERFLOW_TO_DISK}.
   */
  LOCAL_OVERFLOW,
  /**
   * A LOCAL_PERSISTENT_OVERFLOW region only has local state and never sends operations to others
   * but it does write its state to disk and can recover that state when the region
   * is created.
   * It will also remove the values of entries from memory once it detects that the
   * java vm is running low of memory.
   * The actual RegionAttributes for a LOCAL_PERSISTENT_OVERFLOW region set the {@link DataPolicy} to {@link DataPolicy#PERSISTENT_REPLICATE} and {@link EvictionAttributes} are set to {@link EvictionAlgorithm#LRU_HEAP}
   * with {@link EvictionAction#OVERFLOW_TO_DISK}.
   */
  LOCAL_PERSISTENT_OVERFLOW,

  /**
   * A PARTITION_PROXY has no local state and forwards all operations to a PARTITION
   * or a PARTITION_PERSISTENT that exists in its peers.
   * The actual RegionAttributes for a PARTITION_PROXY region set the {@link DataPolicy} to {@link DataPolicy#PARTITION} and the local-max-memory to 0.
   */
  PARTITION_PROXY,
  /**
   * A PARTITION_PROXY_REDUNDANT has no local state and forwards all operations to a PARTITION_REDUNDANT
   * or a PARTITION_REDUNDANT_PERSISTENT that exists in its peers.
   * The actual RegionAttributes for a PARTITION_PROXY_REDUNDANT region set the {@link DataPolicy} to {@link DataPolicy#PARTITION}, the local-max-memory to 0,
   * and the redundant-copies to 1.
   */
  PARTITION_PROXY_REDUNDANT,
  /**
   * A REPLICATE_PROXY has no local state and forwards all operations (except queries) to a REPLICATE
   * or a REPLICATE_PERSISTENT that exists in its peers.
   * Queries will be executed on this PROXY region.
   * The actual RegionAttributes for a REPLICATE_PROXY region set the {@link DataPolicy} to {@link DataPolicy#EMPTY} and {@link Scope} to {@link Scope#DISTRIBUTED_ACK}.
   */
  REPLICATE_PROXY,  
}
