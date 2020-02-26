/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.cache;

/**
 * Each enum represents a predefined {@code RegionAttributes} in a {@code Cache}. These enum values
 * can be used to create regions using a {@code RegionFactory} obtained by calling
 * {@code Cache.createRegionFactory(RegionShortcut)}.
 * <p>
 * Another way to use predefined region attributes is in cache.xml by setting the refid attribute on
 * a region element or region-attributes element to the string of each value.
 *
 * @since GemFire 6.5
 */
public enum RegionShortcut {
  /**
   * A PARTITION has local state that is partitioned across each peer member that created the
   * region. The actual RegionAttributes for a PARTITION region set the {@code DataPolicy} to
   * {@code DataPolicy.PARTITION}.
   */
  PARTITION,
  /**
   * A PARTITION_REDUNDANT has local state that is partitioned across each peer member that created
   * the region. In addition an extra copy of the data is kept in memory. The actual
   * RegionAttributes for a PARTITION_REDUNDANT region set the {@code DataPolicy} to
   * {@code DataPolicy.PARTITION} and the redundant-copies to 1.
   */
  PARTITION_REDUNDANT,
  /**
   * A PARTITION_PERSISTENT has local state that is partitioned across each peer member that created
   * the region. In addition its state is written to disk and recovered from disk when the region is
   * created. The actual RegionAttributes for a PARTITION_PERSISTENT region set the
   * {@code DataPolicy} to {@code DataPolicy.PERSISTENT_PARTITION}.
   */
  PARTITION_PERSISTENT,
  /**
   * A PARTITION_REDUNDANT_PERSISTENT has local state that is partitioned across each peer member
   * that created the region. In addition its state is written to disk and recovered from disk when
   * the region is created. In addition an extra copy of the data is kept in memory. The actual
   * RegionAttributes for a PARTITION_REDUNDANT_PERSISTENT region set the {@code DataPolicy} to
   * {@code DataPolicy.PERSISTENT_PARTITION} and the redundant-copies to 1.
   */
  PARTITION_REDUNDANT_PERSISTENT,
  /**
   * A PARTITION_OVERFLOW has local state that is partitioned across each peer member that created
   * the region. It will also move the values of entries to disk once it detects that the java vm is
   * running low of memory. The actual RegionAttributes for a PARTITION_OVERFLOW region set the
   * {@code DataPolicy} to {@code DataPolicy.PARTITION}. and {@code EvictionAttributes} are set to
   * {@code EvictionAlgorithm.LRU_HEAP} with {@code EvictionAction.OVERFLOW_TO_DISK}.
   */
  PARTITION_OVERFLOW,
  /**
   * A PARTITION_REDUNDANT_OVERFLOW has local state that is partitioned across each peer member that
   * created the region. In addition an extra copy of the data is kept in memory. It will also move
   * the values of entries to disk once it detects that the java vm is running low of memory. The
   * actual RegionAttributes for a PARTITION_REDUNDANT_OVERFLOW region set the {@code DataPolicy} to
   * {@code DataPolicy.PARTITION}, the redundant-copies to 1, and {@code EvictionAttributes} are set
   * to {@code EvictionAlgorithm.LRU_HEAP} with {@code EvictionAction.OVERFLOW_TO_DISK}.
   */
  PARTITION_REDUNDANT_OVERFLOW,
  /**
   * A PARTITION_PERSISTENT_OVERFLOW has local state that is partitioned across each peer member
   * that created the region. In addition its state is written to disk and recovered from disk when
   * the region is created. It will also remove the values of entries from memory once it detects
   * that the java vm is running low of memory. The actual RegionAttributes for a
   * PARTITION_PERSISTENT_OVERFLOW region set the {@code DataPolicy} to
   * {@code DataPolicy.PERSISTENT_PARTITION} and {@code EvictionAttributes} are set to
   * {@code EvictionAlgorithm.LRU_HEAP} with {@code EvictionAction.OVERFLOW_TO_DISK}.
   */
  PARTITION_PERSISTENT_OVERFLOW,
  /**
   * A PARTITION_REDUNDANT_PERSISTENT_OVERFLOW has local state that is partitioned across each peer
   * member that created the region. In addition its state is written to disk and recovered from
   * disk when the region is created. In addition an extra copy of the data is kept in memory. It
   * will also remove the values of entries from memory once it detects that the java vm is running
   * low of memory. The actual RegionAttributes for a PARTITION_REDUNDANT_PERSISTENT_OVERFLOW region
   * set the {@code DataPolicy} to {@code DataPolicy.PERSISTENT_PARTITION}, the redundant-copies to
   * 1, and {@code EvictionAttributes} are set to {@code EvictionAlgorithm.LRU_HEAP} with
   * {@code EvictionAction.OVERFLOW_TO_DISK}.
   */
  PARTITION_REDUNDANT_PERSISTENT_OVERFLOW,
  /**
   * A PARTITION_HEAP_LRU has local state that is partitioned across each peer member that created
   * the region. It will also destroy entries once it detects that the java vm is running low of
   * memory. The actual RegionAttributes for a PARTITION_HEAP_LRU region set the {@code DataPolicy}
   * to {@code DataPolicy.PARTITION} and {@code EvictionAttributes} are set to
   * {@code EvictionAlgorithm.LRU_HEAP} with {@code EvictionAction.LOCAL_DESTROY}.
   */
  PARTITION_HEAP_LRU,
  /**
   * A PARTITION_REDUNDANT_HEAP_LRU has local state that is partitioned across each peer member that
   * created the region. In addition an extra copy of the data is kept in memory. It will also
   * destroy entries once it detects that the java vm is running low of memory. The actual
   * RegionAttributes for a PARTITION_REDUNDANT_HEAP_LRU region set the {@code DataPolicy} to
   * {@code DataPolicy.PARTITION}, the redundant-copies to 1, and {@code EvictionAttributes} are set
   * to {@code EvictionAlgorithm.LRU_HEAP} with {@code EvictionAction.LOCAL_DESTROY}.
   */
  PARTITION_REDUNDANT_HEAP_LRU,

  /**
   * A REPLICATE has local state that is kept in sync with all other replicate regions that exist in
   * its peers. The actual RegionAttributes for a REPLICATE region set the {@code DataPolicy} to
   * {@code DataPolicy.REPLICATE} and {@code Scope} to {@code Scope.DISTRIBUTED_ACK}.
   */
  REPLICATE,
  /**
   * A REPLICATE_PERSISTENT has local state that is kept in sync with all other replicate regions
   * that exist in its peers. In addition its state is written to disk and recovered from disk when
   * the region is created. The actual RegionAttributes for a REPLICATE_PERSISTENT region set the
   * {@code DataPolicy} to {@code DataPolicy.PERSISTENT_REPLICATE} and {@code Scope} to
   * {@code Scope.DISTRIBUTED_ACK}.
   */
  REPLICATE_PERSISTENT,

  /**
   * A REPLICATE_OVERFLOW has local state that is kept in sync with all other replicate regions that
   * exist in its peers. The actual RegionAttributes for a REPLICATE_OVERFLOW region set the
   * {@code DataPolicy} to {@code DataPolicy.REPLICATE}, the {@code Scope} to
   * {@code Scope.DISTRIBUTED_ACK} and {@code EvictionAttributes} are set to
   * {@code EvictionAlgorithm.LRU_HEAP} with {@code EvictionAction.OVERFLOW_TO_DISK}.
   */
  REPLICATE_OVERFLOW,
  /**
   * A REPLICATE_PERSISTENT_OVERFLOW has local state that is kept in sync with all other replicate
   * regions that exist in its peers. In addition its state is written to disk and recovered from
   * disk when the region is created. It will also remove the values of entries from memory once it
   * detects that the java vm is running low of memory. The actual RegionAttributes for a
   * REPLICATE_PERSISTENT_OVERFLOW region set the {@code DataPolicy} to
   * {@code DataPolicy.PERSISTENT_REPLICATE}, the {@code Scope} to {@code Scope.DISTRIBUTED_ACK},
   * and {@code EvictionAttributes} are set to {@code EvictionAlgorithm.LRU_HEAP} with
   * {@code EvictionAction.OVERFLOW_TO_DISK}.
   */
  REPLICATE_PERSISTENT_OVERFLOW,
  /**
   * A REPLICATE_HEAP_LRU has local state that is kept in sync with all other replicate regions that
   * exist in its peers. It will also destroy entries once it detects that the java vm is running
   * low of memory. The actual RegionAttributes for a REPLICATE_HEAP_LRU region set the
   * {@code DataPolicy} to {@code DataPolicy.PRELOADED}, the {@code Scope} to
   * {@code Scope.DISTRIBUTED_ACK}, {@code SubscriptionAttributes} to {@code InterestPolicy.ALL},
   * and {@code EvictionAttributes} are set to {@code EvictionAlgorithm.LRU_HEAP} with
   * {@code EvictionAction.LOCAL_DESTROY}.
   */
  REPLICATE_HEAP_LRU,

  /**
   * A LOCAL region only has local state and never sends operations to others. The actual
   * RegionAttributes for a LOCAL region set the {@code Scope} to {@code Scope.LOCAL}
   * and the {@code DataPolicy} to {@code DataPolicy.NORMAL}.
   */
  LOCAL,
  /**
   * A LOCAL_PERSISTENT region only has local state and never sends operations to others but it does
   * write its state to disk and can recover that state when the region is created. The actual
   * RegionAttributes for a LOCAL_PERSISTENT region set the {@code Scope} to {@code Scope.LOCAL}
   * and the {@code DataPolicy} to {@code DataPolicy.PERSISTENT_REPLICATE}.
   */
  LOCAL_PERSISTENT,

  /**
   * A LOCAL_HEAP_LRU region only has local state and never sends operations to others. It will also
   * destroy entries once it detects that the java vm is running low of memory. The actual
   * RegionAttributes for a LOCAL_HEAP_LRU region set the the {@code Scope} to {@code Scope.LOCAL},
   * the {@code DataPolicy} to {@code DataPolicy.NORMAL}, and {@code EvictionAttributes} are set to
   * {@code EvictionAlgorithm.LRU_HEAP} with {@code EvictionAction.LOCAL_DESTROY}.
   */
  LOCAL_HEAP_LRU,
  /**
   * A LOCAL_OVERFLOW region only has local state and never sends operations to others. It will also
   * move the values of entries to disk once it detects that the java vm is running low of memory.
   * The actual RegionAttributes for a LOCAL_OVERFLOW region set
   * the {@code Scope} to {@code Scope.LOCAL},
   * the {@code DataPolicy} to {@code DataPolicy.NORMAL}, and {@code EvictionAttributes} are set to
   * {@code EvictionAlgorithm.LRU_HEAP} with {@code EvictionAction.OVERFLOW_TO_DISK}.
   */
  LOCAL_OVERFLOW,
  /**
   * A LOCAL_PERSISTENT_OVERFLOW region only has local state and never sends operations to others
   * but it does write its state to disk and can recover that state when the region is created. It
   * will also remove the values of entries from memory once it detects that the java vm is running
   * low of memory. The actual RegionAttributes for a LOCAL_PERSISTENT_OVERFLOW region set
   * the {@code Scope} to {@code Scope.LOCAL}, the
   * {@code DataPolicy} to {@code DataPolicy.PERSISTENT_REPLICATE}, and {@code EvictionAttributes}
   * are set to {@code EvictionAlgorithm.LRU_HEAP} with {@code EvictionAction.OVERFLOW_TO_DISK}.
   */
  LOCAL_PERSISTENT_OVERFLOW,

  /**
   * A PARTITION_PROXY has no local state and forwards all operations to a PARTITION or a
   * PARTITION_PERSISTENT that exists in its peers. The actual RegionAttributes for a
   * PARTITION_PROXY region set the {@code DataPolicy} to {@code DataPolicy.PARTITION} and the
   * local-max-memory to 0.
   */
  PARTITION_PROXY,
  /**
   * A PARTITION_PROXY_REDUNDANT has no local state and forwards all operations to a
   * PARTITION_REDUNDANT or a PARTITION_REDUNDANT_PERSISTENT that exists in its peers. The actual
   * RegionAttributes for a PARTITION_PROXY_REDUNDANT region set the {@code DataPolicy} to
   * {@code DataPolicy.PARTITION}, the local-max-memory to 0, and the redundant-copies to 1.
   */
  PARTITION_PROXY_REDUNDANT,
  /**
   * A REPLICATE_PROXY has no local state and forwards all operations (except queries) to a
   * REPLICATE or a REPLICATE_PERSISTENT that exists in its peers. Queries will be executed on this
   * PROXY region. The actual RegionAttributes for a REPLICATE_PROXY region set the
   * {@code DataPolicy} to {@code DataPolicy.EMPTY} and {@code Scope} to
   * {@code Scope.DISTRIBUTED_ACK}.
   */
  REPLICATE_PROXY;

  public boolean isProxy() {
    return name().contains("PROXY");
  }

  public boolean isLocal() {
    return name().contains("LOCAL");
  }

  public boolean isPartition() {
    return name().contains("PARTITION");
  }

  public boolean isReplicate() {
    return name().contains("REPLICATE");
  }

  public boolean isPersistent() {
    return name().contains("PERSISTENT");
  }

  public boolean isOverflow() {
    return name().contains("OVERFLOW");
  }
}
