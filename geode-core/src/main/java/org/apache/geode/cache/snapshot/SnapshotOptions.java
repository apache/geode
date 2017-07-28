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
package org.apache.geode.cache.snapshot;

import java.io.Serializable;

import org.apache.geode.internal.cache.snapshot.SnapshotFileMapper;

/**
 * Provides a way to configure the behavior of snapshot operations. The default options are:
 * <dl>
 * <dt>filter</dt>
 * <dd>null</dd>
 * </dl>
 * 
 * @param <K> the cache entry key type
 * @param <V> the cache entry value type
 * 
 * @since GemFire 7.0
 */
public interface SnapshotOptions<K, V> extends Serializable {
  /**
   * Defines the available snapshot file formats.
   * 
   * @since GemFire 7.0
   */
  enum SnapshotFormat {
    /** an optimized binary format specific to GemFire */
    GEMFIRE
  }

  /**
   * Sets a filter to apply to snapshot entries. Entries that are accepted by the filter will be
   * included in import and export operations.
   * 
   * @param filter the filter to apply, or null to remove the filter
   * @return the snapshot options
   */
  SnapshotOptions<K, V> setFilter(SnapshotFilter<K, V> filter);

  /**
   * Returns the filter to be applied to snapshot entries. Entries that are accepted by the filter
   * will be included in import and export operations.
   * 
   * @return the filter, or null if the filter is not set
   */
  SnapshotFilter<K, V> getFilter();

  /**
   * Sets whether to invoke callbacks when loading a snapshot. The default is false.
   *
   * @param invokeCallbacks
   *
   * @return the snapshot options
   */
  SnapshotOptions<K, V> invokeCallbacks(boolean invokeCallbacks);

  /**
   * Returns whether loading a snapshot causes callbacks to be invoked
   *
   * @return whether loading a snapshot causes callbacks to be invoked
   */
  boolean shouldInvokeCallbacks();

  /**
   * Returns true if the snapshot operation will proceed in parallel.
   *
   * @return true if the parallel mode has been enabled
   *
   * @since Geode 1.3
   */
  boolean isParallelMode();

  /**
   * Enables parallel mode for snapshot export, which will cause each member of a partitioned region
   * to save its local data set (ignoring redundant copies) to a separate snapshot file.
   *
   * <p>
   * Parallelizing snapshot operations may yield significant performance improvements for large data
   * sets. This is particularly true when each member is writing to separate physical disks.
   * <p>
   * This flag is ignored for replicated regions.
   *
   * @param parallel true if the snapshot operations will be performed in parallel
   * @return the snapshot options
   *
   * @see SnapshotFileMapper
   *
   * @since Geode 1.3
   */
  SnapshotOptions<K, V> setParallelMode(boolean parallel);
}
