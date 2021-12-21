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
package org.apache.geode.internal.cache.snapshot;

import org.apache.geode.cache.snapshot.SnapshotFilter;
import org.apache.geode.cache.snapshot.SnapshotOptions;

/**
 * Implements the snapshot options.
 *
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class SnapshotOptionsImpl<K, V> implements SnapshotOptions<K, V> {
  private static final long serialVersionUID = 1L;

  /** the entry filter */
  private volatile SnapshotFilter<K, V> filter;

  /** true if callbacks should be invoked on load */
  private volatile boolean invokeCallbacks;

  /** true if parallel mode is enabled */
  private volatile boolean parallel;

  /** the file mapper, or null if parallel mode is not enabled */
  private volatile SnapshotFileMapper mapper;

  public SnapshotOptionsImpl() {
    filter = null;
    invokeCallbacks = false;
  }

  @Override
  public SnapshotOptions<K, V> setFilter(SnapshotFilter<K, V> filter) {
    this.filter = filter;
    return this;
  }

  @Override
  public SnapshotFilter<K, V> getFilter() {
    return filter;
  }

  @Override
  public SnapshotOptions<K, V> invokeCallbacks(boolean invokeCallbacks) {
    this.invokeCallbacks = invokeCallbacks;
    return this;
  }

  @Override
  public boolean shouldInvokeCallbacks() {
    return invokeCallbacks;
  }


  @Override
  public SnapshotOptions<K, V> setParallelMode(boolean parallel) {
    this.parallel = parallel;
    return this;
  }

  @Override
  public boolean isParallelMode() {
    return parallel;
  }

  /**
   * Overrides the default file mapping for parallel import and export operations.
   *
   * @param mapper the custom mapper, or null to use the default mapping
   * @return the snapshot options
   * @see #setParallelMode(boolean)
   */
  public SnapshotOptions<K, V> setMapper(SnapshotFileMapper mapper) {
    this.mapper = mapper;
    return this;
  }

  /**
   * Returns the snapshot file mapper for parallel import and export.
   *
   * @return the mapper
   * @see #setParallelMode(boolean)
   */
  public SnapshotFileMapper getMapper() {
    if (mapper == null) {
      mapper = isParallelMode() ? new ParallelSnapshotFileMapper()
          : RegionSnapshotServiceImpl.LOCAL_MAPPER;
    }
    return mapper;
  }

  @Override
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("SnapshotOptionsImpl@").append(System.identityHashCode(this)).append(": ")
        .append("parallel=").append(parallel).append("; invokeCallbacks=").append(invokeCallbacks)
        .append("; filter=").append(filter).append("; mapper=").append(mapper);
    return buf.toString();
  }
}
