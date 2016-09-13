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

  /** true if parallel mode is enabled */
  private volatile boolean parallel;
  
  /** the file mapper, or null if parallel mode is not enabled */
  private volatile SnapshotFileMapper mapper;
  
  public SnapshotOptionsImpl() {
    filter = null;
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

  /**
   * Enables parallel mode for snapshot operations.  This will cause each member
   * of a partitioned region to save its local data set (ignoring redundant 
   * copies) to a separate snapshot file.  During a parallel import, each member
   * may read from one or more snapshot files created during a parallel export.
   * <p>
   * Parallelizing snapshot operations may yield significant performance
   * improvements for large data sets.  This is particularly true when each 
   * member is reading from or writing to separate physical disks.
   * <p>
   * This flag is ignored for replicated regions.
   * <p>
   * If the mapper is not set explicitly, a default mapping implementation is used
   * that assumes each member can access a non-shared disk volume.  The default
   * mapper provides the following behavior:
   * <dl>
   *  <dt>export</dt>
   *  <dd>use the supplied path</dd>
   *  <dt>import</dt>
   *  <dd>if the supplied path is a file, use that file</dd>
   *  <dd>if the supplied path is a directory, use all files in the directory</dd>
   * </dl>
   * If processes are colocated on the same machine and relative pathnames are
   * not used, a custom mapper <b>must</b> be supplied to disambiguate filenames.
   * This rule applies when writing to a shared network volume as well.
   * 
   * @param parallel true if the snapshot operations will be performed in parallel
   * @return the snapshot options
   * 
   * @see SnapshotFileMapper
   */
  public SnapshotOptions<K, V> setParallelMode(boolean parallel) {
    this.parallel = parallel;
    return this;
  }

  /**
   * Returns true if the snapshot operation will proceed in parallel.
   * 
   * @return true if the parallel mode has been enabled
   */
  public boolean isParallelMode() {
    return parallel;
  }
  
  /**
   * Overrides the default file mapping for parallel import and export
   * operations.
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
    return (mapper == null) ? RegionSnapshotServiceImpl.LOCAL_MAPPER : mapper;
  }
}
