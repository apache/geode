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
package com.gemstone.gemfire.cache.snapshot;

import java.io.File;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat;
import com.gemstone.gemfire.pdx.PdxSerializer;

/**
 * Allows a snapshot of cache data to be imported and exported. Each region in 
 * the cache will be included in the snapshot (one snapshot file for each
 * region). Example usage:
 *
 * <pre>
 * // obtain a snapshot
 * CacheSnapshotService snapshot = cache.getSnapshotService();
 * 
 * // export the snapshot, every region in the cache will be exported
 * snapshot.save(new File("."), SnapshotFormat.GEMFIRE);
 * 
 * // import the snapshot files, updates any existing entries in the cache
 * snapshot.load(new File("."), SnapshotFormat.GEMFIRE);
 * </pre>
 * 
 * The default behavior is to perform all I/O operations on the node where the
 * snapshot operations are invoked.  This will involve either collecting or
 * dispersing data over the network if the cache contains a partitioned region.
 * The snapshot behavior can be changed using {@link SnapshotOptions}. For example:
 * <pre>
 * CacheSnapshotService snapshot = cache.getSnapshotService();
 * SnapshotFilter filter = new SnapshotFilter() {
 *   public boolean accept(Entry<K, V> entry) {
 *     return true;
 *   }
 * };
 *
 * SnapshotOptions<Object, Object> options = snapshot.createOptions();
 * options.setFilter(filter);
 * 
 * snapshot.save(new File("."), SnapshotFormat.GEMFIRE, options);
 * </pre>
 * Note that the snapshot does not provide a consistency guarantee. Updates to 
 * data during the course of import/export operations could result data 
 * inconsistencies.
 * 
 * @see Cache#getSnapshotService()
 * @see SnapshotOptions
 * 
 * @since GemFire 7.0
 */
public interface CacheSnapshotService {
  /**
   * Creates a <code>SnapshotOptions</code> object configured with default
   * settings. The options can be used to configure snapshot behavior.
   * 
   * @return the default options
   */
  SnapshotOptions<Object, Object> createOptions();
  
  /**
   * Exports all regions in the cache to the specified directory.  The cache
   * entries in each region will be written to a separate file.
   * 
   * @param dir the directory for writing the snapshots, will be created if
   *            necessary
   * @param format the snapshot format
   * 
   * @throws IOException error writing snapshot
   */
  void save(File dir, SnapshotFormat format)
  throws IOException;

  /**
   * Exports all regions in the cache to the specified directory by applying
   * user-configured options.  The cache entries in each region will be written 
   * to a separate file.
   * 
   * @param dir the directory for writing the snapshots, will be created if
   *            necessary
   * @param format the snapshot format
   * @param options the snapshot options
   * 
   * @throws IOException error writing snapshot
   */
  void save(File dir, SnapshotFormat format, SnapshotOptions<Object, Object> options) 
  throws IOException;  
  
  /**
   * Imports all files in the specified directory into the cache. The cache
   * entries in a given snapshot file are loaded into the same region they were
   * originally exported from (based on a corresponding region name). Files that
   * do not match the supplied snapshot format will cause an import error.
   * <p>
   * Prior to loading data, all regions should have been created and 
   * any necessary serializers (either {@link DataSerializer} or 
   * {@link PdxSerializer}) and {@link Instantiator}s should have been 
   * registered.
   * 
   * @param dir the directory containing the snapshot files
   * @param format the snapshot file format
   * 
   * @throws IOException Unable to import data
   * @throws ClassNotFoundException Unable to import data
   */
  void load(File dir, SnapshotFormat format) 
  throws IOException, ClassNotFoundException;

  /**
   * Imports the specified files into the cache by applying user-configured
   * options. The cache entries in a given snapshot file are loaded into the 
   * same region they were originally exported from (based on a corresponding 
   * region name). Files that do not match the supplied snapshot format will 
   * cause an import error.
   * <p>
   * Prior to loading data, all regions should have been created and any 
   * necessary serializers (either {@link DataSerializer} or 
   * {@link PdxSerializer}) and {@link Instantiator}s should have been 
   * registered.
   * 
   * @param snapshots the snapshot files
   * @param format the snapshot file format
   * @param options the snapshot options
   *
   * @throws IOException Unable to import data
   * @throws ClassNotFoundException Unable to import data
   */
  void load(File[] snapshots, SnapshotFormat format, SnapshotOptions<Object, Object> options) 
  throws IOException, ClassNotFoundException;  
}
