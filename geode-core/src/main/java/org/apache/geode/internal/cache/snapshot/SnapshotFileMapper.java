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

import java.io.File;
import java.io.Serializable;

import org.apache.geode.cache.snapshot.CacheSnapshotService;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.cache.snapshot.SnapshotOptions;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.distributed.DistributedMember;

/**
 * Defines a mapping between the snapshot path and the file(s) to be imported or
 * exported on a specific member.
 * 
 * @see SnapshotOptionsImpl#setMapper(SnapshotFileMapper)
 *
 */
public interface SnapshotFileMapper extends Serializable {
  /**
   * Invoked during a parallel export to map the supplied path to a new file name.
   * This is necessary when the export file must be written to a member-specific
   * location. The result of the mapping must resolve to a valid file path.
   * <p>
   * Example:
   * 
   * <pre>
   * public File mapExportFile(String regionPath, DistributedMember member, File snapshot) {
   *   return new File(snapshot.getAbsolutePath() + "-" + member.getId());
   * }
   * </pre>
   * 
   * If members are writing to a shared location, the implementation <b>must</b>
   * provide a unique file path to avoid an export error.
   * 
   * @param member
   *          the member performing the export
   * @param snapshot
   *          the path specified in the invocation of
   *          {@link RegionSnapshotService#save(File, org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat)
   *          RegionSnapshotService.load()} or
   *          {@link CacheSnapshotService#save(File, org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat)
   *          CacheSnapshotService.load()}
   * @return the mapped filename, or null to use the existing file
   */
  File mapExportPath(DistributedMember member, File snapshot);
  
  /**
   * Invoked during a parallel import to map the supplied path to one or more
   * files. This is necessary when the import files must be read from a
   * member-specific path or when the cache topology differs from the exporting
   * system. The result of the mapping must resolve to one or more valid file
   * paths.
   * <p>
   * Example:
   * <pre>
   * public File[] mapImportPath(DistributedMember member, File snapshot) {
   *   File dir = new File(snapshot, member.getHost() + ":" + member.getProcessId());
   *   return dir.listFiles();
   * }
   * </pre>
   * If members are reading from a shared location, the implementation should
   * partition the files so that a snapshot file is only imported once.
   * 
   * @param member
   *          the member performing the import
   * @param snapshot
   *          the path specified in the invocation of
   *          {@link RegionSnapshotService#load(File, org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat)
   *          RegionSnapshotService.load()} or
   *          {@link CacheSnapshotService#load(File, org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat)
   *          CacheSnapshotService.load()}
   * @return the list of files to import
   */
  File[] mapImportPath(DistributedMember member, File snapshot);
}
