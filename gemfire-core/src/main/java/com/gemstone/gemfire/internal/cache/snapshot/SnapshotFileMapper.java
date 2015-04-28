/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.snapshot;

import java.io.File;
import java.io.Serializable;

import com.gemstone.gemfire.cache.snapshot.CacheSnapshotService;
import com.gemstone.gemfire.cache.snapshot.RegionSnapshotService;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions;
import com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Defines a mapping between the snapshot path and the file(s) to be imported or
 * exported on a specific member.
 * 
 * @see SnapshotOptionsImpl#setMapper(SnapshotFileMapper)
 * @author bakera
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
   *          {@link RegionSnapshotService#save(File, com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat)
   *          RegionSnapshotService.load()} or
   *          {@link CacheSnapshotService#save(File, com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat)
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
   *          {@link RegionSnapshotService#load(File, com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat)
   *          RegionSnapshotService.load()} or
   *          {@link CacheSnapshotService#load(File, com.gemstone.gemfire.cache.snapshot.SnapshotOptions.SnapshotFormat)
   *          CacheSnapshotService.load()}
   * @return the list of files to import
   */
  File[] mapImportPath(DistributedMember member, File snapshot);
}
