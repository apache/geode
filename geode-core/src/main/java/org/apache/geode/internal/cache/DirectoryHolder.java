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
package org.apache.geode.internal.cache;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.VisibleForTesting;

/**
 * A holder for a disk Directory. Used for maintaining the available space and updating disk
 * statistics
 *
 * @since GemFire 5.1
 *
 */
public class DirectoryHolder {



  private final File dir;

  /** capacity of directory in bytes **/
  private final long capacity;

  /** Total size of oplogs in bytes **/
  private final AtomicLong totalOplogSize = new AtomicLong();

  private final int index;

  /** The stats for this region */
  private final DiskDirectoryStats dirStats;

  /** For testing purposes we can set the disk directory size in bytes **/
  private final DiskDirSizesUnit diskDirSizesUnit;

  DirectoryHolder(StatisticsFactory factory, File dir, long space, int index) {
    this(dir.getPath(), factory, dir, space, index);
  }

  DirectoryHolder(String ownersName, StatisticsFactory factory, File dir, long space, int index) {
    this(ownersName, factory, dir, space, index, DiskDirSizesUnit.MEGABYTES);
  }

  @VisibleForTesting
  DirectoryHolder(String ownersName, StatisticsFactory factory, File dir, long space, int index,
      DiskDirSizesUnit unit) {
    this.dir = dir;
    diskDirSizesUnit = unit;
    if (diskDirSizesUnit == DiskDirSizesUnit.BYTES) {
      capacity = space;
    } else if (diskDirSizesUnit == DiskDirSizesUnit.MEGABYTES) {
      // convert megabytes to bytes
      capacity = space * 1024 * 1024;
    } else {
      throw new IllegalArgumentException(
          "Invalid value for disk size units. Only megabytes and bytes are accepted.");
    }
    this.index = index;
    dirStats = new DiskDirectoryStats(factory, ownersName);
    dirStats.setMaxSpace(capacity);
  }

  public long getUsedSpace() {
    return totalOplogSize.get();
  }

  public long getAvailableSpace() {
    return getCapacity() - getUsedSpace();
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("dir=").append(getDir()).append(" maxSpace=").append(getCapacity())
        .append(" usedSpace=").append(getUsedSpace()).append(" availableSpace=")
        .append(getAvailableSpace());
    return sb.toString();
  }

  public void incrementTotalOplogSize(long incrementSize) {
    totalOplogSize.addAndGet(incrementSize);
    dirStats.incDiskSpace(incrementSize);
  }

  public void decrementTotalOplogSize(long decrementSize) {
    totalOplogSize.addAndGet(-decrementSize);
    dirStats.incDiskSpace(-decrementSize);
  }

  public File getDir() {
    return dir;
  }

  public int getArrayIndex() {
    return index;
  }

  public long getCapacity() {
    return capacity;
  }

  public void close() {
    dirStats.close();
  }

  // Added for the stats checking test in OplogJUnitTest
  public long getDirStatsDiskSpaceUsage() {
    return dirStats.getDiskSpace();
  }

  public DiskDirectoryStats getDiskDirectoryStats() {
    return dirStats;
  }
}
