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
package org.apache.geode.internal.cache;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.geode.StatisticsFactory;

/**
 * A holder for a disk Directory. Used for maintaining the available space and
 * updating disk statistics
 * 
 * @since GemFire 5.1
 * 
 */
public class DirectoryHolder
{

  private final File dir;

  /** capacity of directory in bytes **/
  private final long capacity;
  
  /** Total size of oplogs in bytes **/
  private final AtomicLong totalOplogSize = new AtomicLong();
  
  private int index;
  
  /** The stats for this region */
  private final DiskDirectoryStats dirStats;

  /** For testing purposes we can set the disk directory size in bytes **/
  static boolean SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = false;
  
  DirectoryHolder(StatisticsFactory factory, File dir, long space, int index) {
    this(dir.getPath(), factory, dir, space, index);
  }
  DirectoryHolder(String ownersName, StatisticsFactory factory, File dir, long space, int index) {
    this.dir = dir;
    if (SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES) {
      this.capacity = space;
    }
    else {
      //convert megabytes to bytes
      this.capacity = space * 1024 * 1024;
    }
    this.index = index;
    this.dirStats = new DiskDirectoryStats(factory, ownersName);
    this.dirStats.setMaxSpace(this.capacity);
  }

  public long getUsedSpace() {
    return this.totalOplogSize.get();
  }
  public long getAvailableSpace() {
    return getCapacity() - getUsedSpace();
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("dir=").append(getDir())
      .append(" maxSpace=").append(getCapacity())
      .append(" usedSpace=").append(getUsedSpace())
      .append(" availableSpace=").append(getAvailableSpace());
    return sb.toString();
  }

  public void incrementTotalOplogSize(long incrementSize)
  {
    this.totalOplogSize.addAndGet(incrementSize);
    this.dirStats.incDiskSpace(incrementSize);
  }

  public void decrementTotalOplogSize(long decrementSize)
  {
    this.totalOplogSize.addAndGet(-decrementSize);
    this.dirStats.incDiskSpace(-decrementSize);
  }

  public File getDir()
  {
    return dir;
  }

  int getArrayIndex()
  {
    return this.index;
  }

  public long getCapacity()
  {
    return capacity;
  }
  
  public void close() {
    this.dirStats.close();
  }

  //Added for the stats checking test in OplogJUnitTest
  public long getDirStatsDiskSpaceUsage() {
	return this.dirStats.getDiskSpace();
  }
  
  public DiskDirectoryStats getDiskDirectoryStats() {
    return dirStats;
  }
}
