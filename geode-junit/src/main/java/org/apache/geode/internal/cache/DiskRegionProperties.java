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
/*
 * Created on Mar 3, 2006
 *
 * TODO To change the template for this generated file go to Window - Preferences - Java - Code
 * Style - Code Templates
 */
package org.apache.geode.internal.cache;

import java.io.File;


/**
 * A properties object used to create persistent/overflow regions for testing objects
 *
 * @since GemFire 5.1
 *
 */
public class DiskRegionProperties {

  private boolean isPersistBackup = false;
  private boolean isOverflow = false;
  private int overFlowCapacity = 1000;
  private int compactionThreshold = 50;
  private File[] diskDirs;
  private int[] diskDirSize;
  private long timeInterval = -1L;
  private long bytesThreshold = 0L;
  private boolean isRolling = true;
  private boolean allowForceCompaction = false;
  private long maxOplogSize = 1024 * 1024 * 1024 * 10L;
  private boolean isSynchronous = false;
  private String regionName = "testRegion";
  private int concurrencyLevel = 16;
  private int initialCapacity = 16;
  private float loadFactor = 0.75f;
  private boolean statisticsEnabled = false;
  private boolean isHeapEviction = false;

  public DiskRegionProperties() {}


  public long getBytesThreshold() {
    return bytesThreshold;
  }

  public File[] getDiskDirs() {
    return diskDirs;
  }

  public boolean isOverflow() {
    return isOverflow;
  }

  public boolean isHeapEviction() {
    return isHeapEviction;
  }

  public boolean isPersistBackup() {
    return isPersistBackup;
  }

  public boolean isRolling() {
    return isRolling;
  }

  public boolean getAllowForceCompaction() {
    return this.allowForceCompaction;
  }

  public int getCompactionThreshold() {
    return this.compactionThreshold;
  }

  public boolean isSynchronous() {
    return isSynchronous;
  }

  public long getMaxOplogSize() {
    return maxOplogSize;
  }

  public int getOverFlowCapacity() {
    return overFlowCapacity;
  }

  public long getTimeInterval() {
    return timeInterval;
  }

  public int[] getDiskDirSizes() {
    return diskDirSize;
  }

  public void setDiskDirsAndSizes(File[] diskDirs, int[] diskDirSize) {
    this.diskDirs = diskDirs;
    if (diskDirs == null) {
      this.diskDirSize = null;
    } else {
      this.diskDirSize = diskDirSize;
    }
  }

  public void setBytesThreshold(long bytesThreshold) {
    this.bytesThreshold = bytesThreshold;
  }

  public void setDiskDirs(File[] diskDirs) {
    if (diskDirs == null) {
      this.diskDirs = null;
      this.diskDirSize = null;
      return;
    }
    this.diskDirs = diskDirs;
  }

  public void setOverflow(boolean isOverflow) {
    this.isOverflow = isOverflow;
  }

  public void setHeapEviction(boolean isHeapEviction) {
    this.isHeapEviction = isHeapEviction;
  }

  public void setPersistBackup(boolean isPersistBackup) {
    this.isPersistBackup = isPersistBackup;
  }

  public void setRolling(boolean isRolling) {
    this.isRolling = isRolling;
  }

  public void setAllowForceCompaction(boolean v) {
    this.allowForceCompaction = v;
  }

  public void setCompactionThreshold(int v) {
    this.compactionThreshold = v;
  }

  public void setSynchronous(boolean isSynchronous) {
    this.isSynchronous = isSynchronous;
  }

  public void setMaxOplogSize(long maxOplogSize) {
    this.maxOplogSize = maxOplogSize;
  }

  public void setOverFlowCapacity(int overFlowCapacity) {
    this.overFlowCapacity = overFlowCapacity;
  }

  public void setTimeInterval(long timeInterval) {
    this.timeInterval = timeInterval;
  }

  public String getRegionName() {
    return regionName;
  }

  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  public void setStatisticsEnabled(boolean v) {
    this.statisticsEnabled = v;
  }

  public boolean getStatisticsEnabled() {
    return this.statisticsEnabled;
  }

  public void setConcurrencyLevel(int v) {
    this.concurrencyLevel = v;
  }

  public int getConcurrencyLevel() {
    return this.concurrencyLevel;
  }

  public void setInitialCapacity(int v) {
    this.initialCapacity = v;
  }

  public int getInitialCapacity() {
    return this.initialCapacity;
  }

  public void setLoadFactor(float v) {
    this.loadFactor = v;
  }

  public float getLoadFactor() {
    return this.loadFactor;
  }
}
