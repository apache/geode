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
package org.apache.geode.internal.cache.xmlcache;

import java.io.File;
import java.io.Serializable;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.internal.cache.DiskStoreAttributes;
import org.apache.geode.internal.cache.DiskStoreFactoryImpl;
import org.apache.geode.internal.cache.UserSpecifiedDiskStoreAttributes;

/**
 * Represents {@link DiskStoreAttributes} that are created declaratively. Notice that it implements
 * the {@link DiskStore} interface so that this class must be updated when {@link DiskStore} is
 * modified. This class is public for testing purposes.
 *
 *
 * @since GemFire prPersistSprint2
 */
public class DiskStoreAttributesCreation extends UserSpecifiedDiskStoreAttributes
    implements Serializable, DiskStore {

  /*
   * An <code>AttributesFactory</code> for creating default <code>RegionAttribute</code>s
   */
  // private static final DiskStoreFactory defaultFactory = new DiskStoreFactoryImpl();

  /**
   * Creates a new <code>DiskStoreCreation</code> with the default region attributes.
   */
  public DiskStoreAttributesCreation() {}

  /**
   * Creates a new <code>DiskStoreAttributesCreation</code> with the given disk store attributes.
   * NOTE: Currently attrs will not be an instance of DiskStoreAttributesCreation. If it could be
   * then this code should be changed to use attrs' hasXXX methods to initialize the has booleans
   * when defaults is false.
   *
   * @param attrs the attributes with which to initialize this DiskStore.
   */
  public DiskStoreAttributesCreation(DiskStoreAttributes attrs) {
    this.name = attrs.getName();
    this.autoCompact = attrs.getAutoCompact();
    this.compactionThreshold = attrs.getCompactionThreshold();
    this.allowForceCompaction = attrs.getAllowForceCompaction();
    this.maxOplogSizeInBytes = attrs.getMaxOplogSizeInBytes();
    this.timeInterval = attrs.getTimeInterval();
    this.writeBufferSize = attrs.getWriteBufferSize();
    this.queueSize = attrs.getQueueSize();
    this.diskDirs = attrs.getDiskDirs();
    this.diskDirSizes = attrs.getDiskDirSizes();

    setDiskUsageWarningPercentage(attrs.getDiskUsageWarningPercentage());
    setDiskUsageCriticalPercentage(attrs.getDiskUsageCriticalPercentage());

    if (attrs instanceof UserSpecifiedDiskStoreAttributes) {
      // Selectively set has* fields to true, propagating those non-default
      // (aka user specified) fields as such
      UserSpecifiedDiskStoreAttributes nonDefault = (UserSpecifiedDiskStoreAttributes) attrs;
      initHasFields(nonDefault);
    } else {
      // Set all fields to true
      setAllHasFields(true);
    }
  }

  ////////////////////// Instance Methods //////////////////////

  /**
   * Returns whether or not two objects are {@linkplain Object#equals equals} taking
   * <code>null</code> into account.
   */
  static boolean equal(Object o1, Object o2) {
    if (o1 == null) {
      if (o2 != null) {
        return false;

      } else {
        return true;
      }

    } else {
      return o1.equals(o2);
    }
  }

  /**
   * returns true if two long[] are equal
   *
   * @return true if equal
   */
  private boolean equal(long[] array1, long[] array2) {
    if (array1.length != array2.length) {
      return false;
    }
    for (int i = 0; i < array1.length; i++) {
      if (array1[i] != array2[i]) {
        return false;
      }
    }
    return true;
  }


  /**
   * returns true if two int[] are equal
   *
   * @return true if equal
   */
  private boolean equal(int[] array1, int[] array2) {
    if (array1.length != array2.length) {
      return false;
    }
    for (int i = 0; i < array1.length; i++) {
      if (array1[i] != array2[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns whether or not two <code>File</code> arrays specify the same files.
   */
  private boolean equal(File[] array1, File[] array2) {
    if (array1.length != array2.length) {
      return false;
    }

    for (int i = 0; i < array1.length; i++) {
      boolean found = false;
      for (int j = 0; j < array2.length; j++) {
        if (equal(array1[i].getAbsoluteFile(), array2[j].getAbsoluteFile())) {
          found = true;
          break;
        }
      }

      if (!found) {
        StringBuffer sb = new StringBuffer();
        sb.append("Didn't find ");
        sb.append(array1[i]);
        sb.append(" in ");
        for (int k = 0; k < array2.length; k++) {
          sb.append(array2[k]);
          sb.append(" ");
        }
        System.out.println(sb);
        return false;
      }
    }

    return true;
  }

  /**
   * Returns whether or not this <code>DiskStoreCreation</code> is equivalent to another
   * <code>DiskStore</code>.
   */
  public boolean sameAs(DiskStore other) {
    if (this.autoCompact != other.getAutoCompact()) {
      throw new RuntimeException(
          String.format("AutoCompact of disk store %s is not the same: this: %s other: %s",
              new Object[] {name, this.autoCompact, other.getAutoCompact()}));
    }
    if (this.compactionThreshold != other.getCompactionThreshold()) {
      throw new RuntimeException(
          String.format(
              "CompactionThreshold of disk store %s is not the same: this: %s other: %s",

              new Object[] {name, this.compactionThreshold, other.getCompactionThreshold()}));
    }
    if (this.allowForceCompaction != other.getAllowForceCompaction()) {
      throw new RuntimeException(
          String.format(
              "AllowForceCompaction of disk store %s is not the same: this: %s other: %s",

              new Object[] {name, this.allowForceCompaction, other.getAllowForceCompaction()}));
    }
    if (this.maxOplogSizeInBytes != other.getMaxOplogSize() * 1024 * 1024) {
      throw new RuntimeException(
          String.format("MaxOpLogSize of disk store %s is not the same: this: %s other: %s",
              new Object[] {name, this.maxOplogSizeInBytes / 1024 / 1024,
                  other.getMaxOplogSize()}));
    }
    if (this.timeInterval != other.getTimeInterval()) {
      throw new RuntimeException(
          String.format("TimeInterval of disk store %s is not the same: this: %s other: %s",
              new Object[] {name, this.timeInterval, other.getTimeInterval()}));
    }
    if (this.writeBufferSize != other.getWriteBufferSize()) {
      throw new RuntimeException(
          String.format("WriteBufferSize of disk store %s is not the same: this: %s other: %s",

              new Object[] {name, this.writeBufferSize, other.getWriteBufferSize()}));
    }
    if (this.queueSize != other.getQueueSize()) {
      throw new RuntimeException(
          String.format("QueueSize of disk store %s is not the same: this: %s other: %s",
              new Object[] {name, this.queueSize, other.getQueueSize()}));
    }
    if (!equal(this.diskDirs, other.getDiskDirs())) {
      throw new RuntimeException(
          String.format("Disk Dirs of disk store %s are not the same",
              name));
    }
    if (!equal(this.diskDirSizes, other.getDiskDirSizes())) {
      throw new RuntimeException(
          String.format("Disk Dir Sizes of disk store %s are not the same",
              name));
    }
    if (!equal(getDiskUsageWarningPercentage(), other.getDiskUsageWarningPercentage())) {
      throw new RuntimeException(
          String.format("Disk usage warning percentages of disk store %s are not the same",
              name));
    }
    if (!equal(getDiskUsageCriticalPercentage(), other.getDiskUsageCriticalPercentage())) {
      throw new RuntimeException(
          String.format("Disk usage critical percentages of disk store %s are not the same",
              name));
    }
    return true;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setAutoCompact(boolean autoCompact) {
    this.autoCompact = autoCompact;
    this.setHasAutoCompact(true);
  }

  public void setCompactionThreshold(int compactionThreshold) {
    this.compactionThreshold = compactionThreshold;
    this.setHasCompactionThreshold(true);
  }

  public void setAllowForceCompaction(boolean allowForceCompaction) {
    this.allowForceCompaction = allowForceCompaction;
    this.setHasAllowForceCompaction(true);
  }

  public void setMaxOplogSize(long maxOplogSize) {
    this.maxOplogSizeInBytes = maxOplogSize * 1024 * 1024;
    this.setHasMaxOplogSize(true);
  }

  public void setTimeInterval(long timeInterval) {
    this.timeInterval = timeInterval;
    this.setHasTimeInterval(true);
  }

  public void setWriteBufferSize(int writeBufferSize) {
    this.writeBufferSize = writeBufferSize;
    this.setHasWriteBufferSize(true);
  }

  public void setQueueSize(int queueSize) {
    this.queueSize = queueSize;
    this.setHasQueueSize(true);
  }

  public void setDiskDirs(File[] diskDirs) {
    checkIfDirectoriesExist(diskDirs);
    this.diskDirs = diskDirs;
    this.diskDirSizes = new int[diskDirs.length];
    for (int i = 0; i < diskDirs.length; i++) {
      this.diskDirSizes[i] = DiskStoreFactory.DEFAULT_DISK_DIR_SIZE;
    }
    setHasDiskDirs(true);
  }

  public void setDiskDirsAndSize(File[] diskDirs, int[] sizes) {
    checkIfDirectoriesExist(diskDirs);
    this.diskDirs = diskDirs;
    if (sizes.length != this.diskDirs.length) {
      throw new IllegalArgumentException(
          String.format(
              "Number of diskSizes is %s which is not equal to number of disk Dirs which is %s",

              new Object[] {Integer.valueOf(sizes.length), Integer.valueOf(diskDirs.length)}));
    }
    verifyNonNegativeDirSize(sizes);
    this.diskDirSizes = sizes;
    this.setHasDiskDirs(true);
  }

  @Override
  public void setDiskUsageWarningPercentage(float diskUsageWarningPercentage) {
    super.setDiskUsageWarningPercentage(diskUsageWarningPercentage);
    this.setHasDiskUsageWarningPercentage(true);
  }

  @Override
  public void setDiskUsageCriticalPercentage(float diskUsageCriticalPercentage) {
    super.setDiskUsageCriticalPercentage(diskUsageCriticalPercentage);
    this.setHasDiskUsageCriticalPercentage(true);
  }

  /**
   * Checks if directories exist
   *
   */
  private void checkIfDirectoriesExist(File[] disk_dirs) {
    // for (int i=0; i < disk_dirs.length; i++) {
    // if (! disk_dirs[i].isDirectory()) {
    //// throw new
    // IllegalArgumentException(String.format("%s was not an existing directory for disk store
    // %s.",new
    // Object[] {disk_dirs[i], name}));
    // if (!diskDirs[i].mkdirs()) {
    // throw new RuntimeException("Cannot create directory" + diskDirs[i].getAbsolutePath() + "Num
    // disk dirs to be created : " + disk_dirs.length + " Dir Name " + disk_dirs[i].getName());
    // }
    // }
    // }
    DiskStoreFactoryImpl.checkIfDirectoriesExist(disk_dirs);
  }

  private void verifyNonNegativeDirSize(int[] sizes) {
    for (int i = 0; i < sizes.length; i++) {
      if (sizes[i] < 0) {
        throw new IllegalArgumentException(
            String.format("Dir size cannot be negative : %s for disk store %s",
                new Object[] {Integer.valueOf(sizes[i]), name}));
      }
    }
  }
}
