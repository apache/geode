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
import java.util.Arrays;

import org.apache.geode.GemFireIOException;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.internal.cache.backup.BackupService;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.DiskStoreAttributesCreation;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * Implementation of DiskStoreFactory
 *
 * @since GemFire prPersistSprint2
 */
public class DiskStoreFactoryImpl implements DiskStoreFactory {

  private final InternalCache cache;
  private final DiskStoreAttributes attrs = new DiskStoreAttributes();

  public DiskStoreFactoryImpl(InternalCache cache) {
    this.cache = cache;
  }

  public DiskStoreFactoryImpl(InternalCache cache, DiskStoreAttributes attrs) {
    this.attrs.name = attrs.name;
    setAutoCompact(attrs.getAutoCompact());
    setAllowForceCompaction(attrs.getAllowForceCompaction());
    setCompactionThreshold(attrs.getCompactionThreshold());
    setMaxOplogSizeInBytes(attrs.getMaxOplogSizeInBytes());
    setTimeInterval(attrs.getTimeInterval());
    setWriteBufferSize(attrs.getWriteBufferSize());
    setQueueSize(attrs.getQueueSize());
    setDiskDirs(cloneArray(attrs.getDiskDirs()));
    setDiskDirsAndSizes(cloneArray(attrs.getDiskDirs()), cloneArray(attrs.getDiskDirSizes()));
    setDiskUsageWarningPercentage(attrs.getDiskUsageWarningPercentage());
    setDiskUsageCriticalPercentage(attrs.getDiskUsageCriticalPercentage());
    this.cache = cache;
  }

  private static File[] cloneArray(File[] o) {
    File[] result = null;
    if (o != null) {
      result = new File[o.length];
      System.arraycopy(o, 0, result, 0, o.length);
    }
    return result;
  }

  private static int[] cloneArray(int[] o) {
    int[] result = null;
    if (o != null) {
      result = new int[o.length];
      System.arraycopy(o, 0, result, 0, o.length);
    }
    return result;
  }

  public DiskStoreFactory setAutoCompact(boolean autoCompact) {
    this.attrs.autoCompact = autoCompact;
    return this;
  }

  public DiskStoreFactory setAllowForceCompaction(boolean allowForceCompaction) {
    this.attrs.allowForceCompaction = allowForceCompaction;
    return this;
  }

  public DiskStoreFactory setCompactionThreshold(int compactionThreshold) {
    if (compactionThreshold < 0) {
      throw new IllegalArgumentException(
          String.format("%s has to be positive number and the value given %s is not acceptable",

              new Object[] {CacheXml.COMPACTION_THRESHOLD, compactionThreshold}));
    } else if (compactionThreshold > 100) {
      throw new IllegalArgumentException(
          String.format(
              "%s has to be a number that does not exceed %s so the value given %s is not acceptable",

              new Object[] {CacheXml.COMPACTION_THRESHOLD, compactionThreshold, 100}));
    }
    this.attrs.compactionThreshold = compactionThreshold;
    return this;
  }

  public DiskStoreFactory setTimeInterval(long timeInterval) {
    if (timeInterval < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Time Interval specified has to be a non-negative number and the value given %s is not acceptable",
              timeInterval));
    }
    this.attrs.timeInterval = timeInterval;
    return this;
  }

  DiskStoreImpl createOwnedByRegion(String name, boolean isOwnedByPR,
      InternalRegionArguments internalRegionArgs) {
    this.attrs.name = name;
    synchronized (this.cache) {
      DiskStoreImpl ds =
          new DiskStoreImpl(this.cache, this.attrs, true/* ownedByRegion */, internalRegionArgs);
      if (isOwnedByPR) {
        initializeDiskStore(ds);
      }
      this.cache.addRegionOwnedDiskStore(ds);
      return ds;
    }
  }

  public DiskStore create(String name) {
    this.attrs.name = name;
    // As a simple fix for 41290, only allow one DiskStore to be created
    // at a time per cache by syncing on the cache.
    DiskStore result;
    synchronized (this.cache) {
      result = findExisting(name);
      if (result == null) {
        if (this.cache instanceof GemFireCacheImpl) {
          TypeRegistry registry = this.cache.getPdxRegistry();
          DiskStoreImpl dsi = new DiskStoreImpl(this.cache, this.attrs);
          result = dsi;
          // Added for M&M
          this.cache.getInternalDistributedSystem()
              .handleResourceEvent(ResourceEvent.DISKSTORE_CREATE, dsi);
          initializeDiskStore(dsi);
          this.cache.addDiskStore(dsi);
          if (registry != null) {
            registry.creatingDiskStore(dsi);
          }
        } else if (this.cache instanceof CacheCreation) {
          CacheCreation creation = (CacheCreation) this.cache;
          result = new DiskStoreAttributesCreation(this.attrs);
          creation.addDiskStore(result);
        }
      }
    }

    // Don't allow this disk store to be created
    // until an in progress backup is completed. This
    // ensures that nothing that is backed up on another
    // member depends on state that goes into this disk store
    // that isn't backed up.
    if (this.cache instanceof GemFireCacheImpl) {
      BackupService backup = this.cache.getBackupService();
      if (backup != null) {
        backup.waitForBackup();
      }
    }
    return result;
  }

  /**
   * Protected for testing purposes. If during the initial recovery for the disk store, an uncaught
   * exception is thrown, the disk store will not be in a valid state. In this case, we want to
   * ensure the resources of the disk store are cleaned up.
   */
  protected void initializeDiskStore(DiskStoreImpl diskStore) {
    try {
      diskStore.doInitialRecovery();
    } catch (RuntimeException e) {
      diskStore.close();
      throw e;
    }
  }

  private DiskStore findExisting(String name) {
    DiskStore existing = null;
    if (this.cache instanceof GemFireCacheImpl) {
      existing = this.cache.findDiskStore(name);
      if (existing != null) {
        if (((DiskStoreImpl) existing).sameAs(this.attrs)) {
          return existing;
        } else {
          throw new IllegalStateException("DiskStore named \"" + name + "\" already exists");
        }
      }
    }
    return existing;
  }

  public DiskStoreFactory setDiskDirsAndSizes(File[] diskDirs, int[] diskDirSizes) {
    if (diskDirSizes.length != diskDirs.length) {
      throw new IllegalArgumentException(
          String.format(
              "Number of diskSizes is %s which is not equal to number of disk Dirs which is %s",
              new Object[] {diskDirSizes.length, diskDirs.length}));
    }
    verifyNonNegativeDirSize(diskDirSizes);
    checkIfDirectoriesExist(diskDirs);

    this.attrs.diskDirs = new File[diskDirs.length];
    System.arraycopy(diskDirs, 0, this.attrs.diskDirs, 0, diskDirs.length);
    this.attrs.diskDirSizes = new int[diskDirSizes.length];
    System.arraycopy(diskDirSizes, 0, this.attrs.diskDirSizes, 0, diskDirSizes.length);
    return this;
  }

  /**
   * Checks if directories exist, if they don't then create those directories
   */
  public static void checkIfDirectoriesExist(File[] diskDirs) {
    for (int i = 0; i < diskDirs.length; i++) {
      if (!diskDirs[i].isDirectory()) {
        if (!diskDirs[i].mkdirs()) {
          throw new GemFireIOException(
              String.format("Unable to create directory : %s",
                  diskDirs[i]));
        }
      }
    }
  }


  /**
   * Verify all directory sizes are positive
   */
  public static void verifyNonNegativeDirSize(int[] sizes) {
    for (int i = 0; i < sizes.length; i++) {
      if (sizes[i] < 0) {
        throw new IllegalArgumentException(
            String.format("Dir size cannot be negative : %s",
                sizes[i]));
      }
    }
  }

  public DiskStoreFactory setDiskDirs(File[] diskDirs) {
    checkIfDirectoriesExist(diskDirs);
    int[] diskSizes = new int[diskDirs.length];
    Arrays.fill(diskSizes, DEFAULT_DISK_DIR_SIZE);
    return setDiskDirsAndSizes(diskDirs, diskSizes);
  }

  public DiskStoreFactory setMaxOplogSize(long maxOplogSize) {
    long MAX = Long.MAX_VALUE / (1024 * 1024);
    if (maxOplogSize > MAX) {
      throw new IllegalArgumentException(
          String.format(
              "%s has to be a number that does not exceed %s so the value given %s is not acceptable",
              new Object[] {"max oplog size", maxOplogSize, MAX}));
    } else if (maxOplogSize < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Maximum Oplog size specified has to be a non-negative number and the value given %s is not acceptable",
              maxOplogSize));
    }
    this.attrs.maxOplogSizeInBytes = maxOplogSize * (1024 * 1024);
    return this;
  }

  /**
   * Used by unit tests
   */
  public DiskStoreFactory setMaxOplogSizeInBytes(long maxOplogSizeInBytes) {
    if (maxOplogSizeInBytes < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Maximum Oplog size specified has to be a non-negative number and the value given %s is not acceptable",
              maxOplogSizeInBytes));
    }
    this.attrs.maxOplogSizeInBytes = maxOplogSizeInBytes;
    return this;
  }

  public DiskStoreFactory setQueueSize(int queueSize) {
    if (queueSize < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Queue size specified has to be a non-negative number and the value given %s is not acceptable",
              queueSize));
    }
    this.attrs.queueSize = queueSize;
    return this;
  }

  public DiskStoreFactory setWriteBufferSize(int writeBufferSize) {
    if (writeBufferSize < 0) {
      // TODO add a message for WriteBufferSize
      throw new IllegalArgumentException(
          String.format(
              "Queue size specified has to be a non-negative number and the value given %s is not acceptable",
              writeBufferSize));
    }
    this.attrs.writeBufferSize = writeBufferSize;
    return this;
  }

  // used by hyda
  public DiskStoreAttributes getDiskStoreAttributes() {
    return this.attrs;
  }

  @Override
  public DiskStoreFactory setDiskUsageWarningPercentage(float warningPercent) {
    this.attrs.setDiskUsageWarningPercentage(warningPercent);
    return this;
  }

  @Override
  public DiskStoreFactory setDiskUsageCriticalPercentage(float criticalPercent) {
    this.attrs.setDiskUsageCriticalPercentage(criticalPercent);
    return this;
  }
}
