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

import static org.apache.geode.internal.cache.DiskStoreAttributes.checkMinAndMaxOplogSize;
import static org.apache.geode.internal.cache.DiskStoreAttributes.checkMinOplogSize;
import static org.apache.geode.internal.cache.DiskStoreAttributes.checkQueueSize;
import static org.apache.geode.internal.cache.DiskStoreAttributes.checkTimeInterval;
import static org.apache.geode.internal.cache.DiskStoreAttributes.checkWriteBufferSize;
import static org.apache.geode.internal.cache.DiskStoreAttributes.verifyNonNegativeDirSize;

import java.io.File;
import java.util.Arrays;

import org.apache.geode.GemFireIOException;
import org.apache.geode.annotations.VisibleForTesting;
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

  @VisibleForTesting
  public DiskStoreFactory setDiskDirSizesUnit(DiskDirSizesUnit unit) {
    attrs.setDiskDirSizesUnit(unit);
    return this;
  }

  @Override
  public DiskStoreFactory setAutoCompact(boolean autoCompact) {
    attrs.autoCompact = autoCompact;
    return this;
  }

  @Override
  public DiskStoreFactory setAllowForceCompaction(boolean allowForceCompaction) {
    attrs.allowForceCompaction = allowForceCompaction;
    return this;
  }

  @Override
  public DiskStoreFactory setCompactionThreshold(int compactionThreshold) {
    if (compactionThreshold < 0) {
      throw new IllegalArgumentException(
          String.format("%s has to be positive number and the value given %s is not acceptable",
              CacheXml.COMPACTION_THRESHOLD, compactionThreshold));
    } else if (compactionThreshold > 100) {
      throw new IllegalArgumentException(
          String.format(
              "%s has to be a number that does not exceed %s so the value given %s is not acceptable",
              CacheXml.COMPACTION_THRESHOLD, compactionThreshold, 100));
    }
    attrs.compactionThreshold = compactionThreshold;
    return this;
  }

  @Override
  public DiskStoreFactory setTimeInterval(long timeInterval) {
    checkTimeInterval(timeInterval);
    attrs.timeInterval = timeInterval;
    return this;
  }

  DiskStoreImpl createOwnedByRegion(String name, boolean isOwnedByPR,
      InternalRegionArguments internalRegionArgs) {
    attrs.name = name;
    synchronized (cache) {
      DiskStoreImpl ds =
          new DiskStoreImpl(cache, attrs, true/* ownedByRegion */, internalRegionArgs);
      if (isOwnedByPR) {
        initializeDiskStore(ds);
      }
      cache.addRegionOwnedDiskStore(ds);
      return ds;
    }
  }

  @Override
  public DiskStore create(String name) {
    attrs.name = name;
    DiskStore result;
    try {
      cache.lockDiskStore(name);
      result = findExisting(name);
      if (result == null) {
        if (cache instanceof GemFireCacheImpl) {
          TypeRegistry registry = cache.getPdxRegistry();
          DiskStoreImpl dsi = new DiskStoreImpl(cache, attrs);
          result = dsi;
          // Added for M&M
          cache.getInternalDistributedSystem()
              .handleResourceEvent(ResourceEvent.DISKSTORE_CREATE, dsi);
          initializeDiskStore(dsi);
          cache.addDiskStore(dsi);
          if (registry != null) {
            registry.creatingDiskStore(dsi);
          }
        } else if (cache instanceof CacheCreation) {
          CacheCreation creation = (CacheCreation) cache;
          result = new DiskStoreAttributesCreation(attrs);
          creation.addDiskStore(result);
        }
      }
    } finally {
      cache.unlockDiskStore(name);
    }

    // Don't allow this disk store to be created
    // until an in progress backup is completed. This
    // ensures that nothing that is backed up on another
    // member depends on state that goes into this disk store
    // that isn't backed up.
    if (cache instanceof GemFireCacheImpl) {
      BackupService backup = cache.getBackupService();
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
    DiskStore existing;
    if (cache instanceof GemFireCacheImpl) {
      existing = cache.findDiskStore(name);
      if (existing != null) {
        if (((DiskStoreImpl) existing).sameAs(attrs)) {
          return existing;
        } else {
          throw new IllegalStateException("DiskStore named \"" + name + "\" already exists");
        }
      }
    }
    return null;
  }

  @Override
  public DiskStoreFactory setDiskDirsAndSizes(File[] diskDirs, int[] diskDirSizes) {
    if (diskDirSizes.length != diskDirs.length) {
      throw new IllegalArgumentException(
          String.format(
              "Number of diskSizes is %s which is not equal to number of disk Dirs which is %s",
              diskDirSizes.length, diskDirs.length));
    }
    verifyNonNegativeDirSize(diskDirSizes);
    checkIfDirectoriesExist(diskDirs);

    attrs.diskDirs = new File[diskDirs.length];
    System.arraycopy(diskDirs, 0, attrs.diskDirs, 0, diskDirs.length);
    attrs.diskDirSizes = new int[diskDirSizes.length];
    System.arraycopy(diskDirSizes, 0, attrs.diskDirSizes, 0, diskDirSizes.length);
    return this;
  }

  /**
   * Checks if directories exist, if they don't then create those directories
   */
  public static void checkIfDirectoriesExist(File[] diskDirs) {
    for (File diskDir : diskDirs) {
      if (!diskDir.isDirectory()) {
        if (!diskDir.mkdirs()) {
          throw new GemFireIOException(
              String.format("Unable to create directory : %s",
                  diskDir));
        }
      }
    }
  }



  @Override
  public DiskStoreFactory setDiskDirs(File[] diskDirs) {
    checkIfDirectoriesExist(diskDirs);
    int[] diskSizes = new int[diskDirs.length];
    Arrays.fill(diskSizes, DEFAULT_DISK_DIR_SIZE);
    return setDiskDirsAndSizes(diskDirs, diskSizes);
  }

  @Override
  public DiskStoreFactory setMaxOplogSize(long maxOplogSize) {
    checkMinAndMaxOplogSize(maxOplogSize);
    attrs.maxOplogSizeInBytes = maxOplogSize * (1024 * 1024);
    return this;
  }

  /**
   * Used by unit tests
   */
  public DiskStoreFactory setMaxOplogSizeInBytes(long maxOplogSizeInBytes) {
    checkMinOplogSize(maxOplogSizeInBytes);
    attrs.maxOplogSizeInBytes = maxOplogSizeInBytes;
    return this;
  }

  @Override
  public DiskStoreFactory setQueueSize(int queueSize) {
    checkQueueSize(queueSize);
    attrs.queueSize = queueSize;
    return this;
  }

  @Override
  public DiskStoreFactory setWriteBufferSize(int writeBufferSize) {
    checkWriteBufferSize(writeBufferSize);
    attrs.writeBufferSize = writeBufferSize;
    return this;
  }

  // used by hydra
  public DiskStoreAttributes getDiskStoreAttributes() {
    return attrs;
  }

  @Override
  public DiskStoreFactory setDiskUsageWarningPercentage(float warningPercent) {
    attrs.setDiskUsageWarningPercentage(warningPercent);
    return this;
  }

  @Override
  public DiskStoreFactory setDiskUsageCriticalPercentage(float criticalPercent) {
    attrs.setDiskUsageCriticalPercentage(criticalPercent);
    return this;
  }
}
