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
package com.gemstone.gemfire.internal.cache;


import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.distributed.internal.ResourceEvent;
import com.gemstone.gemfire.internal.cache.DiskStoreAttributes;
import com.gemstone.gemfire.internal.cache.persistence.BackupManager;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.DiskStoreAttributesCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;

/**
 * Implementation of DiskStoreFactory 
 * 
 * @since GemFire prPersistSprint2
 */
public class DiskStoreFactoryImpl implements DiskStoreFactory
{
  private final Cache cache;
  private final DiskStoreAttributes attrs = new DiskStoreAttributes(); 

  public DiskStoreFactoryImpl(Cache cache) {
    this.cache = cache;
  }
  
  public DiskStoreFactoryImpl(Cache cache, DiskStoreAttributes attrs) {
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
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_POSITIVE_NUMBER_AND_THE_VALUE_GIVEN_1_IS_NOT_ACCEPTABLE.toLocalizedString(new Object[] {CacheXml.COMPACTION_THRESHOLD, Integer.valueOf(compactionThreshold)}));
    } else if (compactionThreshold > 100) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_LESS_THAN_2_BUT_WAS_1.toLocalizedString(new Object[] {CacheXml.COMPACTION_THRESHOLD, Integer.valueOf(compactionThreshold), Integer.valueOf(100)}));
    }
    this.attrs.compactionThreshold = compactionThreshold;
    return this;
  }
  
  public DiskStoreFactory setTimeInterval(long timeInterval) {
    if (timeInterval < 0) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesFactory_TIME_INTERVAL_SPECIFIED_HAS_TO_BE_A_NONNEGATIVE_NUMBER_AND_THE_VALUE_GIVEN_0_IS_NOT_ACCEPTABLE.toLocalizedString(Long.valueOf(timeInterval)));
    }
    this.attrs.timeInterval = timeInterval;
    return this;
  }

  DiskStoreImpl createOwnedByRegion(String name, boolean isOwnedByPR,
      InternalRegionArguments internalRegionArgs) {
    this.attrs.name = name;
    synchronized (this.cache) {
      assert this.cache instanceof GemFireCacheImpl;
      GemFireCacheImpl gfc = (GemFireCacheImpl)this.cache;
      DiskStoreImpl ds = new DiskStoreImpl(gfc, this.attrs,
          true/*ownedByRegion*/, internalRegionArgs);
      if (isOwnedByPR) {
        ds.doInitialRecovery();
      }
      gfc.addRegionOwnedDiskStore(ds);
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
          GemFireCacheImpl gfc = (GemFireCacheImpl)this.cache;
          TypeRegistry registry = gfc.getPdxRegistry();
          DiskStoreImpl dsi = new DiskStoreImpl(gfc, this.attrs);
          result = dsi;
          /** Added for M&M **/
          gfc.getDistributedSystem().handleResourceEvent(ResourceEvent.DISKSTORE_CREATE, dsi);
          dsi.doInitialRecovery();
          gfc.addDiskStore(dsi);
          if(registry != null) {
            registry.creatingDiskStore(dsi);
          }
        } else if (this.cache instanceof CacheCreation) {
          CacheCreation creation = (CacheCreation)this.cache;
          result = new DiskStoreAttributesCreation(this.attrs);
          creation.addDiskStore(result);
        }
      }
    }
    
    //Don't allow this disk store to be created
    //until an in progress backup is completed. This
    //ensures that nothing that is backed up on another
    //member depends on state that goes into this disk store
    //that isn't backed up.
    if (this.cache instanceof GemFireCacheImpl) {
      GemFireCacheImpl gfc = (GemFireCacheImpl)this.cache;
      BackupManager backup = gfc.getBackupManager();
      if(backup != null) {
        backup.waitForBackup();
      }
    }
    return result;
  }

  private DiskStore findExisting(String name) {
    DiskStore existing = null;
    if (this.cache instanceof GemFireCacheImpl) {
      GemFireCacheImpl gfc = (GemFireCacheImpl)this.cache;
      existing = gfc.findDiskStore(name);
      if (existing != null) {
        if (((DiskStoreImpl)existing).sameAs(this.attrs)) {
          return existing;
        } else {
          throw new IllegalStateException("DiskStore named \"" + name + "\" already exists");
        }
      }
    }
    return existing;
  }

  public DiskStoreFactory setDiskDirsAndSizes(File[] diskDirs,int[] diskDirSizes) {
    if (diskDirSizes.length != diskDirs.length) {
      throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_NUMBER_OF_DISKSIZES_IS_0_WHICH_IS_NOT_EQUAL_TO_NUMBER_OF_DISK_DIRS_WHICH_IS_1.toLocalizedString(new Object[] {Integer.valueOf(diskDirSizes.length), Integer.valueOf(diskDirs.length)}));
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
   * 
   * @param diskDirs
   */
  public static void checkIfDirectoriesExist(File[] diskDirs) {
    for (int i=0; i < diskDirs.length; i++) {
      if (! diskDirs[i].isDirectory()) {
        if (!diskDirs[i].mkdirs()) {
          throw new GemFireIOException(LocalizedStrings.AttributesFactory_UNABLE_TO_CREATE_DISK_STORE_DIRECTORY_0.toLocalizedString(diskDirs[i]));
        } 
      }
    }
  }
  
  
  /**
   * Verify all directory sizes are positive
   * @param sizes
   */
  public static void verifyNonNegativeDirSize(int[] sizes) {
    for(int i=0; i< sizes.length; i++){
      if(sizes[i]<0){
        throw new IllegalArgumentException(LocalizedStrings.AttributesFactory_DIR_SIZE_CANNOT_BE_NEGATIVE_0.toLocalizedString(Integer.valueOf(sizes[i])));
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
    long MAX = Long.MAX_VALUE/(1024*1024);
    if (maxOplogSize > MAX) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesImpl_0_HAS_TO_BE_LESS_THAN_2_BUT_WAS_1.toLocalizedString(new Object[] {"max oplog size", maxOplogSize, MAX}));
    } else if (maxOplogSize < 0) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesFactory_MAXIMUM_OPLOG_SIZE_SPECIFIED_HAS_TO_BE_A_NONNEGATIVE_NUMBER_AND_THE_VALUE_GIVEN_0_IS_NOT_ACCEPTABLE.toLocalizedString(Long.valueOf(maxOplogSize)));
    }
    this.attrs.maxOplogSizeInBytes = maxOplogSize * (1024 * 1024);
    return this;
  }

  /**
   * Used by unit tests
   */
  public DiskStoreFactory setMaxOplogSizeInBytes(long maxOplogSizeInBytes) {
    if (maxOplogSizeInBytes < 0) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesFactory_MAXIMUM_OPLOG_SIZE_SPECIFIED_HAS_TO_BE_A_NONNEGATIVE_NUMBER_AND_THE_VALUE_GIVEN_0_IS_NOT_ACCEPTABLE.toLocalizedString(Long.valueOf(maxOplogSizeInBytes)));
    }
    this.attrs.maxOplogSizeInBytes = maxOplogSizeInBytes;
    return this;
  }

  public DiskStoreFactory setQueueSize(int queueSize) {
    if (queueSize < 0) {
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesFactory_QUEUE_SIZE_SPECIFIED_HAS_TO_BE_A_NONNEGATIVE_NUMBER_AND_THE_VALUE_GIVEN_0_IS_NOT_ACCEPTABLE.toLocalizedString(Integer.valueOf(queueSize)));
    }
    this.attrs.queueSize = queueSize;
    return this;
  }

  public DiskStoreFactory setWriteBufferSize(int writeBufferSize) {
    if (writeBufferSize < 0) {
      // TODO Gester add a message for WriteBufferSize
      throw new IllegalArgumentException(LocalizedStrings.DiskWriteAttributesFactory_QUEUE_SIZE_SPECIFIED_HAS_TO_BE_A_NONNEGATIVE_NUMBER_AND_THE_VALUE_GIVEN_0_IS_NOT_ACCEPTABLE.toLocalizedString(Integer.valueOf(writeBufferSize)));
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
