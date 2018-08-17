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

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.util.ObjectSizer;

/**
 *
 * A testing helper factory to get a disk region with the desired configuration
 *
 *
 */
public class DiskRegionHelperFactory {

  private static Region<Object, Object> getRegion(Cache cache, DiskRegionProperties diskProps,
      Scope regionScope) {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    AttributesFactory factory = new AttributesFactory();
    if (diskProps.getDiskDirs() == null) {
      File dir = new File("testingDirectoryDefault");
      dir.mkdir();
      dir.deleteOnExit();
      File[] dirs = {dir};
      dsf.setDiskDirsAndSizes(dirs, new int[] {Integer.MAX_VALUE});
    } else if (diskProps.getDiskDirSizes() == null) {
      int[] ints = new int[diskProps.getDiskDirs().length];
      for (int i = 0; i < ints.length; i++) {
        ints[i] = Integer.MAX_VALUE;
      }
      dsf.setDiskDirsAndSizes(diskProps.getDiskDirs(), ints);
    } else {
      dsf.setDiskDirsAndSizes(diskProps.getDiskDirs(), diskProps.getDiskDirSizes());
    }
    ((DiskStoreFactoryImpl) dsf).setMaxOplogSizeInBytes(diskProps.getMaxOplogSize());
    dsf.setAutoCompact(diskProps.isRolling());
    dsf.setAllowForceCompaction(diskProps.getAllowForceCompaction());
    dsf.setCompactionThreshold(diskProps.getCompactionThreshold());
    if (diskProps.getTimeInterval() != -1) {
      dsf.setTimeInterval(diskProps.getTimeInterval());
    }

    if (diskProps.getBytesThreshold() > Integer.MAX_VALUE) {
      dsf.setQueueSize(Integer.MAX_VALUE);
    } else {
      dsf.setQueueSize((int) diskProps.getBytesThreshold());
    }
    factory.setDiskSynchronous(diskProps.isSynchronous());
    DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = true;
    try {
      factory.setDiskStoreName(dsf.create(diskProps.getRegionName()).getName());
    } finally {
      DirectoryHolder.SET_DIRECTORY_SIZE_IN_BYTES_FOR_TESTING_PURPOSES = false;
    }
    if (diskProps.isPersistBackup()) {
      factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    }
    factory.setScope(regionScope);

    if (diskProps.isOverflow()) {
      int capacity = diskProps.getOverFlowCapacity();
      factory.setEvictionAttributes(
          EvictionAttributes.createLRUEntryAttributes(capacity, EvictionAction.OVERFLOW_TO_DISK));

    } else if (diskProps.isHeapEviction()) {
      factory.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(ObjectSizer.DEFAULT,
          EvictionAction.OVERFLOW_TO_DISK));
    }

    factory.setConcurrencyLevel(diskProps.getConcurrencyLevel());
    factory.setInitialCapacity(diskProps.getInitialCapacity());
    factory.setLoadFactor(diskProps.getLoadFactor());
    factory.setStatisticsEnabled(diskProps.getStatisticsEnabled());

    Region<Object, Object> region = null;
    try {
      region = cache.createVMRegion(diskProps.getRegionName(), factory.createRegionAttributes());
    } catch (TimeoutException e) {
      throw new RuntimeException(" failed to create region due  to a TimeOutException " + e);
    } catch (RegionExistsException e) {
      throw new RuntimeException(" failed to create region due  to a RegionExistsException " + e);
    }
    return region;
  }

  public static Region<Object, Object> getSyncPersistOnlyRegion(Cache cache,
      DiskRegionProperties diskRegionProperties, Scope regionScope) {
    if (diskRegionProperties == null) {
      diskRegionProperties = new DiskRegionProperties();
    }
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setSynchronous(true);
    return getRegion(cache, diskRegionProperties, regionScope);

  }

  public static Region<Object, Object> getAsyncPersistOnlyRegion(Cache cache,
      DiskRegionProperties diskRegionProperties) {
    if (diskRegionProperties == null) {
      diskRegionProperties = new DiskRegionProperties();
    }
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setSynchronous(false);
    return getRegion(cache, diskRegionProperties, Scope.LOCAL);
  }

  public static Region<Object, Object> getSyncOverFlowOnlyRegion(Cache cache,
      DiskRegionProperties diskRegionProperties) {
    if (diskRegionProperties == null) {
      diskRegionProperties = new DiskRegionProperties();
    }
    diskRegionProperties.setPersistBackup(false);
    diskRegionProperties.setSynchronous(true);
    diskRegionProperties.setOverflow(true);
    return getRegion(cache, diskRegionProperties, Scope.LOCAL);
  }

  public static Region<Object, Object> getAsyncOverFlowOnlyRegion(Cache cache,
      DiskRegionProperties diskRegionProperties) {
    if (diskRegionProperties == null) {
      diskRegionProperties = new DiskRegionProperties();
    }
    diskRegionProperties.setPersistBackup(false);
    diskRegionProperties.setSynchronous(false);
    diskRegionProperties.setOverflow(true);
    return getRegion(cache, diskRegionProperties, Scope.LOCAL);
  }

  public static Region<Object, Object> getSyncOverFlowAndPersistRegion(Cache cache,
      DiskRegionProperties diskRegionProperties) {
    if (diskRegionProperties == null) {
      diskRegionProperties = new DiskRegionProperties();
    }
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setSynchronous(true);
    diskRegionProperties.setOverflow(true);
    return getRegion(cache, diskRegionProperties, Scope.LOCAL);
  }

  public static Region<Object, Object> getAsyncOverFlowAndPersistRegion(Cache cache,
      DiskRegionProperties diskRegionProperties) {
    if (diskRegionProperties == null) {
      diskRegionProperties = new DiskRegionProperties();
    }
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setSynchronous(false);
    diskRegionProperties.setOverflow(true);
    return getRegion(cache, diskRegionProperties, Scope.LOCAL);
  }

  public static Region<Object, Object> getSyncHeapLruAndPersistRegion(Cache cache,
      DiskRegionProperties diskRegionProperties) {
    if (diskRegionProperties == null) {
      diskRegionProperties = new DiskRegionProperties();
    }
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setSynchronous(true);
    diskRegionProperties.setHeapEviction(true);
    return getRegion(cache, diskRegionProperties, Scope.LOCAL);
  }
}
