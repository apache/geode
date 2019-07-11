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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.io.File;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

/**
 * Disk region perf test for Persist only with Async writes and Buffer. Set Rolling oplog to true
 * and setMaxOplogSize to 10240
 *
 * If more than some number of files are open, an Exception is thrown. This ia JDK 1.4 bug. This
 * test should be run after transition to JDK 1.5 to verify that the bug does not exceed.
 *
 * The disk properties will ensure that very many oplog files are created.
 *
 * <p>
 * TRAC #34179: Failed creating operation log. (putDiskRegPersistAsyncWithBufferRollOlgPerf.conf)
 *
 * <pre>
 * Caused by: com.gemstone.gemfire.cache.DiskAccessException: Failed creating operation log because: java.io.FileNotFoundException: /export/marlin2/users/vjadhav/diskTxJan06/tests/results/battery/putDiskRegPersistAsyncWithBufferRollOlgPerf-0222-160447/BACKUP_diskRegion_505.olg (Too many open files)
 *     at com.gemstone.gemfire.internal.cache.Oplog.<init>(Oplog.java:234)
 *     at com.gemstone.gemfire.internal.cache.Oplog.modify(Oplog.java:1355)
 *     at com.gemstone.gemfire.internal.cache.DiskRegion.put(DiskRegion.java:280)
 *     at com.gemstone.gemfire.internal.cache.DiskEntry$Helper.writeToDisk(DiskEntry.java:325)
 *     at com.gemstone.gemfire.internal.cache.DiskEntry$Helper.update(DiskEntry.java:359)
 *     at com.gemstone.gemfire.internal.cache.AbstractDiskRegionEntry.setValue(AbstractDiskRegionEntry.java:66)
 *     at com.gemstone.gemfire.internal.cache.EntryEventImpl.setNewValue(EntryEventImpl.java:356)
 *     at com.gemstone.gemfire.internal.cache.EntryEventImpl.putExistingEntry(EntryEventImpl.java:314)
 *     at com.gemstone.gemfire.internal.cache.AbstractRegionMap.basicPut(AbstractRegionMap.java:786)
 *     at com.gemstone.gemfire.internal.cache.LocalRegion.virtualPut(LocalRegion.java:2147)
 *     at com.gemstone.gemfire.internal.cache.LocalRegion.basicPut(LocalRegion.java:2030)
 *     at com.gemstone.gemfire.internal.cache.LocalRegion.put(LocalRegion.java:826)
 *     at com.gemstone.gemfire.internal.cache.AbstractRegion.put(AbstractRegion.java:122)
 * </pre>
 */
public class OplogTooManyFilesOpenRegressionTest {

  private static final int VALUE_SIZE = 1024;
  private static final int PUT_COUNT = 100_000;

  private Cache cache;
  private Region<Integer, byte[]> region;
  private byte[] value;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    String uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    File temporaryDirectory = temporaryFolder.getRoot();

    value = new byte[VALUE_SIZE];
    Arrays.fill(value, (byte) 77);

    cache = new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();

    DiskRegionProperties diskRegionProperties = new DiskRegionProperties();
    diskRegionProperties.setBytesThreshold(10000l);
    diskRegionProperties.setDiskDirs(new File[] {temporaryDirectory});
    diskRegionProperties.setMaxOplogSize(10240);
    diskRegionProperties.setPersistBackup(true);
    diskRegionProperties.setRolling(true);
    diskRegionProperties.setSynchronous(false);
    diskRegionProperties.setTimeInterval(15000l);

    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAllowForceCompaction(diskRegionProperties.getAllowForceCompaction());
    dsf.setAutoCompact(diskRegionProperties.isRolling());
    dsf.setCompactionThreshold(diskRegionProperties.getCompactionThreshold());
    dsf.setDiskDirsAndSizes(diskRegionProperties.getDiskDirs(), new int[] {Integer.MAX_VALUE});
    dsf.setQueueSize((int) diskRegionProperties.getBytesThreshold());
    dsf.setTimeInterval(diskRegionProperties.getTimeInterval());

    setMaxOplogSizeInBytes(diskRegionProperties, dsf);

    ((DiskStoreFactoryImpl) dsf).setDiskDirSizesUnit(DiskDirSizesUnit.BYTES);
    DiskStore diskStore = dsf.create(uniqueName);

    RegionFactory<Integer, byte[]> regionFactory = cache.createRegionFactory();
    regionFactory.setDiskStoreName(diskStore.getName());
    regionFactory.setDiskSynchronous(diskRegionProperties.isSynchronous());
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    regionFactory.setConcurrencyLevel(diskRegionProperties.getConcurrencyLevel());
    regionFactory.setInitialCapacity(diskRegionProperties.getInitialCapacity());
    regionFactory.setLoadFactor(diskRegionProperties.getLoadFactor());
    regionFactory.setScope(Scope.LOCAL);
    regionFactory.setStatisticsEnabled(diskRegionProperties.getStatisticsEnabled());

    region = regionFactory.create(uniqueName);
  }

  private void setMaxOplogSizeInBytes(DiskRegionProperties diskRegionProperties,
      DiskStoreFactory dsf) {
    DiskStoreFactoryImpl impl = (DiskStoreFactoryImpl) dsf;
    impl.setMaxOplogSizeInBytes(diskRegionProperties.getMaxOplogSize());
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void testPopulate1kbwrites() {
    for (int i = 0; i < PUT_COUNT; i++) {
      region.put(i, value);
    }

    // closes disk file which will flush all buffers
    region.destroyRegion();
  }
}
