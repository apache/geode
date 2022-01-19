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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * Tests offline compaction
 */
public class CompactOfflineDiskStoreJUnitTest {

  // In this test, entry version, region version, member id, each will be 1 byte
  private static final int versionsize = 3;

  private static Cache cache = null;

  private static DistributedSystem ds = null;

  private int getDSID(LocalRegion lr) {
    return lr.getDistributionManager().getDistributedSystemId();
  }

  private void connectDSandCache() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(LOG_LEVEL, "config"); // to keep diskPerf logs smaller
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(STATISTIC_ARCHIVE_FILE, "stats.gfs");
    ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
  }

  @Before
  public void setUp() throws Exception {
    connectDSandCache();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    ds.disconnect();
  }

  @Test
  public void testTwoEntriesNoCompact() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(false);
    String name = "testTwoEntriesNoCompact";
    DiskStore diskStore = dsf.create(name);
    File crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.crf");
    File drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.drf");
    File ifFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + ".if");
    AttributesFactory af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    int extra_byte_num_per_entry =
        InternalDataSerializer.calculateBytesForTSandDSID(getDSID((LocalRegion) r));
    r.put("key1", "value1");
    r.put("key2", "value2");
    cache.close();
    ds.disconnect();

    System.out.println("empty rvv size = " + getRVVSize(0, null, false));
    System.out.println("empty rvvgc size = " + getRVVSize(1, new int[] {0}, true));
    System.out.println("1 member rvv size = " + getRVVSize(1, new int[] {1}, false));
    System.out.println("2 member rvv size = " + getRVVSize(2, new int[] {1, 1}, false));
    DiskStoreImpl.validate(name, diskStore.getDiskDirs());
    int headerSize = Oplog.OPLOG_MAGIC_SEQ_REC_SIZE + Oplog.OPLOG_DISK_STORE_REC_SIZE;
    int crfsize = headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, false)
        + Oplog.OPLOG_NEW_ENTRY_BASE_REC_SIZE;
    int createsize1 = getSize4Create(extra_byte_num_per_entry, "key1", "value1");
    int createsize2 = getSize4Create(extra_byte_num_per_entry, "key2", "value2");
    assertEquals(crfsize + createsize1 + createsize2, crfFile.length());
    assertEquals(headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, true),
        drfFile.length());
    long originalIfLength = ifFile.length();

    DiskStoreImpl dsi = DiskStoreImpl.offlineCompact(name, diskStore.getDiskDirs(), false, -1);
    assertEquals(0, dsi.getDeadRecordCount());
    assertEquals(2, dsi.getLiveEntryCount());
    assertEquals(crfsize + createsize1 + createsize2, crfFile.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, new int[] {0}, true),
        drfFile.length());
    // offline compaction should not have created a new oplog.
    assertEquals(originalIfLength, ifFile.length());

    connectDSandCache();
    dsf = cache.createDiskStoreFactory();
    diskStore = dsf.create(name);
    af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    r = cache.createRegion("r", af.create());
    assertEquals(2, r.size());
    assertEquals("value1", r.get("key1"));
    assertEquals("value2", r.get("key2"));

    // if test passed clean up files
    r.destroyRegion();
    diskStore.destroy();
  }

  @Test
  public void testTwoEntriesWithUpdates() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(false);
    String name = "testTwoEntriesWithUpdates";
    DiskStore diskStore = dsf.create(name);
    File crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.crf");
    File drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.drf");
    File krfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.krf");
    File ifFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + ".if");
    AttributesFactory af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    int extra_byte_num_per_entry =
        InternalDataSerializer.calculateBytesForTSandDSID(getDSID((LocalRegion) r));
    r.put("key1", "value1");
    r.put("key2", "value2");
    r.put("key1", "update1");
    r.put("key2", "update2");
    cache.close();
    ds.disconnect();
    DiskStoreImpl.validate(name, diskStore.getDiskDirs());

    int headerSize = Oplog.OPLOG_MAGIC_SEQ_REC_SIZE + Oplog.OPLOG_DISK_STORE_REC_SIZE;
    int crfsize = headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, false)
        + Oplog.OPLOG_NEW_ENTRY_BASE_REC_SIZE;
    int createsize1 = getSize4Create(extra_byte_num_per_entry, "key1", "value1");
    int createsize2 = getSize4Create(extra_byte_num_per_entry, "key2", "value2");
    int updatesize1 = getSize4UpdateWithoutKey(extra_byte_num_per_entry, "update1");
    int updatesize2 = getSize4UpdateWithoutKey(extra_byte_num_per_entry, "update2");

    assertEquals(crfsize + createsize1 + createsize2 + updatesize1 + updatesize2, crfFile.length());
    assertEquals(headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, true),
        drfFile.length());
    long originalIfLength = ifFile.length();

    DiskStoreImpl dsi = DiskStoreImpl.offlineCompact(name, diskStore.getDiskDirs(), false, -1);
    assertEquals(2, dsi.getDeadRecordCount());
    assertEquals(2, dsi.getLiveEntryCount());
    assertEquals(false, crfFile.exists());
    assertEquals(false, drfFile.exists());
    assertEquals(false, krfFile.exists());
    crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.crf");
    drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.drf");
    assertEquals(true, crfFile.exists());
    assertEquals(true, drfFile.exists());
    {
      krfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.krf");
      assertEquals(true, krfFile.exists());
    }

    // compare file sizes
    // After offline compaction, 2 create entries + 2 update without key entries
    // become 2 update with key entries. No more OPLOG_NEW_ENTRY_BASE_REC.
    // The RVV now contains a single member
    crfsize =
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {1}, false);
    updatesize1 += getStrSizeInOplog("key1");
    updatesize2 += getStrSizeInOplog("key2");
    assertEquals(crfsize + updatesize1 + updatesize2, crfFile.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {0}, true),
        drfFile.length());
    assertEquals(originalIfLength, ifFile.length());

    connectDSandCache();
    dsf = cache.createDiskStoreFactory();
    diskStore = dsf.create(name);
    af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    r = cache.createRegion("r", af.create());
    assertEquals(2, r.size());
    assertEquals("update1", r.get("key1"));
    assertEquals("update2", r.get("key2"));

    // if test passed clean up files
    r.destroyRegion();
    diskStore.destroy();
  }

  @Test
  public void testTwoEntriesWithUpdateAndDestroy() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(false);
    String name = "testTwoEntriesWithUpdateAndDestroy";
    DiskStore diskStore = dsf.create(name);
    File crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.crf");
    File drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.drf");
    File krfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.krf");
    File ifFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + ".if");
    AttributesFactory af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    int extra_byte_num_per_entry =
        InternalDataSerializer.calculateBytesForTSandDSID(getDSID((LocalRegion) r));
    r.put("key1", "value1");
    r.put("key2", "value2");
    r.put("key1", "update1");
    r.put("key2", "update2");
    r.remove("key2");
    cache.close();
    ds.disconnect();
    DiskStoreImpl.validate(name, diskStore.getDiskDirs());

    int headerSize = Oplog.OPLOG_MAGIC_SEQ_REC_SIZE + Oplog.OPLOG_DISK_STORE_REC_SIZE;
    int crfsize = headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, false)
        + Oplog.OPLOG_NEW_ENTRY_BASE_REC_SIZE;
    int createsize1 = getSize4Create(extra_byte_num_per_entry, "key1", "value1");
    int createsize2 = getSize4Create(extra_byte_num_per_entry, "key2", "value2");
    int updatesize1 = getSize4UpdateWithoutKey(extra_byte_num_per_entry, "update1");
    int updatesize2 = getSize4UpdateWithoutKey(extra_byte_num_per_entry, "update2");
    // 1 tombstone without key
    int tombstonesize1 = getSize4TombstoneWithoutKey(extra_byte_num_per_entry);

    assertEquals(crfsize + createsize1 + createsize2 + updatesize1 + updatesize2 + tombstonesize1,
        crfFile.length());
    assertEquals(headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, true),
        drfFile.length());
    long originalIfLength = ifFile.length();

    DiskStoreImpl dsi = DiskStoreImpl.offlineCompact(name, diskStore.getDiskDirs(), false, -1);
    assertEquals(3, dsi.getDeadRecordCount());
    assertEquals(2, dsi.getLiveEntryCount());
    assertEquals(false, crfFile.exists());
    assertEquals(false, drfFile.exists());
    assertEquals(false, krfFile.exists());
    crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.crf");
    drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.drf");
    {
      krfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.krf");
      assertEquals(true, krfFile.exists());
    }
    assertEquals(true, crfFile.exists());
    assertEquals(true, drfFile.exists());

    // compare file sizes
    // After offline compaction, only one update with key and one tombstone with key are left
    // No more OPLOG_NEW_ENTRY_BASE_REC.
    // The crf now contains an RVV with one entry
    crfsize =
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {1}, false);
    updatesize1 += getStrSizeInOplog("key1");
    tombstonesize1 += getStrSizeInOplog("key2");
    assertEquals(crfsize + updatesize1 + tombstonesize1, crfFile.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {0}, true),
        drfFile.length());
    assertEquals(originalIfLength, ifFile.length());

    connectDSandCache();
    dsf = cache.createDiskStoreFactory();
    diskStore = dsf.create(name);
    af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    r = cache.createRegion("r", af.create());
    assertEquals(1, r.size());
    assertEquals("update1", r.get("key1"));

    // if test passed clean up files
    r.destroyRegion();
    diskStore.destroy();
  }

  @Test
  public void testbug41862() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(false);
    String name = "testbug41862";
    DiskStore diskStore = dsf.create(name);
    File crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.crf");
    File drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.drf");
    File krfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.krf");
    File ifFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + ".if");
    AttributesFactory af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    int extra_byte_num_per_entry =
        InternalDataSerializer.calculateBytesForTSandDSID(getDSID((LocalRegion) r));
    r.create("key1", "value1");
    r.create("key2", "value2"); // to keep this oplog from going empty
    ((LocalRegion) r).getDiskStore().forceRoll();
    r.create("key3", "value3");
    r.remove("key1");
    cache.close();
    ds.disconnect();
    DiskStoreImpl.validate(name, diskStore.getDiskDirs());

    int headerSize = Oplog.OPLOG_MAGIC_SEQ_REC_SIZE + Oplog.OPLOG_DISK_STORE_REC_SIZE;
    int crfsize = headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, false)
        + Oplog.OPLOG_NEW_ENTRY_BASE_REC_SIZE;
    int createsize1 = getSize4Create(extra_byte_num_per_entry, "key1", "value1");
    int createsize2 = getSize4Create(extra_byte_num_per_entry, "key2", "value2");
    int createsize3 = getSize4Create(extra_byte_num_per_entry, "key3", "value3");
    // 1 tombstone with key
    int tombstonesize1 = getSize4TombstoneWithKey(extra_byte_num_per_entry, "key1");

    assertEquals(crfsize + createsize1 + createsize2, crfFile.length());
    assertEquals(headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, true),
        drfFile.length());

    File crf2File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.crf");
    File drf2File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.drf");

    crfsize += (getRVVSize(1, new int[] {1}, false) - getRVVSize(0, null, false)); // adjust rvv
                                                                                   // size
    assertEquals(crfsize + createsize3 + tombstonesize1, crf2File.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {0}, true),
        drf2File.length());
    long originalIfLength = ifFile.length();

    DiskStoreImpl dsi = DiskStoreImpl.offlineCompact(name, diskStore.getDiskDirs(), false, -1);
    assertEquals(1, dsi.getDeadRecordCount());
    assertEquals(3, dsi.getLiveEntryCount());
    assertEquals(false, crfFile.exists());
    assertEquals(false, drfFile.exists());
    assertEquals(false, krfFile.exists());

    // offline compaction did not change _2.crf and _2.drf not changed.
    assertEquals(crfsize + createsize3 + tombstonesize1, crf2File.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {0}, true),
        drf2File.length());

    // offline compaction reset rvv to be empty, create-entry becomes one update-with-key-entry in
    // _3.crf,
    // since there's no creates, then there's no OPLOG_NEW_ENTRY_BASE_REC_SIZE
    crfsize =
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {1}, false);
    int updatesize1 = getSize4UpdateWithKey(extra_byte_num_per_entry, "key3", "value3");
    File crf3File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_3.crf");
    File drf3File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_3.drf");
    File krf3File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_3.krf");
    assertEquals(true, krf3File.exists());
    assertEquals(true, crf3File.exists());
    assertEquals(true, drf3File.exists());
    assertEquals(crfsize + updatesize1, crf3File.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {0}, true),
        drf3File.length());
    assertEquals(originalIfLength, ifFile.length());

    connectDSandCache();
    dsf = cache.createDiskStoreFactory();
    diskStore = dsf.create(name);
    af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    r = cache.createRegion("r", af.create());
    assertEquals(2, r.size());
    assertEquals("value2", r.get("key2"));
    assertEquals("value3", r.get("key3"));

    // if test passed clean up files
    r.destroyRegion();
    diskStore.destroy();
  }

  @Test
  public void testTwoEntriesWithRegionClear() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(false);
    String name = "testTwoEntriesWithRegionClear";
    DiskStore diskStore = dsf.create(name);
    File crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.crf");
    File drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.drf");
    File krfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.krf");
    File ifFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + ".if");
    AttributesFactory af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    int extra_byte_num_per_entry =
        InternalDataSerializer.calculateBytesForTSandDSID(getDSID((LocalRegion) r));
    r.put("key1", "value1");
    r.put("key2", "value2");
    r.put("key1", "update1");
    r.put("key2", "update2");
    r.remove("key2");
    r.clear();
    Region r2 = cache.createRegion("r2", af.create());
    // Put something live in the oplog to keep it alive.
    // This is needed because we now force a roll during ds close
    // so that a krf will be generated for the last oplog.
    r2.put("r2key1", "rwvalue1");
    cache.close();
    ds.disconnect();
    DiskStoreImpl.validate(name, diskStore.getDiskDirs());

    int headerSize = Oplog.OPLOG_MAGIC_SEQ_REC_SIZE + Oplog.OPLOG_DISK_STORE_REC_SIZE;
    int crfsize = headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, false)
        + Oplog.OPLOG_NEW_ENTRY_BASE_REC_SIZE;
    int clearsize_in_crf = getRVVSize(1, new int[] {1}, false); // write extra RVV and RVVGC for
                                                                // clear operation
    int clearsize_in_drf = getRVVSize(1, new int[] {1}, true); // write extra RVV and RVVGC for
                                                               // clear operation
    int createsize1 = getSize4Create(extra_byte_num_per_entry, "key1", "value1");
    int createsize2 = getSize4Create(extra_byte_num_per_entry, "key2", "value2");
    int updatesize1 = getSize4UpdateWithoutKey(extra_byte_num_per_entry, "update1");
    int updatesize2 = getSize4UpdateWithoutKey(extra_byte_num_per_entry, "update2");
    // 1 tombstone without key
    int tombstonesize1 = getSize4TombstoneWithoutKey(extra_byte_num_per_entry);
    int createsize3 = getSize4Create(extra_byte_num_per_entry, "r2key1", "rwvalue1");
    assertEquals(crfsize + createsize1 + createsize2 + updatesize1 + updatesize2 + tombstonesize1
        + createsize3 + clearsize_in_crf, crfFile.length());
    assertEquals(headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, true)
        + clearsize_in_drf, drfFile.length());
    long originalIfLength = ifFile.length();

    DiskStoreImpl dsi = DiskStoreImpl.offlineCompact(name, diskStore.getDiskDirs(), false, -1);
    assertEquals(5, dsi.getDeadRecordCount());
    assertEquals(1, dsi.getLiveEntryCount());
    assertEquals(false, crfFile.exists());
    assertEquals(false, drfFile.exists());
    assertEquals(false, krfFile.exists());
    crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.crf");
    drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.drf");
    krfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.krf");
    assertEquals(true, krfFile.exists());
    assertEquals(true, crfFile.exists());
    assertEquals(true, drfFile.exists());

    // offline compaction changed the only create-entry to be an update-with-key entry
    int updatesize3 = getSize4UpdateWithKey(extra_byte_num_per_entry, "r2key1", "rwvalue1");
    crfsize =
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(2, new int[] {1, 1}, false);
    assertEquals(crfsize + updatesize3, crfFile.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(2, new int[] {1, 0}, true),
        drfFile.length());
    assertEquals(originalIfLength, ifFile.length());

    connectDSandCache();
    dsf = cache.createDiskStoreFactory();
    diskStore = dsf.create(name);
    af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    r = cache.createRegion("r", af.create());
    assertEquals(0, r.size());

    // if test passed clean up files
    r.destroyRegion();
    diskStore.destroy();
  }

  @Test
  public void testTwoEntriesWithRegionDestroy() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(false);
    String name = "testTwoEntriesWithRegionDestroy";
    DiskStore diskStore = dsf.create(name);
    File crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.crf");
    File drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.drf");
    File krfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.krf");
    File ifFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + ".if");
    AttributesFactory af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    // create a dummy region to keep diskstore files "alive" once we destroy the real region
    cache.createRegion("r2dummy", af.create());
    Region r = cache.createRegion("r", af.create());
    int extra_byte_num_per_entry =
        InternalDataSerializer.calculateBytesForTSandDSID(getDSID((LocalRegion) r));
    r.put("key1", "value1");
    r.put("key2", "value2");
    r.put("key1", "update1");
    r.put("key2", "update2");
    r.remove("key2");
    r.clear();
    r.localDestroyRegion();
    Region r2 = cache.createRegion("r2", af.create());
    // Put something live in the oplog to keep it alive.
    // This is needed because we now force a roll during ds close
    // so that a krf will be generated for the last oplog.
    r2.put("r2key1", "rwvalue1");
    cache.close();
    ds.disconnect();
    DiskStoreImpl.validate(name, diskStore.getDiskDirs());

    int headerSize = Oplog.OPLOG_MAGIC_SEQ_REC_SIZE + Oplog.OPLOG_DISK_STORE_REC_SIZE;
    int crfsize = headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, false)
        + Oplog.OPLOG_NEW_ENTRY_BASE_REC_SIZE;
    int clearsize_in_crf = getRVVSize(1, new int[] {1}, false); // write extra RVV and RVVGC for
                                                                // clear operation
    int clearsize_in_drf = getRVVSize(1, new int[] {1}, true); // write extra RVV and RVVGC for
                                                               // clear operation
    int createsize1 = getSize4Create(extra_byte_num_per_entry, "key1", "value1");
    int createsize2 = getSize4Create(extra_byte_num_per_entry, "key2", "value2");
    int updatesize1 = getSize4UpdateWithoutKey(extra_byte_num_per_entry, "update1");
    int updatesize2 = getSize4UpdateWithoutKey(extra_byte_num_per_entry, "update2");
    // 1 tombstone without key
    int tombstonesize1 = getSize4TombstoneWithoutKey(extra_byte_num_per_entry);
    int createsize3 = getSize4Create(extra_byte_num_per_entry, "r2key1", "rwvalue1");
    assertEquals(crfsize + createsize1 + createsize2 + updatesize1 + updatesize2 + tombstonesize1
        + createsize3 + clearsize_in_crf, crfFile.length());
    assertEquals(headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, true)
        + clearsize_in_drf, drfFile.length());
    long originalIfLength = ifFile.length();

    DiskStoreImpl dsi = DiskStoreImpl.offlineCompact(name, diskStore.getDiskDirs(), false, -1);
    assertEquals(5, dsi.getDeadRecordCount());
    assertEquals(1, dsi.getLiveEntryCount());
    assertEquals(false, crfFile.exists());
    assertEquals(false, drfFile.exists());
    assertEquals(false, krfFile.exists());
    crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.crf");
    drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.drf");
    krfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.krf");
    assertEquals(true, krfFile.exists());
    assertEquals(true, crfFile.exists());
    assertEquals(true, drfFile.exists());

    // offline compaction changed the only create-entry to be an update-with-key entry
    int updatesize3 = getSize4UpdateWithKey(extra_byte_num_per_entry, "r2key1", "rwvalue1");
    crfsize =
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(2, new int[] {1, 1}, false);
    assertEquals(crfsize + updatesize3, crfFile.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(2, new int[] {0, 0}, true),
        drfFile.length());
    // Now we preallocate spaces for if files and also crfs and drfs. So the below check is not true
    // any more.
    // if (originalIfLength <= ifFile.length()) {
    // fail("expected " + ifFile.length() + " to be < " + originalIfLength);
    // }

    connectDSandCache();
    dsf = cache.createDiskStoreFactory();
    diskStore = dsf.create(name);
    af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    r = cache.createRegion("r", af.create());
    assertEquals(0, r.size());

    // if test passed clean up files
    r.destroyRegion();
    diskStore.destroy();
  }

  @Test
  public void testForceRollTwoEntriesWithUpdates() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(false);
    String name = "testForceRollTwoEntriesWithUpdates";
    DiskStore diskStore = dsf.create(name);
    File crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.crf");
    File drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.drf");
    File krfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.krf");
    File crf2File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.crf");
    File drf2File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.drf");
    File ifFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + ".if");
    AttributesFactory af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    int extra_byte_num_per_entry =
        InternalDataSerializer.calculateBytesForTSandDSID(getDSID((LocalRegion) r));
    r.put("key0", "value0"); // extra key to keep oplog1 from being empty
    r.put("key1", "value1");
    r.put("key2", "value2");
    diskStore.forceRoll();
    r.put("key1", "update1");
    r.put("key2", "update2");
    cache.close();
    ds.disconnect();
    DiskStoreImpl.validate(name, diskStore.getDiskDirs());

    int headerSize = Oplog.OPLOG_MAGIC_SEQ_REC_SIZE + Oplog.OPLOG_DISK_STORE_REC_SIZE;
    int crfsize = headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, false)
        + Oplog.OPLOG_NEW_ENTRY_BASE_REC_SIZE;
    int createsize0 = getSize4Create(extra_byte_num_per_entry, "key0", "value0");
    int createsize1 = getSize4Create(extra_byte_num_per_entry, "key1", "value1");
    int createsize2 = getSize4Create(extra_byte_num_per_entry, "key2", "value2");
    int updatesize1 = getSize4UpdateWithKey(extra_byte_num_per_entry, "key1", "update1");
    int updatesize2 = getSize4UpdateWithKey(extra_byte_num_per_entry, "key2", "update2");

    assertEquals(crfsize + createsize0 + createsize1 + createsize2, crfFile.length());
    assertEquals(headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, true),
        drfFile.length());
    crfsize =
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {1}, false);
    assertEquals(crfsize + updatesize1 + updatesize2, crf2File.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {0}, true),
        drf2File.length());
    long originalIfLength = ifFile.length();

    DiskStoreImpl dsi = DiskStoreImpl.offlineCompact(name, diskStore.getDiskDirs(), false, -1);
    assertEquals(2, dsi.getDeadRecordCount());
    assertEquals(3, dsi.getLiveEntryCount());
    assertEquals(false, crfFile.exists());
    assertEquals(false, drfFile.exists());
    assertEquals(false, krfFile.exists());
    // oplog2 contains two updates so it remains unchanged
    assertEquals(crfsize + updatesize1 + updatesize2, crf2File.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {0}, true),
        drf2File.length());

    File crf3File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_3.crf");
    File drf3File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_3.drf");
    File krf3File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_3.krf");
    assertEquals(true, krf3File.exists());
    assertEquals(true, crf3File.exists());
    assertEquals(true, drf3File.exists());
    // after offline compaction, rvv is reset, and only one update-with-key, i.e. key0 in _3.crf
    crfsize =
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {1}, false);
    int updatesize0 = getSize4UpdateWithKey(extra_byte_num_per_entry, "key0", "value0");
    assertEquals(crfsize + updatesize0, crf3File.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {0}, true),
        drf3File.length());
    assertEquals(originalIfLength, ifFile.length());

    connectDSandCache();
    dsf = cache.createDiskStoreFactory();
    diskStore = dsf.create(name);
    af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    r = cache.createRegion("r", af.create());
    assertEquals(3, r.size());
    assertEquals("value0", r.get("key0"));
    assertEquals("update1", r.get("key1"));
    assertEquals("update2", r.get("key2"));

    // if test passed clean up files
    r.destroyRegion();
    diskStore.destroy();
  }

  @Test
  public void testForceRollTwoEntriesWithUpdateAndDestroy() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setAutoCompact(false);
    String name = "testForceRollTwoEntriesWithUpdateAndDestroy";
    DiskStore diskStore = dsf.create(name);
    File crfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.crf");
    File drfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.drf");
    File krfFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_1.krf");
    File crf2File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.crf");
    File drf2File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.drf");
    File krf2File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_2.krf");
    File ifFile = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + ".if");
    AttributesFactory af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region r = cache.createRegion("r", af.create());
    int extra_byte_num_per_entry =
        InternalDataSerializer.calculateBytesForTSandDSID(getDSID((LocalRegion) r));
    r.put("key0", "value0"); // extra key to keep oplog1 from being empty
    r.put("key1", "value1");
    r.put("key2", "value2");
    diskStore.forceRoll();
    r.put("key1", "update1");
    r.put("key2", "update2");
    r.remove("key2");
    cache.close();
    ds.disconnect();
    DiskStoreImpl.validate(name, diskStore.getDiskDirs());

    int headerSize = Oplog.OPLOG_MAGIC_SEQ_REC_SIZE + Oplog.OPLOG_DISK_STORE_REC_SIZE;
    int crfsize = headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, false)
        + Oplog.OPLOG_NEW_ENTRY_BASE_REC_SIZE;
    int createsize0 = getSize4Create(extra_byte_num_per_entry, "key0", "value0");
    int createsize1 = getSize4Create(extra_byte_num_per_entry, "key1", "value1");
    int createsize2 = getSize4Create(extra_byte_num_per_entry, "key2", "value2");
    int updatesize1 = getSize4UpdateWithKey(extra_byte_num_per_entry, "key1", "update1");
    int updatesize2 = getSize4UpdateWithKey(extra_byte_num_per_entry, "key2", "update2");
    int tombstonesize1 = getSize4TombstoneWithoutKey(extra_byte_num_per_entry);

    assertEquals(crfsize + createsize0 + createsize1 + createsize2, crfFile.length());
    assertEquals(headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(0, null, true),
        drfFile.length());
    crfsize =
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {1}, false);
    assertEquals(crfsize + updatesize1 + updatesize2 + tombstonesize1, crf2File.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {0}, true),
        drf2File.length());
    long originalIfLength = ifFile.length();

    DiskStoreImpl dsi = DiskStoreImpl.offlineCompact(name, diskStore.getDiskDirs(), false, -1);
    assertEquals(3, dsi.getDeadRecordCount());
    assertEquals(3, dsi.getLiveEntryCount());
    assertEquals(false, crfFile.exists());
    assertEquals(false, drfFile.exists());
    assertEquals(false, krfFile.exists());
    assertEquals(false, crf2File.exists());
    assertEquals(false, drf2File.exists());
    assertEquals(false, krf2File.exists());
    File crf3File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_3.crf");
    File drf3File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_3.drf");
    File krf3File = new File(diskStore.getDiskDirs()[0], "BACKUP" + name + "_3.krf");
    assertEquals(true, crf3File.exists());
    assertEquals(true, drf3File.exists());
    assertEquals(true, krf3File.exists());

    // after offline compaction, rvv is reset, and only 3 update-with-key,
    // i.e. key0, key1, key2(tombstone) in _3.crf
    crfsize =
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {1}, false);
    int updatesize0 = getSize4UpdateWithKey(extra_byte_num_per_entry, "key0", "value0");
    tombstonesize1 = getSize4TombstoneWithKey(extra_byte_num_per_entry, "key2");
    assertEquals(crfsize + updatesize0 + updatesize1 + tombstonesize1, crf3File.length());
    assertEquals(
        headerSize + Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + getRVVSize(1, new int[] {0}, true),
        drf3File.length());

    // Now we preallocate spaces for if files and also crfs and drfs. So the below check is not true
    // any more.
    // if (originalIfLength <= ifFile.length()) {
    // fail("expected " + ifFile.length() + " to be < " + originalIfLength);
    // }

    connectDSandCache();
    dsf = cache.createDiskStoreFactory();
    diskStore = dsf.create(name);
    af = new AttributesFactory();
    af.setDiskStoreName(name);
    af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    r = cache.createRegion("r", af.create());
    assertEquals(2, r.size());
    assertEquals("value0", r.get("key0"));
    assertEquals("update1", r.get("key1"));

    // if test passed clean up files
    r.destroyRegion();
    diskStore.destroy();
  }

  // uitl methods for calculation
  private static int getValueSizeInOplog(Object value) {
    if (value instanceof String) {
      return getStrSizeInOplog((String) value);
    } else if (value instanceof byte[]) {
      byte[] val = (byte[]) value;
      return val.length + 4;
    }
    return -1;
  }

  private static int getStrSizeInOplog(String str) {
    // string saved in UTF format will use 3 bytes extra.
    // 4 is hard-coded overhead in Oplog for each string
    return str.length() + 3 + 4;
  }

  static int getSize4Create(int extra_byte_num_per_entry, String key, Object value) {
    int createsize = 1 /* opcode */ + 1 /* userbits */ + versionsize + extra_byte_num_per_entry
        + getStrSizeInOplog(key) + 1 /* drid */ + getValueSizeInOplog(value)
        + 1 /* END_OF_RECORD_ID */;
    return createsize;
  }

  private static int getSize4UpdateWithKey(int extra_byte_num_per_entry, String key, Object value) {
    return getSize4UpdateWithoutKey(extra_byte_num_per_entry, value) + getStrSizeInOplog(key);
  }

  private static int getSize4UpdateWithoutKey(int extra_byte_num_per_entry, Object value) {
    int updatesize = 1 /* opcode */ + 1 /* userbits */ + versionsize + extra_byte_num_per_entry
        + 1 /* drid */ + getValueSizeInOplog(value) + 1 /* delta */ + 1 /* END_OF_RECORD_ID */;
    return updatesize;
  }

  static int getSize4TombstoneWithKey(int extra_byte_num_per_entry, String key) {
    return getSize4TombstoneWithoutKey(extra_byte_num_per_entry) + getStrSizeInOplog(key);
  }

  private static int getSize4TombstoneWithoutKey(int extra_byte_num_per_entry) {
    int tombstonesize = 1 /* opcode */ + 1 /* userbits */ + versionsize + extra_byte_num_per_entry
        + 1 /* drid */ + 1 /* delta */ + 1 /* END_OF_RECORD_ID */;
    return tombstonesize;
  }

  static int getRVVSize(int drMapSize, int[] numOfMemberPerDR, boolean gcRVV) {
    // if there's one member in rvv, total size is 9 bytes:
    // 0: OPLOG_RVV. 1: drMap.size()==1, 2: disRegionId, 3: getRVVTrusted
    // 4: memberToVersion.size()==1, 5: memberid, 6-7: versionHolder 8: END_OF_RECORD_ID
    // but not every diskRegion has a member in RVV
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    RegionVersionHolder dummyHolder = new RegionVersionHolder(1);
    try {
      dummyHolder.toData(out);
    } catch (IOException ignored) {
    }
    int holderSize = out.size();
    out.close();

    int size = 1 /* OPLOG_RVV */;
    size++; /* drMap.size */
    for (int i = 0; i < drMapSize; i++) {
      size++; /* disRegionId */
      if (gcRVV) {
        size++; /* numOfMember, i.e. memberToVersion.size */
        if (numOfMemberPerDR != null && numOfMemberPerDR[i] > 0) {
          for (int j = 0; j < numOfMemberPerDR[i]; j++) {
            size++; /* memberid */
            size++; /* gcversion */
          }
        }
      } else {
        size++; /* getRVVTrusted */
        size++; /* numOfMember, i.e. memberToVersion.size */
        if (numOfMemberPerDR != null && numOfMemberPerDR[i] > 0) {
          for (int j = 0; j < numOfMemberPerDR[i]; j++) {
            size++; /* memberid */
            size += holderSize;
          }
        }
      }
    }
    size++; /* END_OF_RECORD_ID */
    return size;
  }
}
