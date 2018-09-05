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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.cache.DiskInitFile.DiskRegionFlag;
import org.apache.geode.internal.cache.DiskStoreImpl.OplogEntryIdSet;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.versions.DiskRegionVersionVector;

public class OplogRVVJUnitTest {
  private File testDirectory;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    testDirectory = temporaryFolder.newFolder("_" + getClass().getSimpleName());
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
  }

  @After
  public void tearDown() throws Exception {
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
  }

  @Test
  public void testRecoverRVV() {
    final DiskInitFile df = mock(DiskInitFile.class);
    final GemFireCacheImpl cache = mock(GemFireCacheImpl.class);

    // Create a mock disk store impl.
    final DiskStoreImpl parent = mock(DiskStoreImpl.class);
    final StatisticsFactory sf = mock(StatisticsFactory.class);
    final DiskStoreID ownerId = DiskStoreID.random();
    final DiskStoreID m1 = DiskStoreID.random();
    final DiskStoreID m2 = DiskStoreID.random();
    final DiskRecoveryStore drs = mock(DiskRecoveryStore.class);
    when(df.getOrCreateCanonicalId(m1)).thenReturn(1);
    when(df.getOrCreateCanonicalId(m2)).thenReturn(2);
    when(df.getOrCreateCanonicalId(ownerId)).thenReturn(3);
    when(df.getCanonicalObject(1)).thenReturn(m1);
    when(df.getCanonicalObject(2)).thenReturn(m2);
    when(df.getCanonicalObject(3)).thenReturn(ownerId);
    when(sf.createStatistics(any(), anyString())).thenReturn(mock(Statistics.class));
    when(sf.createAtomicStatistics(any(), anyString())).thenReturn(mock(Statistics.class));

    DirectoryHolder dirHolder = new DirectoryHolder(sf, testDirectory, 0, 0);
    when(cache.cacheTimeMillis()).thenReturn(System.currentTimeMillis());
    when(parent.getCache()).thenReturn(cache);
    when(parent.getMaxOplogSizeInBytes()).thenReturn(10000L);
    when(parent.getName()).thenReturn("test");
    DiskStoreStats diskStoreStats = new DiskStoreStats(sf, "stats");
    when(parent.getStats()).thenReturn(diskStoreStats);
    when(parent.getDiskInitFile()).thenReturn(df);
    when(parent.getDiskStoreID()).thenReturn(DiskStoreID.random());
    when(parent.getBackupLock()).thenReturn(mock(ReentrantLock.class));
    when(parent.getCancelCriterion()).thenReturn(mock(CancelCriterion.class));

    final DiskRegionVersionVector rvv = new DiskRegionVersionVector(ownerId);
    rvv.recordVersion(m1, 0);
    rvv.recordVersion(m1, 1);
    rvv.recordVersion(m1, 2);
    rvv.recordVersion(m1, 10);
    rvv.recordVersion(m1, 7);
    rvv.recordVersion(m2, 0);
    rvv.recordVersion(m2, 1);
    rvv.recordVersion(m2, 2);
    rvv.recordGCVersion(m1, 1);
    rvv.recordGCVersion(m2, 0);

    // create the oplog
    final AbstractDiskRegion diskRegion = mock(AbstractDiskRegion.class);
    final PersistentOplogSet oplogSet = mock(PersistentOplogSet.class);
    final Map<Long, AbstractDiskRegion> map = new HashMap<>();
    map.put(5L, diskRegion);
    when(diskRegion.getRegionVersionVector()).thenReturn(rvv);
    when(diskRegion.getRVVTrusted()).thenReturn(true);
    when(parent.getAllDiskRegions()).thenReturn(map);
    when(oplogSet.getCurrentlyRecovering(5L)).thenReturn(drs);
    when(oplogSet.getParent()).thenReturn(parent);
    when(diskRegion.getFlags()).thenReturn(EnumSet.of(DiskRegionFlag.IS_WITH_VERSIONING));

    Oplog oplog = new Oplog(1, oplogSet, dirHolder);
    oplog.close();

    oplog = new Oplog(1, oplogSet);
    Collection<File> drfFiles = FileUtils.listFiles(testDirectory, new String[] {"drf"}, true);
    assertThat(drfFiles.size()).isEqualTo(1);
    Collection<File> crfFiles = FileUtils.listFiles(testDirectory, new String[] {"crf"}, true);
    assertThat(crfFiles.size()).isEqualTo(1);
    oplog.addRecoveredFile(drfFiles.iterator().next(), dirHolder);
    oplog.addRecoveredFile(crfFiles.iterator().next(), dirHolder);
    OplogEntryIdSet deletedIds = new OplogEntryIdSet();
    oplog.recoverDrf(deletedIds, false, true);
    oplog.recoverCrf(deletedIds, true, true, false, Collections.singleton(oplog), true);
    verify(drs, times(1)).recordRecoveredGCVersion(m1, 1);
    verify(drs, times(1)).recordRecoveredGCVersion(m2, 0);
    verify(drs, times(1)).recordRecoveredVersonHolder(ownerId,
        rvv.getMemberToVersion().get(ownerId), true);
    verify(drs, times(1)).recordRecoveredVersonHolder(m1, rvv.getMemberToVersion().get(m1), true);
    verify(drs, times(1)).recordRecoveredVersonHolder(m2, rvv.getMemberToVersion().get(m2), true);
    verify(drs, times(1)).setRVVTrusted(true);
  }
}
