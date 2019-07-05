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
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.entries.DiskEntry.Helper.ValueWrapper;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;

public class PersistentOplogSetTest {

  private DiskStoreImpl diskStore;

  private DiskStoreStats diskStoreStats;
  private InternalRegion region;
  private DiskEntry entry;
  private ValueWrapper value;
  private Oplog oplog;

  private PersistentOplogSet persistentOplogSet;

  @Before
  public void setUp() {
    diskStore = mock(DiskStoreImpl.class);

    diskStoreStats = mock(DiskStoreStats.class);
    region = mock(InternalRegion.class);
    entry = mock(DiskEntry.class);
    value = mock(ValueWrapper.class);
    oplog = mock(Oplog.class);

    persistentOplogSet = new PersistentOplogSet(diskStore, System.out);
  }

  @Test
  public void createDelegatesToChildOplog() {
    persistentOplogSet.setChild(oplog);

    persistentOplogSet.create(region, entry, value, true);

    verify(oplog).create(same(region), same(entry), same(value), eq(true));
  }

  @Test
  public void modifyDelegatesToChildOplog() {
    persistentOplogSet.setChild(oplog);

    persistentOplogSet.modify(region, entry, value, true);

    verify(oplog).modify(same(region), same(entry), same(value), eq(true));
  }

  @Test
  public void removeDelegatesToChildOplog() {
    persistentOplogSet.setChild(oplog);

    persistentOplogSet.remove(region, entry, false, true);

    verify(oplog).remove(same(region), same(entry), eq(false), eq(true));
  }

  @Test
  public void getChildReturnsChildOplogIfOplogIdMatches() {
    persistentOplogSet.setChild(oplog);
    when(oplog.getOplogId()).thenReturn(42L);

    CompactableOplog value = persistentOplogSet.getChild(42L);

    assertThat(value).isSameAs(oplog);
  }

  @Test
  public void getChildReturnsNullIfOplogIdDoesNotMatch() {
    persistentOplogSet.setChild(oplog);
    when(oplog.getOplogId()).thenReturn(42L);

    CompactableOplog value = persistentOplogSet.getChild(24L);

    assertThat(value).isNull();
  }

  @Test
  public void getChildReturnsMatchingEntryFromOplogIdToOplogMap() {
    Oplog oplog = mock(Oplog.class);
    persistentOplogSet.setChild(null);
    persistentOplogSet.getOplogIdToOplog().put(24L, oplog);

    CompactableOplog value = persistentOplogSet.getChild(24L);

    assertThat(value).isSameAs(oplog);
  }

  @Test
  public void getChildReturnsNullIfEntriesInOplogIdToOplogMapDoNotMatch() {
    Oplog oplog = mock(Oplog.class);
    persistentOplogSet.setChild(null);
    persistentOplogSet.getOplogIdToOplog().put(42L, oplog);

    CompactableOplog value = persistentOplogSet.getChild(24L);

    assertThat(value).isNull();
  }

  @Test
  public void getChildReturnsMatchingEntryFromInactiveOplogMap() {
    Oplog oplog = mock(Oplog.class);
    persistentOplogSet.setChild(null);
    when(oplog.getOplogId()).thenReturn(24L);
    when(diskStore.getStats()).thenReturn(diskStoreStats);
    persistentOplogSet.addInactive(oplog);

    CompactableOplog value = persistentOplogSet.getChild(24L);

    assertThat(value).isSameAs(oplog);
  }

  @Test
  public void getChildReturnsNullIfEntriesInInactiveOplogMapDoNotMatch() {
    Oplog oplog = mock(Oplog.class);
    persistentOplogSet.setChild(null);
    when(oplog.getOplogId()).thenReturn(42L);
    when(diskStore.getStats()).thenReturn(diskStoreStats);
    persistentOplogSet.addInactive(oplog);

    CompactableOplog value = persistentOplogSet.getChild(24L);

    assertThat(value).isNull();
  }

  @Test
  public void getCompactableOplogsGathersOnlyOplogsThatNeedCompaction() {
    Oplog oplogNeedingCompaction1 = oplog(true);
    Oplog oplogNeedingCompaction2 = oplog(true);
    Map<Long, Oplog> oplogMap = persistentOplogSet.getOplogIdToOplog();
    oplogMap.put(0L, oplog(false));
    oplogMap.put(1L, oplogNeedingCompaction1);
    oplogMap.put(2L, oplogNeedingCompaction2);
    oplogMap.put(3L, oplog(false));
    List<CompactableOplog> compactableOplogs = new ArrayList<>();
    persistentOplogSet.getCompactableOplogs(compactableOplogs, 5);

    assertThat(compactableOplogs)
        .containsExactlyInAnyOrder(oplogNeedingCompaction1, oplogNeedingCompaction2);
  }

  @Test
  public void getCompactableOplogsGathersOplogsInOrderUpToMax() {
    Oplog oplogNeedingCompaction1 = oplog(true);
    Oplog oplogNeedingCompaction2 = oplog(true);
    Oplog oplogNeedingCompaction3 = oplog(true);
    Map<Long, Oplog> oplogMap = persistentOplogSet.getOplogIdToOplog();
    oplogMap.put(1L, oplogNeedingCompaction1);
    oplogMap.put(2L, oplogNeedingCompaction2);
    oplogMap.put(3L, oplogNeedingCompaction3);
    List<CompactableOplog> compactableOplogs = new ArrayList<>();
    persistentOplogSet.getCompactableOplogs(compactableOplogs, 2);

    assertThat(compactableOplogs)
        .containsExactly(oplogNeedingCompaction1, oplogNeedingCompaction2) // in order
        .doesNotContain(oplogNeedingCompaction3);
  }

  @Test
  public void getCompactableOplogsGathersAdditionalOplogsUpToMaxSize() {
    Oplog oplogNeedingCompaction1 = oplog(true);
    Oplog oplogNeedingCompaction2 = oplog(true);
    Map<Long, Oplog> oplogMap = persistentOplogSet.getOplogIdToOplog();
    oplogMap.put(1L, oplogNeedingCompaction1);
    oplogMap.put(2L, oplogNeedingCompaction2);
    List<CompactableOplog> compactableOplogs = new ArrayList<>();

    Oplog existingOplogAlreadyInList = oplog(true);
    compactableOplogs.add(existingOplogAlreadyInList);

    persistentOplogSet.getCompactableOplogs(compactableOplogs, 2);

    assertThat(compactableOplogs)
        .containsExactly(existingOplogAlreadyInList, oplogNeedingCompaction1) // in order
        .doesNotContain(oplogNeedingCompaction2);
  }

  @Test
  public void getCompactableOplogsDoesNotAddToListOfMaxSize() {
    Oplog oplogNeedingCompaction1 = oplog(true);
    Map<Long, Oplog> oplogMap = persistentOplogSet.getOplogIdToOplog();
    oplogMap.put(1L, oplogNeedingCompaction1);
    List<CompactableOplog> compactableOplogs = new ArrayList<>();

    Oplog existingOplogAlreadyInList1 = oplog(true);
    Oplog existingOplogAlreadyInList2 = oplog(true);
    compactableOplogs.add(existingOplogAlreadyInList1);
    compactableOplogs.add(existingOplogAlreadyInList2);

    persistentOplogSet.getCompactableOplogs(compactableOplogs, 2);

    assertThat(compactableOplogs)
        .containsExactly(existingOplogAlreadyInList1, existingOplogAlreadyInList2) // in order
        .doesNotContain(oplogNeedingCompaction1);
  }

  @Test
  public void recoverRegionsThatAreReadyPrintsBucketEntries() {
    PrintStream outputStream = mock(PrintStream.class);
    persistentOplogSet = new PersistentOplogSet(diskStore, outputStream);
    when(diskStore.getDiskInitFile()).thenReturn(mock(DiskInitFile.class));
    when(diskStore.isValidating()).thenReturn(true);
    when(diskStore.getStats()).thenReturn(mock(DiskStoreStats.class));

    String prName = "prName";
    int entryCount = 9;
    Map<Long, DiskRecoveryStore> currentRecoveryMap = persistentOplogSet.getPendingRecoveryMap();
    ValidatingDiskRegion validatingDiskRegion = validatingDiskRegionForBucket(prName, entryCount);
    currentRecoveryMap.put(0L, validatingDiskRegion);

    persistentOplogSet.recoverRegionsThatAreReady();

    ArgumentCaptor<String> printedString = ArgumentCaptor.forClass(String.class);
    verify(outputStream).println(printedString.capture());

    assertThat(printedString.getValue())
        .startsWith(prName)
        .contains("entryCount=" + 9)
        .contains("bucketCount=" + 1);
  }

  @Test
  public void recoverRegionsThatAreReadyPrintsRegionSize() {
    PrintStream outputStream = mock(PrintStream.class);
    persistentOplogSet = new PersistentOplogSet(diskStore, outputStream);
    when(diskStore.getDiskInitFile()).thenReturn(mock(DiskInitFile.class));
    when(diskStore.isValidating()).thenReturn(true);
    when(diskStore.getStats()).thenReturn(mock(DiskStoreStats.class));

    String regionName = "regionName";
    int entryCount = 13;
    Map<Long, DiskRecoveryStore> currentRecoveryMap = persistentOplogSet.getPendingRecoveryMap();
    ValidatingDiskRegion validatingDiskRegion = validatingDiskRegion(regionName, entryCount);
    currentRecoveryMap.put(0L, validatingDiskRegion);

    persistentOplogSet.recoverRegionsThatAreReady();

    ArgumentCaptor<String> printedString = ArgumentCaptor.forClass(String.class);
    verify(outputStream).println(printedString.capture());

    assertThat(printedString.getValue())
        .startsWith(regionName)
        .contains("entryCount=" + entryCount);
  }

  private ValidatingDiskRegion validatingDiskRegionForBucket(String prName, int entryCount) {
    ValidatingDiskRegion validatingDiskRegion = mock(ValidatingDiskRegion.class);
    when(validatingDiskRegion.isBucket()).thenReturn(true);
    when(validatingDiskRegion.getDiskRegionView()).thenReturn(mock(DiskRegionView.class));
    when(validatingDiskRegion.getPrName()).thenReturn(prName);
    when(validatingDiskRegion.size()).thenReturn(entryCount);
    return validatingDiskRegion;
  }

  private ValidatingDiskRegion validatingDiskRegion(String regionName, int entryCount) {
    ValidatingDiskRegion validatingDiskRegion = mock(ValidatingDiskRegion.class);
    when(validatingDiskRegion.getName()).thenReturn(regionName);
    when(validatingDiskRegion.isBucket()).thenReturn(false);
    when(validatingDiskRegion.getDiskRegionView()).thenReturn(mock(DiskRegionView.class));
    when(validatingDiskRegion.size()).thenReturn(entryCount);
    return validatingDiskRegion;
  }

  private Oplog oplog(boolean needsCompaction) {
    Oplog oplog = mock(Oplog.class);
    when(oplog.needsCompaction()).thenReturn(needsCompaction);
    return oplog;
  }
}
