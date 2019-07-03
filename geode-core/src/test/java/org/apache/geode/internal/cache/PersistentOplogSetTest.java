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

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.entries.DiskEntry.Helper.ValueWrapper;

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

    persistentOplogSet = new PersistentOplogSet(diskStore);
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
}
