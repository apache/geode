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
package org.apache.geode.internal.cache.entries;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.DiskRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.offheap.StoredObject;

public class DiskEntryHelperTest {


  private InternalRegion internalRegion = mock(InternalRegion.class);

  private DiskRegion diskRegion = mock(DiskRegion.class);

  private boolean callDoSynchronousWrite() {
    return DiskEntry.Helper.doSynchronousWrite(internalRegion, diskRegion);
  }

  @Test
  public void doSynchronousWriteReturnsTrueWhenDiskRegionIsSync() {
    when(diskRegion.isSync()).thenReturn(true);
    boolean result = callDoSynchronousWrite();
    assertThat(result).isTrue();
  }

  @Test
  public void doSynchronousWriteReturnsTrueWhenPersistentRegionIsInitializing() {
    when(diskRegion.isSync()).thenReturn(false);
    when(diskRegion.isBackup()).thenReturn(true);
    when(internalRegion.isInitialized()).thenReturn(false);

    boolean result = callDoSynchronousWrite();

    assertThat(result).isTrue();
  }

  @Test
  public void doSynchronousWriteReturnsFalseWhenOverflowOnly() {
    when(diskRegion.isSync()).thenReturn(false);
    when(diskRegion.isBackup()).thenReturn(false);
    when(internalRegion.isInitialized()).thenReturn(false);

    boolean result = callDoSynchronousWrite();

    assertThat(result).isFalse();
  }

  @Test
  public void doSynchronousWriteReturnsFalseWhenPersistentRegionIsInitialized() {
    when(diskRegion.isSync()).thenReturn(false);
    when(diskRegion.isBackup()).thenReturn(true);
    when(internalRegion.isInitialized()).thenReturn(true);

    boolean result = callDoSynchronousWrite();

    assertThat(result).isFalse();
  }

  @Test
  public void whenHelperUpdateCalledAndDiskRegionAcquireReadLockThrowsRegionDestroyedExceptionThenStoredObjectShouldBeReleased()
      throws Exception {
    LocalRegion lr = mock(LocalRegion.class);
    DiskEntry diskEntry = mock(DiskEntry.class);
    when(diskEntry.getDiskId()).thenReturn(mock(DiskId.class));
    EntryEventImpl entryEvent = mock(EntryEventImpl.class);
    DiskRegion diskRegion = mock(DiskRegion.class);
    when(lr.getDiskRegion()).thenReturn(diskRegion);
    Mockito.doThrow(new RegionDestroyedException("Region Destroyed", "mocked region"))
        .when(diskRegion).acquireReadLock();
    StoredObject storedObject = mock(StoredObject.class);
    try {
      DiskEntry.Helper.update(diskEntry, lr, storedObject, entryEvent);
      fail();
    } catch (RegionDestroyedException rde) {
      verify(storedObject, times(1)).release();
    }
  }


  @Test
  public void whenBasicUpdateWithDiskRegionBackupAndEntryNotSetThenReleaseOnStoredObjectShouldBeCalled()
      throws Exception {
    StoredObject storedObject = mock(StoredObject.class);
    LocalRegion lr = mock(LocalRegion.class);
    DiskEntry diskEntry = mock(DiskEntry.class);
    when(diskEntry.getDiskId()).thenReturn(mock(DiskId.class));
    EntryEventImpl entryEvent = mock(EntryEventImpl.class);
    DiskRegion diskRegion = mock(DiskRegion.class);
    when(diskRegion.isBackup()).thenReturn(true);
    doThrow(new RegionDestroyedException("", "")).when(diskRegion).put(eq(diskEntry), eq(lr),
        ArgumentMatchers.any(DiskEntry.Helper.ValueWrapper.class), anyBoolean());
    when(lr.getDiskRegion()).thenReturn(diskRegion);
    try {
      DiskEntry.Helper.basicUpdateForTesting(diskEntry, lr, storedObject, entryEvent);
      fail();
    } catch (RegionDestroyedException rde) {
      verify(storedObject, times(1)).release();
    }
  }

  @Test
  public void whenBasicUpdateWithDiskRegionBackupAndAsyncWritesAndEntryNotSetThenReleaseOnStoredObjectShouldBeCalled()
      throws Exception {
    StoredObject storedObject = mock(StoredObject.class);
    LocalRegion lr = mock(LocalRegion.class);
    DiskEntry diskEntry = mock(DiskEntry.class);
    when(diskEntry.getDiskId()).thenReturn(mock(DiskId.class));
    EntryEventImpl entryEvent = mock(EntryEventImpl.class);
    DiskRegion diskRegion = mock(DiskRegion.class);
    when(diskRegion.isBackup()).thenReturn(true);
    doThrow(new RegionDestroyedException("", "")).when(diskRegion).put(eq(diskEntry), eq(lr),
        ArgumentMatchers.any(DiskEntry.Helper.ValueWrapper.class), anyBoolean());
    when(lr.getDiskRegion()).thenReturn(diskRegion);

    when(diskRegion.isSync()).thenReturn(false);
    when(lr.isInitialized()).thenReturn(true);
    when(lr.getConcurrencyChecksEnabled()).thenThrow(new RegionDestroyedException("", ""));
    try {
      DiskEntry.Helper.basicUpdateForTesting(diskEntry, lr, storedObject, entryEvent);
      fail();
    } catch (RegionDestroyedException rde) {
      verify(storedObject, times(1)).release();
    }
  }

  @Test
  public void whenBasicUpdateButNotBackupAndEntrySet() throws Exception {
    StoredObject storedObject = mock(StoredObject.class);
    LocalRegion lr = mock(LocalRegion.class);
    DiskEntry diskEntry = mock(DiskEntry.class);
    when(diskEntry.getDiskId()).thenReturn(mock(DiskId.class));
    EntryEventImpl entryEvent = mock(EntryEventImpl.class);
    DiskRegion diskRegion = mock(DiskRegion.class);
    when(diskRegion.isBackup()).thenReturn(false);
    when(lr.getDiskRegion()).thenReturn(diskRegion);
    DiskEntry.Helper.basicUpdateForTesting(diskEntry, lr, storedObject, entryEvent);
    verify(storedObject, times(0)).release();
  }

  @Test
  public void whenBasicUpdateButNotBackupAndDiskIdIsNullAndEntrySet() throws Exception {
    StoredObject storedObject = mock(StoredObject.class);
    LocalRegion lr = mock(LocalRegion.class);
    DiskEntry diskEntry = mock(DiskEntry.class);
    EntryEventImpl entryEvent = mock(EntryEventImpl.class);
    DiskRegion diskRegion = mock(DiskRegion.class);
    when(diskRegion.isBackup()).thenReturn(false);
    when(lr.getDiskRegion()).thenReturn(diskRegion);
    DiskEntry.Helper.basicUpdateForTesting(diskEntry, lr, storedObject, entryEvent);
    verify(storedObject, times(0)).release();
  }
}
