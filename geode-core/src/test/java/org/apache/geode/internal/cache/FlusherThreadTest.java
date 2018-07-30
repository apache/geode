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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.DiskAccessException;

public class FlusherThreadTest {

  private DiskStoreImpl diskStoreImpl;
  private DiskStoreStats diskStoreStats;
  private DiskStoreImpl.FlusherThread flusherThread;

  private final int DRAIN_LIST_SIZE = 5;

  @Before
  public void setup() {
    diskStoreImpl = mock(DiskStoreImpl.class);
    diskStoreStats = mock(DiskStoreStats.class);
    PersistentOplogSet persistentOpLogSet = mock(PersistentOplogSet.class);

    when(diskStoreImpl.getAsyncMonitor()).thenReturn(new Object());
    when(diskStoreImpl.getForceFlushCount()).thenReturn(new AtomicInteger(1));
    when(diskStoreImpl.fillDrainList()).thenReturn(DRAIN_LIST_SIZE).thenReturn(0);
    when(diskStoreImpl.getDrainList()).thenReturn(new ArrayList());
    when(diskStoreImpl.getPersistentOplogs()).thenReturn(persistentOpLogSet);
    when(diskStoreImpl.getStats()).thenReturn(diskStoreStats);
    when(diskStoreImpl.checkAndClearForceFlush()).thenReturn(true);
    when(diskStoreImpl.isStopFlusher()).thenReturn(false).thenReturn(true);

    flusherThread = new DiskStoreImpl.FlusherThread(diskStoreImpl);
  }

  @Test
  public void asyncFlushIncrementsQueueSizeStat() {
    flusherThread.doAsyncFlush();

    verify(diskStoreStats, times(1)).incQueueSize(-DRAIN_LIST_SIZE);
  }

  @Test
  public void asyncFlushDoesNotIncrementQueueSizeWhenExceptionThrown() {
    when(diskStoreImpl.getDrainList()).thenThrow(DiskAccessException.class);

    flusherThread.doAsyncFlush();

    verify(diskStoreStats, never()).incQueueSize(anyInt());
  }

}
