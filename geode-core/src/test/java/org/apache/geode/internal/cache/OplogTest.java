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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;


public class OplogTest {
  private final DiskStoreImpl.OplogCompactor compactor = mock(DiskStoreImpl.OplogCompactor.class);
  private final PersistentOplogSet parent = mock(PersistentOplogSet.class);
  private final long oplogId = 1;
  private Oplog oplogSpy;

  @Before
  public void setup() {
    DiskStoreImpl parentDiskStore = mock(DiskStoreImpl.class);
    when(parentDiskStore.getWriteBufferSize()).thenReturn(32768);
    when(parent.getParent()).thenReturn(parentDiskStore);
    oplogSpy = spy(new Oplog(oplogId, parent));
    doReturn(true).when(oplogSpy).needsCompaction();
    doReturn(true).when(oplogSpy).calledByCompactorThread();
  }

  @Test
  public void noCompactIfDoesNotNeedCompaction() {
    doReturn(false).when(oplogSpy).needsCompaction();
    assertThat(oplogSpy.compact(compactor)).isEqualTo(0);
  }

  @Test
  public void noCompactIfNotKeepCompactorRunning() {
    when(compactor.keepCompactorRunning()).thenReturn(false);
    assertThat(oplogSpy.compact(compactor)).isEqualTo(0);
  }

  @Test
  public void handlesNoLiveValuesIfNoLiveValueInOplog() {
    when(compactor.keepCompactorRunning()).thenReturn(true);

    doReturn(true).when(oplogSpy).hasNoLiveValues();
    assertThat(oplogSpy.compact(compactor)).isEqualTo(0);
    verify(oplogSpy, times(1)).handleNoLiveValues();
  }

  @Test
  public void invockeCleanupAfterCompaction() {
    when(compactor.keepCompactorRunning()).thenReturn(true);
    doReturn(mock(DiskStoreStats.class)).when(oplogSpy).getStats();
    doReturn(false).when(oplogSpy).hasNoLiveValues();
    oplogSpy.compact(compactor);
    verify(oplogSpy, times(1)).cleanupAfterCompaction(eq(false));
  }

  @Test
  public void handlesNoLiveValuesIfCompactSuccessful() {
    oplogSpy.getTotalLiveCount().set(5);
    oplogSpy.cleanupAfterCompaction(false);
    verify(oplogSpy, times(1)).handleNoLiveValues();
    assertThat(oplogSpy.getTotalLiveCount().get()).isEqualTo(0);
  }

  @Test
  public void writeBufferSizeValueIsObtainedFromParentIfSystemPropertyNotDefined() {
    when(oplogSpy.writeBufferSizeSystemPropertyIsDefined()).thenReturn(false);
    assertThat(oplogSpy.getWriteBufferCapacity()).isEqualTo(32768);
  }

  @Test
  public void writeBufferSizeValueIsObtainedFromSystemPropertyWhenDefined() {
    when(oplogSpy.writeBufferSizeSystemPropertyIsDefined()).thenReturn(true);
    assertThat(oplogSpy.getWriteBufferCapacity()).isNull();
  }
}
