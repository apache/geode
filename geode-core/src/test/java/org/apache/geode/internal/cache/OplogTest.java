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
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OplogTest {
  private final DiskStoreImpl.OplogCompactor compactor = mock(DiskStoreImpl.OplogCompactor.class);
  private final PersistentOplogSet parent = mock(PersistentOplogSet.class);
  private final long oplogId = 1;
  private Oplog oplog;

  @Before
  public void setup() {
    when(parent.getParent()).thenReturn(mock(DiskStoreImpl.class));
    oplog = spy(new Oplog(oplogId, parent));
    doReturn(true).when(oplog).needsCompaction();
    doReturn(true).when(oplog).calledByCompactorThread();
  }

  @Test
  public void noCompactIfDoesNotNeedCompaction() {
    doReturn(false).when(oplog).needsCompaction();
    assertThat(oplog.compact(compactor)).isEqualTo(0);
  }

  @Test
  public void noCompactIfNotKeepCompactorRunning() {
    when(compactor.keepCompactorRunning()).thenReturn(false);
    assertThat(oplog.compact(compactor)).isEqualTo(0);
  }

  @Test
  public void handlesNoLiveValuesIfNoLiveValueInOplog() {
    when(compactor.keepCompactorRunning()).thenReturn(true);

    doReturn(true).when(oplog).hasNoLiveValues();
    assertThat(oplog.compact(compactor)).isEqualTo(0);
    verify(oplog, times(1)).handleNoLiveValues();
  }

  @Test
  public void invockeCleanupAfterCompaction() {
    when(compactor.keepCompactorRunning()).thenReturn(true);
    doReturn(mock(DiskStoreStats.class)).when(oplog).getStats();
    doReturn(false).when(oplog).hasNoLiveValues();
    oplog.compact(compactor);
    verify(oplog, times(1)).cleanupAfterCompaction(eq(false));
  }

  @Test
  public void handlesNoLiveValuesIfCompactSuccessful() {
    oplog.getTotalLiveCount().set(5);
    oplog.cleanupAfterCompaction(false);
    verify(oplog, times(1)).handleNoLiveValues();
    assertThat(oplog.getTotalLiveCount().get()).isEqualTo(0);
  }
}
