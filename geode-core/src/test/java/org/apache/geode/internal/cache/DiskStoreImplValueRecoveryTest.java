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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.cache.control.InternalResourceManager;

public class DiskStoreImplValueRecoveryTest {

  private DiskStoreImpl diskStore;
  private InternalResourceManager internalResourceManager;

  @Before
  public void setUp() {
    InternalCache cache = mock(InternalCache.class);
    DiskStoreAttributes diskStoreAttributes = new DiskStoreAttributes();
    StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    internalResourceManager = mock(InternalResourceManager.class);

    when(statisticsFactory.createStatistics(any(), any())).thenReturn(mock(Statistics.class));
    when(cache.getCachePerfStats()).thenReturn(mock(CachePerfStats.class));
    when(cache.getDiskStoreMonitor()).thenReturn(mock(DiskStoreMonitor.class));

    diskStore = new DiskStoreImpl(cache, "name", diskStoreAttributes, false, null, false, false,
        false, false, false, false, statisticsFactory,
        internalResourceManager);
  }

  @After
  public void tearDown() {
    diskStore.close();
  }

  @Test
  public void scheduleValueRecoveryAddsStartupTask() {
    diskStore.scheduleValueRecovery(Collections.emptySet(), Collections.emptyMap());

    verify(internalResourceManager).addStartupTask(any());
  }

  @Test
  public void startupTaskCompletesWhenValueRecoveryCompletes() {
    diskStore.scheduleValueRecovery(Collections.emptySet(), Collections.emptyMap());

    ArgumentCaptor<CompletableFuture<Void>> startupTaskCaptor =
        ArgumentCaptor.forClass(CompletableFuture.class);
    verify(internalResourceManager).addStartupTask(startupTaskCaptor.capture());

    CompletableFuture startupTask = startupTaskCaptor.getValue();

    await().until(() -> isCompleted(startupTask));
  }

  @Test
  public void startupTaskCompletesExceptionallyWhenValueRecoveryThrows() {
    Oplog oplog = mock(Oplog.class);
    RuntimeException exception = new RuntimeException();
    doThrow(exception).when(oplog).recoverValuesIfNeeded(any());

    diskStore.scheduleValueRecovery(Collections.singleton(oplog), Collections.emptyMap());

    ArgumentCaptor<CompletableFuture<Void>> startupTaskCaptor =
        ArgumentCaptor.forClass(CompletableFuture.class);
    verify(internalResourceManager).addStartupTask(startupTaskCaptor.capture());

    CompletableFuture startupTask = startupTaskCaptor.getValue();

    await().until(startupTask::isCompletedExceptionally);
  }

  private boolean isCompleted(CompletableFuture completableFuture) {
    return completableFuture.isDone() && !completableFuture.isCompletedExceptionally()
        && !completableFuture.isCancelled();
  }
}
