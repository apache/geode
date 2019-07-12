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
package org.apache.geode.internal.cache.control;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.Statistics;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;

public class InternalResourceManagerTest {

  private InternalResourceManager resourceManager;

  private final CountDownLatch hangLatch = new CountDownLatch(1);
  private InternalCache cache;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    cache = mock(InternalCache.class);
    DistributedSystem distributedSystem = mock(DistributedSystem.class);

    when(cache.getDistributedSystem()).thenReturn(distributedSystem);
    when(distributedSystem.createAtomicStatistics(any(), anyString()))
        .thenReturn(mock(Statistics.class));

    resourceManager = InternalResourceManager.createResourceManager(cache);
  }

  @After
  public void tearDown() {
    hangLatch.countDown();
    resourceManager.close();
  }

  @Test
  public void closeDoesNotHangWaitingForExecutorTasks() {
    ScheduledExecutorService executor = resourceManager.getExecutor();

    Future<Boolean> submittedTask =
        executor.submit(() -> hangLatch.await(getTimeout().getValue(), SECONDS));

    resourceManager.close();

    await("Submitted task is done")
        .until(() -> submittedTask.isDone());
  }

    @Test
    public void nonExecutedRunnablesShouldBeInterruptedSoFutureGetDoesNotHang()
    throws InterruptedException, ExecutionException {
        ScheduledExecutorService executor = resourceManager.getExecutor();
        
        Future<Boolean> submittedTask =
        executor.schedule(() -> {
            return true;
        }, 1, TimeUnit.DAYS);
        
        resourceManager.close();
        
        thrown.expect(CancellationException.class);
        submittedTask.get();
    }
    
  @Test
  public void startsWithASuccessfullyCompletedAsyncStartupStage() {
    InternalResourceManager resourceManager = InternalResourceManager.createResourceManager(cache);

    CompletionStage<Void> startupStage = resourceManager.getStartupStage();

    assertThat(startupStage).isCompleted();
  }

  @Test
  public void remembersTheAddedStartupStage() {
    InternalResourceManager resourceManager = InternalResourceManager.createResourceManager(cache);
    @SuppressWarnings("unchecked")
    CompletionStage<Void> expectedStartupStage =
        (CompletionStage<Void>) mock(CompletionStage.class);

    resourceManager.addStartupStage(expectedStartupStage);
    CompletionStage<Void> actualStartupStage = resourceManager.getStartupStage();

    // Cast to Object so that AssertJ won't convert our mock CompletionStage to a bogus
    // CompletableFuture before comparing.
    assertThat((Object) actualStartupStage)
        .isSameAs(expectedStartupStage);
  }
}
