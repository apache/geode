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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
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

  private final CountDownLatch hangLatch = new CountDownLatch(1);
  private InternalResourceManager resourceManager;
  private CountDownLatch numberOfStartupTasksRemaining;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    InternalCache cache = mock(InternalCache.class);
    DistributedSystem distributedSystem = mock(DistributedSystem.class);

    when(cache.getDistributedSystem()).thenReturn(distributedSystem);
    when(distributedSystem.createAtomicStatistics(any(), anyString()))
        .thenReturn(mock(Statistics.class));

    resourceManager = InternalResourceManager.createResourceManager(cache);
    numberOfStartupTasksRemaining = new CountDownLatch(1);
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
        executor.submit(() -> hangLatch.await(getTimeout().getSeconds(), SECONDS));

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
  public void addStartupTaskThrowsIfStartupTaskIsNull() {
    Throwable thrown = catchThrowable(() -> resourceManager.addStartupTask(null));

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void allOfStartupTasksIsDoneIfZeroStartupTasks() {
    CompletableFuture<Void> withoutStartupTasks = resourceManager.allOfStartupTasks();

    assertThat(withoutStartupTasks).isDone();
  }

  @Test
  public void allOfStartupTasksIsDoneAfterOneStartupTaskCompletes() {
    resourceManager.addStartupTask(CompletableFuture.completedFuture(null));

    CompletableFuture<Void> withOneStartupTask = resourceManager.allOfStartupTasks();

    assertThat(withOneStartupTask).isDone();
  }

  @Test
  public void allOfStartupTasksIsNotDoneIfOnlyStartupTaskIsNotComplete() {
    resourceManager.addStartupTask(new CompletableFuture<>());

    CompletableFuture<Void> withOneStartupTask = resourceManager.allOfStartupTasks();

    assertThat(withOneStartupTask).isNotDone();
  }

  @Test
  public void allOfStartupTasksIsDoneAfterManyStartupTasksComplete() {
    resourceManager.addStartupTask(CompletableFuture.completedFuture(null));
    resourceManager.addStartupTask(CompletableFuture.completedFuture(null));

    CompletableFuture<Void> withManyStartupTasks = resourceManager.allOfStartupTasks();

    assertThat(withManyStartupTasks).isDone();
  }

  @Test
  public void allOfStartupTasksIsNotDoneIfAnyStartupTaskIsNotComplete() {
    resourceManager.addStartupTask(CompletableFuture.completedFuture(null));
    resourceManager.addStartupTask(new CompletableFuture<>());

    CompletableFuture<Void> withManyStartupTasks = resourceManager.allOfStartupTasks();

    assertThat(withManyStartupTasks).isNotDone();
  }

  @Test
  public void allOfStartupTasksClearsAddedStartupTasks() {
    resourceManager.addStartupTask(new CompletableFuture<>());

    // Clears the startup tasks
    resourceManager.allOfStartupTasks();

    CompletableFuture<Void> withoutStartupTasks = resourceManager.allOfStartupTasks();

    assertThat(withoutStartupTasks).isDone();
  }

  @Test
  public void allOfStartupTasksIsCompletedExceptionallyIfFirstStartupTaskThrows() {
    CompletableFuture<Void> completesExceptionally = new CompletableFuture<>();
    completesExceptionally.completeExceptionally(new RuntimeException());

    resourceManager.addStartupTask(completesExceptionally);
    resourceManager.addStartupTask(CompletableFuture.completedFuture(null));

    CompletableFuture<Void> withManyStartupTasks = resourceManager.allOfStartupTasks();

    assertThat(withManyStartupTasks).isCompletedExceptionally();
  }

  @Test
  public void allOfStartupTasksIsCompletedExceptionallyIfLastStartupTaskThrows() {
    CompletableFuture<Void> completesExceptionally = new CompletableFuture<>();
    completesExceptionally.completeExceptionally(new RuntimeException());

    resourceManager.addStartupTask(CompletableFuture.completedFuture(null));
    resourceManager.addStartupTask(completesExceptionally);

    CompletableFuture<Void> withManyStartupTasks = resourceManager.allOfStartupTasks();

    assertThat(withManyStartupTasks).isCompletedExceptionally();
  }
}
