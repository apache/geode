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
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;

public class InternalResourceManagerTest {

  private final CountDownLatch hangLatch = new CountDownLatch(1);
  private InternalResourceManager resourceManager;
  private CountDownLatch numberOfStartupTasksRemaining;

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
        executor.submit(() -> hangLatch.await(getTimeout().getValue(), SECONDS));

    resourceManager.close();

    await("Submitted task is done")
        .until(() -> submittedTask.isDone());
  }

  @Test
  public void runsIfZeroStartupTask() {
    AtomicBoolean completionActionHasRun = new AtomicBoolean(false);
    Runnable completionAction = () -> completionActionHasRun.set(true);

    resourceManager.runWhenStartupTasksComplete(completionAction, exceptionAction());

    assertThat(completionActionHasRun)
        .withFailMessage("Startup complete action did not run after startup tasks finished")
        .isTrue();
  }

  @Test
  public void runsAfterOneStartupTaskComplete() {
    AtomicBoolean completionActionHasRun = new AtomicBoolean(false);
    Runnable completionAction = () -> completionActionHasRun.set(true);

    resourceManager.addStartupTask(CompletableFuture.runAsync(waitingTask()));

    resourceManager.runWhenStartupTasksComplete(completionAction, exceptionAction());

    assertThat(completionActionHasRun)
        .withFailMessage("Startup complete action ran before startup tasks finished")
        .isFalse();

    completeAnyWaitingTasks();

    await().untilAsserted(() -> assertThat(completionActionHasRun)
        .withFailMessage("Startup complete action did not run after startup tasks finished")
        .isTrue());
  }

  @Test
  public void runsAfterManyStartupTaskCompletes() {
    AtomicBoolean completionActionHasRun = new AtomicBoolean(false);
    Runnable completionAction = () -> completionActionHasRun.set(true);

    resourceManager.addStartupTask(CompletableFuture.runAsync(waitingTask()));
    resourceManager.addStartupTask(CompletableFuture.runAsync(waitingTask()));

    resourceManager.runWhenStartupTasksComplete(completionAction, exceptionAction());

    assertThat(completionActionHasRun)
        .withFailMessage("Startup complete action ran before startup tasks finished")
        .isFalse();

    completeAnyWaitingTasks();

    await().untilAsserted(() -> assertThat(completionActionHasRun)
        .withFailMessage("Startup complete action did not run after startup tasks finished")
        .isTrue());
  }

  @Test
  public void runClearsAddedStartupTasks() {
    AtomicBoolean completionActionHasRun = new AtomicBoolean(false);
    Runnable completionAction = () -> completionActionHasRun.set(true);

    resourceManager.addStartupTask(CompletableFuture.runAsync(waitingTask()));

    resourceManager.runWhenStartupTasksComplete(() -> {
    }, exceptionAction());

    resourceManager.runWhenStartupTasksComplete(completionAction, exceptionAction());

    assertThat(completionActionHasRun)
        .withFailMessage("Did not clear added startup tasks")
        .isTrue();
  }

  @Test
  public void runsExceptionActionAfterOneStartupTaskThrowsException() {
    AtomicReference<Throwable> exceptionActionConsumed = new AtomicReference<>();
    Consumer<Throwable> exceptionAction = exceptionActionConsumed::set;

    Runnable completionAction = () -> fail("Ran completion action");

    CompletableFuture<Void> taskCompletedExceptionally = new CompletableFuture<>();
    taskCompletedExceptionally.completeExceptionally(new IllegalStateException("Error message"));

    resourceManager.addStartupTask(taskCompletedExceptionally);
    resourceManager.runWhenStartupTasksComplete(completionAction, exceptionAction);

    assertThat(exceptionActionConsumed.get())
        .withFailMessage("Did not run exception action")
        .isInstanceOf(CompletionException.class);

    assertThat(exceptionActionConsumed.get().getCause())
        .withFailMessage("Completion exception did not have cause")
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Error message");
  }

  private void completeAnyWaitingTasks() {
    numberOfStartupTasksRemaining.countDown();
  }

  private Runnable waitingTask() {
    return () -> {
      try {
        numberOfStartupTasksRemaining.await();
      } catch (InterruptedException e) {
        // ignore
      }
    };
  }

  private Consumer<Throwable> exceptionAction() {
    return (throwable) -> fail("Ran exception action");
  }
}
