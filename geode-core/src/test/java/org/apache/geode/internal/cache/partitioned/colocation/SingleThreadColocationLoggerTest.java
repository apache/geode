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
package org.apache.geode.internal.cache.partitioned.colocation;

import static java.lang.System.lineSeparator;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.util.CollectionUtils;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class SingleThreadColocationLoggerTest {

  private static final long TIMEOUT_MILLIS = GeodeAwaitility.getTimeout().toMillis();

  private Function<PartitionedRegion, Set<String>> allColocationRegionsProvider;
  private Consumer<String> logger;
  private PartitionedRegion region;
  private String regionName;
  private ExecutorService executorService;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void setUp() {
    allColocationRegionsProvider = mock(Function.class);
    logger = mock(Consumer.class);
    region = mock(PartitionedRegion.class);
    regionName = "theRegionName";
    executorService = executorServiceRule.getExecutorService();

    InternalDistributedSystem system = mock(InternalDistributedSystem.class);

    when(region.getFullPath())
        .thenReturn(Region.SEPARATOR + regionName);
    when(region.getName())
        .thenReturn(regionName);
    when(region.getSystem())
        .thenReturn(system);
    when(system.getCancelCriterion())
        .thenReturn(mock(CancelCriterion.class));
  }

  @Test
  public void startWhenAlreadyRunningThrowsIllegalStateException() {
    SingleThreadColocationLogger colocationLogger =
        new SingleThreadColocationLogger(region, 500, 1000, logger,
            allColocationRegionsProvider, executorService);
    colocationLogger.start();

    Throwable thrown = catchThrowable(() -> colocationLogger.start());

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("ColocationLogger for " + regionName + " is already running");
  }

  @Test
  public void missingChildrenIsEmptyByDefault() {
    SingleThreadColocationLogger colocationLogger =
        new SingleThreadColocationLogger(region, 100, 200, logger,
            allColocationRegionsProvider, executorService);

    colocationLogger.start();

    assertThat(colocationLogger.getMissingChildren()).isEmpty();
  }

  @Test
  public void completesIfMissingChildRegionIsNeverAdded() throws Exception {
    SingleThreadColocationLogger colocationLogger =
        new SingleThreadColocationLogger(region, 100, 200, logger,
            allColocationRegionsProvider, executorService);
    colocationLogger.start();
    Future<?> completed = colocationLogger.getFuture();
    completed.get(TIMEOUT_MILLIS, MILLISECONDS);

    boolean value = completed.isDone();

    assertThat(value).isTrue();
  }

  @Test
  public void addMissingChildRegionAddsToMissingChildren() {
    SingleThreadColocationLogger colocationLogger =
        new SingleThreadColocationLogger(region, 100, 200, logger,
            allColocationRegionsProvider, executorService);
    colocationLogger.start();

    String missingChild1 = "/childRegion1";
    colocationLogger.addMissingChildRegion(missingChild1);

    String missingChild2 = "/childRegion2";
    colocationLogger.addMissingChildRegion(missingChild2);

    String missingChild3 = "/childRegion3";
    colocationLogger.addMissingChildRegion(missingChild3);

    assertThat(colocationLogger.getMissingChildren())
        .containsExactly(missingChild1, missingChild2, missingChild3);
  }

  @Test
  public void logsMissingChildRegion() {
    SingleThreadColocationLogger colocationLogger =
        new SingleThreadColocationLogger(region, 100, 200, logger,
            allColocationRegionsProvider, executorService);
    colocationLogger.start();
    String missingChild = "/childRegion";

    colocationLogger.addMissingChildRegion(missingChild);

    String message = String.format(
        "Persistent data recovery for region %s is prevented by offline colocated %s%s%s",
        region.getFullPath(), "region", lineSeparator() + '\t', missingChild);

    verify(logger,
        timeout(TIMEOUT_MILLIS).atLeastOnce())
            .accept(message);
  }

  @Test
  public void logsMissingChildRegionUntilCompletion() throws Exception {
    SingleThreadColocationLogger colocationLogger =
        new SingleThreadColocationLogger(region, 100, 200, logger,
            allColocationRegionsProvider, executorService);
    colocationLogger.start();
    Future<?> completed = colocationLogger.getFuture();
    String missingChild = "/childRegion";
    when(allColocationRegionsProvider.apply(eq(region)))
        .thenReturn(singleton(missingChild));
    colocationLogger.addMissingChildRegion(missingChild);

    verify(logger,
        timeout(TIMEOUT_MILLIS).atLeastOnce())
            .accept(anyString());

    colocationLogger.updateAndGetMissingChildRegions();

    completed.get(TIMEOUT_MILLIS, MILLISECONDS);

    verifyNoMoreInteractions(logger);
  }

  @Test
  public void addMissingChildRegionAfterCompletionDoesNotLog() throws Exception {
    SingleThreadColocationLogger colocationLogger =
        new SingleThreadColocationLogger(region, 100, 200, logger,
            allColocationRegionsProvider, executorService);
    colocationLogger.start();
    Future<?> completed = colocationLogger.getFuture();
    completed.get(TIMEOUT_MILLIS, MILLISECONDS);

    colocationLogger.addMissingChildRegion("/childRegion");

    verifyZeroInteractions(logger);
  }

  @Test
  public void updateAndGetMissingChildRegionsUpdatesMissingChildren() {
    SingleThreadColocationLogger colocationLogger =
        new SingleThreadColocationLogger(region, 100, 200, logger,
            allColocationRegionsProvider, executorService);
    colocationLogger.start();
    String missingChild1 = "/childRegion1";
    colocationLogger.addMissingChildRegion(missingChild1);
    String missingChild2 = "/childRegion2";
    colocationLogger.addMissingChildRegion(missingChild2);
    String missingChild3 = "/childRegion3";
    colocationLogger.addMissingChildRegion(missingChild3);
    when(allColocationRegionsProvider.apply(eq(region)))
        .thenReturn(CollectionUtils.asSet(missingChild1, missingChild2, missingChild3));

    colocationLogger.updateAndGetMissingChildRegions();

    assertThat(colocationLogger.getMissingChildren())
        .isEmpty();
  }
}
