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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import org.apache.geode.internal.cache.PartitionedRegion;

public class SingleThreadColocationLoggerTest {

  private Function<PartitionedRegion, Set<String>> allColocationRegionsProvider;
  private Consumer<String> logger;
  private PartitionedRegion region;
  private String regionName;
  private Function<Runnable, Thread> threadProvider;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(STRICT_STUBS);

  @Before
  public void setUp() {
    allColocationRegionsProvider = mock(Function.class);
    logger = mock(Consumer.class);
    region = mock(PartitionedRegion.class);
    regionName = "theRegionName";
    threadProvider = mock(Function.class);

    when(region.getName())
        .thenReturn(regionName);
    when(threadProvider.apply(any()))
        .thenReturn(mock(Thread.class));
  }

  @Test
  public void startWhenAlreadyRunningThrowsIllegalStateException() {
    SingleThreadColocationLogger colocationLogger =
        new SingleThreadColocationLogger(region, 500, 1000, logger,
            allColocationRegionsProvider, threadProvider);
    colocationLogger.start();

    Throwable thrown = catchThrowable(() -> colocationLogger.start());

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("ColocationLogger for " + regionName + " is already running");
  }
}
