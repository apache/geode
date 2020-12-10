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
package org.apache.geode.distributed.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

public class ClusterOperationExecutorsTest {
  private DistributionStats stats;
  private InternalDistributedSystem system;
  private DistributionConfig config;

  @Before
  public void setup() {
    stats = mock(DistributionStats.class);
    system = mock(InternalDistributedSystem.class);
    config = mock(DistributionConfig.class);
    when(system.getConfig()).thenReturn(config);
  }

  @Test
  public void numOfFEThreadsIsAtLeast100() {
    int minNumberOfFunctionExecutionThreads = 100;

    ClusterOperationExecutors executors = new ClusterOperationExecutors(stats, system);

    assertThat(executors.MAX_FE_THREADS)
        .isGreaterThanOrEqualTo(minNumberOfFunctionExecutionThreads);
  }

  @Test
  public void numOfFEThreadsCanBeSet() {
    int numberOfFunctionExecutionThreads = 400;
    String functionExecutionThreadsPropertyName = "DistributionManager.MAX_FE_THREADS";
    System.setProperty(functionExecutionThreadsPropertyName,
        Integer.toString(numberOfFunctionExecutionThreads));

    try {
      ClusterOperationExecutors executors = new ClusterOperationExecutors(stats, system);

      assertThat(executors.MAX_FE_THREADS).isEqualTo(numberOfFunctionExecutionThreads);
    } finally {
      System.clearProperty(functionExecutionThreadsPropertyName);
    }
  }
}
