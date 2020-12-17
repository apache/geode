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

import static org.apache.geode.distributed.internal.OperationExecutors.PARTITIONED_REGION_EXECUTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class ClusterOperationExecutorsTest {
  private DistributionStats stats;
  private InternalDistributedSystem system;

  @Before
  public void setup() {
    stats = mock(DistributionStats.class);
    system = mock(InternalDistributedSystem.class);
    DistributionConfig config = mock(DistributionConfig.class);
    when(system.getConfig()).thenReturn(config);
  }

  @Test
  public void numOfThreadsIsAtLeast300() {
    int minNumberOfThreads = 300;

    ClusterOperationExecutors executors = new ClusterOperationExecutors(stats, system);

    assertThat(executors.MAX_THREADS).isGreaterThanOrEqualTo(minNumberOfThreads);
  }

  @Test
  public void numOfFEThreadsIsAtLeast100() {
    int minNumberOfFunctionExecutionThreads = 100;

    ClusterOperationExecutors executors = new ClusterOperationExecutors(stats, system);

    assertThat(executors.MAX_FE_THREADS)
        .isGreaterThanOrEqualTo(minNumberOfFunctionExecutionThreads);
  }

  @Test
  public void numOfPRThreadsIsAtLeast200() {
    int minNumberOfPartitionedRegionThreads = 200;

    ClusterOperationExecutors executors = new ClusterOperationExecutors(stats, system);
    int threads = ((ThreadPoolExecutor) executors.getExecutor(PARTITIONED_REGION_EXECUTOR,
        mock(InternalDistributedMember.class))).getMaximumPoolSize();

    assertThat(threads).isGreaterThanOrEqualTo(minNumberOfPartitionedRegionThreads);
  }
}
