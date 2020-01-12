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

import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class PartitionedRegionDestroyJUnitTest {

  private PartitionedRegion partitionedRegion;
  private DistributionAdvisor advisor;
  private DistributionManager manager;

  @Before
  public void setup() {
    manager = mock(DistributionManager.class);
    when(manager.getDistributionManagerId())
        .thenReturn(new InternalDistributedMember("localhost", 1));

    partitionedRegion = mock(PartitionedRegion.class);
    advisor = mock(DistributionAdvisor.class);
    when(partitionedRegion.getDistributionAdvisor()).thenReturn(advisor);
    when(partitionedRegion.getRegionIdentifier()).thenReturn("testRegion");
  }

  @Test
  public void destroyMessageRequiresReattemptIfRegionInitializing() {
    when(advisor.isInitialized()).thenReturn(Boolean.FALSE);
    DestroyPartitionedRegionMessage message = new DestroyPartitionedRegionMessage();
    Throwable exception = message.processCheckForPR(partitionedRegion, manager);
    assertTrue(exception instanceof ForceReattemptException);
  }

  @Test
  public void destroyMessageRequiresNoReattemptIfRegionInitialized() {
    when(advisor.isInitialized()).thenReturn(Boolean.TRUE);
    DestroyPartitionedRegionMessage message = new DestroyPartitionedRegionMessage();
    Throwable exception = message.processCheckForPR(partitionedRegion, manager);
    assertNull(exception);
  }
}
