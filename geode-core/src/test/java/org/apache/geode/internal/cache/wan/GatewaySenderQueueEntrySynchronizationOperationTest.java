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
package org.apache.geode.internal.cache.wan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InternalRegion;

public class GatewaySenderQueueEntrySynchronizationOperationTest {
  private DistributionManager distributionManager;
  private InternalDistributedMember recipient;
  private GatewaySenderQueueEntrySynchronizationOperation operation;
  private InternalRegion region;
  private GemFireCacheImpl cache;

  @Before
  public void setup() {
    distributionManager = mock(DistributionManager.class, RETURNS_DEEP_STUBS);
    recipient = mock(InternalDistributedMember.class);
    region = mock(InternalRegion.class);
    cache = mock(GemFireCacheImpl.class);
  }

  @Test
  public void ReplyProcessorGetCacheDelegateToDistributionManager() {
    operation = mock(GatewaySenderQueueEntrySynchronizationOperation.class);
    GatewaySenderQueueEntrySynchronizationOperation.GatewaySenderQueueEntrySynchronizationReplyProcessor processor =
        new GatewaySenderQueueEntrySynchronizationOperation.GatewaySenderQueueEntrySynchronizationReplyProcessor(
            distributionManager, recipient, operation);
    when(distributionManager.getCache()).thenReturn(cache);

    assertThat(processor.getCache()).isEqualTo(cache);
  }

  @Test
  public void GatewaySenderQueueEntrySynchronizationOperationGetCacheDelegateToDistributionManager() {
    InitialImageOperation.Entry entry = mock(InitialImageOperation.Entry.class);
    List<InitialImageOperation.Entry> list = new ArrayList<>();
    list.add(entry);
    operation = new GatewaySenderQueueEntrySynchronizationOperation(recipient, region, list);
    when(region.getDistributionManager()).thenReturn(distributionManager);
    when(distributionManager.getCache()).thenReturn(cache);

    assertThat(operation.getCache()).isEqualTo(cache);
  }
}
