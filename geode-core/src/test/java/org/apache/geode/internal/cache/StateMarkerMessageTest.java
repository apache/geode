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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireIOException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.StateFlushOperation.StateMarkerMessage;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class StateMarkerMessageTest {

  @Test
  public void shouldBeMockable() throws Exception {
    StateMarkerMessage mockStateMarkerMessage = mock(StateMarkerMessage.class);
    when(mockStateMarkerMessage.getProcessorType()).thenReturn(1);
    assertThat(mockStateMarkerMessage.getProcessorType()).isEqualTo(1);
  }

  @Test
  public void testProcessWithWaitForCurrentOperationsThatTimesOut() {
    InternalDistributedMember relayRecipient = mock(InternalDistributedMember.class);
    DistributionManager dm = mock(DistributionManager.class);
    InternalCache gfc = mock(InternalCache.class);
    DistributedRegion region = mock(DistributedRegion.class);
    CacheDistributionAdvisor distributionAdvisor = mock(CacheDistributionAdvisor.class);

    when(dm.getDistributionManagerId()).thenReturn(relayRecipient);
    when(dm.getExistingCache()).thenReturn(gfc);
    when(region.isInitialized()).thenReturn(true);
    when(region.getDistributionAdvisor()).thenReturn(distributionAdvisor);
    when(gfc.getRegionByPathForProcessing(any())).thenReturn(region);
    doThrow(new GemFireIOException("expected in fatal log message")).when(distributionAdvisor)
        .waitForCurrentOperations();

    StateMarkerMessage message = new StateMarkerMessage();
    message.relayRecipient = relayRecipient;

    message.process(dm);

    verify(dm, times(1)).putOutgoing(any());
  }
}
