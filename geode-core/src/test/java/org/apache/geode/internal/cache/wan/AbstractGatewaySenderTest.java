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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.internal.cache.RegionQueue;

public class AbstractGatewaySenderTest {

  @Test
  public void getSynchronizationEventCanHandleRegionIsNullCase() {
    AbstractGatewaySender sender = mock(AbstractGatewaySender.class);
    Object key = new Object();
    long timestamp = 1;
    GatewaySenderEventImpl gatewaySenderEvent = mock(GatewaySenderEventImpl.class);
    when(gatewaySenderEvent.getKey()).thenReturn(key);
    when(gatewaySenderEvent.getVersionTimeStamp()).thenReturn(timestamp);
    Region region = mock(Region.class);
    Collection collection = new ArrayList();
    collection.add(gatewaySenderEvent);
    when(region.values()).thenReturn(collection);
    Set<RegionQueue> queues = new HashSet<>();
    RegionQueue queue1 = mock(RegionQueue.class);
    RegionQueue queue2 = mock(RegionQueue.class);
    queues.add(queue2);
    queues.add(queue1);
    when(queue1.getRegion()).thenReturn(null);
    when(queue2.getRegion()).thenReturn(region);
    when(sender.getQueues()).thenReturn(queues);
    when(sender.getStatistics()).thenReturn(mock(GatewaySenderStats.class));
    when(sender.getSynchronizationEvent(key, timestamp)).thenCallRealMethod();

    GatewayQueueEvent event = sender.getSynchronizationEvent(key, timestamp);

    assertThat(event).isSameAs(gatewaySenderEvent);
  }
}
