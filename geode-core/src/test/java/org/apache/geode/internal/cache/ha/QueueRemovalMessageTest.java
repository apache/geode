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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;

public class QueueRemovalMessageTest {
  private QueueRemovalMessage queueRemovalMessage;
  private List<Object> messagesList;

  private final ClusterDistributionManager dm = mock(ClusterDistributionManager.class);
  private final InternalCache cache = mock(InternalCache.class);
  private final String regionName1 = "region1";
  private final String regionName2 = "region2";
  private final HARegion region1 = mock(HARegion.class);
  private final HARegion region2 = mock(HARegion.class);
  private final HARegionQueue regionQueue1 = mock(HARegionQueue.class);
  private final HARegionQueue regionQueue2 = mock(HARegionQueue.class);
  private final EventID eventID1 = mock(EventID.class);
  private final EventID eventID2 = mock(EventID.class);
  private final EventID eventID3 = mock(EventID.class);
  private final int region1EventSize = 1;
  private final int region2EventSize = 2;

  @Before
  public void setup() {
    queueRemovalMessage = spy(new QueueRemovalMessage());
    messagesList = new LinkedList<>();
    queueRemovalMessage.setMessagesList(messagesList);

    long maxWaitTimeForInitialization = 30000;
    when(cache.getRegion(regionName1)).thenReturn(uncheckedCast(region1));
    when(cache.getRegion(regionName2)).thenReturn(uncheckedCast(region2));
    when(region1.getOwnerWithWait(maxWaitTimeForInitialization)).thenReturn(regionQueue1);
    when(region2.getOwnerWithWait(maxWaitTimeForInitialization)).thenReturn(regionQueue2);
    when(regionQueue1.isQueueInitialized()).thenReturn(true);
    when(regionQueue2.isQueueInitialized()).thenReturn(true);
  }

  @Test
  public void messageProcessInvokesProcessRegionQueues() {
    when(dm.getCache()).thenReturn(cache);

    queueRemovalMessage.process(dm);

    verify(queueRemovalMessage).processRegionQueues(eq(cache), any(Iterator.class));
  }

  @Test
  public void processRegionQueuesCanProcessEachRegionQueue() {
    addToMessagesList();
    Iterator iterator = messagesList.iterator();

    queueRemovalMessage.processRegionQueues(cache, iterator);

    verify(queueRemovalMessage).processRegionQueue(iterator, regionName1, region1EventSize,
        regionQueue1);
    verify(queueRemovalMessage).processRegionQueue(iterator, regionName2, region2EventSize,
        regionQueue2);
    verify(queueRemovalMessage).removeQueueEvent(regionName1, regionQueue1, eventID1);
    verify(queueRemovalMessage).removeQueueEvent(regionName2, regionQueue2, eventID2);
    verify(queueRemovalMessage).removeQueueEvent(regionName2, regionQueue2, eventID3);
  }

  private void addToMessagesList() {
    messagesList.add(regionName1);
    messagesList.add(region1EventSize);
    messagesList.add(eventID1);
    messagesList.add(regionName2);
    messagesList.add(region2EventSize);
    messagesList.add(eventID2);
    messagesList.add(eventID3);
  }

  @Test
  public void canProcessRegionQueuesWithoutHARegionInCache() {
    addToMessagesList();
    Iterator iterator = messagesList.iterator();
    when(cache.getRegion(regionName1)).thenReturn(null);

    queueRemovalMessage.processRegionQueues(cache, iterator);

    verify(queueRemovalMessage).processRegionQueue(iterator, regionName1, region1EventSize, null);
    verify(queueRemovalMessage).processRegionQueue(iterator, regionName2, region2EventSize,
        regionQueue2);
    verify(queueRemovalMessage, never()).removeQueueEvent(regionName1, regionQueue1, eventID1);
    verify(queueRemovalMessage).removeQueueEvent(regionName2, regionQueue2, eventID2);
    verify(queueRemovalMessage).removeQueueEvent(regionName2, regionQueue2, eventID3);
  }

  @Test
  public void canProcessRegionQueuesWhenHARegionQueueIsNotInitialized() {
    addToMessagesList();
    Iterator iterator = messagesList.iterator();
    when(regionQueue2.isQueueInitialized()).thenReturn(false);

    queueRemovalMessage.processRegionQueues(cache, iterator);

    verify(queueRemovalMessage).processRegionQueue(iterator, regionName1, region1EventSize,
        regionQueue1);
    verify(queueRemovalMessage).processRegionQueue(iterator, regionName2, region2EventSize,
        regionQueue2);
    verify(queueRemovalMessage).removeQueueEvent(regionName1, regionQueue1, eventID1);
    verify(queueRemovalMessage, never()).removeQueueEvent(regionName2, regionQueue2, eventID2);
    verify(queueRemovalMessage, never()).removeQueueEvent(regionName2, regionQueue2, eventID3);
  }

  @Test
  public void processRegionQueuesStopsIfProcessRegionQueueReturnsFalse() {
    addToMessagesList();
    Iterator iterator = messagesList.iterator();
    doReturn(false).when(queueRemovalMessage).processRegionQueue(iterator, regionName1,
        region1EventSize, regionQueue1);

    queueRemovalMessage.processRegionQueues(cache, iterator);

    verify(queueRemovalMessage).processRegionQueue(iterator, regionName1, region1EventSize,
        regionQueue1);
    verify(queueRemovalMessage, never()).processRegionQueue(iterator, regionName2, region2EventSize,
        regionQueue2);
  }

  @Test
  public void processRegionQueueReturnsFalseIfRemoveQueueEventReturnsFalse() {
    messagesList.add(eventID1);
    Iterator iterator = messagesList.iterator();
    doReturn(false).when(queueRemovalMessage).removeQueueEvent(regionName1, regionQueue1, eventID1);

    assertThat(queueRemovalMessage.processRegionQueue(iterator, regionName1, region1EventSize,
        regionQueue1)).isFalse();
  }

  @Test
  public void removeQueueEventRemovesEvents() throws Exception {
    assertThat(queueRemovalMessage.removeQueueEvent(regionName2, regionQueue2, eventID2)).isTrue();

    verify(regionQueue2).removeDispatchedEvents(eventID2);
  }

  @Test
  public void removeQueueEventReturnsTrueIfRemovalThrowsCacheException() throws Exception {
    doThrow(new EntryNotFoundException("")).when(regionQueue2).removeDispatchedEvents(eventID2);

    assertThat(queueRemovalMessage.removeQueueEvent(regionName2, regionQueue2, eventID2)).isTrue();
  }

  @Test
  public void removeQueueEventReturnsTrueIfRemovalThrowsRegionDestroyedException()
      throws Exception {
    doThrow(new RegionDestroyedException("", "")).when(regionQueue2)
        .removeDispatchedEvents(eventID2);

    assertThat(queueRemovalMessage.removeQueueEvent(regionName2, regionQueue2, eventID2)).isTrue();
  }

  @Test
  public void removeQueueEventReturnsFalseIfRemovalThrowsCancelException() throws Exception {
    doThrow(new CacheClosedException()).when(regionQueue2).removeDispatchedEvents(eventID2);

    assertThat(queueRemovalMessage.removeQueueEvent(regionName2, regionQueue2, eventID2)).isFalse();
  }

  @Test
  public void removeQueueEventReturnsFalseIfRemovalThrowsInterruptedException() throws Exception {
    doThrow(new InterruptedException()).when(regionQueue2).removeDispatchedEvents(eventID2);

    assertThat(queueRemovalMessage.removeQueueEvent(regionName2, regionQueue2, eventID2)).isFalse();
  }

  @Test
  public void removeQueueEventReturnsTrueIfRemovalThrowsRejectedExecutionException()
      throws Exception {
    doThrow(new RejectedExecutionException()).when(regionQueue2).removeDispatchedEvents(eventID2);

    assertThat(queueRemovalMessage.removeQueueEvent(regionName2, regionQueue2, eventID2)).isTrue();
  }

  @Test
  public void synchronizeQueueWithPrimaryInvokedAfterProcessEachRegionQueue() {
    addToMessagesList();
    Iterator<Object> iterator = messagesList.iterator();
    InternalDistributedMember sender = mock(InternalDistributedMember.class);
    doReturn(sender).when(queueRemovalMessage).getSender();

    queueRemovalMessage.processRegionQueues(cache, iterator);

    verify(queueRemovalMessage).processRegionQueue(iterator, regionName1, region1EventSize,
        regionQueue1);
    verify(regionQueue1).synchronizeQueueWithPrimary(sender, cache);
    verify(queueRemovalMessage).processRegionQueue(iterator, regionName2, region2EventSize,
        regionQueue2);
    verify(regionQueue2).synchronizeQueueWithPrimary(sender, cache);
    verify(queueRemovalMessage).removeQueueEvent(regionName1, regionQueue1, eventID1);
    verify(queueRemovalMessage).removeQueueEvent(regionName2, regionQueue2, eventID2);
    verify(queueRemovalMessage).removeQueueEvent(regionName2, regionQueue2, eventID3);
  }
}
