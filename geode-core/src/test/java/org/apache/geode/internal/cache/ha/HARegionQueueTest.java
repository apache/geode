/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

import static junit.framework.TestCase.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class HARegionQueueTest {

  private HARegionQueue haRegionQueue;

  @Before
  public void setup() throws IOException, ClassNotFoundException, InterruptedException {
    InternalCache internalCache = mock(InternalCache.class);

    StoppableReentrantReadWriteLock giiLock = mock(StoppableReentrantReadWriteLock.class);
    when(giiLock.readLock())
        .thenReturn(mock(StoppableReentrantReadWriteLock.StoppableReadLock.class));

    StoppableReentrantReadWriteLock rwLock = mock(StoppableReentrantReadWriteLock.class);
    when(rwLock.writeLock())
        .thenReturn(mock(StoppableReentrantReadWriteLock.StoppableWriteLock.class));
    when(rwLock.readLock())
        .thenReturn(mock(StoppableReentrantReadWriteLock.StoppableReadLock.class));

    HARegion haRegion = mock(HARegion.class);
    HashMap map = new HashMap();
    when(haRegion.put(any(), any())).then((invocationOnMock) -> {
      return map.put(invocationOnMock.getArgument(0), invocationOnMock.getArgument(1));
    });
    when(haRegion.get(any())).then((invocationOnMock) -> {
      return map.get(invocationOnMock.getArgument(0));
    });
    when(haRegion.getGemFireCache()).thenReturn(internalCache);
    haRegionQueue = new HARegionQueue("haRegion", haRegion, internalCache,
        new HAContainerMap(new ConcurrentHashMap()), null, (byte) 1, true,
        mock(HARegionQueueStats.class), giiLock, rwLock, mock(CancelCriterion.class), false,
        mock(StatisticsClock.class));
  }

  @Test
  public void conflateConflatableEntriesAndDoNotConflateNonConflatableEntries() throws Exception {
    EventID eventId1 = new EventID(new byte[] {1}, 1, 1);
    EventID eventId2 = new EventID(new byte[] {1}, 1, 2);
    EventID eventId3 = new EventID(new byte[] {1}, 1, 3);
    EventID eventId4 = new EventID(new byte[] {1}, 1, 4);
    EventID eventId5 = new EventID(new byte[] {1}, 1, 5);
    EventID eventId6 = new EventID(new byte[] {1}, 1, 6);
    EventID eventId7 = new EventID(new byte[] {1}, 1, 7);

    haRegionQueue.put(new ConflatableObject("key", "value1", eventId1, false, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value2", eventId2, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value3", eventId3, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value4", eventId4, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value5", eventId5, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value6", eventId6, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value7", eventId7, false, "someRegion"));
    assertEquals(3, haRegionQueue.size());
  }

  @Test
  public void queueShouldconflateConflatableEntries() throws Exception {
    EventID eventId1 = new EventID(new byte[] {1}, 1, 1);
    EventID eventId2 = new EventID(new byte[] {1}, 1, 2);
    EventID eventId3 = new EventID(new byte[] {1}, 1, 3);
    EventID eventId4 = new EventID(new byte[] {1}, 1, 4);
    EventID eventId5 = new EventID(new byte[] {1}, 1, 5);
    EventID eventId6 = new EventID(new byte[] {1}, 1, 6);
    EventID eventId7 = new EventID(new byte[] {1}, 1, 7);

    haRegionQueue.put(new ConflatableObject("key", "value1", eventId1, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value2", eventId2, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value3", eventId3, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value4", eventId4, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value5", eventId5, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value6", eventId6, true, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value7", eventId7, true, "someRegion"));
    assertEquals(1, haRegionQueue.size());
  }

  @Test
  public void queuePutElidesSequenceIdLowerThanOrEqualToLastSeenSequenceId() throws Exception {
    EventID eventId1 = new EventID(new byte[] {1}, 1, 1);
    EventID eventId2 = new EventID(new byte[] {1}, 1, 2);
    EventID eventId3 = new EventID(new byte[] {1}, 1, 0);
    EventID eventId4 = new EventID(new byte[] {1}, 1, 3);

    haRegionQueue.put(new ConflatableObject("key", "value1", eventId1, false, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value2", eventId2, false, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value3", eventId3, false, "someRegion"));
    haRegionQueue.put(new ConflatableObject("key", "value3", eventId4, false, "someRegion"));
    assertEquals(3, haRegionQueue.size());
  }

}
