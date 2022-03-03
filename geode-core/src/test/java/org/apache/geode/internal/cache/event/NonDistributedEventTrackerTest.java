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
package org.apache.geode.internal.cache.event;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCacheEvent;

public class NonDistributedEventTrackerTest {
  private final NonDistributedEventTracker tracker = NonDistributedEventTracker.getInstance();

  @Test
  public void getStateReturnsNull() {
    assertNull(tracker.getState());
  }

  @Test
  public void hasSeenEventReturnsFalse() {
    assertFalse(tracker.hasSeenEvent(mock(InternalCacheEvent.class)));
    assertFalse(tracker.hasSeenEvent(mock(EventID.class)));
    assertFalse(tracker.hasSeenEvent(mock(EventID.class), mock(InternalCacheEvent.class)));
  }

  @Test
  public void findVersionTagForSequenceReturnsNull() {
    assertNull(tracker.findVersionTagForSequence(mock(EventID.class)));
  }

  @Test
  public void findVersionTagForBulkOpReturnsNull() {
    assertNull(tracker.findVersionTagForBulkOp(mock(EventID.class)));
  }

  @Test
  public void returnsCorrectName() {
    assertEquals(NonDistributedEventTracker.NAME, tracker.getName());
  }

  @Test
  public void syncBulkOpExecutesProvidedRunnable() {
    Runnable runnable = mock(Runnable.class);
    tracker.syncBulkOp(runnable, mock(EventID.class), false);
    tracker.syncBulkOp(runnable, mock(EventID.class), true);
    verify(runnable, times(2)).run();
  }

  @Test
  public void isInitializedReturnsTrue() {
    assertTrue(tracker.isInitialized());
  }

  @Test
  public void isInitialImageProviderReturnsFalse() {
    assertFalse(tracker.isInitialImageProvider(mock(DistributedMember.class)));
  }

  @Test
  public void getRecordedBulkOpVersionTagsReturnsNull() {
    assertNull(tracker.getRecordedBulkOpVersionTags());
  }

  @Test
  public void getRecordedEventsReturnsNull() {
    assertNull(tracker.getRecordedEvents());
  }

}
