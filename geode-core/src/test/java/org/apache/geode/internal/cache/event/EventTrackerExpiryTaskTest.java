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
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;

public class EventTrackerExpiryTaskTest {
  private static final long TIME_TO_LIVE = 100;

  private EventTrackerExpiryTask task;

  @Before
  public void setup() {
    task = new EventTrackerExpiryTask(TIME_TO_LIVE);
  }

  @Test
  public void hasNoTrackersWhenInitialized() {
    assertEquals(0, task.getNumberOfTrackers());
  }

  @Test
  public void addsTrackersCorrectly() {
    task.addTracker(NonDistributedEventTracker.getInstance());
    assertEquals(1, task.getNumberOfTrackers());
  }

  @Test
  public void removedTrackersCorrectly() {
    task.addTracker(NonDistributedEventTracker.getInstance());
    task.removeTracker(NonDistributedEventTracker.getInstance());
    assertEquals(0, task.getNumberOfTrackers());
  }

  @Test
  public void removesExpiredSequenceIdHolder() {
    DistributedEventTracker tracker = constructTestTracker();
    task.addTracker(tracker);
    EventSequenceNumberHolder sequenceIdHolder = new EventSequenceNumberHolder(0L, null);
    tracker.recordSequenceNumber(new ThreadIdentifier(new byte[0], 0L), sequenceIdHolder);
    sequenceIdHolder.setEndOfLifeTimestamp(System.currentTimeMillis() - TIME_TO_LIVE);
    task.run2();
    assertEquals(0, tracker.getRecordedEvents().size());
  }

  @Test
  public void doesNotRemoveNonExpiredSequenceIdHolder() {
    DistributedEventTracker tracker = constructTestTracker();
    task.addTracker(tracker);
    EventSequenceNumberHolder sequenceIdHolder = new EventSequenceNumberHolder(0L, null);
    tracker.recordSequenceNumber(new ThreadIdentifier(new byte[0], 0L), sequenceIdHolder);
    sequenceIdHolder.setEndOfLifeTimestamp(System.currentTimeMillis() + 10000);
    task.run2();
    assertEquals(1, tracker.getRecordedEvents().size());
  }

  @Test
  public void doesNotRemoveNewSequenceIdHolder() {
    DistributedEventTracker tracker = constructTestTracker();
    task.addTracker(tracker);
    EventSequenceNumberHolder sequenceIdHolder = new EventSequenceNumberHolder(0L, null);
    tracker.recordSequenceNumber(new ThreadIdentifier(new byte[0], 0L), sequenceIdHolder);
    task.run2();
    assertEquals(1, tracker.getRecordedEvents().size());
  }

  private DistributedEventTracker constructTestTracker() {
    return new DistributedEventTracker(mock(InternalCache.class), mock(CancelCriterion.class),
        "test region");
  }
}
