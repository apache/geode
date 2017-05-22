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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.EventTracker.BulkOpHolder;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ConcurrentMap;

@Category(UnitTest.class)
public class EventTrackerTest {
  LocalRegion lr;
  RegionAttributes<?, ?> ra;
  EntryEventImpl[] events;
  EventTracker eventTracker;
  ClientProxyMembershipID memberId;
  DistributedMember member;

  @Before
  public void setUp() {
    lr = mock(LocalRegion.class);
    ra = mock(RegionAttributes.class);
    when(lr.createStopper()).thenCallRealMethod();
    CancelCriterion stopper = lr.createStopper();
    when(lr.getStopper()).thenReturn(stopper);
    memberId = mock(ClientProxyMembershipID.class);
    when(lr.getAttributes()).thenReturn(ra);
    when(ra.getDataPolicy()).thenReturn(mock(DataPolicy.class));
    when(lr.getConcurrencyChecksEnabled()).thenReturn(true);

    member = mock(DistributedMember.class);
  }

  @Test
  public void retriedBulkOpDoesNotRemoveRecordedBulkOpVersionTags() {
    byte[] memId = {1, 2, 3};
    long threadId = 1;
    long retrySeqId = 1;
    ThreadIdentifier tid = new ThreadIdentifier(memId, threadId);
    EventID retryEventID = new EventID(memId, threadId, retrySeqId);
    boolean skipCallbacks = true;
    int size = 5;
    recordPutAllEvents(memId, threadId, skipCallbacks, size);

    ConcurrentMap<ThreadIdentifier, BulkOpHolder> map = eventTracker.getRecordedBulkOpVersionTags();
    BulkOpHolder holder = map.get(tid);
    int beforeSize = holder.entryVersionTags.size();

    eventTracker.recordBulkOpStart(tid, retryEventID);
    map = eventTracker.getRecordedBulkOpVersionTags();
    holder = map.get(tid);
    // Retried bulk op should not remove exiting BulkOpVersionTags
    assertTrue(holder.entryVersionTags.size() == beforeSize);
  }

  private void recordPutAllEvents(byte[] memId, long threadId, boolean skipCallbacks, int size) {
    events = new EntryEventImpl[size];
    eventTracker = new EventTracker(lr);
    for (int i = 0; i < size; i++) {
      events[i] = EntryEventImpl.create(lr, Operation.PUTALL_CREATE, "key" + i, "value" + i, null,
          false, member, !skipCallbacks, new EventID(memId, threadId, i + 1));
      events[i].setContext(memberId);
      events[i].setVersionTag(mock(VersionTag.class));
      eventTracker.recordEvent(events[i]);
    }
  }
}
