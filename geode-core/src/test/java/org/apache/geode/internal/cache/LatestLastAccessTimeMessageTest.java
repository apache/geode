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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalStatisticsDisabledException;

public class LatestLastAccessTimeMessageTest {
  private final ClusterDistributionManager dm = mock(ClusterDistributionManager.class);
  private final InternalCache cache = mock(InternalCache.class);
  private final InternalDistributedRegion region = mock(InternalDistributedRegion.class);
  private final RegionEntry regionEntry = mock(RegionEntry.class);
  private final long LAST_ACCESSED = 47;

  private LatestLastAccessTimeMessage<String> lastAccessTimeMessage;

  private void setupMessage() {
    final LatestLastAccessTimeReplyProcessor replyProcessor =
        mock(LatestLastAccessTimeReplyProcessor.class);
    final Set<InternalDistributedMember> recipients =
        Collections.singleton(mock(InternalDistributedMember.class));
    lastAccessTimeMessage =
        spy(new LatestLastAccessTimeMessage<>(replyProcessor, recipients, region, "foo"));
    lastAccessTimeMessage.setSender(mock(InternalDistributedMember.class));
  }

  private void setupRegion(boolean hasRegion, boolean hasEntry) {
    when(dm.getCache()).thenReturn(cache);
    if (hasRegion) {
      when(cache.getRegion(any())).thenReturn(region);
    } else {
      when(cache.getRegion(any())).thenReturn(null);
    }
    if (hasEntry) {
      when(region.getRegionEntry(any())).thenReturn(regionEntry);
      try {
        when(regionEntry.getLastAccessed()).thenReturn(LAST_ACCESSED);
      } catch (InternalStatisticsDisabledException e) {
        throw new RuntimeException("should never happen when mocking", e);
      }
    } else {
      when(region.getRegionEntry(any())).thenReturn(null);
    }
  }

  @Test
  public void processWithNullCacheRepliesZero() {
    setupMessage();
    when(dm.getCache()).thenReturn(null);

    lastAccessTimeMessage.process(dm);

    verify(lastAccessTimeMessage).sendReply(dm, 0);
  }

  @Test
  public void processWithNullRegionRepliesZero() {
    setupMessage();
    setupRegion(false, false);

    lastAccessTimeMessage.process(dm);

    verify(lastAccessTimeMessage).sendReply(dm, 0);
  }

  @Test
  public void processWithNullEntryRepliesZero() {
    setupMessage();
    setupRegion(true, false);

    lastAccessTimeMessage.process(dm);

    verify(lastAccessTimeMessage).sendReply(dm, 0);
  }

  @Test
  public void processWithEntryStatsDisabledRepliesZero() throws Exception {
    setupMessage();
    setupRegion(true, true);
    when(regionEntry.getLastAccessed()).thenThrow(new InternalStatisticsDisabledException());

    lastAccessTimeMessage.process(dm);

    verify(lastAccessTimeMessage).sendReply(dm, 0);
  }

  @Test
  public void processWithRegionEntryRepliesWithLastAccessed() {
    setupMessage();
    setupRegion(true, true);

    lastAccessTimeMessage.process(dm);

    verify(lastAccessTimeMessage).sendReply(dm, LAST_ACCESSED);
  }

  @Test
  public void processWithRemovedRegionEntryRepliesZero() {
    setupMessage();
    setupRegion(true, true);
    when(regionEntry.isInvalidOrRemoved()).thenReturn(true);

    lastAccessTimeMessage.process(dm);

    verify(lastAccessTimeMessage).sendReply(dm, 0);
  }
}
