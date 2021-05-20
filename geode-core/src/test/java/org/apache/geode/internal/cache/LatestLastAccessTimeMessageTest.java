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
import org.apache.geode.internal.inet.LocalHostUtil;

public class LatestLastAccessTimeMessageTest {

  @Test
  public void processWithNullCacheRepliesZero() throws Exception {
    final LatestLastAccessTimeReplyProcessor replyProcessor =
        mock(LatestLastAccessTimeReplyProcessor.class);
    final InternalDistributedRegion region = mock(InternalDistributedRegion.class);
    Set<InternalDistributedMember> recipients = Collections.singleton(new InternalDistributedMember(
        LocalHostUtil.getLocalHost(), 1234));
    final LatestLastAccessTimeMessage<String> realLastAccessTimeMessage =
        new LatestLastAccessTimeMessage<>(replyProcessor, recipients, region, "foo");
    final LatestLastAccessTimeMessage<String> lastAccessTimeMessage =
        spy(realLastAccessTimeMessage);
    lastAccessTimeMessage.setSender(new InternalDistributedMember(
        LocalHostUtil.getLocalHost(), 12345));
    final ClusterDistributionManager dm = mock(ClusterDistributionManager.class);
    when(dm.getCache()).thenReturn(null);

    lastAccessTimeMessage.process(dm);

    verify(lastAccessTimeMessage).sendReply(dm, 0);
  }

  @Test
  public void processWithNullRegionRepliesZero() throws Exception {
    final LatestLastAccessTimeReplyProcessor replyProcessor =
        mock(LatestLastAccessTimeReplyProcessor.class);
    final InternalDistributedRegion region = mock(InternalDistributedRegion.class);
    Set<InternalDistributedMember> recipients = Collections.singleton(new InternalDistributedMember(
        LocalHostUtil.getLocalHost(), 1234));
    final LatestLastAccessTimeMessage<String> realLastAccessTimeMessage =
        new LatestLastAccessTimeMessage<>(replyProcessor, recipients, region, "foo");
    final LatestLastAccessTimeMessage<String> lastAccessTimeMessage =
        spy(realLastAccessTimeMessage);
    lastAccessTimeMessage.setSender(new InternalDistributedMember(
        LocalHostUtil.getLocalHost(), 12345));
    final ClusterDistributionManager dm = mock(ClusterDistributionManager.class);
    final InternalCache cache = mock(InternalCache.class);
    when(dm.getCache()).thenReturn(cache);
    when(cache.getRegion(any())).thenReturn(null);

    lastAccessTimeMessage.process(dm);

    verify(lastAccessTimeMessage).sendReply(dm, 0);
  }

  @Test
  public void processWithNullEntryRepliesZero() throws Exception {
    final LatestLastAccessTimeReplyProcessor replyProcessor =
        mock(LatestLastAccessTimeReplyProcessor.class);
    final InternalDistributedRegion region = mock(InternalDistributedRegion.class);
    Set<InternalDistributedMember> recipients = Collections.singleton(new InternalDistributedMember(
        LocalHostUtil.getLocalHost(), 1234));
    final LatestLastAccessTimeMessage<String> realLastAccessTimeMessage =
        new LatestLastAccessTimeMessage<>(replyProcessor, recipients, region, "foo");
    final LatestLastAccessTimeMessage<String> lastAccessTimeMessage =
        spy(realLastAccessTimeMessage);
    lastAccessTimeMessage.setSender(new InternalDistributedMember(
        LocalHostUtil.getLocalHost(), 12345));
    final ClusterDistributionManager dm = mock(ClusterDistributionManager.class);
    final InternalCache cache = mock(InternalCache.class);
    when(dm.getCache()).thenReturn(cache);
    when(cache.getRegion(any())).thenReturn(region);
    when(region.getRegionEntry(any())).thenReturn(null);

    lastAccessTimeMessage.process(dm);

    verify(lastAccessTimeMessage).sendReply(dm, 0);
  }

  @Test
  public void processWithEntryStatsDisabledRepliesZero() throws Exception {
    final LatestLastAccessTimeReplyProcessor replyProcessor =
        mock(LatestLastAccessTimeReplyProcessor.class);
    final InternalDistributedRegion region = mock(InternalDistributedRegion.class);
    Set<InternalDistributedMember> recipients = Collections.singleton(new InternalDistributedMember(
        LocalHostUtil.getLocalHost(), 1234));
    final LatestLastAccessTimeMessage<String> realLastAccessTimeMessage =
        new LatestLastAccessTimeMessage<>(replyProcessor, recipients, region, "foo");
    final LatestLastAccessTimeMessage<String> lastAccessTimeMessage =
        spy(realLastAccessTimeMessage);
    lastAccessTimeMessage.setSender(new InternalDistributedMember(
        LocalHostUtil.getLocalHost(), 12345));
    final ClusterDistributionManager dm = mock(ClusterDistributionManager.class);
    final InternalCache cache = mock(InternalCache.class);
    when(dm.getCache()).thenReturn(cache);
    when(cache.getRegion(any())).thenReturn(region);
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(region.getRegionEntry(any())).thenReturn(regionEntry);
    when(regionEntry.getLastAccessed()).thenThrow(new InternalStatisticsDisabledException());

    lastAccessTimeMessage.process(dm);

    verify(lastAccessTimeMessage).sendReply(dm, 0);
  }

  @Test
  public void processWithRegionEntryRepliesWithLastAccessed() throws Exception {
    final LatestLastAccessTimeReplyProcessor replyProcessor =
        mock(LatestLastAccessTimeReplyProcessor.class);
    final InternalDistributedRegion region = mock(InternalDistributedRegion.class);
    Set<InternalDistributedMember> recipients = Collections.singleton(new InternalDistributedMember(
        LocalHostUtil.getLocalHost(), 1234));
    final LatestLastAccessTimeMessage<String> realLastAccessTimeMessage =
        new LatestLastAccessTimeMessage<>(replyProcessor, recipients, region, "foo");
    final LatestLastAccessTimeMessage<String> lastAccessTimeMessage =
        spy(realLastAccessTimeMessage);
    lastAccessTimeMessage.setSender(new InternalDistributedMember(
        LocalHostUtil.getLocalHost(), 12345));
    final ClusterDistributionManager dm = mock(ClusterDistributionManager.class);
    final InternalCache cache = mock(InternalCache.class);
    when(dm.getCache()).thenReturn(cache);
    when(cache.getRegion(any())).thenReturn(region);
    RegionEntry regionEntry = mock(RegionEntry.class);
    when(region.getRegionEntry(any())).thenReturn(regionEntry);
    final long LAST_ACCESSED = 47;
    when(regionEntry.getLastAccessed()).thenReturn(LAST_ACCESSED);

    lastAccessTimeMessage.process(dm);

    verify(lastAccessTimeMessage).sendReply(dm, LAST_ACCESSED);
  }
}
