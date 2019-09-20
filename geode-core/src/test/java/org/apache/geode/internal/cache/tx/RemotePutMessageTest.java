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
package org.apache.geode.internal.cache.tx;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalDataView;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;

public class RemotePutMessageTest {

  @Rule
  public TestName testName = new TestName();

  @Test
  public void testDistributeNotFailWithRegionDestroyedException() throws RemoteOperationException {
    EntryEventImpl event = mock(EntryEventImpl.class);
    EventID eventID = mock(EventID.class);
    DistributedRegion region = mock(DistributedRegion.class);
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    DistributionManager dm = mock(DistributionManager.class);
    CancelCriterion cc = mock(CancelCriterion.class);
    CacheDistributionAdvisor advisor = mock(CacheDistributionAdvisor.class);
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    Set<InternalDistributedMember> replicates = new HashSet<>(Arrays.asList(member));
    RemotePutMessage.RemotePutResponse response = mock(RemotePutMessage.RemotePutResponse.class);
    Object expectedOldValue = new Object();

    when(event.getRegion()).thenReturn(region);
    when(event.getEventId()).thenReturn(eventID);
    when(region.getCacheDistributionAdvisor()).thenReturn(advisor);
    when(advisor.adviseInitializedReplicates()).thenReturn(replicates);
    when(response.waitForResult()).thenThrow(new RegionDestroyedException("", ""));
    when(region.getSystem()).thenReturn(ids);
    when(region.getDistributionManager()).thenReturn(dm);
    when(ids.getDistributionManager()).thenReturn(dm);
    when(dm.getCancelCriterion()).thenReturn(cc);
    when(dm.putOutgoing(any())).thenReturn(null);

    RemotePutMessage.distribute(event, 1, false, false, expectedOldValue, false, false);
  }

  @Test
  public void testSendReplyInvokedOnceWithFalseResult() throws RemoteOperationException {
    // Create the RemotePutMessage
    InternalDistributedMember recipient = mock(InternalDistributedMember.class);
    EntryEventImpl event = mock(EntryEventImpl.class);
    EventID eventID = mock(EventID.class);
    when(event.getEventId()).thenReturn(eventID);
    RemotePutMessage.RemotePutResponse response = mock(RemotePutMessage.RemotePutResponse.class);
    long lastModified = 0l;
    boolean ifNew = false, ifOld = false, requiredOldValue = false;
    Object expectedOldValue = null;
    RemotePutMessage message = new RemotePutMessage(recipient, testName.getMethodName(),
        response, event, lastModified, ifNew,
        ifOld, expectedOldValue, requiredOldValue, false,
        false);
    message.setSender(mock(InternalDistributedMember.class));
    RemotePutMessage messageSpy = spy(message);

    // Invoke operateOnRegion on the spy
    ClusterDistributionManager manager = mock(ClusterDistributionManager.class);
    LocalRegion region = mock(LocalRegion.class);
    InternalCache cache = mock(InternalCache.class);
    when(region.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(mock(InternalDistributedSystem.class));
    InternalDataView dataView = mock(InternalDataView.class);
    when(region.getDataView()).thenReturn(dataView);
    when(dataView.putEntry(event, ifNew, ifOld, expectedOldValue, requiredOldValue, lastModified,
        true, false)).thenReturn(false);
    long startTime = 0l;
    messageSpy.operateOnRegion(manager, region, startTime);

    // Verify the sendReply method containing the exception was called once
    verify(messageSpy, times(1)).sendReply(eq(message.getSender()), eq(0), eq(manager),
        any(ReplyException.class), eq(region), eq(0l));

    // Verify the normal sendReply method was not called
    verify(messageSpy, times(0)).sendReply(eq(message.getSender()), eq(0), eq(manager), eq(null),
        eq(region), eq(0l), any(EntryEventImpl.class));
  }
}
