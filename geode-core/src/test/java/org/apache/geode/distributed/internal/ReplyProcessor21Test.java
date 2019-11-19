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
package org.apache.geode.distributed.internal;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class ReplyProcessor21Test {
  private static final long WAIT_FOR_REPLIES_MILLIS = 2000;

  @Test
  public void testReplyProcessorInitiatesSuspicion() throws Exception {
    DistributionManager dm = mock(DistributionManager.class);
    DMStats stats = mock(DMStats.class);

    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    DistributionConfig distributionConfig = mock(DistributionConfig.class);
    when(distributionConfig.getAckWaitThreshold()).thenReturn(1);
    when(distributionConfig.getAckSevereAlertThreshold()).thenReturn(10);
    when(system.getConfig()).thenReturn(distributionConfig);

    Distribution distribution = mock(Distribution.class);
    when(dm.getStats()).thenReturn(stats);
    when(dm.getSystem()).thenReturn(system);

    InternalDistributedMember member = mock(InternalDistributedMember.class);
    List<InternalDistributedMember> members = Arrays.asList(member);
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    when(dm.getCancelCriterion()).thenReturn(cancelCriterion);
    when(dm.getDistribution()).thenReturn(distribution);
    when(dm.getViewMembers()).thenReturn(members);
    when(dm.getDistributionManagerIds()).thenReturn(new HashSet<>(members));
    when(dm.addMembershipListenerAndGetDistributionManagerIds(any(
        org.apache.geode.distributed.internal.MembershipListener.class)))
            .thenReturn(new HashSet(members));

    List<InternalDistributedMember> mbrs = new ArrayList<>(1);

    mbrs.add(member);
    ReplyProcessor21 rp = new ReplyProcessor21(dm, mbrs);
    rp.enableSevereAlertProcessing();
    boolean result = rp.waitForReplies(WAIT_FOR_REPLIES_MILLIS);
    assertFalse(result); // the wait should have timed out
    verify(distribution, atLeastOnce()).suspectMembers(any(), anyString());
  }


  @Test
  public void shouldBeMockable() throws Exception {
    ReplyProcessor21 mockReplyProcessor21 = mock(ReplyProcessor21.class);

    mockReplyProcessor21.waitForRepliesUninterruptibly();
    mockReplyProcessor21.finished();

    verify(mockReplyProcessor21, times(1)).waitForRepliesUninterruptibly();
    verify(mockReplyProcessor21, times(1)).finished();
  }
}
