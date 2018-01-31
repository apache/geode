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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.RemoteOperationMessage.RemoteOperationResponse;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class RemoteOperationResponseTest {

  private RemoteOperationResponse replyProcessor; // the class under test

  private InternalDistributedMember recipient;
  private InternalDistributedMember sender;
  private final String regionPath = "regionPath";

  private GemFireCacheImpl cache;
  private InternalDistributedSystem system;
  private ClusterDistributionManager dm;
  private LocalRegion r;
  private Collection<InternalDistributedMember> initialMembers;

  @Before
  public void setUp() throws Exception {
    cache = Fakes.cache();
    system = cache.getSystem();
    dm = (ClusterDistributionManager) system.getDistributionManager();
    r = mock(LocalRegion.class);
    when(cache.getRegionByPathForProcessing(regionPath)).thenReturn(r);

    sender = mock(InternalDistributedMember.class);

    recipient = mock(InternalDistributedMember.class);
    initialMembers = new ArrayList<>();
    initialMembers.add(recipient);

    // make it a spy to aid verification
    replyProcessor = spy(new RemoteOperationResponse(system, initialMembers, true));
  }

  @After
  public void cleanUp() {
    replyProcessor.cleanup();
  }

  @Test
  public void whenMemberDepartedThatWeAreWaitingForExceptionIsSet() {
    replyProcessor.memberDeparted(recipient, false);
    assertThat(replyProcessor.getMemberDepartedException())
        .hasMessageContaining("memberDeparted event");
  }

  @Test
  public void whenMemberDepartedThatWeAreNotWaitingForExceptionIsNotSet() {
    replyProcessor.memberDeparted(mock(InternalDistributedMember.class), false);
    assertThat(replyProcessor.getMemberDepartedException()).isNull();
  }

  @Test
  public void waitForCacheExceptionReturnsNormallyWhenWaitForRepliesUninterruptiblyDoesNothing()
      throws Exception {
    doNothing().when(replyProcessor).waitForRepliesUninterruptibly();

    replyProcessor.waitForCacheException();

    verify(replyProcessor, times(1)).waitForRepliesUninterruptibly();
  }

  @Test
  public void waitForCacheExceptionWithResponseRequiredThrowsException() throws Exception {
    doNothing().when(replyProcessor).waitForRepliesUninterruptibly();
    replyProcessor.requireResponse();

    assertThatThrownBy(() -> replyProcessor.waitForCacheException())
        .isInstanceOf(RemoteOperationException.class).hasMessage("Attempt failed");

    verify(replyProcessor, times(1)).waitForRepliesUninterruptibly();
  }

  @Test
  public void waitForCacheExceptionWithMemberDepartedThrowsException() throws Exception {
    doNothing().when(replyProcessor).waitForRepliesUninterruptibly();
    replyProcessor.memberDeparted(recipient, false);

    assertThatThrownBy(() -> replyProcessor.waitForCacheException())
        .isInstanceOf(RemoteOperationException.class).hasMessage("Attempt failed")
        .hasCauseInstanceOf(ForceReattemptException.class);

    verify(replyProcessor, times(1)).waitForRepliesUninterruptibly();
  }

  @Test
  public void waitForCacheExceptionWithReplyExceptionWithNoCauseCallsHandleAsUnexpected()
      throws Exception {
    ReplyException replyException = mock(ReplyException.class);
    doThrow(replyException).when(replyProcessor).waitForRepliesUninterruptibly();

    replyProcessor.waitForCacheException();

    verify(replyException, times(1)).handleAsUnexpected();
  }

  @Test
  public void waitForCacheExceptionWithReplyExceptionWithCacheExceptionCauseThrowsThatCause()
      throws Exception {
    ReplyException replyException = mock(ReplyException.class);
    CacheExistsException cause = new CacheExistsException(cache, "msg");
    when(replyException.getCause()).thenReturn(cause);
    doThrow(replyException).when(replyProcessor).waitForRepliesUninterruptibly();

    assertThatThrownBy(() -> replyProcessor.waitForCacheException()).isSameAs(cause);
  }

  @Test
  public void waitForCacheExceptionWithReplyExceptionWithRegionDestroyedExceptionCauseThrowsThatCause()
      throws Exception {
    ReplyException replyException = mock(ReplyException.class);
    RegionDestroyedException cause = new RegionDestroyedException("msg", "regionName");
    when(replyException.getCause()).thenReturn(cause);
    doThrow(replyException).when(replyProcessor).waitForRepliesUninterruptibly();

    assertThatThrownBy(() -> replyProcessor.waitForCacheException()).isSameAs(cause);
  }

  @Test
  public void waitForCacheExceptionWithReplyExceptionWithLowMemoryExceptionCauseThrowsThatCause()
      throws Exception {
    ReplyException replyException = mock(ReplyException.class);
    LowMemoryException cause = new LowMemoryException();
    when(replyException.getCause()).thenReturn(cause);
    doThrow(replyException).when(replyProcessor).waitForRepliesUninterruptibly();

    assertThatThrownBy(() -> replyProcessor.waitForCacheException()).isSameAs(cause);
  }

}
