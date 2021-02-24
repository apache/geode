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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedCacheOperation.CacheOperationMessage;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.versions.VersionTag;

public class DistributedCacheOperationTest {
  private TestCacheOperationMessage message;
  private InternalDistributedMember sender;
  private LocalRegion region;
  private VersionTag<?> versionTag;
  private Scope scope;
  private OperationExecutors executors;

  private final ClusterDistributionManager dm = mock(ClusterDistributionManager.class);
  private final int processorId = 1;

  @Before
  public void setup() {
    message = spy(new TestCacheOperationMessage());
    sender = mock(InternalDistributedMember.class);
    versionTag = mock(VersionTag.class);

    region = mock(LocalRegion.class);
    executors = mock(OperationExecutors.class);
    scope = mock(Scope.class);
    when(region.getScope()).thenReturn(scope);
    when(dm.getExecutors()).thenReturn(executors);

    message.setVersionTag(versionTag);
    message.setSender(sender);
    message.setProcessorId();

    doReturn(region).when(message).getLocalRegionForProcessing(dm);
  }

  @Test
  public void shouldBeMockable() throws Exception {
    DistributedCacheOperation mockDistributedCacheOperation = mock(DistributedCacheOperation.class);
    CacheOperationMessage mockCacheOperationMessage = mock(CacheOperationMessage.class);
    Map<InternalDistributedMember, PersistentMemberID> persistentIds = new HashMap<>();
    when(mockDistributedCacheOperation.supportsDirectAck()).thenReturn(false);

    mockDistributedCacheOperation.waitForAckIfNeeded(mockCacheOperationMessage, persistentIds);

    verify(mockDistributedCacheOperation, times(1)).waitForAckIfNeeded(mockCacheOperationMessage,
        persistentIds);

    assertThat(mockDistributedCacheOperation.supportsDirectAck()).isFalse();
  }

  /**
   * The startOperation and endOperation methods of DistributedCacheOperation record the
   * beginning and end of distribution of an operation. If startOperation is invoked it
   * is essential that endOperation be invoked or the state-flush operation will hang.<br>
   * This test ensures that if distribution of the operation throws an exception then
   * endOperation is correctly invoked before allowing the exception to escape the startOperation
   * method.
   */
  @Test
  public void endOperationIsInvokedOnDistributionError() {
    DistributedRegion region = mock(DistributedRegion.class);
    CacheDistributionAdvisor advisor = mock(CacheDistributionAdvisor.class);
    when(region.getDistributionAdvisor()).thenReturn(advisor);
    TestOperation operation = new TestOperation(null);
    operation.region = region;
    try {
      operation.startOperation();
    } catch (RuntimeException e) {
      assertEquals("boom", e.getMessage());
    }
    assertTrue(operation.endOperationInvoked);
  }

  @Test
  public void processReplacesVersionTagNullIDs() {
    message.process(dm);

    verify(versionTag).replaceNullIDs(sender);
  }

  @Test
  public void processSendsReplyIfAdminDM() {
    when(dm.getDMType()).thenReturn(ClusterDistributionManager.ADMIN_ONLY_DM_TYPE);

    message.process(dm);

    verify(message, never()).basicProcess(dm, region);
    verify(message).sendReply(
        eq(sender),
        eq(processorId),
        eq(null),
        eq(dm));
  }

  @Test
  public void processInvokesBasicProcessIfLocalRegionIsNull() {
    doReturn(null).when(message).getLocalRegionForProcessing(dm);

    message.process(dm);

    verify(message).basicProcess(dm, null);
  }

  @Test
  public void processSendsReplyIfGotCacheClosedException() {
    CacheClosedException cacheClosedException = new CacheClosedException();
    doThrow(cacheClosedException).when(message).getLocalRegionForProcessing(dm);

    message.process(dm);

    assertThat(message.closed).isTrue();
    verify(message, never()).basicProcess(dm, region);
    verify(message).sendReply(
        eq(sender),
        eq(processorId),
        eq(null),
        eq(dm));
  }

  @Test
  public void processSendsReplyExceptionIfGotRuntimeException() {
    RuntimeException exception = new RuntimeException();
    doThrow(exception).when(message).getLocalRegionForProcessing(dm);

    message.process(dm);

    verify(message, never()).basicProcess(dm, region);
    ArgumentCaptor<ReplyException> captor = ArgumentCaptor.forClass(ReplyException.class);
    verify(message).sendReply(
        eq(sender),
        eq(processorId),
        captor.capture(),
        eq(dm));
    assertThat(captor.getValue().getCause()).isSameAs(exception);
  }

  @Test
  public void processPerformsBasicProcessIfNotDistributedNoAck() {
    when(scope.isDistributedNoAck()).thenReturn(false);

    message.process(dm);

    verify(message).basicProcess(dm, region);
    verify(executors, never()).getWaitingThreadPool();
  }

  @Test
  public void processUsesWaitingThreadPoolIfDistributedNoAck() {
    when(scope.isDistributedNoAck()).thenReturn(true);

    message.process(dm);

    verify(executors).getWaitingThreadPool();
  }

  @Test
  public void processDoesNotSendReplyIfDistributedNoAck() {
    when(scope.isDistributedNoAck()).thenReturn(true);

    message.process(dm);

    verify(message, never()).sendReply(
        eq(sender),
        eq(processorId),
        eq(null),
        eq(dm));
  }

  static class TestOperation extends DistributedCacheOperation {
    boolean endOperationInvoked;
    DistributedRegion region;

    TestOperation(CacheEvent event) {
      super(event);
    }

    @Override
    public DistributedRegion getRegion() {
      return region;
    }

    @Override
    public boolean containsRegionContentChange() {
      return true;
    }

    @Override
    public void endOperation(long viewVersion) {
      endOperationInvoked = true;
      super.endOperation(viewVersion);
    }

    @Override
    protected CacheOperationMessage createMessage() {
      return null;
    }

    @Override
    protected void _distribute() {
      throw new RuntimeException("boom");
    }
  }

  private static class TestCacheOperationMessage extends CacheOperationMessage {
    @Override
    protected InternalCacheEvent createEvent(DistributedRegion rgn) throws EntryNotFoundException {
      return null;
    }

    @Override
    protected boolean operateOnRegion(CacheEvent event, ClusterDistributionManager dm)
        throws EntryNotFoundException {
      return false;
    }

    @Override
    public int getDSFID() {
      return 0;
    }

    void setProcessorId() {
      processorId = 1;
    }
  }
}
