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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.test.fake.Fakes;


public class RemoteOperationMessageTest {
  private TestableRemoteOperationMessage msg; // the class under test

  private InternalDistributedMember sender;
  private final String regionPath = "regionPath";

  private GemFireCacheImpl cache;
  private InternalDistributedSystem system;
  private ClusterDistributionManager dm;
  private LocalRegion r;
  private TXManagerImpl txMgr;
  private long startTime = 0;
  private TXStateProxy tx;

  @Before
  public void setUp() throws Exception {
    cache = Fakes.cache();
    system = cache.getSystem();
    dm = (ClusterDistributionManager) system.getDistributionManager();
    r = mock(LocalRegion.class);
    txMgr = mock(TXManagerImpl.class);
    tx = mock(TXStateProxyImpl.class);
    OperationExecutors executors = mock(OperationExecutors.class);
    ExecutorService executorService = mock(ExecutorService.class);
    when(cache.getRegionByPathForProcessing(regionPath)).thenReturn(r);
    when(cache.getTxManager()).thenReturn(txMgr);
    when(dm.getExecutors()).thenReturn(executors);
    when(executors.getWaitingThreadPool()).thenReturn(executorService);

    sender = mock(InternalDistributedMember.class);

    InternalDistributedMember recipient = mock(InternalDistributedMember.class);
    ReplyProcessor21 processor = mock(ReplyProcessor21.class);
    // make it a spy to aid verification
    msg = spy(new TestableRemoteOperationMessage(recipient, regionPath, processor));
  }

  @Test
  public void messageWithNoTXPerformsOnRegion() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(null);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);

    verify(msg, times(1)).operateOnRegion(dm, r, startTime);
    verify(dm, times(1)).putOutgoing(any());
  }

  @Test
  public void messageForNotFinishedTXPerformsOnRegion() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(tx.isInProgress()).thenReturn(true);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);

    verify(msg, times(1)).operateOnRegion(dm, r, startTime);
    verify(dm, times(1)).putOutgoing(any());
  }

  @Test
  public void messageForFinishedTXDoesNotPerformOnRegion() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(tx.isInProgress()).thenReturn(false);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);

    verify(msg, times(0)).operateOnRegion(dm, r, startTime);
    // A reply is sent even though we do not call operationOnRegion
    verify(dm, times(1)).putOutgoing(any());
  }

  @Test
  public void messageForFinishedTXRepliesWithException() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(tx.isInProgress()).thenReturn(false);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);

    verify(msg, times(1)).sendReply(
        eq(sender),
        eq(0),
        eq(dm),
        argThat(ex -> ex != null && ex.getCause() instanceof TransactionException),
        eq(r),
        eq(startTime));
  }

  @Test
  public void noNewTxProcessingAfterTXManagerImplClosed() throws Exception {
    when(txMgr.masqueradeAs(msg)).thenReturn(tx);
    when(txMgr.isClosed()).thenReturn(true);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);

    verify(msg, times(0)).operateOnRegion(dm, r, startTime);
    // If we do not respond what prevents the sender from waiting forever?
    verify(dm, times(0)).putOutgoing(any());
  }

  @Test
  public void processWithNullCacheSendsReplyContainingCacheClosedException() throws Exception {
    when(dm.getExistingCache()).thenReturn(null);
    msg.setSender(sender);

    msg.process(dm);
    verify(msg, times(0)).operateOnRegion(dm, r, startTime);
    verify(dm, times(1)).putOutgoing(any());
    ArgumentCaptor<ReplyException> captor = ArgumentCaptor.forClass(ReplyException.class);
    verify(msg, times(1)).sendReply(any(), anyInt(), eq(dm), captor.capture(), any(),
        eq(startTime));
    assertThat(captor.getValue().getCause()).isInstanceOf(CacheClosedException.class);
  }

  @Test
  public void processWithDisconnectingDSAndClosedCacheSendsReplyContainingCachesClosedException()
      throws Exception {
    CacheClosedException reasonCacheWasClosed = mock(CacheClosedException.class);
    when(system.isDisconnecting()).thenReturn(true);
    when(cache.getCacheClosedException(any())).thenReturn(reasonCacheWasClosed);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);
    verify(msg, times(0)).operateOnRegion(dm, r, startTime);
    verify(dm, times(1)).putOutgoing(any());
    ArgumentCaptor<ReplyException> captor = ArgumentCaptor.forClass(ReplyException.class);
    verify(msg, times(1)).sendReply(any(), anyInt(), eq(dm), captor.capture(), any(),
        eq(startTime));
    assertThat(captor.getValue().getCause()).isSameAs(reasonCacheWasClosed);
  }

  @Test
  public void processWithNullPointerExceptionFromOperationOnRegionWithNoSystemFailureSendsReplyWithNPE()
      throws Exception {
    when(msg.operateOnRegion(dm, r, startTime)).thenThrow(NullPointerException.class);
    doNothing().when(msg).checkForSystemFailure();
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);
    verify(dm, times(1)).putOutgoing(any());
    ArgumentCaptor<ReplyException> captor = ArgumentCaptor.forClass(ReplyException.class);
    verify(msg, times(1)).sendReply(any(), anyInt(), eq(dm), captor.capture(), eq(r),
        eq(startTime));
    assertThat(captor.getValue().getCause()).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void processWithNullPointerExceptionFromOperationOnRegionWithNoSystemFailureAndIsDisconnectingSendsReplyWithRemoteOperationException()
      throws Exception {
    when(msg.operateOnRegion(dm, r, startTime)).thenThrow(NullPointerException.class);
    doNothing().when(msg).checkForSystemFailure();
    when(system.isDisconnecting()).thenReturn(false).thenReturn(true);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);
    verify(dm, times(1)).putOutgoing(any());
    ArgumentCaptor<ReplyException> captor = ArgumentCaptor.forClass(ReplyException.class);
    verify(msg, times(1)).sendReply(any(), anyInt(), eq(dm), captor.capture(), eq(r),
        eq(startTime));
    assertThat(captor.getValue().getCause()).isInstanceOf(RemoteOperationException.class);
  }

  @Test
  public void processWithRegionDestroyedExceptionFromOperationOnRegionSendsReplyWithSameRegionDestroyedException()
      throws Exception {
    RegionDestroyedException ex = mock(RegionDestroyedException.class);
    when(msg.operateOnRegion(dm, r, startTime)).thenThrow(ex);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);
    verify(dm, times(1)).putOutgoing(any());
    ArgumentCaptor<ReplyException> captor = ArgumentCaptor.forClass(ReplyException.class);
    verify(msg, times(1)).sendReply(any(), anyInt(), eq(dm), captor.capture(), eq(r),
        eq(startTime));
    assertThat(captor.getValue().getCause()).isSameAs(ex);
  }

  @Test
  public void processWithRegionDoesNotExistSendsReplyWithRegionDestroyedExceptionReply()
      throws Exception {
    when(cache.getRegionByPathForProcessing(regionPath)).thenReturn(null);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);
    verify(msg, never()).operateOnRegion(any(), any(), anyLong());
    verify(dm, times(1)).putOutgoing(any());
    ArgumentCaptor<ReplyException> captor = ArgumentCaptor.forClass(ReplyException.class);
    verify(msg, times(1)).sendReply(any(), anyInt(), eq(dm), captor.capture(), eq(null),
        eq(startTime));
    assertThat(captor.getValue().getCause()).isInstanceOf(RegionDestroyedException.class);
  }

  @Test
  public void processWithDistributedSystemDisconnectedExceptionFromOperationOnRegionDoesNotSendReply()
      throws Exception {
    when(msg.operateOnRegion(dm, r, startTime))
        .thenThrow(DistributedSystemDisconnectedException.class);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);
    verify(dm, never()).putOutgoing(any());
  }

  @Test
  public void processWithOperateOnRegionReturningFalseDoesNotSendReply() {
    msg.setOperationOnRegionResult(false);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);
    verify(dm, never()).putOutgoing(any());
  }

  @Test
  public void processWithRemoteOperationExceptionFromOperationOnRegionSendsReplyWithSameRemoteOperationException()
      throws Exception {
    RemoteOperationException theException = mock(RemoteOperationException.class);
    when(msg.operateOnRegion(dm, r, startTime)).thenThrow(theException);
    msg.setSender(sender);

    msg.doRemoteOperation(dm, cache);
    verify(dm, times(1)).putOutgoing(any());
    ArgumentCaptor<ReplyException> captor = ArgumentCaptor.forClass(ReplyException.class);
    verify(msg, times(1)).sendReply(any(), anyInt(), eq(dm), captor.capture(), eq(r),
        eq(startTime));
    assertThat(captor.getValue().getCause()).isSameAs(theException);
  }

  @Test
  public void processWithNullPointerExceptionFromOperationOnRegionWithSystemFailureSendsReplyWithRemoteOperationException()
      throws Exception {
    when(msg.operateOnRegion(dm, r, startTime)).thenThrow(NullPointerException.class);
    doThrow(new RuntimeException("SystemFailure")).when(msg).checkForSystemFailure();
    msg.setSender(sender);

    assertThatThrownBy(() -> msg.doRemoteOperation(dm, cache)).isInstanceOf(RuntimeException.class)
        .hasMessage("SystemFailure");
    verify(dm, times(1)).putOutgoing(any());
    ArgumentCaptor<ReplyException> captor = ArgumentCaptor.forClass(ReplyException.class);
    verify(msg, times(1)).sendReply(any(), anyInt(), eq(dm), captor.capture(), eq(r),
        eq(startTime));
    assertThat(captor.getValue().getCause()).isInstanceOf(RemoteOperationException.class)
        .hasMessageContaining("system failure");
  }


  @Test
  public void processInvokesDoRemoteOperationIfThreadOwnsResources() {
    when(system.threadOwnsResources()).thenReturn(true);
    doNothing().when(msg).doRemoteOperation(dm, cache);

    msg.process(dm);

    verify(msg).doRemoteOperation(dm, cache);
    verify(msg, never()).isTransactional();
  }

  @Test
  public void processInvokesDoRemoteOperationIfThreadDoesNotOwnResourcesAndNotTransactional() {
    when(system.threadOwnsResources()).thenReturn(false);
    doReturn(false).when(msg).isTransactional();
    doNothing().when(msg).doRemoteOperation(dm, cache);

    msg.process(dm);

    verify(msg).doRemoteOperation(dm, cache);
    verify(msg).isTransactional();
  }

  @Test
  public void isTransactionalReturnsFalseIfTXUniqueIdIsNOTX() {
    assertThat(msg.getTXUniqId()).isEqualTo(TXManagerImpl.NOTX);
    assertThat(msg.isTransactional()).isFalse();
  }

  @Test
  public void isTransactionalReturnsFalseIfCannotParticipateInTransaction() {
    doReturn(1).when(msg).getTXUniqId();
    doReturn(false).when(msg).canParticipateInTransaction();

    assertThat(msg.isTransactional()).isFalse();
  }

  @Test
  public void isTransactionalReturnsTrueIfHasTXUniqueIdAndCanParticipateInTransaction() {
    doReturn(1).when(msg).getTXUniqId();

    assertThat(msg.canParticipateInTransaction()).isTrue();
    assertThat(msg.isTransactional()).isTrue();
  }

  private static class TestableRemoteOperationMessage extends RemoteOperationMessage {

    private boolean operationOnRegionResult = true;

    TestableRemoteOperationMessage(InternalDistributedMember recipient, String regionPath,
        ReplyProcessor21 processor) {
      super(recipient, regionPath, processor);
    }

    @Override
    public int getDSFID() {
      return 0;
    }

    @Override
    protected boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r, long startTime)
        throws RemoteOperationException {
      return operationOnRegionResult;
    }

    void setOperationOnRegionResult(boolean v) {
      this.operationOnRegionResult = v;
    }

  }
}
