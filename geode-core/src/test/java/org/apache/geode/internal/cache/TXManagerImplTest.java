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

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.partitioned.DestroyMessage;
import org.apache.geode.test.fake.Fakes;


public class TXManagerImplTest {
  private TXManagerImpl txMgr;
  private TXId txid;
  private DestroyMessage msg;
  private TXCommitMessage txCommitMsg;
  private TXId completedTxid;
  private TXId notCompletedTxid;
  private InternalDistributedMember member;
  private CountDownLatch latch;
  private TXStateProxy tx1, tx2;
  private ClusterDistributionManager dm;
  private TXRemoteRollbackMessage rollbackMsg;
  private TXRemoteCommitMessage commitMsg;
  private InternalCache cache;
  private TXManagerImpl spyTxMgr;
  private InternalCache spyCache;
  private SystemTimer timer;

  @Before
  public void setUp() {
    cache = Fakes.cache();
    dm = mock(ClusterDistributionManager.class);
    txid = new TXId(null, 0);
    msg = mock(DestroyMessage.class);
    txCommitMsg = mock(TXCommitMessage.class);
    member = mock(InternalDistributedMember.class);
    completedTxid = new TXId(member, 1);
    notCompletedTxid = new TXId(member, 2);
    latch = new CountDownLatch(1);
    rollbackMsg = new TXRemoteRollbackMessage();
    commitMsg = new TXRemoteCommitMessage();

    when(msg.canStartRemoteTransaction()).thenReturn(true);
    when(msg.canParticipateInTransaction()).thenReturn(true);

    spyCache = spy(Fakes.cache());
    InternalDistributedSystem distributedSystem = mock(InternalDistributedSystem.class);
    doReturn(distributedSystem).when(spyCache).getDistributedSystem();
    when(distributedSystem.getDistributionManager()).thenReturn(dm);
    when(distributedSystem.getDistributedMember()).thenReturn(member);
    spyTxMgr = spy(new TXManagerImpl(mock(CachePerfStats.class), spyCache, disabledClock()));
    timer = mock(SystemTimer.class);
    doReturn(timer).when(spyCache).getCCPTimer();

    txMgr = new TXManagerImpl(mock(CachePerfStats.class), cache, disabledClock());

  }

  @Test
  public void getOrSetHostedTXStateAbleToSetTXStateAndGetLock() {
    TXStateProxy tx = txMgr.getOrSetHostedTXState(txid, msg);

    assertThat(tx).isNotNull();
    assertThat(txMgr.getHostedTXState(txid)).isEqualTo(tx);
    assertThat(txMgr.getLock(tx, txid)).isTrue();

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void testBeginJTAOverflowUniqIdToZero() {
    TXManagerImpl.INITIAL_UNIQUE_ID_VALUE = Integer.MAX_VALUE;
    TXManagerImpl txManager = new TXManagerImpl(mock(CachePerfStats.class), cache, disabledClock());
    txManager.setDistributed(false);
    TXStateProxy proxy = txManager.beginJTA();
    assertThat(proxy.getTxId().getUniqId()).isEqualTo(1);
    assertThat(txManager).isNotNull();
    TXManagerImpl.INITIAL_UNIQUE_ID_VALUE = 0;
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNotNull();

    txManager.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void testBeginJTAUniqIdIncrement() {
    TXManagerImpl txManager = new TXManagerImpl(mock(CachePerfStats.class), cache, disabledClock());
    txManager.setDistributed(false);
    TXStateProxy proxy = txManager.beginJTA();
    assertThat(proxy.getTxId().getUniqId()).isEqualTo(1);
    assertThat(txManager).isNotNull();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNotNull();

    txManager.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void getLockAfterTXStateRemoved() throws InterruptedException {
    TXStateProxy tx = txMgr.getOrSetHostedTXState(txid, msg);

    assertThat(txMgr.getHostedTXState(txid)).isEqualTo(tx);
    assertThat(txMgr.getLock(tx, txid)).isTrue();
    assertThat(tx).isNotNull();
    assertThat(txMgr.getLock(tx, txid)).isTrue();
    tx.getLock().unlock();

    TXStateProxy oldtx = txMgr.getOrSetHostedTXState(txid, msg);
    assertThat(oldtx).isEqualTo(tx);

    Thread t1 = new Thread(() -> txMgr.removeHostedTXState(txid));
    t1.start();

    t1.join();

    TXStateProxy curTx = txMgr.getHostedTXState(txid);
    assertThat(curTx).isNull();

    // after failover command removed the txid from hostedTXState,
    // getLock should put back the original TXStateProxy
    assertThat(txMgr.getLock(tx, txid)).isTrue();
    assertThat(txMgr.getHostedTXState(txid)).isEqualTo(tx);

    tx.getLock().unlock();
  }

  @Test
  public void getLockAfterTXStateReplaced() throws InterruptedException {
    TXStateProxy oldtx = txMgr.getOrSetHostedTXState(txid, msg);

    assertThat(txMgr.getHostedTXState(txid)).isEqualTo(oldtx);
    assertThat(txMgr.getLock(oldtx, txid)).isTrue();
    assertThat(oldtx).isNotNull();
    oldtx.getLock().unlock();

    TXStateProxy tx = txMgr.getOrSetHostedTXState(txid, msg);
    assertThat(oldtx).isEqualTo(tx);

    Thread t1 = new Thread(() -> {
      txMgr.removeHostedTXState(txid);
      // replace with new TXState
      txMgr.getOrSetHostedTXState(txid, msg);
    });
    t1.start();

    t1.join();

    TXStateProxy curTx = txMgr.getHostedTXState(txid);
    assertThat(curTx).isNotNull();
    // replaced
    assertThat(curTx).isNotEqualTo(tx);

    // after TXStateProxy replaced, getLock will not get
    assertThat(txMgr.getLock(tx, txid)).isFalse();

  }

  @Test
  public void getLockAfterTXStateCommitted() throws InterruptedException {
    TXStateProxy oldtx = txMgr.getOrSetHostedTXState(txid, msg);

    assertThat(txMgr.getHostedTXState(txid)).isEqualTo(oldtx);
    assertThat(txMgr.getLock(oldtx, txid)).isTrue();
    assertThat(oldtx).isNotNull();
    oldtx.getLock().unlock();

    TXStateProxy tx = txMgr.getOrSetHostedTXState(txid, msg);
    assertThat(oldtx).isEqualTo(tx);

    Thread t1 = new Thread(() -> {
      when(msg.getTXOriginatorClient()).thenReturn(mock(InternalDistributedMember.class));
      TXStateProxy tx3;
      try {
        tx3 = txMgr.masqueradeAs(commitMsg);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
      tx3.setCommitOnBehalfOfRemoteStub(true);
      try {
        tx3.commit();
      } finally {
        txMgr.unmasquerade(tx3);
      }
      txMgr.removeHostedTXState(txid);
      txMgr.saveTXCommitMessageForClientFailover(txid, txCommitMsg);
    });
    t1.start();

    t1.join();

    TXStateProxy curTx = txMgr.getHostedTXState(txid);
    assertThat(curTx).isNull();

    assertThat(tx.isInProgress()).isFalse();
    // after TXStateProxy committed, getLock will get the lock for the oldtx
    // but caller should not perform ops on this TXStateProxy
    assertThat(txMgr.getLock(tx, txid)).isTrue();
  }

  @Test
  public void masqueradeAsCanGetLock() throws InterruptedException {
    TXStateProxy tx;

    tx = txMgr.masqueradeAs(msg);
    assertThat(tx).isNotNull();

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void masqueradeAsCanGetLockAfterTXStateIsReplaced() throws InterruptedException {
    TXStateProxy tx;

    Thread t1 = new Thread(() -> {
      tx1 = txMgr.getHostedTXState(txid);
      assertThat(tx1).isNull();
      tx1 = txMgr.getOrSetHostedTXState(txid, msg);
      assertThat(tx1).isNotNull();
      assertThat(txMgr.getLock(tx1, txid)).isTrue();

      latch.countDown();

      await()
          .until(() -> tx1.getLock().hasQueuedThreads());

      txMgr.removeHostedTXState(txid);

      tx2 = txMgr.getOrSetHostedTXState(txid, msg);
      assertThat(tx2).isNotNull();
      assertThat(txMgr.getLock(tx2, txid)).isTrue();

      tx2.getLock().unlock();
      tx1.getLock().unlock();
    });
    t1.start();

    assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();

    tx = txMgr.masqueradeAs(msg);
    assertThat(tx).isNotNull();
    assertThat(tx2).isEqualTo(tx);
    tx.getLock().unlock();

    t1.join();

  }

  @Test
  public void testTxStateWithNotFinishedTx() {
    TXStateProxy tx = txMgr.getOrSetHostedTXState(notCompletedTxid, msg);
    assertThat(tx.isInProgress()).isTrue();

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void testTxStateWithCommittedTx() throws InterruptedException {
    when(msg.getTXOriginatorClient()).thenReturn(mock(InternalDistributedMember.class));
    setupTx();

    TXStateProxy tx = txMgr.masqueradeAs(commitMsg);
    try {
      tx.commit();
    } finally {
      txMgr.unmasquerade(tx);
    }
    assertThat(tx.isInProgress()).isFalse();

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void testTxStateWithRolledBackTx() throws InterruptedException {
    when(msg.getTXOriginatorClient()).thenReturn(mock(InternalDistributedMember.class));
    setupTx();

    TXStateProxy tx = txMgr.masqueradeAs(rollbackMsg);
    try {
      tx.rollback();
    } finally {
      txMgr.unmasquerade(tx);
    }
    assertThat(tx.isInProgress()).isFalse();

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  private void setupTx() throws InterruptedException {
    TXStateProxy tx = txMgr.masqueradeAs(msg);
    tx.setCommitOnBehalfOfRemoteStub(true);
    txMgr.unmasquerade(tx);
  }

  @Test
  public void txRolledbackShouldCompleteTx() throws InterruptedException {
    when(msg.getTXOriginatorClient()).thenReturn(mock(InternalDistributedMember.class));

    Thread t1 = new Thread(() -> {
      try {
        tx1 = txMgr.masqueradeAs(msg);
      } catch (InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      msg.process(dm);

      TXStateProxy existingTx = masqueradeToRollback();
      latch.countDown();
      await()
          .until(() -> tx1.getLock().hasQueuedThreads());

      rollbackTransaction(existingTx);
    });
    t1.start();

    assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();

    TXStateProxy tx = txMgr.masqueradeAs(rollbackMsg);
    assertThat(tx1).isEqualTo(tx);
    t1.join();
    rollbackTransaction(tx);
  }

  private TXStateProxy masqueradeToRollback() {
    TXStateProxy existingTx;
    try {
      existingTx = txMgr.masqueradeAs(rollbackMsg);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    return existingTx;
  }

  private void rollbackTransaction(TXStateProxy existingTx) {
    try {
      if (!txMgr.isHostedTxRecentlyCompleted(txid)) {
        txMgr.rollback();
      }
    } finally {
      txMgr.unmasquerade(existingTx);
    }
  }

  @Test
  public void txStateNotCleanedupIfNotRemovedFromHostedTxStatesMap() {
    tx1 = txMgr.getOrSetHostedTXState(txid, msg);
    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) tx1;
    assertThat(txStateProxy).isNotNull();
    assertThat(txStateProxy.getLocalRealDeal().isClosed()).isFalse();

    txMgr.masqueradeAs(tx1);
    txMgr.unmasquerade(tx1);
    assertThat(txStateProxy.getLocalRealDeal().isClosed()).isFalse();

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void txStateCleanedUpIfRemovedFromHostedTxStatesMapCausedByFailover() {
    tx1 = txMgr.getOrSetHostedTXState(txid, msg);
    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) tx1;
    assertThat(txStateProxy).isNotNull();
    assertThat(txStateProxy.getLocalRealDeal().isClosed()).isFalse();
    txStateProxy.setRemovedCausedByFailover(true);

    txMgr.masqueradeAs(tx1);
    // during TX failover, tx can be removed from the hostedTXStates map by FindRemoteTXMessage
    txMgr.getHostedTXStates().remove(txid);
    txMgr.unmasquerade(tx1);
    assertThat(txStateProxy.getLocalRealDeal().isClosed()).isTrue();

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void txStateDoesNotCleanUpIfRemovedFromHostedTxStatesMapNotCausedByFailover() {
    tx1 = txMgr.getOrSetHostedTXState(txid, msg);
    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) tx1;
    assertThat(txStateProxy).isNotNull();
    assertThat(txStateProxy.getLocalRealDeal().isClosed()).isFalse();

    txMgr.masqueradeAs(tx1);
    // during TX failover, tx can be removed from the hostedTXStates map by FindRemoteTXMessage
    txMgr.getHostedTXStates().remove(txid);
    txMgr.unmasquerade(tx1);
    assertThat(txStateProxy.getLocalRealDeal().isClosed()).isFalse();

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void clientTransactionWithIdleTimeLongerThanTransactionTimeoutIsRemoved()
      throws Exception {
    when(msg.getTXOriginatorClient()).thenReturn(mock(InternalDistributedMember.class));
    TXStateProxyImpl tx = spy((TXStateProxyImpl) txMgr.getOrSetHostedTXState(txid, msg));
    doReturn(true).when(tx).isOverTransactionTimeoutLimit();

    txMgr.scheduleToRemoveExpiredClientTransaction(txid);

    assertThat(txMgr.isHostedTXStatesEmpty()).isTrue();

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void clientTransactionsToBeRemovedAndDistributedAreSentToRemoveServerIfWithNoTimeout() {
    Set<TXId> txIds = (Set<TXId>) mock(Set.class);
    doReturn(0).when(spyTxMgr).getTransactionTimeToLive();
    when(txIds.iterator()).thenAnswer(
        (Answer<Iterator<TXId>>) invocation -> Arrays.asList(txid, mock(TXId.class)).iterator());

    spyTxMgr.expireDisconnectedClientTransactions(txIds, true);

    verify(spyTxMgr, times(1)).expireClientTransactionsOnRemoteServer(eq(txIds));
  }

  @Test
  public void clientTransactionsToBeExpiredAreRemovedAndNotDistributedIfWithNoTimeout() {
    doReturn(1).when(spyTxMgr).getTransactionTimeToLive();
    TXId txId1 = mock(TXId.class);
    TXId txId2 = mock(TXId.class);
    TXId txId3 = mock(TXId.class);
    tx1 = spyTxMgr.getOrSetHostedTXState(txId1, msg);
    tx2 = spyTxMgr.getOrSetHostedTXState(txId2, msg);
    Set<TXId> txIds = spy(new HashSet<>());
    txIds.add(txId1);
    doReturn(0).when(spyTxMgr).getTransactionTimeToLive();
    when(txIds.iterator()).thenAnswer(
        (Answer<Iterator<TXId>>) invocation -> Arrays.asList(txId1, txId3).iterator());
    assertThat(spyTxMgr.getHostedTXStates()).hasSize(2);

    spyTxMgr.expireDisconnectedClientTransactions(txIds, false);

    verify(spyTxMgr, never()).expireClientTransactionsOnRemoteServer(eq(txIds));
    verify(spyTxMgr, times(1)).removeHostedTXState(eq(txIds));
    verify(spyTxMgr, times(1)).removeHostedTXState(eq(txId1));
    verify(spyTxMgr, times(1)).removeHostedTXState(eq(txId3));
    assertThat(spyTxMgr.getHostedTXStates().get(txId2)).isEqualTo(tx2);
    assertThat(spyTxMgr.getHostedTXStates()).hasSize(1);
  }

  @Test
  public void clientTransactionsToBeExpiredAndDistributedAreSentToRemoveServer() {
    Set<TXId> txIds = mock(Set.class);

    spyTxMgr.expireDisconnectedClientTransactions(txIds, true);

    verify(spyTxMgr, times(1)).expireClientTransactionsOnRemoteServer(eq(txIds));
  }

  @Test
  public void clientTransactionsNotToBeDistributedAreNotSentToRemoveServer() {
    Set<TXId> txIds = mock(Set.class);

    spyTxMgr.expireDisconnectedClientTransactions(txIds, false);

    verify(spyTxMgr, never()).expireClientTransactionsOnRemoteServer(eq(txIds));
  }

  @Test
  public void clientTransactionsToBeExpiredIsScheduledToBeRemoved() {
    doReturn(1).when(spyTxMgr).getTransactionTimeToLive();
    TXId txId1 = mock(TXId.class);
    TXId txId2 = mock(TXId.class);
    TXId txId3 = mock(TXId.class);
    tx1 = spyTxMgr.getOrSetHostedTXState(txId1, msg);
    tx2 = spyTxMgr.getOrSetHostedTXState(txId2, msg);
    Set<TXId> set = new HashSet<>();
    set.add(txId1);
    set.add(txId2);

    spyTxMgr.expireDisconnectedClientTransactions(set, false);

    verify(spyTxMgr, times(1)).scheduleToRemoveClientTransaction(eq(txId1), eq(1100L));
    verify(spyTxMgr, times(1)).scheduleToRemoveClientTransaction(eq(txId2), eq(1100L));
    verify(spyTxMgr, never()).scheduleToRemoveClientTransaction(eq(txId3), eq(1100L));
  }

  @Test
  public void clientTransactionIsRemovedIfWithNoTimeout() {
    spyTxMgr.scheduleToRemoveClientTransaction(txid, 0);

    verify(spyTxMgr, times(1)).removeHostedTXState(eq(txid));
  }

  @Test
  public void clientTransactionIsScheduledToBeRemovedIfWithTimeout() {
    spyTxMgr.scheduleToRemoveClientTransaction(txid, 1000);

    verify(timer, times(1)).schedule(any(), eq(1000L));
  }

  @Test
  public void unmasqueradeReleasesTheLockHeld() {
    tx1 = mock(TXStateProxyImpl.class);
    ReentrantLock lock = mock(ReentrantLock.class);
    when(tx1.getLock()).thenReturn(lock);

    spyTxMgr.unmasquerade(tx1);

    verify(lock, times(1)).unlock();
  }

  @Test(expected = RuntimeException.class)
  public void unmasqueradeReleasesTheLockHeldWhenCleanupTransactionIfNoLongerHostFailedWithException() {
    tx1 = mock(TXStateProxyImpl.class);
    ReentrantLock lock = mock(ReentrantLock.class);
    when(tx1.getLock()).thenReturn(lock);
    doThrow(new RuntimeException()).when(spyTxMgr)
        .cleanupTransactionIfNoLongerHostCausedByFailover(tx1);

    spyTxMgr.unmasquerade(tx1);

    verify(lock, times(1)).unlock();
  }

  @Test
  public void masqueradeAsSetsTarget() throws InterruptedException {
    TXStateProxy tx;

    tx = txMgr.masqueradeAs(msg);
    assertThat(tx.getTarget()).isNotNull();

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void removeHostedTXStateSetFlagIfCausedByFailover() {
    Map<TXId, TXStateProxy> hostedTXStates = txMgr.getHostedTXStates();
    TXStateProxyImpl txStateProxy = mock(TXStateProxyImpl.class);
    hostedTXStates.put(txid, txStateProxy);

    txMgr.removeHostedTXState(txid, true);

    verify(txStateProxy).setRemovedCausedByFailover(eq(true));

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

  @Test
  public void removeHostedTXStateDoesNotSetFlagIfNotCausedByFailover() {
    Map<TXId, TXStateProxy> hostedTXStates = txMgr.getHostedTXStates();
    TXStateProxyImpl txStateProxy = mock(TXStateProxyImpl.class);
    hostedTXStates.put(txid, txStateProxy);

    txMgr.removeHostedTXState(txid);

    verify(txStateProxy, never()).setRemovedCausedByFailover(eq(true));

    txMgr.close();
    assertThat(TXManagerImpl.getCurrentInstanceForTest()).isNull();
  }

}
