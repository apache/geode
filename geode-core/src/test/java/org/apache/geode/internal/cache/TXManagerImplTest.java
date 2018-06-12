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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.partitioned.DestroyMessage;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.UnitTest;


@Category(UnitTest.class)
public class TXManagerImplTest {
  private TXManagerImpl txMgr;
  TXId txid;
  DestroyMessage msg;
  TXCommitMessage txCommitMsg;
  TXId completedTxid;
  TXId notCompletedTxid;
  InternalDistributedMember member;
  CountDownLatch latch;
  TXStateProxy tx1, tx2;
  ClusterDistributionManager dm;
  TXRemoteRollbackMessage rollbackMsg;
  TXRemoteCommitMessage commitMsg;

  @Before
  public void setUp() {
    InternalCache cache = Fakes.cache();
    dm = mock(ClusterDistributionManager.class);
    txMgr = new TXManagerImpl(mock(CachePerfStats.class), cache);
    txid = new TXId(null, 0);
    msg = mock(DestroyMessage.class);
    txCommitMsg = mock(TXCommitMessage.class);
    member = mock(InternalDistributedMember.class);
    completedTxid = new TXId(member, 1);
    notCompletedTxid = new TXId(member, 2);
    latch = new CountDownLatch(1);
    rollbackMsg = new TXRemoteRollbackMessage();
    commitMsg = new TXRemoteCommitMessage();

    when(this.msg.canStartRemoteTransaction()).thenReturn(true);
    when(this.msg.canParticipateInTransaction()).thenReturn(true);

  }

  @Test
  public void getOrSetHostedTXStateAbleToSetTXStateAndGetLock() {
    TXStateProxy tx = txMgr.getOrSetHostedTXState(txid, msg);

    assertNotNull(tx);
    assertEquals(tx, txMgr.getHostedTXState(txid));
    assertTrue(txMgr.getLock(tx, txid));
  }

  @Test
  public void getLockAfterTXStateRemoved() throws InterruptedException {
    TXStateProxy tx = txMgr.getOrSetHostedTXState(txid, msg);

    assertEquals(tx, txMgr.getHostedTXState(txid));
    assertTrue(txMgr.getLock(tx, txid));
    assertNotNull(tx);
    assertTrue(txMgr.getLock(tx, txid));
    tx.getLock().unlock();

    TXStateProxy oldtx = txMgr.getOrSetHostedTXState(txid, msg);
    assertEquals(tx, oldtx);

    Thread t1 = new Thread(new Runnable() {
      public void run() {
        txMgr.removeHostedTXState(txid);
      }
    });
    t1.start();

    t1.join();

    TXStateProxy curTx = txMgr.getHostedTXState(txid);
    assertNull(curTx);

    // after failover command removed the txid from hostedTXState,
    // getLock should put back the original TXStateProxy
    assertTrue(txMgr.getLock(tx, txid));
    assertEquals(tx, txMgr.getHostedTXState(txid));

    tx.getLock().unlock();
  }

  @Test
  public void getLockAfterTXStateReplaced() throws InterruptedException {
    TXStateProxy oldtx = txMgr.getOrSetHostedTXState(txid, msg);

    assertEquals(oldtx, txMgr.getHostedTXState(txid));
    assertTrue(txMgr.getLock(oldtx, txid));
    assertNotNull(oldtx);
    oldtx.getLock().unlock();

    TXStateProxy tx = txMgr.getOrSetHostedTXState(txid, msg);
    assertEquals(tx, oldtx);

    Thread t1 = new Thread(new Runnable() {
      public void run() {
        txMgr.removeHostedTXState(txid);
        // replace with new TXState
        txMgr.getOrSetHostedTXState(txid, msg);
      }
    });
    t1.start();

    t1.join();

    TXStateProxy curTx = txMgr.getHostedTXState(txid);
    assertNotNull(curTx);
    // replaced
    assertNotEquals(tx, curTx);

    // after TXStateProxy replaced, getLock will not get
    assertFalse(txMgr.getLock(tx, txid));

  }

  @Test
  public void getLockAfterTXStateCommitted() throws InterruptedException {
    TXStateProxy oldtx = txMgr.getOrSetHostedTXState(txid, msg);

    assertEquals(oldtx, txMgr.getHostedTXState(txid));
    assertTrue(txMgr.getLock(oldtx, txid));
    assertNotNull(oldtx);
    oldtx.getLock().unlock();

    TXStateProxy tx = txMgr.getOrSetHostedTXState(txid, msg);
    assertEquals(tx, oldtx);

    Thread t1 = new Thread(new Runnable() {
      public void run() {
        when(msg.getTXOriginatorClient()).thenReturn(mock(InternalDistributedMember.class));
        TXStateProxy tx;
        try {
          tx = txMgr.masqueradeAs(commitMsg);
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
        tx.setCommitOnBehalfOfRemoteStub(true);
        try {
          tx.commit();
        } finally {
          txMgr.unmasquerade(tx);
        }
        txMgr.removeHostedTXState(txid);
        txMgr.saveTXCommitMessageForClientFailover(txid, txCommitMsg);
      }
    });
    t1.start();

    t1.join();

    TXStateProxy curTx = txMgr.getHostedTXState(txid);
    assertNull(curTx);

    assertFalse(tx.isInProgress());
    // after TXStateProxy committed, getLock will get the lock for the oldtx
    // but caller should not perform ops on this TXStateProxy
    assertTrue(txMgr.getLock(tx, txid));
  }

  @Test
  public void masqueradeAsCanGetLock() throws InterruptedException {
    TXStateProxy tx;

    tx = txMgr.masqueradeAs(msg);
    assertNotNull(tx);
  }

  @Test
  public void masqueradeAsCanGetLockAfterTXStateIsReplaced() throws InterruptedException {
    TXStateProxy tx;

    Thread t1 = new Thread(new Runnable() {
      public void run() {
        tx1 = txMgr.getHostedTXState(txid);
        assertNull(tx1);
        tx1 = txMgr.getOrSetHostedTXState(txid, msg);
        assertNotNull(tx1);
        assertTrue(txMgr.getLock(tx1, txid));

        latch.countDown();

        Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS)
            .pollDelay(10, TimeUnit.MILLISECONDS).atMost(30, TimeUnit.SECONDS)
            .until(() -> tx1.getLock().hasQueuedThreads());

        txMgr.removeHostedTXState(txid);

        tx2 = txMgr.getOrSetHostedTXState(txid, msg);
        assertNotNull(tx2);
        assertTrue(txMgr.getLock(tx2, txid));

        tx2.getLock().unlock();
        tx1.getLock().unlock();
      }
    });
    t1.start();

    assertTrue(latch.await(60, TimeUnit.SECONDS));

    tx = txMgr.masqueradeAs(msg);
    assertNotNull(tx);
    assertEquals(tx, tx2);
    tx.getLock().unlock();

    t1.join();

  }

  @Test
  public void testTxStateWithNotFinishedTx() {
    TXStateProxy tx = txMgr.getOrSetHostedTXState(notCompletedTxid, msg);
    assertTrue(tx.isInProgress());
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
    assertFalse(tx.isInProgress());
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
    assertFalse(tx.isInProgress());
  }

  private void setupTx() throws InterruptedException {
    TXStateProxy tx = txMgr.masqueradeAs(msg);
    tx.setCommitOnBehalfOfRemoteStub(true);
    txMgr.unmasquerade(tx);
  }

  @Test
  public void txRolledbackShouldCompleteTx() throws InterruptedException {
    when(msg.getTXOriginatorClient()).thenReturn(mock(InternalDistributedMember.class));

    Thread t1 = new Thread(new Runnable() {
      public void run() {
        try {
          tx1 = txMgr.masqueradeAs(msg);
        } catch (InterruptedException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }

        msg.process(dm);

        TXStateProxy existingTx = masqueradeToRollback();
        latch.countDown();
        Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS)
            .pollDelay(10, TimeUnit.MILLISECONDS).atMost(30, TimeUnit.SECONDS)
            .until(() -> tx1.getLock().hasQueuedThreads());

        rollbackTransaction(existingTx);
      }
    });
    t1.start();

    assertTrue(latch.await(60, TimeUnit.SECONDS));

    TXStateProxy tx = txMgr.masqueradeAs(rollbackMsg);
    assertEquals(tx, tx1);
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
    assertNotNull(txStateProxy);
    assertFalse(txStateProxy.getLocalRealDeal().isClosed());

    txMgr.masqueradeAs(tx1);
    txMgr.unmasquerade(tx1);
    assertFalse(txStateProxy.getLocalRealDeal().isClosed());

  }

  @Test
  public void txStateCleanedupIfRemovedFromHostedTxStatesMap() {
    tx1 = txMgr.getOrSetHostedTXState(txid, msg);
    TXStateProxyImpl txStateProxy = (TXStateProxyImpl) tx1;
    assertNotNull(txStateProxy);
    assertFalse(txStateProxy.getLocalRealDeal().isClosed());

    txMgr.masqueradeAs(tx1);
    // during TX failover, tx can be removed from the hostedTXStates map by FindRemoteTXMessage
    txMgr.getHostedTXStates().remove(txid);
    txMgr.unmasquerade(tx1);
    assertTrue(txStateProxy.getLocalRealDeal().isClosed());
  }
}
