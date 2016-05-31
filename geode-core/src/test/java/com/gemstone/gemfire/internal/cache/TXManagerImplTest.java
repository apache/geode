/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.partitioned.DestroyMessage;
import com.gemstone.gemfire.test.fake.Fakes;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import com.jayway.awaitility.Awaitility;


@Category(UnitTest.class)
public class TXManagerImplTest {
  private TXManagerImpl txMgr;
  TXId txid;
  DestroyMessage msg;
  TXCommitMessage txCommitMsg;
  TXId completedTxid;
  TXId notCompletedTxid;
  InternalDistributedMember member;
  CountDownLatch latch = new CountDownLatch(1);
  TXStateProxy tx1, tx2;

  @Before
  public void setUp() {
    Cache cache = Fakes.cache();
    txMgr = new TXManagerImpl(null, cache);
    txid = new TXId(null, 0);
    msg = mock(DestroyMessage.class);    
    txCommitMsg = mock(TXCommitMessage.class);
    member = mock(InternalDistributedMember.class);
    completedTxid = new TXId(member, 1);
    notCompletedTxid = new TXId(member, 2);
    
    when(this.msg.canStartRemoteTransaction()).thenReturn(true);
    when(this.msg.canParticipateInTransaction()).thenReturn(true);
  }
  
  @Test
  public void getOrSetHostedTXStateAbleToSetTXStateAndGetLock(){    
    TXStateProxy tx = txMgr.getOrSetHostedTXState(txid, msg);
    
    assertNotNull(tx);
    assertEquals(tx, txMgr.getHostedTXState(txid));  
    assertTrue(txMgr.getLock(tx, txid));
  }

  @Test
  public void getLockAfterTXStateRemoved() throws InterruptedException{
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
    
    //after failover command removed the txid from hostedTXState,
    //getLock should put back the original TXStateProxy
    assertTrue(txMgr.getLock(tx, txid));
    assertEquals(tx, txMgr.getHostedTXState(txid));
    
    tx.getLock().unlock();
  }
  
  @Test
  public void getLockAfterTXStateReplaced() throws InterruptedException{  
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
        //replace with new TXState
        txMgr.getOrSetHostedTXState(txid, msg);
      }
    });
    t1.start();
    
    t1.join();
   
    TXStateProxy curTx = txMgr.getHostedTXState(txid);
    assertNotNull(curTx);
    //replaced
    assertNotEquals(tx, curTx);
    
    //after TXStateProxy replaced, getLock will not get 
    assertFalse(txMgr.getLock(tx, txid));
    
  }
  
  @Test
  public void getLockAfterTXStateCommitted() throws InterruptedException{  
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
        txMgr.saveTXCommitMessageForClientFailover(txid, txCommitMsg);
      }
    });
    t1.start();
    
    t1.join();
   
    TXStateProxy curTx = txMgr.getHostedTXState(txid);
    assertNull(curTx);
    
    //after TXStateProxy committed, getLock will get the lock for the oldtx
    //but caller should not perform ops on this TXStateProxy
    assertTrue(txMgr.getLock(tx, txid));    
  } 
  
  @Test
  public void masqueradeAsCanGetLock() throws InterruptedException{  
    TXStateProxy tx;

    tx = txMgr.masqueradeAs(msg);
    assertNotNull(tx);
  }
  
  @Test
  public void masqueradeAsCanGetLockAfterTXStateIsReplaced() throws InterruptedException{  
    TXStateProxy tx;
    
    Thread t1 = new Thread(new Runnable() {
      public void run() {
        tx1 = txMgr.getHostedTXState(txid);
        assertNull(tx1);
        tx1 =txMgr.getOrSetHostedTXState(txid, msg);
        assertNotNull(tx1);
        assertTrue(txMgr.getLock(tx1, txid));

        latch.countDown();
        
        Awaitility.await().pollInterval(10, TimeUnit.MILLISECONDS).pollDelay(10, TimeUnit.MILLISECONDS)
        .atMost(30, TimeUnit.SECONDS).until(() -> tx1.getLock().hasQueuedThreads()); 
        
        txMgr.removeHostedTXState(txid);
        
        tx2 =txMgr.getOrSetHostedTXState(txid, msg);
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
  public void hasTxAlreadyFinishedDetectsNoTx() {   
    assertFalse(txMgr.hasTxAlreadyFinished(null, txid));
  }
  
  @Test
  public void hasTxAlreadyFinishedDetectsTxNotFinished() {
    TXStateProxy tx = txMgr.getOrSetHostedTXState(notCompletedTxid, msg);
    assertFalse(txMgr.hasTxAlreadyFinished(tx, notCompletedTxid));
  }
  
  @Test
  public void hasTxAlreadyFinishedDetectsTxFinished() throws InterruptedException {
    TXStateProxy tx = txMgr.getOrSetHostedTXState(completedTxid, msg);    
    txMgr.saveTXCommitMessageForClientFailover(completedTxid, txCommitMsg); 
    assertTrue(txMgr.hasTxAlreadyFinished(tx, completedTxid));
  }
  
}
